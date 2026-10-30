// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::error::Error;

use super::*;
use crate::runner::attempt::{AttemptError, AttemptFailure};

/// The public bounded mutation must settle its timed-out process tree and erase
/// echoed credentials before handing typed completion evidence to its caller.
#[tokio::test]
async fn bounded_lock_reaps_stalled_vendor_and_redacts_its_output() {
    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("flint");
    // An isolated writer prevents concurrent test forks from inheriting a
    // writable executable descriptor, which could cause ETXTBSY on launch.
    let writer = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::process::Command::new("/bin/sh")
            .args([
                "-c",
                "set -eu; umask 077; printf '%s' \"$2\" > \"$1\"; chmod 700 \"$1\"",
                "fixture-writer",
            ])
            .arg(&executable)
            .arg(concat!(
                "#!/bin/sh\n",
                "printf '%s' \"$5\"; printf '%s' \"$5\" >&2\n",
                "/bin/sleep 30 &\n",
                "printf '%s %s' \"$$\" \"$!\" > \"$0.pids\"\n",
                "wait\n",
            ))
            .kill_on_drop(true)
            .status(),
    )
    .await
    .expect("fixture writer must finish within its watchdog")
    .unwrap();
    assert!(writer.success(), "{writer}");
    let manager =
        LockdownManager::with_runner(FlintRunner::with_path(executable.to_str().unwrap()));
    let executor = crate::runner::test_firmware_executor();
    let key = "0123aBcD4567eF89";
    let error = tokio::time::timeout(
        Duration::from_secs(15),
        manager.lock_device_bounded("0000:03:00.0", key, &executor, Duration::from_secs(1)),
    )
    .await
    .expect("bounded mutation must finish before the fixture's natural exit")
    .unwrap_err();

    let mut source: Option<&(dyn Error + 'static)> = Some(&error);
    while let Some(current) = source {
        assert!(!current.to_string().contains(key));
        assert!(!format!("{current:?}").contains(key));
        source = current.source();
    }
    let attempt = error
        .source()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<AttemptError>()
        .expect("caller must be able to inspect typed process settlement");
    let AttemptError::Started {
        status,
        stdout,
        stderr,
        reason,
        cleanup_errors,
        ..
    } = attempt
    else {
        panic!("fixture did not start: {attempt}");
    };
    assert!(matches!(reason, AttemptFailure::Deadline(_)));
    assert!(status.is_some());
    assert!(cleanup_errors.is_empty(), "{cleanup_errors:?}");
    assert!(stdout.is_empty() && stderr.is_empty());
    let pids = std::fs::read_to_string(executable.with_extension("pids")).unwrap();
    assert_eq!(pids.split_whitespace().count(), 2);
    for pid in pids.split_whitespace() {
        let pid: u32 = pid.parse().unwrap();
        assert!(
            !std::path::Path::new("/proc").join(pid.to_string()).exists(),
            "bounded mutation left process {pid} behind"
        );
    }
}
