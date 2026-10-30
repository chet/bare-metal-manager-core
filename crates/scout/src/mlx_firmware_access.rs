// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Version-bound access reconciliation for generic NIC firmware orchestration.

use std::future::Future;
use std::time::Duration;

use carbide_libmlx_model::device::identity::MlxDeviceIdentity;
use carbide_libmlx_model::device::info::MlxDeviceInfo;
use libmlx::device::discovery::{
    DeviceDiscoveryError, discover_device_bounded_with_executor, normalize_pci_address,
};
use libmlx::firmware::operation::SupervisorConfig;
use libmlx::lockdown::error::{MlxError, MlxResult};
use libmlx::lockdown::lockdown::{LockdownManager, StatusReport};
use libmlx::lockdown::runner::FlintRunner;
use libmlx::runner::FirmwareToolExecutor;
use rpc::forge_agent_control_response::MlxDeviceReconcileFirmwareAccess;
use rpc::forge_agent_control_response::mlx_device_reconcile_firmware_access::Request;
use rpc::protos::mlx_device as pb;
use rpc::protos::mlx_device::mlx_device_firmware_access_result::Result as AccessResult;

use crate::mlx_access::{DirectMlxAccess, DirectMlxAccessError};

// Match bounded inventory: each tool attempt includes descendants/output, then
// permits up to five seconds for cleanup and five for result delivery. This is
// not a deadline for the controller's durable reconciliation lifetime.
const ACCESS_TIMEOUT: Duration = Duration::from_secs(30);

/// Validate physical identity under the same claim as the subsequent command.
/// A mutation acknowledgment never substitutes for a separate status read.
pub(super) async fn reconcile(
    device: &str,
    task: &MlxDeviceReconcileFirmwareAccess,
    config: &SupervisorConfig,
) -> pb::MlxObservation {
    let executor = match config.supervisor_path.as_ref() {
        Some(path) => FirmwareToolExecutor::with_supervisor_path(path),
        None => FirmwareToolExecutor::default(),
    };
    // Automatic inventory and access commands require the MFT tools on PATH.
    // Explicit construction avoids the synchronous version probes of new().
    let manager = LockdownManager::with_runner(FlintRunner::with_path("flint"));
    reconcile_with_handler(
        device,
        task,
        config,
        async {
            discover_device_bounded_with_executor(device, &executor)
                .await
                .map(Into::into)
        },
        |request| execute_access(request, &manager, &executor),
    )
    .await
}

async fn execute_access(
    request: Request,
    manager: &LockdownManager,
    executor: &FirmwareToolExecutor,
) -> MlxResult<AccessResult> {
    let changed = || AccessResult::Changed(pb::mlx_device_firmware_access_result::Changed {});
    match request {
        Request::Status(request) => manager
            .get_status_bounded(&request.device_id, executor, ACCESS_TIMEOUT)
            .await
            .map(|status| {
                AccessResult::StatusReport(StatusReport::new(request.device_id, status).into())
            }),
        Request::Lock(request) => match manager
            .lock_device_bounded(&request.device_id, &request.key, executor, ACCESS_TIMEOUT)
            .await
        {
            Ok(_) | Err(MlxError::AlreadyLocked) => Ok(changed()),
            Err(error) => Err(error),
        },
        Request::Unlock(request) => match manager
            .unlock_device_bounded(&request.device_id, &request.key, executor, ACCESS_TIMEOUT)
            .await
        {
            Ok(_) | Err(MlxError::AlreadyUnlocked) => Ok(changed()),
            Err(error) => Err(error),
        },
    }
}

async fn reconcile_with_handler<ExecuteFuture>(
    device: &str,
    task: &MlxDeviceReconcileFirmwareAccess,
    config: &SupervisorConfig,
    discover: impl Future<Output = Result<pb::MlxDeviceInfo, DeviceDiscoveryError>>,
    execute: impl FnOnce(Request) -> ExecuteFuture,
) -> pb::MlxObservation
where
    ExecuteFuture: Future<Output = MlxResult<AccessResult>>,
{
    let Ok(identity) = validate_request(device, task) else {
        return request_error(device, task, "invalid NIC firmware access request");
    };
    let Some(request) = task.request.clone() else {
        return request_error(device, task, "nic firmware access request is missing");
    };
    let mut access = match DirectMlxAccess::acquire(config, device) {
        Ok(access) => access,
        Err(_) => {
            return request_error(device, task, "nic hardware access is unavailable");
        }
    };
    let observation = match discover.await {
        Ok(device_info) => {
            let typed_device = MlxDeviceInfo::try_from(device_info.clone());
            if !typed_device.as_ref().is_ok_and(|observed| {
                normalize_pci_address(&observed.pci_name).as_deref() == Some(device)
                    && identity.matches(observed)
            }) {
                request_error(
                    device,
                    task,
                    "observed NIC does not match the expected device",
                )
            } else {
                match execute(request).await {
                    Ok(reply) => pb::MlxObservation {
                        device_info: Some(device_info),
                        firmware_access_result: Some(result(device, task, reply)),
                        ..Default::default()
                    },
                    Err(error) => {
                        // Keep typed settlement before mapping to the redacted
                        // correlated failure that Core stores for this revision.
                        access.observe_error(&error);
                        request_error(device, task, "nic firmware access command failed")
                    }
                }
            }
        }
        Err(error) => {
            access.observe_error(&error);
            tracing::warn!(pci_name = device, "NIC firmware access discovery failed");
            request_error(device, task, "nic firmware access discovery failed")
        }
    };
    match access.finish() {
        Ok(()) => observation,
        Err(DirectMlxAccessError::Release(error)) => {
            // The process has stopped. Retain the completed response; a stale
            // marker blocks later requests until release can be reconciled.
            tracing::warn!(pci_name = device, %error, "NIC access completed but claim release failed");
            observation
        }
        Err(_) => request_error(device, task, "nic access process settlement is unknown"),
    }
}

fn validate_request(
    device: &str,
    task: &MlxDeviceReconcileFirmwareAccess,
) -> Result<MlxDeviceIdentity, ()> {
    if task.controller_state_version.is_empty()
        || task.controller_state_version.len() > 256
        || !task
            .controller_state_version
            .bytes()
            .all(|byte| byte.is_ascii_graphic())
        || normalize_pci_address(device).as_deref() != Some(device)
    {
        return Err(());
    }
    let inner_device = match task.request.as_ref().ok_or(())? {
        Request::Status(request) => &request.device_id,
        Request::Lock(request) => &request.device_id,
        Request::Unlock(request) => &request.device_id,
    };
    if inner_device != device {
        return Err(());
    }
    task.expected_device
        .clone()
        .ok_or(())?
        .try_into()
        .map_err(|_| ())
}

fn result(
    device: &str,
    task: &MlxDeviceReconcileFirmwareAccess,
    result: AccessResult,
) -> pb::MlxDeviceFirmwareAccessResult {
    pb::MlxDeviceFirmwareAccessResult {
        controller_state_version: task.controller_state_version.clone(),
        pci_name: device.into(),
        expected_device: task.expected_device.clone(),
        result: Some(result),
    }
}

/// Report a correlated failure without credentials or unauthenticated device facts.
pub(super) fn request_error(
    device: &str,
    task: &MlxDeviceReconcileFirmwareAccess,
    message: &'static str,
) -> pb::MlxObservation {
    pb::MlxObservation {
        firmware_access_result: Some(result(
            device,
            task,
            AccessResult::Error(pb::MlxDeviceStreamError {
                status: pb::MlxDeviceStreamErrorStatus::Internal.into(),
                message: message.into(),
            }),
        )),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use carbide_test_support::{Case, Outcome, check_cases};

    use super::*;

    const DEVICE: &str = "0000:03:00.0";

    fn task(request: Request) -> MlxDeviceReconcileFirmwareAccess {
        MlxDeviceReconcileFirmwareAccess {
            controller_state_version: "state-version-1".into(),
            expected_device: Some(
                MlxDeviceIdentity::from_mac_str("02:00:00:00:00:01")
                    .unwrap()
                    .into(),
            ),
            request: Some(request),
        }
    }

    /// A controller revision, exact locator, and physical selector are separate
    /// required inputs; none can be inferred from the others.
    #[test]
    fn access_request_rejects_missing_or_contradictory_identity() {
        let valid = task(Request::Status(pb::MlxDeviceLockdownStatusRequest {
            device_id: DEVICE.into(),
        }));
        let mut missing_version = valid.clone();
        missing_version.controller_state_version.clear();
        let mut redirected = valid.clone();
        redirected.request = Some(Request::Status(pb::MlxDeviceLockdownStatusRequest {
            device_id: "0000:04:00.0".into(),
        }));
        let mut missing_selector = valid;
        missing_selector.expected_device = None;
        check_cases(
            [
                Case {
                    scenario: "a result needs a controller revision to bind it",
                    input: missing_version,
                    expect: Outcome::Fails,
                },
                Case {
                    scenario: "the nested command cannot redirect hardware access",
                    input: redirected,
                    expect: Outcome::Fails,
                },
                Case {
                    scenario: "a BDF alone cannot identify the physical card",
                    input: missing_selector,
                    expect: Outcome::Fails,
                },
            ],
            |task| validate_request(DEVICE, &task),
        );
    }

    /// A replacement at the same BDF cannot inherit access work. Discovery and
    /// the matching command retain one claim and preserve the revision/selector.
    #[tokio::test]
    async fn checked_access_requires_physical_identity_under_one_claim() {
        let directory = match std::env::var_os("LIBMLX_TEST_STATE_DIRECTORY") {
            Some(parent) => tempfile::tempdir_in(parent),
            None => tempfile::tempdir(),
        }
        .unwrap();
        let config = SupervisorConfig {
            state_directory: directory.path().join("state"),
            ..Default::default()
        };
        let lock = Request::Lock(pb::MlxDeviceLockdownLockRequest {
            device_id: DEVICE.into(),
            key: "0123456789abcdef".into(),
        });
        for (observed_mac, request, expected_result) in [
            // PN, PSID, and location match: only physical identity differs.
            (
                "02:00:00:00:00:02",
                lock.clone(),
                AccessResult::Error(pb::MlxDeviceStreamError {
                    status: pb::MlxDeviceStreamErrorStatus::Internal.into(),
                    message: "observed NIC does not match the expected device".into(),
                }),
            ),
            // An eligible command's response retains the independently queried identity.
            (
                "02:00:00:00:00:01",
                lock,
                AccessResult::Changed(pb::mlx_device_firmware_access_result::Changed {}),
            ),
        ] {
            let task = task(request.clone());
            let should_execute = !matches!(&expected_result, AccessResult::Error(_));
            let mut executed = false;
            let observed = pb::MlxDeviceInfo {
                pci_name: DEVICE.into(),
                base_mac: observed_mac.into(),
                part_number: "part".into(),
                psid: "psid".into(),
                ..Default::default()
            };
            let observation = reconcile_with_handler(
                DEVICE,
                &task,
                &config,
                async {
                    assert!(config.state_directory.join("mutation.json").exists());
                    Ok(observed.clone())
                },
                |command| {
                    assert_eq!(command, request);
                    assert!(config.state_directory.join("mutation.json").exists());
                    executed = true;
                    std::future::ready(Ok(expected_result.clone()))
                },
            )
            .await;
            assert_eq!(executed, should_execute);
            assert_eq!(observation.device_info, should_execute.then_some(observed));
            let result = observation.firmware_access_result.unwrap();
            assert_eq!(
                result.controller_state_version,
                task.controller_state_version
            );
            assert_eq!(result.expected_device, task.expected_device);
            assert_eq!(result.pci_name, DEVICE);
            assert_eq!(result.result, Some(expected_result));
            assert!(!config.state_directory.join("mutation.json").exists());
        }
    }

    /// Sanitizing bounded execution errors must retain their cleanup evidence:
    /// a settled failure releases the host, but unknown descendants do not.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn bounded_access_errors_preserve_settlement_through_redaction() {
        use std::os::unix::process::ExitStatusExt;

        use libmlx::firmware::error::{AttemptError, AttemptFailure};

        let directory = match std::env::var_os("LIBMLX_TEST_STATE_DIRECTORY") {
            Some(parent) => tempfile::tempdir_in(parent),
            None => tempfile::tempdir(),
        }
        .unwrap();
        for (scenario, cleanup_errors, retains_claim) in [
            ("settled", Vec::new(), false),
            (
                "unknown-cleanup",
                vec![std::io::Error::other("fixture-key leaked child diagnostic")],
                true,
            ),
        ] {
            let config = SupervisorConfig {
                state_directory: directory.path().join(scenario),
                ..Default::default()
            };
            let task = task(Request::Lock(pb::MlxDeviceLockdownLockRequest {
                device_id: DEVICE.into(),
                key: "0123456789abcdef".into(),
            }));
            let failure = MlxError::CommandAttempt(
                AttemptError::Started {
                    command: "flint fixture-key".into(),
                    status: Some(std::process::ExitStatus::from_raw(9)),
                    stdout: b"fixture-key".to_vec(),
                    stderr: b"fixture-key".to_vec(),
                    reason: AttemptFailure::Deadline(ACCESS_TIMEOUT),
                    cleanup_errors,
                }
                .into(),
            );
            let observation = reconcile_with_handler(
                DEVICE,
                &task,
                &config,
                async {
                    Ok(pb::MlxDeviceInfo {
                        pci_name: DEVICE.into(),
                        base_mac: "02:00:00:00:00:01".into(),
                        ..Default::default()
                    })
                },
                |_| std::future::ready(Err(failure)),
            )
            .await;
            assert!(observation.device_info.is_none());
            assert!(matches!(
                observation.firmware_access_result.as_ref().unwrap().result,
                Some(AccessResult::Error(_))
            ));
            assert!(!format!("{observation:?}").contains("fixture-key"));
            assert_eq!(
                config.state_directory.join("mutation.json").exists(),
                retains_claim
            );
            if retains_claim {
                let next = reconcile_with_handler(
                    DEVICE,
                    &task,
                    &config,
                    async { panic!("uncertain child cleanup must block the next discovery") },
                    |_| async { panic!("uncertain child cleanup must block another mutation") },
                )
                .await;
                assert!(next.device_info.is_none());
                assert!(config.state_directory.join("mutation.json").exists());
            }
        }
    }

    /// Successful and already-applied mutations both require a new status
    /// request. The real bounded helper must execute each read independently.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn bounded_access_changes_then_reads_back_status() {
        let directory = match std::env::var_os("LIBMLX_TEST_STATE_DIRECTORY") {
            Some(parent) => tempfile::tempdir_in(parent),
            None => tempfile::tempdir(),
        }
        .unwrap();
        let tool = directory.path().join("flint");
        let script = r#"#!/bin/sh
set -eu
[ "$1" = -d ] && [ "$2" = 0000:03:00.0 ] || exit 40
case "$3" in
    hw_access)
        [ "$#" -eq 5 ] && [ "$5" = 0123456789abcdef ] || exit 41
        printf '%s\n' "$4" >> "$0.calls"
        if [ "$4" = disable ]; then
            if [ -f "$0.locked" ]; then printf 'already disabled\n'; exit 1; fi
            printf 'locked' > "$0.locked"
        elif [ "$4" = enable ]; then
            if [ ! -f "$0.locked" ]; then printf 'already enabled\n'; exit 1; fi
            /bin/rm "$0.locked"
        else exit 42; fi
        ;;
    q)
        [ "$#" -eq 3 ] || exit 43
        printf 'query\n' >> "$0.calls"
        if [ -f "$0.locked" ]; then printf 'HW access is disabled\n'; exit 1; fi
        ;;
    *) exit 44 ;;
esac
"#;
        // A child writes the executable so another test's concurrent fork
        // cannot inherit a writable descriptor and cause ETXTBSY at execution.
        let writer = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::process::Command::new("/bin/sh")
                .args([
                    "-c",
                    "set -eu; umask 077; printf '%s' \"$2\" > \"$1\"; chmod 700 \"$1\"",
                    "fixture-writer",
                ])
                .arg(&tool)
                .arg(script)
                .kill_on_drop(true)
                .status(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(writer.success());
        let helper = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("mlx-firmware-supervisor");
        assert!(
            helper.is_file(),
            "build the NIC firmware test helper before Scout tests"
        );
        let executor = FirmwareToolExecutor::with_supervisor_path(helper);
        let manager = LockdownManager::with_runner(FlintRunner::with_path(tool.to_string_lossy()));
        let config = SupervisorConfig {
            state_directory: directory.path().join("state"),
            ..Default::default()
        };
        let lock = Request::Lock(pb::MlxDeviceLockdownLockRequest {
            device_id: DEVICE.into(),
            key: "0123456789abcdef".into(),
        });
        let unlock = Request::Unlock(pb::MlxDeviceLockdownUnlockRequest {
            device_id: DEVICE.into(),
            key: "0123456789abcdef".into(),
        });
        let status = Request::Status(pb::MlxDeviceLockdownStatusRequest {
            device_id: DEVICE.into(),
        });
        tokio::time::timeout(Duration::from_secs(20), async {
            for (step, request, expected_status) in [
                ("lock", lock.clone(), None),
                ("already locked", lock, None),
                ("read locked", status.clone(), Some(pb::LockStatus::Locked)),
                ("unlock", unlock.clone(), None),
                ("already unlocked", unlock, None),
                ("read unlocked", status, Some(pb::LockStatus::Unlocked)),
            ] {
                let mut task = task(request);
                task.controller_state_version = step.replace(' ', "-");
                let observed = pb::MlxDeviceInfo {
                    pci_name: DEVICE.into(),
                    base_mac: "02:00:00:00:00:01".into(),
                    ..Default::default()
                };
                let observation = reconcile_with_handler(
                    DEVICE,
                    &task,
                    &config,
                    async {
                        assert!(config.state_directory.join("mutation.json").exists());
                        Ok(observed.clone())
                    },
                    |request| execute_access(request, &manager, &executor),
                )
                .await;
                assert_eq!(observation.device_info, Some(observed), "{step}");
                let result = observation.firmware_access_result.unwrap();
                assert_eq!(
                    result.controller_state_version,
                    task.controller_state_version
                );
                match (expected_status, result.result) {
                    (None, Some(AccessResult::Changed(_))) => {}
                    (Some(expected), Some(AccessResult::StatusReport(status))) => {
                        assert_eq!(status.device_id, DEVICE);
                        assert_eq!(status.status, expected as i32);
                        assert!(chrono::DateTime::parse_from_rfc3339(&status.timestamp).is_ok());
                    }
                    unexpected => panic!("{step}: unexpected response {unexpected:?}"),
                }
                assert!(!config.state_directory.join("mutation.json").exists());
            }
        })
        .await
        .expect("bounded fake access flow completed");
        assert_eq!(
            std::fs::read_to_string(tool.with_extension("calls")).unwrap(),
            "disable\ndisable\nquery\nenable\nenable\nquery\n"
        );
    }
}
