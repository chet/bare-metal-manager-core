/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! carbide-host-support is a library that is used by applications that run on
//! carbide managed hosts

use std::sync::Once;

use carbide_log_stream::{LogStream, LogStreamLayer};
use tracing::metadata::LevelFilter;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::prelude::*;
use tracing_subscriber::util::SubscriberInitExt;

pub mod agent_config;
pub mod dpa_cmds;
#[cfg(feature = "linux-build")]
pub mod hardware_enumeration;
pub mod registration;

static LOG_SETUP: Once = Once::new();

/// Initialize global logging output to STDOUT. Applies to all threads.
/// Use `export RUST_LOG=trace|debug|info|warn|error` to change log level.
pub fn init_logging() -> eyre::Result<()> {
    LOG_SETUP.call_once(|| {
        subscriber()
            .try_init()
            .expect("tracing_subscriber setup failed");
    });
    Ok(())
}

/// Like [`init_logging`], but additionally installs an in-process log tap and
/// returns a handle to it. The tap mirrors what goes to STDOUT (same
/// `EnvFilter`) into a bounded broadcast channel plus a byte-capped ring buffer
/// (`max_bytes`), so the caller can stream its own logs elsewhere — e.g. scout
/// relaying its logs to the carbide-api admin web UI over ScoutStream. Use the
/// returned [`LogStream`] to `subscribe()` to live lines or replay recent ones.
///
/// As with [`init_logging`], logging is only initialized once per process; if it
/// was already initialized, the returned handle simply receives no lines.
pub fn init_logging_with_log_stream(max_bytes: usize) -> eyre::Result<LogStream> {
    let log_stream = LogStream::with_max_bytes(max_bytes);
    let tap = LogStreamLayer::new(log_stream.clone());
    LOG_SETUP.call_once(|| {
        tracing_subscriber::registry()
            .with(logfmt::layer().with_filter(env_filter()))
            .with(tap.with_filter(env_filter()))
            .try_init()
            .expect("tracing_subscriber setup failed");
    });
    Ok(log_stream)
}

// A logging subscriber for use on the current thread.
// Usually you want `init_logging()` instead.
//
// Usage: `let guard = subscriber().set_default()`
// Subscriber is unregistered when guard is dropped.
pub fn subscriber() -> impl SubscriberInitExt {
    Box::new(tracing_subscriber::registry().with(logfmt::layer().with_filter(env_filter())))
}

// The shared STDOUT log filter: INFO by default (override via `RUST_LOG`), with
// a few chatty crates pinned quieter. Built fresh per layer since `EnvFilter`
// isn't `Clone`.
fn env_filter() -> EnvFilter {
    EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy()
        .add_directive("tower=warn".parse().unwrap())
        .add_directive("rustls=warn".parse().unwrap())
        .add_directive("hyper=warn".parse().unwrap())
        .add_directive("sqlx=info".parse().unwrap())
        .add_directive("tokio_util::codec=warn".parse().unwrap())
        .add_directive("h2=warn".parse().unwrap())
        .add_directive("hickory_resolver::error=info".parse().unwrap())
        .add_directive("hickory_proto::xfer=info".parse().unwrap())
        .add_directive("hickory_resolver::name_server=info".parse().unwrap())
        .add_directive("hickory_proto=info".parse().unwrap())
        .add_directive("netlink_proto=warn".parse().unwrap())
}
