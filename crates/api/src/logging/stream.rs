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

//! Re-export of the shared in-process log tap.
//!
//! The implementation lives in the [`carbide_log_stream`] crate so that both
//! carbide-api and the scout agent install the same `tracing` tap and stream
//! lines to the admin web UI log viewer with identical structure. The API's
//! logging wiring ([`crate::logging::setup`]) and the web log handlers
//! ([`crate::web::logs`]) refer to these types through this module, so their
//! paths stay stable.

pub use carbide_log_stream::{LogLine, LogStream, LogStreamLayer};
