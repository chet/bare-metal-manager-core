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

//! Admin UI live log viewer.
//!
//! `page()` serves up the unified logs hub. `stream()` is a Server-Sent Events
//! endpoint: for `source=api` it replays the recent in-process tracing buffer
//! and then tails live events from [`crate::logging::stream::LogStream`]; for
//! `source=scout` it opens a log-stream flow to a connected scout agent over
//! ScoutStream and forwards the lines it relays back. `scout_connections()`
//! lists the agents available to tail. DPU agents will join the same way.

use std::convert::Infallible;
use std::sync::Arc;

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use carbide_uuid::machine::MachineId;
use futures::stream::{self, StreamExt};
use rpc::protos::forge as proto;
use tokio::sync::broadcast::error::RecvError;

use super::Base;
use crate::api::Api;
use crate::logging::stream::LogLine;
use crate::scout_stream::ConnectionRegistry;

/// Hard cap on a single page, so a client can't request an unbounded slice
/// regardless of the configured default page size.
const MAX_PAGE_SIZE: usize = 5000;

#[derive(Template)]
#[template(path = "api_logs.html")]
struct LogsPage {}

impl Base for LogsPage {}

/// `GET /admin/logs` — the unified live log viewer hub.
pub async fn page() -> Html<String> {
    Html(LogsPage {}.render().unwrap())
}

/// Query parameters for the live stream endpoint. `machine_id` selects which
/// scout to tail; it is required for `source=scout` and ignored for `source=api`.
#[derive(serde::Deserialize)]
pub struct StreamQuery {
    machine_id: Option<String>,
}

/// Handle `GET /admin/logs/{source}/stream`. For `source=api` this tails
/// carbide-api's own in-process logs; for `source=scout` it opens a log-stream
/// flow to the scout agent named by the `machine_id` query parameter and
/// forwards its lines. Both are Server-Sent Events of the same JSON shape.
pub async fn stream(
    State(state): State<Arc<Api>>,
    Path(source): Path<String>,
    Query(query): Query<StreamQuery>,
) -> Response {
    match source.as_str() {
        "api" => stream_api(&state),
        "scout" => match query.machine_id {
            Some(machine_id) => stream_scout(state, machine_id).await,
            None => (
                StatusCode::BAD_REQUEST,
                "scout log stream requires a machine_id query parameter",
            )
                .into_response(),
        },
        other => (
            StatusCode::NOT_FOUND,
            format!("log source {other:?} is not available"),
        )
            .into_response(),
    }
}

/// SSE tail of carbide-api's own in-process logs: replay the recent buffer, then
/// stream live events from [`crate::logging::stream::LogStream`].
fn stream_api(state: &Api) -> Response {
    let page_size = state.runtime_config.log_history.page_size;
    let log_stream = state.dynamic_settings.log_stream.clone();
    // Subscribe before snapshotting the backlog so no line slips through the gap
    // between the two.
    let rx = log_stream.subscribe();
    let backlog = log_stream.latest(page_size);

    let replay = stream::iter(
        backlog
            .into_iter()
            .map(|line| Ok::<_, Infallible>(line_event(line.as_ref()))),
    );

    let live = stream::unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Ok(line) => Some((Ok::<_, Infallible>(line_event(line.as_ref())), rx)),
            // A subscriber that fell behind the bounded channel: tell the viewer
            // how many lines it missed rather than dropping the connection.
            Err(RecvError::Lagged(skipped)) => {
                let ev = Event::default().event("lag").data(skipped.to_string());
                Some((Ok(ev), rx))
            }
            Err(RecvError::Closed) => None,
        }
    });

    Sse::new(replay.chain(live))
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// SSE tail of a connected scout agent's logs: open a log-stream flow to the
/// agent over ScoutStream and forward each [`LogLine`] it sends back. The flow
/// is torn down (and the agent told to stop) when the client disconnects, via
/// [`ScoutLogStreamGuard`].
async fn stream_scout(state: Arc<Api>, machine_id: String) -> Response {
    let Ok(machine_id) = machine_id.parse::<MachineId>() else {
        return (
            StatusCode::BAD_REQUEST,
            format!("invalid machine_id: {machine_id:?}"),
        )
            .into_response();
    };

    let registry = state.scout_stream_registry.clone();
    if !registry.is_connected(machine_id).await {
        return (
            StatusCode::NOT_FOUND,
            format!("scout agent {machine_id} is not connected"),
        )
            .into_response();
    }

    let start = proto::ScoutStreamScoutBoundMessage::new_flow(
        proto::scout_stream_scout_bound_message::Payload::StartLogStreamRequest(
            proto::ScoutStreamStartLogStreamRequest {},
        ),
    );
    let (flow_uuid, rx) = match registry.open_stream_flow(machine_id, start).await {
        Ok(flow) => flow,
        Err(status) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to start scout log stream: {}", status.message()),
            )
                .into_response();
        }
    };

    // The guard rides along in the stream state so the flow is closed (and the
    // agent told to stop relaying) whenever the SSE stream is dropped — whether
    // the client disconnects or the flow ends.
    let guard = ScoutLogStreamGuard {
        registry,
        machine_id,
        flow_uuid,
    };

    let events = stream::unfold((rx, guard), |(mut rx, guard)| async move {
        loop {
            match rx.recv().await {
                Some(message) => {
                    if let Some(proto::scout_stream_api_bound_message::Payload::LogLine(line)) =
                        message.payload
                    {
                        let line = log_line_from_proto(line);
                        return Some((Ok::<_, Infallible>(line_event(&line)), (rx, guard)));
                    }
                    // Ignore any non-LogLine message on this flow and keep waiting.
                }
                None => return None,
            }
        }
    });

    Sse::new(events)
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// Closes a scout log-stream flow when dropped: removes the flow and tells the
/// scout agent to stop relaying. Cleanup is async, so it is spawned.
struct ScoutLogStreamGuard {
    registry: ConnectionRegistry,
    machine_id: MachineId,
    flow_uuid: uuid::Uuid,
}

impl Drop for ScoutLogStreamGuard {
    fn drop(&mut self) {
        let registry = self.registry.clone();
        let machine_id = self.machine_id;
        let flow_uuid = self.flow_uuid;
        tokio::spawn(async move {
            let stop = proto::ScoutStreamScoutBoundMessage::from_flow(
                flow_uuid,
                proto::scout_stream_scout_bound_message::Payload::StopLogStreamRequest(
                    proto::ScoutStreamStopLogStreamRequest {},
                ),
            );
            registry
                .close_stream_flow(machine_id, flow_uuid, stop)
                .await;
        });
    }
}

/// Rebuild a [`LogLine`] from its wire form for rendering, normalizing the level
/// string back to one of the tap's static labels.
fn log_line_from_proto(line: proto::ScoutStreamLogLine) -> LogLine {
    LogLine {
        seq: line.seq,
        timestamp: line.timestamp,
        level: LogLine::normalize_level(&line.level),
        target: line.target,
        message: line.message,
        fields: line.fields.into_iter().collect(),
        location: line.location,
        span_id: line.span_id,
    }
}

/// Query parameters for the scrollback history endpoint.
#[derive(serde::Deserialize)]
pub struct HistoryQuery {
    /// Return lines older than this `seq` cursor. Absent = newest page.
    before: Option<u64>,
    /// Max lines to return; clamped to `MAX_PAGE_SIZE`. Absent = `DEFAULT_PAGE_SIZE`.
    limit: Option<usize>,
}

/// `GET /admin/logs/{source}/history?before=<seq>&limit=<n>` — one page of
/// buffered lines (oldest-first) for scrollback. With `before`, returns the
/// page just older than that cursor; without it, the newest page.
pub async fn history(
    State(state): State<Arc<Api>>,
    Path(source): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> Response {
    if source != "api" {
        return (
            StatusCode::NOT_FOUND,
            format!("log source {source:?} is not available yet"),
        )
            .into_response();
    }

    let limit = query
        .limit
        .unwrap_or(state.runtime_config.log_history.page_size)
        .min(MAX_PAGE_SIZE);
    let log_stream = &state.dynamic_settings.log_stream;
    let lines = match query.before {
        Some(before) => log_stream.history(before, limit),
        None => log_stream.latest(limit),
    };

    let body = serde_json::to_string(&lines).unwrap_or_else(|_| "[]".to_string());
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

/// Serialize a log line into an SSE data frame (one JSON object per event).
fn line_event(line: &LogLine) -> Event {
    Event::default().data(serde_json::to_string(line).unwrap_or_default())
}

/// `GET /admin/logs/{source}/connections` — for `source=scout`, the JSON list of
/// currently connected scout agents, for the viewer's machine picker.
pub async fn scout_connections(
    State(state): State<Arc<Api>>,
    Path(source): Path<String>,
) -> Response {
    if source != "scout" {
        return (
            StatusCode::NOT_FOUND,
            format!("log source {source:?} has no connections list"),
        )
            .into_response();
    }

    let mut connections: Vec<ScoutConnection> = state
        .scout_stream_registry
        .list_connected()
        .await
        .into_iter()
        .map(|(machine_id, connected_at)| ScoutConnection {
            machine_id: machine_id.to_string(),
            connected_at: format_system_time(connected_at),
            uptime_seconds: connected_at.elapsed().unwrap_or_default().as_secs(),
        })
        .collect();
    // Stable order so the picker doesn't shuffle between refreshes.
    connections.sort_by(|a, b| a.machine_id.cmp(&b.machine_id));

    let body = serde_json::to_string(&connections).unwrap_or_else(|_| "[]".to_string());
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

/// One connected scout agent, as returned by [`scout_connections`].
#[derive(serde::Serialize)]
struct ScoutConnection {
    machine_id: String,
    connected_at: String,
    uptime_seconds: u64,
}

/// Format a `SystemTime` as an RFC 3339 string (or `"unknown"`).
fn format_system_time(time: std::time::SystemTime) -> String {
    match time.duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => chrono::DateTime::from_timestamp(duration.as_secs() as i64, 0)
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_else(|| "unknown".to_string()),
        Err(_) => "unknown".to_string(),
    }
}
