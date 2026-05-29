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

use std::collections::HashMap;
use std::time::Duration;

use carbide_log_stream::{LogLine, LogStream};
use carbide_uuid::machine::MachineId;
use libmlx::profile::error::MlxProfileError;
use rpc::forge::ScoutStreamApiBoundMessage;
use rpc::protos::forge::{
    ScoutStreamLogLine, scout_stream_api_bound_message, scout_stream_scout_bound_message,
};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;

use crate::cfg::Options;
use crate::{client, mlx_device};

// ScoutStreamError represents errors that can
// occur during the life of a scout stream connection.
#[derive(Debug, thiserror::Error)]
pub enum ScoutStreamError {
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),
    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("Profile error: {0}")]
    Profile(#[from] MlxProfileError),
    #[error("Connection lost")]
    ConnectionLost,
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    #[error("Invalid URI: {0}")]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error("Client initialization error: {0}")]
    ClientError(String),
}

// start_scout_stream spawns a background task that manages the streaming
// gRPC connection to carbide-api for scout stream operations.
pub fn start_scout_stream(
    machine_id: MachineId,
    options: &Options,
    log_stream: LogStream,
) -> tokio::task::JoinHandle<()> {
    let options = options.clone();
    tokio::spawn(async move {
        loop {
            tracing::info!(
                "scout stream starting (api:{}, machine_id:{machine_id})",
                options.api
            );

            match run_scout_stream_loop(machine_id, &options, &log_stream).await {
                Ok(_) => {
                    tracing::info!(
                        "scout stream closed (api:{}, machine_id:{machine_id})",
                        options.api
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "scout stream error (api:{}, machine_id:{machine_id}): {e}",
                        options.api
                    );
                }
            }
            tracing::warn!(
                "scout stream reconnecting (api:{}, machine_id:{machine_id}): 10s delay",
                options.api
            );
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    })
}

// run_scout_stream_loop establishes the scout stream connection,
// processing requests and reconnecting if the connection is closed.
async fn run_scout_stream_loop(
    machine_id: MachineId,
    options: &Options,
    log_stream: &LogStream,
) -> Result<(), ScoutStreamError> {
    let mut client = client::create_forge_client(options)
        .await
        .map_err(|e| ScoutStreamError::ClientError(e.to_string()))?;

    // Create channels for bidirectional streaming.
    let (tx, rx) = mpsc::channel::<ScoutStreamApiBoundMessage>(100);
    let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    // Send initial request with machine_id.
    let init_request = ScoutStreamApiBoundMessage {
        // Init doesn't take a flow_uuid.
        flow_uuid: None,
        payload: Some(scout_stream_api_bound_message::Payload::Init(
            rpc::protos::forge::ScoutStreamInitRequest {
                machine_id: machine_id.into(),
            },
        )),
    };

    tx.send(init_request).await.map_err(|e| {
        ScoutStreamError::InvalidRequest(format!("scout stream failed to send init request: {e}"))
    })?;

    // Now create the response handler.
    let mut response_stream = client.scout_stream(request_stream).await?.into_inner();

    tracing::info!(
        "scout stream connection established (api:{}, machine_id:{machine_id})",
        options.api
    );

    let replay_lines = options.log_replay_lines;
    // Active log-stream relays, keyed by flow_uuid: one background task per
    // viewer tailing this scout's logs back over the stream. Started on
    // StartLogStreamRequest, cancelled on StopLogStreamRequest, and all torn
    // down when the connection ends (below).
    let mut log_flows: HashMap<uuid::Uuid, tokio::task::JoinHandle<()>> = HashMap::new();

    // ...and start processing streaming updates. Structured as an explicit loop
    // (rather than `while let ... ?`) so that however we exit — clean close,
    // gRPC error, or a malformed message — we always fall through to the relay
    // teardown below.
    let outcome = loop {
        let response = match response_stream.message().await {
            Ok(Some(response)) => response,
            Ok(None) => break Err(ScoutStreamError::ConnectionLost),
            Err(e) => break Err(e.into()),
        };

        let Some(request) = response.payload else {
            continue;
        };

        // In the current model, an incoming message must have a flow_uuid
        // associated with it. If it doesn't, reject the message.
        let Some(flow_uuid_pb) = response.flow_uuid else {
            break Err(ScoutStreamError::InvalidRequest(
                "cannot determine flow, flow_uuid empty from API".to_string(),
            ));
        };
        let flow_uuid: uuid::Uuid = match flow_uuid_pb.try_into() {
            Ok(flow_uuid) => flow_uuid,
            Err(e) => {
                break Err(ScoutStreamError::ClientError(format!(
                    "failed to convert flow_uuid: {e}"
                )));
            }
        };

        match request {
            // Log streaming is a long-lived, multi-response flow rather than a
            // single request/response: spawn a relay task that tails this
            // scout's logs back over the stream under this flow_uuid.
            scout_stream_scout_bound_message::Payload::StartLogStreamRequest(_) => {
                tracing::info!("[scout_stream] starting log stream for flow_uuid: {flow_uuid}");
                let handle =
                    spawn_log_relay(flow_uuid, log_stream.clone(), tx.clone(), replay_lines);
                // A repeated start on the same flow replaces the old relay.
                if let Some(previous) = log_flows.insert(flow_uuid, handle) {
                    previous.abort();
                }
            }
            scout_stream_scout_bound_message::Payload::StopLogStreamRequest(_) => {
                tracing::info!("[scout_stream] stopping log stream for flow_uuid: {flow_uuid}");
                if let Some(handle) = log_flows.remove(&flow_uuid) {
                    handle.abort();
                }
            }
            // Everything else is a oneshot: handle the request and send back a
            // single ScoutStreamApiBoundMessage "response".
            request => {
                let payload = handle_scout_stream_api_bound_message(flow_uuid, machine_id, request);
                if let Err(e) = tx.send(payload).await {
                    tracing::error!(
                        "scout stream failed to send response (api:{}, machine_id:{machine_id}): {e}",
                        options.api
                    );
                    break Err(ScoutStreamError::ConnectionLost);
                }
            }
        }
    };

    // Stop any log relays still running so they don't outlive the connection
    // (a lingering relay would otherwise keep trying to send into a closed
    // stream).
    for handle in log_flows.into_values() {
        handle.abort();
    }

    outcome
}

// spawn_log_relay starts a background task that streams this scout's own log
// lines back to carbide-api under `flow_uuid`: it replays the most recent
// `replay_lines` buffered lines, then tails live ones, until carbide-api stops
// the flow (the task is aborted) or the stream closes.
fn spawn_log_relay(
    flow_uuid: uuid::Uuid,
    log_stream: LogStream,
    tx: mpsc::Sender<ScoutStreamApiBoundMessage>,
    replay_lines: usize,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Subscribe before snapshotting the replay backlog so no line slips
        // through the gap between the two.
        let mut rx = log_stream.subscribe();

        for line in log_stream.latest(replay_lines) {
            if !send_log_line(&tx, flow_uuid, &line).await {
                return;
            }
        }

        loop {
            match rx.recv().await {
                Ok(line) => {
                    if !send_log_line(&tx, flow_uuid, &line).await {
                        return;
                    }
                }
                // Fell behind the bounded broadcast: skip ahead and keep going
                // (the viewer sees a gap rather than a dropped connection).
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return,
            }
        }
    })
}

// send_log_line wraps one LogLine as a ScoutStreamApiBoundMessage on `flow_uuid`
// and sends it. Returns false when carbide-api has closed the stream, signalling
// the relay to stop.
async fn send_log_line(
    tx: &mpsc::Sender<ScoutStreamApiBoundMessage>,
    flow_uuid: uuid::Uuid,
    line: &LogLine,
) -> bool {
    let message = ScoutStreamApiBoundMessage::from_flow(
        flow_uuid,
        scout_stream_api_bound_message::Payload::LogLine(log_line_to_proto(line)),
    );
    tx.send(message).await.is_ok()
}

// log_line_to_proto converts an in-process LogLine into its wire form. The
// fields mirror those the admin web UI already renders for carbide-api's own
// logs, so the same viewer renders scout lines identically.
fn log_line_to_proto(line: &LogLine) -> ScoutStreamLogLine {
    ScoutStreamLogLine {
        seq: line.seq,
        timestamp: line.timestamp.clone(),
        level: line.level.to_string(),
        target: line.target.clone(),
        message: line.message.clone(),
        fields: line.fields.clone().into_iter().collect(),
        location: line.location.clone(),
        span_id: line.span_id.clone(),
    }
}

// handle_scout_stream_api_bound_message routes incoming oneof-based requests
// to the appropriate handler.
fn handle_scout_stream_api_bound_message(
    flow_uuid: uuid::Uuid,
    machine_id: MachineId,
    request: scout_stream_scout_bound_message::Payload,
) -> ScoutStreamApiBoundMessage {
    tracing::info!(
        "[scout_stream] processing incoming request for flow_uuid: {}",
        flow_uuid
    );
    match request {
        scout_stream_scout_bound_message::Payload::ScoutStreamAgentPingRequest(req) => {
            let response = handle_ping(machine_id, req);
            ScoutStreamApiBoundMessage::from_flow(
                flow_uuid,
                scout_stream_api_bound_message::Payload::ScoutStreamAgentPingResponse(response),
            )
        }
        scout_stream_scout_bound_message::Payload::MlxDeviceProfileSyncRequest(req) => {
            let response = mlx_device::handle_profile_sync(req);
            ScoutStreamApiBoundMessage::from_flow(
                flow_uuid,
                scout_stream_api_bound_message::Payload::MlxDeviceProfileSyncResponse(response),
            )
        }
        scout_stream_scout_bound_message::Payload::MlxDeviceProfileCompareRequest(req) => {
            let response = mlx_device::handle_profile_compare(req);
            ScoutStreamApiBoundMessage::from_flow(
                flow_uuid,
                scout_stream_api_bound_message::Payload::MlxDeviceProfileCompareResponse(response),
            )
        }
        scout_stream_scout_bound_message::Payload::MlxDeviceLockdownLockRequest(req) => {
            let response = mlx_device::handle_lockdown_lock(req);
            ScoutStreamApiBoundMessage::from_flow(
                flow_uuid,
                scout_stream_api_bound_message::Payload::MlxDeviceLockdownResponse(response),
            )
        }
        scout_stream_scout_bound_message::Payload::MlxDeviceLockdownUnlockRequest(req) => {
            let response = mlx_device::handle_lockdown_unlock(req);
            ScoutStreamApiBoundMessage::from_flow(
                flow_uuid,
                scout_stream_api_bound_message::Payload::MlxDeviceLockdownResponse(response),
            )
        }
        scout_stream_scout_bound_message::Payload::MlxDeviceLockdownStatusRequest(req) => {
            let response = mlx_device::handle_lockdown_status(req);
            ScoutStreamApiBoundMessage::from_flow(
                flow_uuid,
                scout_stream_api_bound_message::Payload::MlxDeviceLockdownResponse(response),
            )
        }
        scout_stream_scout_bound_message::Payload::MlxDeviceInfoDeviceRequest(req) => {
            let response = mlx_device::handle_info_device(req);
            ScoutStreamApiBoundMessage::from_flow(
                flow_uuid,
                scout_stream_api_bound_message::Payload::MlxDeviceInfoDeviceResponse(response),
            )
        }
        scout_stream_scout_bound_message::Payload::MlxDeviceInfoReportRequest(req) => {
            let response = mlx_device::handle_info_report(req);
            ScoutStreamApiBoundMessage::from_flow(
                flow_uuid,
                scout_stream_api_bound_message::Payload::MlxDeviceInfoReportResponse(response),
            )
        }
        scout_stream_scout_bound_message::Payload::MlxDeviceRegistryListRequest(req) => {
            let response = mlx_device::handle_registry_list(req);
            ScoutStreamApiBoundMessage::from_flow(
                flow_uuid,
                scout_stream_api_bound_message::Payload::MlxDeviceRegistryListResponse(response),
            )
        }
        scout_stream_scout_bound_message::Payload::MlxDeviceRegistryShowRequest(req) => {
            let response = mlx_device::handle_registry_show(req);
            ScoutStreamApiBoundMessage::from_flow(
                flow_uuid,
                scout_stream_api_bound_message::Payload::MlxDeviceRegistryShowResponse(response),
            )
        }
        scout_stream_scout_bound_message::Payload::MlxDeviceConfigQueryRequest(req) => {
            let response = mlx_device::handle_config_query(req);
            ScoutStreamApiBoundMessage::from_flow(
                flow_uuid,
                scout_stream_api_bound_message::Payload::MlxDeviceConfigQueryResponse(response),
            )
        }
        scout_stream_scout_bound_message::Payload::MlxDeviceConfigSetRequest(req) => {
            let response = mlx_device::handle_config_set(req);
            ScoutStreamApiBoundMessage::from_flow(
                flow_uuid,
                scout_stream_api_bound_message::Payload::MlxDeviceConfigSetResponse(response),
            )
        }
        scout_stream_scout_bound_message::Payload::MlxDeviceConfigSyncRequest(req) => {
            let response = mlx_device::handle_config_sync(req);
            ScoutStreamApiBoundMessage::from_flow(
                flow_uuid,
                scout_stream_api_bound_message::Payload::MlxDeviceConfigSyncResponse(response),
            )
        }
        scout_stream_scout_bound_message::Payload::MlxDeviceConfigCompareRequest(req) => {
            let response = mlx_device::handle_config_compare(req);
            ScoutStreamApiBoundMessage::from_flow(
                flow_uuid,
                scout_stream_api_bound_message::Payload::MlxDeviceConfigCompareResponse(response),
            )
        }
        // Log-stream control messages are long-lived / multi-response and are
        // handled directly in run_scout_stream_loop (which spawns or cancels a
        // relay task), so they never reach this oneshot dispatcher.
        scout_stream_scout_bound_message::Payload::StartLogStreamRequest(_)
        | scout_stream_scout_bound_message::Payload::StopLogStreamRequest(_) => {
            unreachable!("log-stream control messages are handled in run_scout_stream_loop")
        }
    }
}

// handle_ping handles a scout stream agent ping
pub fn handle_ping(
    machine_id: MachineId,
    _request: rpc::forge::ScoutStreamAgentPingRequest,
) -> rpc::forge::ScoutStreamAgentPingResponse {
    tracing::info!("[scout_stream::ping] ping requested",);

    rpc::forge::ScoutStreamAgentPingResponse {
        reply: Some(rpc::forge::scout_stream_agent_ping_response::Reply::Pong(
            format!("pong from {machine_id}"),
        )),
    }
}
