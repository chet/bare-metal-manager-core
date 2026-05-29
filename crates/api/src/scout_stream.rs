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

// scout_stream.rs
// This module contains code related to managing scout agent connections.
// It includes the AgentConnection type, which holds the channels used for
// streaming communication, and the ConnectionRegistry, which contains a map
// of machine_id to AgentConnection along with an interface to send messages
// through it.

use std::collections::HashMap;
use std::sync::Arc;

use ::rpc::protos::forge::{ScoutStreamApiBoundMessage, ScoutStreamScoutBoundMessage};
use carbide_uuid::machine::MachineId;
use tokio::sync::{RwLock, mpsc, oneshot};
use tonic::Status;

use crate::CarbideError;

// How many buffered messages a log-stream flow can hold before the connection
// router starts dropping lines for a viewer that has fallen behind.
const STREAM_FLOW_CAPACITY: usize = 1024;

// Flow is a registered request flow over a scout connection, keyed by flow_uuid.
enum Flow {
    // Oneshot is a single request/response: removed from the flow table once its
    // one response has been delivered.
    Oneshot(oneshot::Sender<ScoutStreamApiBoundMessage>),
    // Stream is a long-lived, multi-response flow (e.g. log streaming): it stays
    // registered, receiving every message tagged with its flow_uuid, until it is
    // explicitly closed (or its receiver is dropped).
    Stream(mpsc::Sender<ScoutStreamApiBoundMessage>),
}

// FlowRoute is how the connection router should deliver one response, decided
// while briefly holding the flow-table lock and then acted on without it.
enum FlowRoute {
    Oneshot,
    Stream(mpsc::Sender<ScoutStreamApiBoundMessage>),
    Unknown,
}

// AgentConnection represents an active streaming connection to
// a scout agent. It contains the corresponding machine_id, the
// channels used to pass messages, and any additional metadata
// that we'd like.
struct AgentConnection {
    // machine_id is the identifier for this agent's machine.
    machine_id: MachineId,
    // connected_at is when this connection was established.
    connected_at: std::time::SystemTime,
    // flows are the active request/response flows currently
    // in flight over this connection.
    // tx is the sender for sending requests to the scout agent.
    tx: mpsc::Sender<Result<ScoutStreamScoutBoundMessage, Status>>,
    // rx is the receiver for getting responses from the scout agent.
    rx: Arc<RwLock<mpsc::Receiver<ScoutStreamApiBoundMessage>>>,
    flows: Arc<RwLock<HashMap<uuid::Uuid, Flow>>>,
}

// ConnectionRegistry is the interface for working with active
// scout agent connections. It maintains a map of machine ID
// to the AgentConnection, and exposes an interface to show
// current connections and send messages across them.
#[derive(Clone)]
pub struct ConnectionRegistry {
    // connections is used to map a machine_id to a scout
    // agent connection.
    connections: Arc<RwLock<HashMap<MachineId, AgentConnection>>>,
}

impl ConnectionRegistry {
    // new creates a new connection registry.
    pub fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // register adds a new scout agent connection to the registry,
    // provisioning data structures necessary for tracking the machine,
    // its singular connection, and active flows over the connection.
    pub async fn register(
        &self,
        machine_id: MachineId,
        tx: mpsc::Sender<Result<ScoutStreamScoutBoundMessage, Status>>,
        rx: mpsc::Receiver<ScoutStreamApiBoundMessage>,
    ) {
        let connection = AgentConnection {
            machine_id,
            connected_at: std::time::SystemTime::now(),
            tx,
            rx: Arc::new(RwLock::new(rx)),
            flows: Arc::new(RwLock::new(HashMap::new())),
        };

        // And now background a connection-specific receiver whose job it
        // is to receive messages over the singular connection channel
        // and map the embedded message flow_uuid to an underlying oneshot
        // flow channel.
        let connection_flows = Arc::clone(&connection.flows);
        let connection_rx = Arc::clone(&connection.rx);
        tokio::spawn(async move {
            loop {
                let response = {
                    let mut rx_guard = connection_rx.write().await;
                    rx_guard.recv().await
                };

                let Some(response) = response else {
                    tracing::info!("scout agent connection closed (machine_id={machine_id})");
                    break;
                };

                // Extract and validate flow_uuid.
                let flow_uuid = match extract_flow_uuid(&response, machine_id) {
                    Ok(uuid) => uuid,
                    Err(_) => continue,
                };

                // Route the response to its flow. Oneshot flows are removed once
                // their single response is delivered; stream flows stay
                // registered and receive every message until closed. Decide the
                // route while holding the lock, then act (cloning the stream
                // sender so we never send while borrowing the table).
                let mut flows = connection_flows.write().await;
                let route = match flows.get(&flow_uuid) {
                    Some(Flow::Oneshot(_)) => FlowRoute::Oneshot,
                    Some(Flow::Stream(sender)) => FlowRoute::Stream(sender.clone()),
                    None => FlowRoute::Unknown,
                };
                match route {
                    FlowRoute::Oneshot => {
                        if let Some(Flow::Oneshot(sender)) = flows.remove(&flow_uuid)
                            && let Err(send_err) = sender.send(response)
                        {
                            tracing::warn!(
                                "error relaying flow response (machine_id={machine_id}, flow_uuid={flow_uuid}): {send_err:?}"
                            );
                        }
                    }
                    FlowRoute::Stream(sender) => match sender.try_send(response) {
                        Ok(()) => {}
                        // Viewer fell behind: drop this line rather than stall the
                        // shared router (it serves every flow on this connection).
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            tracing::warn!(
                                "scout log stream lagging, dropping line (machine_id={machine_id}, flow_uuid={flow_uuid})"
                            );
                        }
                        // Receiver gone (viewer disconnected): drop the flow.
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            flows.remove(&flow_uuid);
                        }
                    },
                    FlowRoute::Unknown => {
                        tracing::warn!(
                            "dropping flow response for unknown flow_uuid (machine_id={machine_id}, flow_uuid={flow_uuid}): {response:?}"
                        );
                    }
                }
            }
        });

        let mut connections = self.connections.write().await;
        connections.insert(machine_id, connection);
        tracing::info!("registered scout agent connection for machine: {machine_id}");
    }

    // unregister removes a scout agent connection from the registry.
    pub async fn unregister(&self, machine_id: MachineId) -> bool {
        let mut connections = self.connections.write().await;
        if connections.remove(&machine_id).is_some() {
            tracing::info!("unregistered scout agent connection for machine: {machine_id}");
            true
        } else {
            tracing::info!(
                "could not unregister scout agent connection for machine (not found): {machine_id}"
            );
            false
        }
    }

    // send_request sends a request to a scout agent and waits for a response.
    pub async fn send_request(
        &self,
        machine_id: MachineId,
        request: ScoutStreamScoutBoundMessage,
    ) -> Result<ScoutStreamApiBoundMessage, Status> {
        let flow_uuid = decode_request_flow_uuid(&request, machine_id)?;

        let (connection_tx, connection_flows) = {
            let connections = self.connections.read().await;
            let connection =
                connections
                    .get(&machine_id)
                    .ok_or_else(|| CarbideError::NotFoundError {
                        kind: "scout stream connection",
                        id: machine_id.to_string(),
                    })?;
            (connection.tx.clone(), Arc::clone(&connection.flows))
        };

        // Now create the oneshot channel flow specific
        // to this request/response flow. What happens is we create
        // the flow_uuid-associated send/recv channel here, then send
        // the request off through our connection channel. Next,
        // our connection message processor will map the flow_uuid
        // to the corresponding response_tx, push the message to it,
        // and then our response_rx will receive it here.
        let (response_tx, response_rx) = oneshot::channel();
        {
            let mut flows = connection_flows.write().await;
            flows.insert(flow_uuid, Flow::Oneshot(response_tx));
        }

        // And now the request to the scout agent.
        tracing::info!(
            "sending request to scout agent (machine_id={machine_id}, flow_uuid={flow_uuid})"
        );

        connection_tx.send(Ok(request)).await.map_err(|e| CarbideError::Internal {
                message: format!(
                    "failed to send request to scout agent (machine_id={machine_id}, flow_uuid={flow_uuid}): {e}"
                ),
        })?;

        // And now we wait for a response from the agent.
        // TODO(chet): This is where we'd put timeout handling.
        response_rx.await.map_err(|e| -> Status {
            CarbideError::Internal {
                message: format!(
                    "response channel error (machine_id={machine_id}, flow_uuid={flow_uuid}): {e}",
                ),
            }
            .into()
        })
    }

    // open_stream_flow registers a long-lived, multi-response flow for the
    // scout-bound `request`, sends it, and returns the flow_uuid plus the
    // receiver the caller drains. Used for log streaming; the caller must call
    // `close_stream_flow` with the returned flow_uuid when finished.
    pub async fn open_stream_flow(
        &self,
        machine_id: MachineId,
        request: ScoutStreamScoutBoundMessage,
    ) -> Result<(uuid::Uuid, mpsc::Receiver<ScoutStreamApiBoundMessage>), Status> {
        let flow_uuid = decode_request_flow_uuid(&request, machine_id)?;

        let (connection_tx, connection_flows) = {
            let connections = self.connections.read().await;
            let connection =
                connections
                    .get(&machine_id)
                    .ok_or_else(|| CarbideError::NotFoundError {
                        kind: "scout stream connection",
                        id: machine_id.to_string(),
                    })?;
            (connection.tx.clone(), Arc::clone(&connection.flows))
        };

        let (response_tx, response_rx) = mpsc::channel(STREAM_FLOW_CAPACITY);
        {
            let mut flows = connection_flows.write().await;
            flows.insert(flow_uuid, Flow::Stream(response_tx));
        }

        tracing::info!(
            "opening stream flow to scout agent (machine_id={machine_id}, flow_uuid={flow_uuid})"
        );

        if let Err(e) = connection_tx.send(Ok(request)).await {
            // Roll back the flow we just registered if the request can't be sent.
            connection_flows.write().await.remove(&flow_uuid);
            return Err(CarbideError::Internal {
                message: format!(
                    "failed to send stream request to scout agent (machine_id={machine_id}, flow_uuid={flow_uuid}): {e}"
                ),
            }
            .into());
        }

        Ok((flow_uuid, response_rx))
    }

    // close_stream_flow removes a stream flow opened by `open_stream_flow` and
    // sends `stop_request` to the scout agent so it stops relaying. Best effort:
    // if the connection is already gone, the flow is gone with it.
    pub async fn close_stream_flow(
        &self,
        machine_id: MachineId,
        flow_uuid: uuid::Uuid,
        stop_request: ScoutStreamScoutBoundMessage,
    ) {
        let Some((connection_tx, connection_flows)) = ({
            let connections = self.connections.read().await;
            connections
                .get(&machine_id)
                .map(|c| (c.tx.clone(), Arc::clone(&c.flows)))
        }) else {
            return;
        };

        connection_flows.write().await.remove(&flow_uuid);

        tracing::info!(
            "closing stream flow to scout agent (machine_id={machine_id}, flow_uuid={flow_uuid})"
        );
        // Best effort: the connection may already be closing.
        let _ = connection_tx.send(Ok(stop_request)).await;
    }

    // is_connected checks if a machine is currently connected.
    pub async fn is_connected(&self, machine_id: MachineId) -> bool {
        let connections = self.connections.read().await;
        connections.contains_key(&machine_id)
    }

    // list_connected returns a list of all connected machines with connection info.
    pub async fn list_connected(&self) -> Vec<(MachineId, std::time::SystemTime)> {
        let connections = self.connections.read().await;
        connections
            .iter()
            .map(|(machine_id, conn)| {
                tracing::debug!("active scout stream connection: {}", conn.machine_id);
                (*machine_id, conn.connected_at)
            })
            .collect()
    }
}

// extract_flow_uuid is a little helper to extract and validate flow_uuid,
// logging warnings depending on things that happen.
fn extract_flow_uuid(
    response: &ScoutStreamApiBoundMessage,
    machine_id: MachineId,
) -> Result<uuid::Uuid, ()> {
    let flow_uuid_pb = response.flow_uuid.as_ref().ok_or_else(|| {
        tracing::warn!(
            "dropping flow response with empty flow_uuid (machine_id={machine_id}): {response:?}"
        );
    })?;

    flow_uuid_pb.clone().try_into().map_err(|e| {
        tracing::warn!(
            "failed to decode flow_uuid (machine_id={machine_id}): {flow_uuid_pb:?}: {e:?}"
        );
    })
}

// decode_request_flow_uuid extracts and decodes the flow_uuid that a scout-bound
// request must carry, returning a gRPC-style error if it is missing or
// malformed.
fn decode_request_flow_uuid(
    request: &ScoutStreamScoutBoundMessage,
    machine_id: MachineId,
) -> Result<uuid::Uuid, Status> {
    let Some(flow_uuid_pb) = request.flow_uuid.as_ref() else {
        return Err(CarbideError::Internal {
            message: format!("flow_uuid empty for flow with {machine_id}, unable to build flow"),
        }
        .into());
    };

    flow_uuid_pb.clone().try_into().map_err(|e| {
        CarbideError::Internal {
            message: format!(
                "failed to decode flow_uuid (machine_id={machine_id}): {flow_uuid_pb:?}: {e:?}"
            ),
        }
        .into()
    })
}
