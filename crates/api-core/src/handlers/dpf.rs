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

use std::future::Future;
use std::time::Duration;

use ::rpc::forge as rpc;
use carbide_dpf::dpu_node_cr_name;
use carbide_uuid::machine::{HostMachineId, MachineId};
use db::ObjectFilter;
use db::machine::find_one;
use model::machine::machine_search_config::MachineSearchConfig;
use tonic::{Request, Response, Status};

use crate::CarbideError;
use crate::api::{Api, RequestReceivedAt, log_request_data};
use crate::grpc_timeout::parse_grpc_timeout;
use crate::handlers::utils::convert_and_log_machine_id;

// Bound DPF work when the caller supplies no timeout. This cap also matches
// `rpc::carbide_tls_client::REQUEST_TIMEOUT`.
const DPF_PREPARATION_TIMEOUT: Duration = Duration::from_secs(300);

/// Bounds DPF pool acquisition, row admission, retries, waits, and commit.
/// The listener's receipt timestamp includes time in authorization and admission;
/// direct handler calls fall back to handler entry. Missing or malformed
/// `grpc-timeout` metadata keeps the 300-second cap; a valid shorter value
/// reduces it. The parser accepts the gRPC digit and unit grammar.
struct DpfPreparationDeadline {
    deadline: tokio::time::Instant,
}

impl DpfPreparationDeadline {
    fn from_request<T>(request: &tonic::Request<T>) -> Self {
        let (started_at, timeout) = match request.extensions().get::<RequestReceivedAt>() {
            Some(received) => (received.instant, received.grpc_timeout),
            None => (
                tokio::time::Instant::now(),
                request
                    .metadata()
                    .get("grpc-timeout")
                    .and_then(|value| value.to_str().ok())
                    .and_then(parse_grpc_timeout),
            ),
        };
        let budget = timeout
            .unwrap_or(DPF_PREPARATION_TIMEOUT)
            .min(DPF_PREPARATION_TIMEOUT);
        Self {
            deadline: started_at + budget,
        }
    }

    /// `bound` constructs the transaction-owning future only while time remains.
    /// Expiry drops it; the session guard discards its connection. This does not
    /// guarantee immediate PostgreSQL cancellation or resolve an in-flight commit.
    async fn bound<T, MakeWork, Work>(self, make_work: MakeWork) -> Result<T, tonic::Status>
    where
        MakeWork: FnOnce() -> Work,
        Work: Future<Output = Result<T, tonic::Status>>,
    {
        let expired = || tonic::Status::from(CarbideError::DpfPreparationDeadlineExceeded);
        if tokio::time::Instant::now() >= self.deadline {
            return Err(expired());
        }
        let mut work = std::pin::pin!(async move { make_work().await });
        tokio::select! {
            // Tokio rounds timers to milliseconds. Check the exact clock before
            // each work poll and after it, including a ready result.
            biased;
            _ = tokio::time::sleep_until(self.deadline) => Err(expired()),
            result = std::future::poll_fn(|cx| {
                if tokio::time::Instant::now() >= self.deadline {
                    return std::task::Poll::Ready(Err(expired()));
                }
                let result = work.as_mut().poll(cx);
                if tokio::time::Instant::now() >= self.deadline {
                    std::task::Poll::Ready(Err(expired()))
                } else {
                    result
                }
            }) => result,
        }
    }
}

async fn lock_host_for_dpf_update(
    txn: &mut sqlx::PgConnection,
    machine_id: &HostMachineId,
) -> Result<(), CarbideError> {
    if !db::machine::lock_by_id_for_update_if_present(txn, &(*machine_id).into()).await? {
        return Err(CarbideError::NotFoundError {
            kind: "snapshot",
            id: machine_id.to_string(),
        });
    }
    Ok(())
}

pub(crate) async fn modify_dpf_state(
    api: &Api,
    request: Request<rpc::ModifyDpfStateRequest>,
) -> Result<Response<()>, Status> {
    let deadline = DpfPreparationDeadline::from_request(&request);
    log_request_data(&request);
    let request = request.get_ref();
    let machine_id: HostMachineId = convert_and_log_machine_id(request.machine_id.as_ref())?;

    deadline
        .bound(|| async {
            loop {
                let mut session = db::DbSessionGuard::acquire(api.pg_pool()).await?;
                let mut txn = db::Transaction::begin_inner(session.as_pgconn()).await?;
                lock_host_for_dpf_update(txn.as_mut(), &machine_id).await?;
                match db::machine::try_lock_host_interfaces(txn.as_mut(), &machine_id).await? {
                    db::machine::HostInterfaceAdmission::Acquired => {}
                    db::machine::HostInterfaceAdmission::Contended(interface_id) => {
                        txn.rollback().await?;
                        session.return_to_pool();
                        db::machine::wait_for_host_interface(api.pg_pool(), &interface_id).await?;
                        continue;
                    }
                    db::machine::HostInterfaceAdmission::Changed => {
                        txn.rollback().await?;
                        session.return_to_pool();
                        continue;
                    }
                }
                let admission = db::machine::try_lock_dpf_dpus(txn.as_mut(), &machine_id)
                    .await?
                    .ok_or_else(|| CarbideError::NotFoundError {
                        kind: "snapshot",
                        id: machine_id.to_string(),
                    })?;
                let target = admission.target;

                if !request.dpf_enabled && target.dpf.used_for_ingestion {
                    txn.rollback().await?;
                    session.return_to_pool();
                    return Err(CarbideError::FailedPrecondition(format!(
                        "cannot disable DPF for host {}: machine was ingested via DPF",
                        machine_id
                    ))
                    .into());
                }

                let mut dpu_ids = target.attached_dpu_ids;
                dpu_ids.sort_unstable();
                dpu_ids.dedup();
                // `record_dpu_network_status` and queued controller writes can
                // hold a DPU row before they update its host or interfaces.
                // See api-db/LOCK_ORDERING.md for the rejection/wait protocol.
                let contended = dpu_ids
                    .iter()
                    .find(|id| !admission.locked_dpu_ids.contains(id))
                    .copied();
                if let Some(dpu_id) = contended {
                    txn.rollback().await?;
                    session.return_to_pool();
                    db::machine::wait_for_machine_no_key_update(api.pg_pool(), &dpu_id.into())
                        .await?;
                    continue;
                }
                let mut machine_ids = dpu_ids.into_iter().map(MachineId::from).collect::<Vec<_>>();
                machine_ids.push(machine_id.into());
                db::machine::modify_dpf_state(&mut txn, &machine_ids, request.dpf_enabled).await?;
                txn.commit().await?;
                session.return_to_pool();
                return Ok(Response::new(()));
            }
        })
        .await
}

// Since this function sends only a bool with ids, we might not need pagination for this.
pub(crate) async fn get_dpf_state(
    api: &Api,
    request: Request<rpc::GetDpfStateRequest>,
) -> Result<Response<rpc::DpfStateResponse>, Status> {
    log_request_data(&request);
    let request = request.get_ref();

    for machine_id in &request.machine_ids {
        if machine_id.machine_type().is_dpu() {
            return Err(
                CarbideError::InvalidArgument("only host id is expected!!".to_string()).into(),
            );
        }
    }

    let mut txn = api.txn_begin().await?;
    let filter = if request.machine_ids.is_empty() {
        ObjectFilter::All
    } else {
        ObjectFilter::List(&request.machine_ids)
    };

    let dpf_states = db::machine::find(&mut txn, filter, MachineSearchConfig::default()).await?;
    txn.commit().await?;

    Ok(Response::new(rpc::DpfStateResponse {
        dpf_states: dpf_states
            .into_iter()
            .map(|machine| machine.into())
            .collect(),
    }))
}

pub(crate) async fn get_dpf_host_snapshot(
    api: &Api,
    request: Request<rpc::GetDpfHostSnapshotRequest>,
) -> Result<Response<rpc::DpfHostSnapshotResponse>, Status> {
    log_request_data(&request);
    let request = request.get_ref();
    let machine_id: HostMachineId = convert_and_log_machine_id(request.host_machine_id.as_ref())?;

    let Some(ops) = api.dpf_sdk.as_ref() else {
        return Err(CarbideError::InvalidArgument(
            "DPF is not enabled on this carbide instance".to_string(),
        )
        .into());
    };

    let mut txn = api.txn_begin().await?;
    let machine = find_one(&mut txn, &machine_id, MachineSearchConfig::default())
        .await?
        .ok_or_else(|| CarbideError::NotFoundError {
            kind: "machine",
            id: machine_id.to_string(),
        })?;
    txn.commit().await?;

    let host_dpf_id = machine.dpf_id().ok_or_else(|| {
        CarbideError::InvalidArgument(format!(
            "host {machine_id} has no BMC MAC; cannot derive DPF node name"
        ))
    })?;
    let node_name = dpu_node_cr_name(&host_dpf_id);

    let snapshot = ops
        .snapshot_host(&node_name)
        .await
        .map_err(CarbideError::DpfError)?;

    let json_payload = serde_json::to_string_pretty(&snapshot).map_err(|e| {
        CarbideError::internal(format!("failed to serialize DPF host snapshot: {e}"))
    })?;

    Ok(Response::new(rpc::DpfHostSnapshotResponse { json_payload }))
}

pub(crate) async fn get_dpf_service_versions(
    api: &Api,
    request: Request<rpc::GetDpfServiceVersionsRequest>,
) -> Result<Response<rpc::DpfServiceVersionsResponse>, Status> {
    log_request_data(&request);

    let cfg = &api.runtime_config.dpf.services;
    let configured = [
        &cfg.dts,
        &cfg.doca_hbn,
        &cfg.dpu_agent,
        &cfg.dhcp_server,
        &cfg.fmds,
        &cfg.otel,
    ];

    let live = if let Some(ops) = api.dpf_sdk.as_ref() {
        ops.list_service_template_versions()
            .await
            .map_err(CarbideError::DpfError)?
    } else {
        Vec::new()
    };

    let services = configured
        .iter()
        .map(|svc_cfg| {
            let matched = live
                .iter()
                .find(|t| t.deployment_service_name == svc_cfg.name);
            rpc::DpfServiceVersion {
                service: svc_cfg.name.clone(),
                config_helm_version: svc_cfg.helm_version.clone(),
                config_docker_image_tag: svc_cfg.docker_image_tag.clone(),
                live_helm_version: matched.map(|t| t.helm_version.clone()).unwrap_or_default(),
                live_docker_image_tag: matched
                    .map(|t| t.docker_image_tag.clone())
                    .unwrap_or_default(),
            }
        })
        .collect();

    Ok(Response::new(rpc::DpfServiceVersionsResponse { services }))
}

#[cfg(test)]
mod tests {
    use carbide_test_support::value_scenarios;
    use carbide_uuid::machine::{HostMachineId, MachineId, MachineIdSource, MachineType};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn dpf_preparation_deadline_uses_the_finite_caller_budget() {
        value_scenarios!(
            run = |header: Option<&str>| {
                let mut request = tonic::Request::new(());
                if let Some(header) = header {
                    request.metadata_mut().insert("grpc-timeout", header.parse().unwrap());
                }
                DpfPreparationDeadline::from_request(&request)
                    .deadline.duration_since(tokio::time::Instant::now())
            };
            "finite fallback and cap" {
                None => Duration::from_secs(300),
                Some("600S") => Duration::from_secs(300),
            }
            "valid shorter and maximum parsed values" {
                Some("99999999H") => Duration::from_secs(300),
                Some("1S") => Duration::from_secs(1),
                Some("0n") => Duration::ZERO,
            }
            "malformed metadata keeps the finite cap" {
                Some("S") => Duration::from_secs(300),
            }
        );
    }

    // Use the listener's receipt and admission layers with Tonic's real unary
    // decoder. Raw HTTP/2 trailers must not replace the initial timeout, and
    // malformed zero budgets must not become admission deadlines.
    #[tokio::test]
    async fn dpf_timeout_uses_initial_http2_headers() -> Result<(), Box<dyn std::error::Error>> {
        use std::collections::VecDeque;
        use std::convert::Infallible;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        use axum::Router;
        use hyper::body::{Body as _, Bytes, Frame};
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use prost::Message as _;
        use tokio::task::JoinSet;
        use tokio_util::sync::CancellationToken;

        struct WireBody(VecDeque<Frame<Bytes>>);

        impl hyper::body::Body for WireBody {
            type Data = Bytes;
            type Error = Infallible;

            fn poll_frame(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
                Poll::Ready(self.0.pop_front().map(Ok))
            }
        }

        struct DeadlineProbe;

        impl tonic::server::UnaryService<rpc::ModifyDpfStateRequest> for DeadlineProbe {
            type Response = ();
            type Future = Pin<Box<dyn Future<Output = Result<Response<()>, Status>> + Send>>;

            fn call(&mut self, request: Request<rpc::ModifyDpfStateRequest>) -> Self::Future {
                Box::pin(async move {
                    let received = request
                        .extensions()
                        .get::<RequestReceivedAt>()
                        .expect("listener captured initial headers");
                    if received.grpc_timeout == Some(Duration::ZERO) {
                        assert_eq!(request.metadata().get("grpc-timeout").unwrap(), "300S");
                    }
                    DpfPreparationDeadline::from_request(&request)
                        .bound(|| std::future::ready(Err(Status::not_found("timeout probe"))))
                        .await
                })
            }
        }

        async fn decode(
            request: axum::extract::Request,
        ) -> axum::http::Response<tonic::body::Body> {
            tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                .unary(DeadlineProbe, request)
                .await
        }

        // Admission owns its own outer deadline. Its disabled case proves the
        // handler independently preserves the initial header through decoding.
        for (name, enabled, header, trailer, expected) in [
            (
                "initial header without admission",
                false,
                "0n",
                Some("300S"),
                "4",
            ),
            (
                "initial header with admission",
                true,
                "0n",
                Some("300S"),
                "4",
            ),
            ("signed malformed header", true, "+0S", None, "5"),
            ("overlong malformed header", true, "000000000S", None, "5"),
        ] {
            let cancel = CancellationToken::new();
            let mut tasks = JoinSet::new();
            let mut router = Router::new().route(
                ::rpc::service_path!("ModifyDPFState"),
                axum::routing::post(decode),
            );
            if enabled {
                let meter = carbide_utils::test_support::test_meter::TestMeter::default();
                let config = crate::cfg::file::ApiAdmissionControlConfig {
                    enabled: true,
                    max_work_in_flight: 1,
                    max_pending: 1,
                    max_work_in_flight_per_client: 1,
                    max_pending_per_client: 1,
                    pending_timeout: Duration::from_secs(5),
                    max_execution_time: Duration::from_secs(120),
                    client_idle_timeout: Duration::from_secs(60),
                    service_limits: Default::default(),
                };
                let control = crate::admission::ApiAdmissionControl::from_config(
                    &config,
                    &meter.meter(),
                    carbide_instrument::Instrumentation::new(meter.meter()),
                    cancel.clone(),
                    &mut tasks,
                )?
                .expect("enabled admission");
                router = router.layer(axum::middleware::from_fn_with_state(
                    control,
                    crate::admission::enforce_grpc,
                ));
            }
            let router = router.layer(crate::api::grpc_receipt_layer());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let shutdown = cancel.clone();
            tasks.spawn(async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(shutdown.cancelled_owned())
                    .await
                    .expect("HTTP/2 test listener");
            });
            let connection = tokio::net::TcpStream::connect(address).await?;
            let (mut sender, connection) = hyper::client::conn::http2::handshake(
                TokioExecutor::new(),
                TokioIo::new(connection),
            )
            .await?;
            tasks.spawn(async move { connection.await.expect("HTTP/2 test client") });
            let payload = rpc::ModifyDpfStateRequest::default().encode_to_vec();
            let mut frame = vec![0];
            frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            frame.extend_from_slice(&payload);
            let mut frames = VecDeque::from([Frame::data(Bytes::from(frame))]);
            if let Some(trailer) = trailer {
                let mut trailers = axum::http::HeaderMap::new();
                trailers.insert("grpc-timeout", trailer.parse()?);
                frames.push_back(Frame::trailers(trailers));
            }
            let request = axum::http::Request::builder()
                .method("POST")
                .version(axum::http::Version::HTTP_2)
                .uri(format!(
                    "http://{address}{}",
                    ::rpc::service_path!("ModifyDPFState")
                ))
                .header("content-type", "application/grpc")
                .header("te", "trailers")
                .header("grpc-timeout", header)
                .body(WireBody(frames))?;
            let mut response =
                tokio::time::timeout(Duration::from_secs(5), sender.send_request(request))
                    .await??;
            let metadata = tokio::time::timeout(Duration::from_secs(5), async {
                let mut metadata = response.headers().clone();
                while let Some(frame) =
                    std::future::poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx)).await
                {
                    if let Ok(trailers) = frame?.into_trailers() {
                        metadata.extend(trailers);
                    }
                }
                Ok::<_, hyper::Error>(metadata)
            })
            .await??;
            assert_eq!(metadata.get("grpc-status").unwrap(), expected, "{name}");
            if !enabled {
                assert_eq!(metadata.get("nico-error-code").unwrap(), "NICO-API-504");
            }
            drop(sender);
            cancel.cancel();
            tokio::time::timeout(Duration::from_secs(5), async {
                while let Some(result) = tasks.join_next().await {
                    result.expect("joined HTTP/2 test task");
                }
            })
            .await?;
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn expired_dpf_preparation_does_not_construct_ready_work() {
        for (scenario, budget, elapsed) in [
            ("zero", Duration::ZERO, Duration::ZERO),
            (
                "expired",
                Duration::from_micros(1500),
                Duration::from_micros(1750),
            ),
        ] {
            let mut request = tonic::Request::new(());
            request.set_timeout(budget);
            let deadline = DpfPreparationDeadline::from_request(&request);
            tokio::time::advance(elapsed).await;
            let constructed = std::cell::Cell::new(false);
            let error = deadline
                .bound(|| {
                    constructed.set(true);
                    std::future::ready(Ok::<_, tonic::Status>(()))
                })
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::DeadlineExceeded, "{scenario}");
            assert!(
                !constructed.get(),
                "{scenario}: expired work was constructed"
            );
        }
    }

    // Make both branches ready after expiry. A successful result must not hide
    // the deadline or replace its typed error metadata.
    #[tokio::test(start_paused = true)]
    async fn dpf_preparation_expiry_precedes_an_already_ready_result() {
        // Every selection must prefer expiry, including repeated ready ties.
        for _ in 0..32 {
            let mut request = tonic::Request::new(());
            request.set_timeout(Duration::from_secs(1));
            let deadline = DpfPreparationDeadline::from_request(&request);
            let (ready, result) = tokio::sync::oneshot::channel();
            let work = deadline.bound(|| async move {
                result.await.unwrap();
                Ok(())
            });
            tokio::pin!(work);
            assert!(futures_util::poll!(&mut work).is_pending());
            tokio::time::advance(Duration::from_secs(1) + Duration::from_millis(1)).await;
            ready.send(()).unwrap();
            let error = work.await.unwrap_err();
            assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
            assert_eq!(
                error.metadata().get("nico-error-code").unwrap(),
                "NICO-API-504"
            );
        }
    }

    // Advance the exact clock in the final work poll, after select polled its
    // timer. A ready result must still lose to the elapsed request deadline.
    #[tokio::test(start_paused = true)]
    async fn dpf_preparation_checks_the_clock_after_ready_work() {
        let mut request = tonic::Request::new(());
        request.set_timeout(Duration::from_micros(1));
        let deadline = DpfPreparationDeadline::from_request(&request);
        let error = deadline
            .bound(|| async {
                let advance = tokio::time::advance(Duration::from_micros(2));
                tokio::pin!(advance);
                let _ = futures_util::poll!(&mut advance);
                Ok(())
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    }

    // A queued request must not receive a fresh metadata budget at handler entry.
    #[tokio::test(start_paused = true)]
    async fn dpf_preparation_deadline_includes_time_before_handler_entry() {
        let mut request = tonic::Request::new(());
        request.set_timeout(Duration::from_secs(1));
        request.extensions_mut().insert(RequestReceivedAt {
            instant: tokio::time::Instant::now(),
            grpc_timeout: Some(Duration::from_secs(1)),
        });
        tokio::time::advance(Duration::from_secs(2)).await;
        let constructed = std::cell::Cell::new(false);
        let error = DpfPreparationDeadline::from_request(&request)
            .bound(|| {
                constructed.set(true);
                std::future::ready(Ok::<_, tonic::Status>(()))
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert!(!constructed.get(), "expired queued work was constructed");
    }

    #[crate::sqlx_test]
    async fn dpf_host_admission_rejects_missing_host(
        pool: sqlx::PgPool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let host_id = HostMachineId::try_from(MachineId::new(
            MachineIdSource::ProductBoardChassisSerial,
            [53; 32],
            MachineType::Host,
        ))?;
        let mut attempt = pool.begin().await?;
        let rejected = lock_host_for_dpf_update(attempt.as_mut(), &host_id).await;
        attempt.rollback().await?;
        assert!(matches!(
            rejected,
            Err(CarbideError::NotFoundError { kind: "snapshot", id }) if id == host_id.to_string()
        ));
        Ok(())
    }
}
