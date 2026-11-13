// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ordinary reboot requests cross the RPC, durable ownership and existing controller boundary.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bmc_backend::BmcConnector;
use carbide_machine_controller::metrics::MachineMetrics;
use carbide_redfish::libredfish::test_support::{RedfishSim, RedfishSimAction};
use carbide_redfish::libredfish::{RedfishAuth, RedfishClientCreationError, RedfishClientPool};
use carbide_secrets::credentials::CredentialReader;
use carbide_uuid::instance::InstanceId;
use carbide_uuid::machine::HostMachineId;
use chrono::{DateTime, Utc};
use libredfish::model::sel::LogEntry;
use libredfish::model::service_root::RedfishVendor;
use libredfish::{PowerState, Redfish, RedfishError, RedfishFuture, SystemPowerControl};
use model::hardware_dispatch::{HardwareDispatch, HardwareDispatchOwner};
use model::machine::{
    FactoryResetBmcState, FailureCause, FailureDetails, FailureSource, HostMachine,
    HostPlatformConfigurationState, InstanceState, ManagedHostState,
};
use rpc::forge::forge_server::Forge;
use state_controller::db_write_batch::DbWriteBatch;
use state_controller::state_handler::{
    StateHandler, StateHandlerContext, StateHandlerError, StateHandlerOutcome,
};
use tokio::sync::oneshot;
use tonic::Request;

use crate::tests::common::api_fixtures::{
    TestEnv, TestEnvOverrides, TestManagedHost, create_managed_host,
    create_test_env as create_default_test_env, create_test_env_with_overrides, get_config,
};
use crate::tests::instance_ipxe_behaviors::create_instance;

/// Opt in only this suite's queued requests; production and unrelated tests stay disabled.
async fn create_test_env(pool: sqlx::PgPool) -> TestEnv {
    let mut config = get_config();
    config
        .machine_state_controller
        .instance_reboot_queue_enabled = true;
    create_test_env_with_overrides(pool, TestEnvOverrides::with_config(config)).await
}

/// The default keeps the existing immediate BMC response and writes no queued owner.
#[crate::sqlx_test]
async fn instance_reboot_queue_is_disabled_by_default(pool: sqlx::PgPool) {
    let env = create_default_test_env(pool).await;
    assert!(
        !env.api
            .runtime_config
            .machine_state_controller
            .instance_reboot_queue_enabled
    );
    let segment = env.create_vpc_and_tenant_segment().await;
    let host = create_managed_host(&env).await;
    let instance = create_instance(&env, &host, false, segment).await;
    let before = machine(&env, *host.id).await;
    let actions = env.redfish_sim.timepoint();
    env.api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap();
    let after = machine(&env, *host.id).await;
    assert!(after.status.hardware_dispatch.is_none());
    assert_eq!(
        after.status.last_reboot_requested,
        before.status.last_reboot_requested
    );
    assert_eq!(
        env.redfish_sim.actions_since(&actions).all_hosts(),
        vec![RedfishSimAction::Power(SystemPowerControl::ForceRestart)]
    );

    // With the queue disabled, a custom-PXE request before Ready still uses the
    // original immediate BMC path rather than setting the controller request flag.
    let mut txn = env.pool.begin().await.unwrap();
    db::machine::update_state(
        txn.as_mut(),
        &host.id,
        &ManagedHostState::Assigned {
            instance_state: InstanceState::WaitingForDpusToUp,
        },
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    let actions = env.redfish_sim.timepoint();
    let mut reboot = request(instance.id).into_inner();
    reboot.boot_with_custom_ipxe = true;
    env.api
        .invoke_instance_power(Request::new(reboot))
        .await
        .unwrap();
    assert!(
        machine(&env, *host.id)
            .await
            .status
            .hardware_dispatch
            .is_none()
    );
    let mut txn = env.pool.begin().await.unwrap();
    assert!(
        !instance
            .db_instance(&mut txn)
            .await
            .custom_pxe_reboot_requested
    );
    txn.commit().await.unwrap();
    assert_eq!(
        env.redfish_sim.actions_since(&actions).all_hosts(),
        vec![RedfishSimAction::Power(SystemPowerControl::ForceRestart)]
    );
    // The disabled rollout preserves the old API even with unresolved mlx* work.
    let mut txn = env.pool.begin().await.unwrap();
    db::hardware_dispatch::begin(
        &mut txn,
        uuid::Uuid::new_v4(),
        &HardwareDispatchOwner::MlxAdmin {
            machine_id: host.id.into(),
            command: model::hardware_dispatch::MlxAdminCommand::ConfigSet,
        },
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    let mlx = dispatch(&env, *host.id).await;
    let actions = env.redfish_sim.timepoint();
    env.api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap();
    assert_eq!(dispatch(&env, *host.id).await, mlx);
    assert_eq!(
        env.redfish_sim.actions_since(&actions).all_hosts(),
        vec![RedfishSimAction::Power(SystemPowerControl::ForceRestart)]
    );
    env.redfish_sim.set_create_client_error("BMC unavailable");
    assert!(
        env.api
            .invoke_instance_power(request(instance.id))
            .await
            .is_err()
    );
    assert_eq!(dispatch(&env, *host.id).await, mlx);
}

/// A terminal provisioning failure must reject a reboot the Ready handler cannot send.
#[crate::sqlx_test]
async fn instance_reboot_rejects_failed_provisioning(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    let host = create_managed_host(&env).await;
    let instance = create_instance(&env, &host, false, segment).await;
    let mut txn = env.pool.begin().await.unwrap();
    // Discovery failures have no automatic transition back to Ready.
    db::machine::update_state(
        txn.as_mut(),
        &host.id,
        &ManagedHostState::Assigned {
            instance_state: InstanceState::Failed {
                details: FailureDetails {
                    cause: FailureCause::Discovery {
                        err: "discovery failed".into(),
                    },
                    failed_at: Utc::now(),
                    source: FailureSource::StateMachine,
                },
                machine_id: host.id.into(),
            },
        },
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    let before = machine(&env, *host.id).await;
    let actions = env.redfish_sim.timepoint();

    let error = env
        .api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("instance provisioning has failed"));
    let after = machine(&env, *host.id).await;
    assert_eq!(after.state.value, before.state.value);
    assert_eq!(after.state.version, before.state.version);
    assert_eq!(
        after.status.hardware_dispatch,
        before.status.hardware_dispatch
    );
    assert_eq!(
        after.status.last_reboot_requested,
        before.status.last_reboot_requested
    );
    assert!(
        env.redfish_sim
            .actions_since(&actions)
            .all_hosts()
            .is_empty()
    );
    for state in [
        ManagedHostState::ForceDeletion,
        ManagedHostState::Reset {
            reset_state: model::machine::ResetState::DeletingInstance,
        },
    ] {
        let mut txn = env.pool.begin().await.unwrap();
        db::machine::update_state(&mut txn, &host.id, &state)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        let error = env
            .api
            .invoke_instance_power(request(instance.id))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        let after = machine(&env, *host.id).await;
        assert_eq!(after.state.value, state);
        assert!(after.status.hardware_dispatch.is_none());
        assert!(
            env.redfish_sim
                .actions_since(&actions)
                .all_hosts()
                .is_empty()
        );
    }
}

/// A later provisioning failure cancels unsent work but cannot establish an attempted result.
#[crate::sqlx_test]
async fn instance_reboot_cancels_only_unsent_work_when_provisioning_fails(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    for attempted in [false, true] {
        let host = create_managed_host(&env).await;
        let instance = create_instance(&env, &host, false, segment).await;
        let mut txn = env.pool.begin().await.unwrap();
        db::machine::update_state(
            txn.as_mut(),
            &host.id,
            &ManagedHostState::Assigned {
                instance_state: InstanceState::WaitingForNetworkConfig,
            },
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
        env.api
            .invoke_instance_power(request(instance.id))
            .await
            .unwrap();
        let queued = dispatch(&env, *host.id).await;

        let mut txn = env.pool.begin().await.unwrap();
        // The attempted control models a sender that stopped after its durable checkpoint.
        let saved = if attempted {
            db::hardware_dispatch::record_instance_reboot_attempt(txn.as_mut(), *host.id, &queued)
                .await
                .unwrap()
                .unwrap()
        } else {
            queued
        };
        db::machine::update_state(
            txn.as_mut(),
            &host.id,
            &ManagedHostState::Assigned {
                instance_state: InstanceState::Failed {
                    details: FailureDetails {
                        cause: FailureCause::Discovery {
                            err: "discovery failed after queueing".into(),
                        },
                        failed_at: Utc::now(),
                        source: FailureSource::StateMachine,
                    },
                    machine_id: host.id.into(),
                },
            },
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
        let before = machine(&env, *host.id).await;
        let actions = env.redfish_sim.timepoint();

        env.run_machine_state_controller_iteration_without_requeue()
            .await;

        // Reload the exact request: unchanged visible state alone would miss this cleanup.
        let after = machine(&env, *host.id).await;
        let retained = after.status.hardware_dispatch.as_ref().unwrap();
        assert_eq!(retained.token, saved.token);
        assert_eq!(retained.owner, saved.owner);
        assert_eq!(retained.started_at, saved.started_at);
        assert_eq!(retained.is_pending(), attempted);
        assert_eq!(after.state.value, before.state.value);
        assert_eq!(after.state.version, before.state.version);
        assert_eq!(
            after.status.last_reboot_requested,
            before.status.last_reboot_requested
        );
        assert!(
            env.redfish_sim
                .actions_since(&actions)
                .all_hosts()
                .is_empty()
        );

        // Unsent work no longer masks the existing Failed-state API diagnostic.
        let error = env
            .api
            .invoke_instance_power(request(instance.id))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains(if attempted {
            "unfinished hardware request"
        } else {
            "instance provisioning has failed"
        }));
    }
}

/// Controls BMC preparation and responses. An absent gate proceeds immediately;
/// an absent log models a BMC that cannot report restart events.
#[derive(Default)]
struct Evidence {
    preparation: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    before_power: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    after_power: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    lose_reply: AtomicBool,
    calls: AtomicUsize,
    nonretry_clients: AtomicUsize,
    log: Mutex<Option<Vec<LogEntry>>>,
    log_requests: Mutex<Vec<Option<DateTime<Utc>>>>,
    power_states: Mutex<VecDeque<PowerState>>,
    ipmi: Option<Arc<dyn carbide_ipmi::IPMITool>>,
    connector: Option<BmcConnector>,
}

/// Uses the existing BMC simulator, with optional preparation and post-reset faults.
struct Pool {
    sim: Arc<RedfishSim>,
    evidence: Arc<Evidence>,
}

#[async_trait]
impl RedfishClientPool for Pool {
    async fn create_client(
        &self,
        host: &str,
        port: Option<u16>,
        auth: RedfishAuth,
        vendor: Option<RedfishVendor>,
    ) -> Result<Box<dyn Redfish>, RedfishClientCreationError> {
        Ok(Box::new(Client {
            inner: self.sim.create_client(host, port, auth, vendor).await?,
            evidence: self.evidence.clone(),
        }))
    }

    // Notify the test once preparation begins, then wait for its database change.
    // This gate runs before returning a usable client and holds no database transaction.
    async fn create_client_without_mutation_retries(
        &self,
        host: &str,
        port: Option<u16>,
        auth: RedfishAuth,
        vendor: Option<RedfishVendor>,
    ) -> Result<Box<dyn Redfish>, RedfishClientCreationError> {
        self.evidence
            .nonretry_clients
            .fetch_add(1, Ordering::SeqCst);
        let gate = self.evidence.preparation.lock().unwrap().take();
        if let Some((entered, release)) = gate {
            entered.send(()).unwrap();
            release.await.unwrap();
        }
        Ok(Box::new(Client {
            inner: self.sim.create_client(host, port, auth, vendor).await?,
            evidence: self.evidence.clone(),
        }))
    }

    fn credential_reader(&self) -> &dyn CredentialReader {
        self.sim.credential_reader()
    }
}

/// Applies simulator power effects before optionally losing the reply, so recovery
/// exercises an uncertain send rather than a command that never reached hardware.
struct Client {
    inner: Box<dyn Redfish>,
    evidence: Arc<Evidence>,
}

impl Redfish for Client {
    fn std_redfish(&self) -> &libredfish::standard::RedfishStandard {
        self.inner.std_redfish()
    }

    fn get_power_state(&self) -> RedfishFuture<'_, Result<PowerState, RedfishError>> {
        let next = self.evidence.power_states.lock().unwrap().pop_front();
        match next {
            Some(state) => Box::pin(async move { Ok(state) }),
            None => self.inner.get_power_state(),
        }
    }

    fn power(&self, action: SystemPowerControl) -> RedfishFuture<'_, Result<(), RedfishError>> {
        Box::pin(async move {
            self.evidence.calls.fetch_add(1, Ordering::SeqCst);
            let gate = self.evidence.before_power.lock().unwrap().take();
            if let Some((entered, release)) = gate {
                entered.send(()).unwrap();
                release.await.unwrap();
            }
            self.inner.power(action).await?;
            let gate = self.evidence.after_power.lock().unwrap().take();
            if let Some((entered, release)) = gate {
                entered.send(()).unwrap();
                release.await.unwrap();
            }
            if self.evidence.lose_reply.swap(false, Ordering::SeqCst) {
                return Err(RedfishError::GenericError {
                    error: "lost reply after accepted restart".into(),
                });
            }
            Ok(())
        })
    }

    fn get_bmc_event_log(
        &self,
        from: Option<DateTime<Utc>>,
    ) -> RedfishFuture<'_, Result<Vec<LogEntry>, RedfishError>> {
        Box::pin(async move {
            self.evidence.log_requests.lock().unwrap().push(from);
            self.evidence
                .log
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| RedfishError::GenericError {
                    error: "event log unsupported".into(),
                })
        })
    }
}

/// An initially off host is powered on; a later skipped restart cannot settle the request.
#[crate::sqlx_test]
async fn instance_reboot_distinguishes_power_on_from_a_skipped_restart(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    for (name, observations, expected) in [
        (
            "initially off",
            vec![PowerState::Off],
            Some(SystemPowerControl::On),
        ),
        (
            "powered off between reads",
            vec![PowerState::On, PowerState::Off],
            None,
        ),
    ] {
        let host = create_managed_host(&env).await;
        let instance = create_instance(&env, &host, false, segment).await;
        let before = machine(&env, *host.id).await;
        if expected.is_some() {
            env.redfish_sim.set_power_state(
                &before.status.bmc_info.ip.unwrap().to_string(),
                PowerState::Off,
            );
        }
        env.api
            .invoke_instance_power(request(instance.id))
            .await
            .unwrap();
        let evidence = Arc::new(Evidence {
            power_states: Mutex::new(observations.into()),
            ..Default::default()
        });
        let actions = env.redfish_sim.timepoint();
        iterate(
            &env,
            &host,
            Arc::new(Pool {
                sim: env.redfish_sim.clone(),
                evidence,
            }),
        )
        .await
        .unwrap();
        let after = machine(&env, *host.id).await;
        assert_eq!(
            after
                .status
                .hardware_dispatch
                .as_ref()
                .unwrap()
                .is_pending(),
            expected.is_none(),
            "{name}"
        );
        assert_eq!(
            env.redfish_sim.actions_since(&actions).all_hosts(),
            expected
                .into_iter()
                .map(RedfishSimAction::Power)
                .collect::<Vec<_>>(),
            "{name}"
        );
        if after
            .status
            .hardware_dispatch
            .as_ref()
            .unwrap()
            .is_pending()
        {
            assert_eq!(
                after.status.last_reboot_requested, before.status.last_reboot_requested,
                "{name}: a skipped restart must not record BMC acceptance"
            );
            assert!(
                matches!(
                    after.status.hardware_dispatch.unwrap().owner,
                    HardwareDispatchOwner::InstanceReboot {
                        last_attempt_at: None,
                        ..
                    }
                ),
                "a proven non-send must remain cancellable as unsent work"
            );
        }
    }
}

fn request(id: InstanceId) -> Request<rpc::forge::InstancePowerRequest> {
    Request::new(rpc::forge::InstancePowerRequest {
        instance_id: Some(id),
        ..Default::default()
    })
}

async fn machine(env: &TestEnv, host: HostMachineId) -> HostMachine {
    db::machine::find_one(&env.pool, &host, Default::default())
        .await
        .unwrap()
        .unwrap()
}

async fn dispatch(env: &TestEnv, host: HostMachineId) -> HardwareDispatch {
    machine(env, host)
        .await
        .status
        .hardware_dispatch
        .expect("saved reboot request")
}

/// Commit the production handler's writes for a pass that must not transition state.
/// Transition proofs use the complete processor instead of this transport-fault fixture.
async fn iterate(
    env: &TestEnv,
    host: &TestManagedHost,
    pool: Arc<Pool>,
) -> Result<(), StateHandlerError> {
    let mut txn = env.pool.begin().await.unwrap();
    let mut snapshot = host.snapshot(&mut txn).await;
    txn.commit().await.unwrap();
    let state = snapshot.managed_state.clone();
    let id = snapshot.host_snapshot.id;
    let mut services = env.machine_state_handler_services();
    // Disabling new API queue entries must not strand already-saved controller work.
    let mut site_config = env.api.runtime_config.machine_state_handler_site_config();
    site_config
        .machine_state_controller
        .instance_reboot_queue_enabled = false;
    services.site_config = Arc::new(site_config);
    if let Some(ipmi) = &pool.evidence.ipmi {
        services.ipmi_tool = ipmi.clone();
    }
    services.bmc_connector = pool
        .evidence
        .connector
        .clone()
        .unwrap_or_else(|| BmcConnector::Libredfish(pool.clone()));
    services.redfish_client_pool = pool;
    let mut metrics = MachineMetrics::default();
    let mut writes = DbWriteBatch::new();
    let mut ctx = StateHandlerContext {
        services: &mut services,
        metrics: &mut metrics,
        pending_db_writes: &mut writes,
    };
    let mut outcome = env
        .machine_state_handler
        .handle_object_state(&id, &mut snapshot, &state, &mut ctx)
        .await?;
    assert!(
        matches!(
            outcome,
            StateHandlerOutcome::DoNothing { .. } | StateHandlerOutcome::Wait { .. }
        ),
        "the reboot-only fixture must not discard a controller transition"
    );
    let mut txn = match outcome.take_transaction() {
        Some(txn) => txn,
        None => env.pool.begin().await.unwrap(),
    };
    writes.apply_all(&mut txn).await?;
    txn.commit().await.unwrap();
    Ok(())
}

/// Cancellation after the effect keeps the intent committed before the first send.
#[crate::sqlx_test]
async fn instance_reboot_cancellation_retains_the_committed_attempt(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    let host = create_managed_host(&env).await;
    let instance = create_instance(&env, &host, false, segment).await;
    env.api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap();
    let (entered, before) = oneshot::channel();
    let (send, allow_send) = oneshot::channel();
    let (applied, after) = oneshot::channel();
    let (reply, withhold_reply) = oneshot::channel();
    let evidence = Arc::new(Evidence {
        before_power: Mutex::new(Some((entered, allow_send))),
        after_power: Mutex::new(Some((applied, withhold_reply))),
        ..Default::default()
    });
    let transport = Arc::new(Pool {
        sim: env.redfish_sim.clone(),
        evidence: evidence.clone(),
    });
    let actions = env.redfish_sim.timepoint();
    let mut pass = Box::pin(iterate(&env, &host, transport.clone()));
    let observe = async {
        before.await.unwrap();
        // Another connection sees the committed attempt and can acquire both rows
        // before the simulated hardware action is allowed to start.
        let mut txn = env.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM instances WHERE id = $1 FOR UPDATE NOWAIT")
            .bind(instance.id)
            .fetch_one(txn.as_mut())
            .await
            .unwrap();
        sqlx::query("SELECT id FROM machines WHERE id = $1 FOR NO KEY UPDATE NOWAIT")
            .bind(*host.id)
            .fetch_one(txn.as_mut())
            .await
            .unwrap();
        let saved = db::machine::find_one(txn.as_mut(), &host.id, Default::default())
            .await
            .unwrap()
            .unwrap()
            .status
            .hardware_dispatch
            .unwrap();
        assert!(matches!(
            saved.owner,
            HardwareDispatchOwner::InstanceReboot {
                last_attempt_at: Some(_),
                ..
            }
        ));
        txn.commit().await.unwrap();
        send.send(()).unwrap();
        after.await.unwrap();
        saved
    };
    let attempted = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::select! {
            saved = observe => saved,
            result = &mut pass => panic!("controller finished while reply was withheld: {result:?}"),
        }
    }).await.unwrap();
    drop(pass);
    drop(reply);
    assert_eq!(dispatch(&env, *host.id).await, attempted);
    assert!(attempted.is_pending());
    assert_eq!(
        env.redfish_sim.actions_since(&actions).all_hosts(),
        vec![RedfishSimAction::Power(SystemPowerControl::ForceRestart)]
    );
    iterate(&env, &host, transport).await.unwrap();
    assert_eq!(
        evidence.calls.load(Ordering::SeqCst),
        1,
        "the next pass must retain the retry wait after cancellation"
    );
    assert_eq!(dispatch(&env, *host.id).await, attempted);
}

/// Each backend retains its transport route while the controller owns uncertain retries.
#[crate::sqlx_test]
async fn instance_reboot_uses_one_backend_attempt(pool: sqlx::PgPool) {
    use std::net::SocketAddr;

    use axum::body::{Body, Bytes};
    use axum::extract::State;
    use axum::http::Response;
    use carbide_secrets::credentials::CredentialKey;

    async fn lost_reset_reply(State(calls): State<Arc<AtomicUsize>>) -> Response<Body> {
        // The POST reached the endpoint, but its truncated response leaves delivery uncertain.
        calls.fetch_add(1, Ordering::SeqCst);
        Response::builder()
            .header("content-length", "10")
            .body(Body::from_stream(futures_util::stream::once(async {
                Err::<Bytes, _>(std::io::Error::other("lost reset response"))
            })))
            .unwrap()
    }

    #[derive(Default)]
    struct LostReplyIpmi {
        calls: AtomicUsize,
        targets: Mutex<Vec<(SocketAddr, mac_address::MacAddress)>>,
    }
    #[async_trait]
    impl carbide_ipmi::IPMITool for LostReplyIpmi {
        async fn restart_once(
            &self,
            address: SocketAddr,
            key: &CredentialKey,
        ) -> Result<(), eyre::Report> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let CredentialKey::BmcCredentials {
                credential_type:
                    carbide_secrets::credentials::BmcCredentialType::BmcRoot { bmc_mac_address },
            } = key
            else {
                panic!("expected the captured BMC credential key");
            };
            self.targets
                .lock()
                .unwrap()
                .push((address, *bmc_mac_address));
            Err(eyre::eyre!("IPMI attempt returned an uncertain result"))
        }
        async fn restart(
            &self,
            _: SocketAddr,
            _: bool,
            _: &CredentialKey,
        ) -> Result<(), eyre::Report> {
            panic!("a queued reboot must not select the retrying IPMI method")
        }
        async fn bmc_cold_reset(
            &self,
            _: SocketAddr,
            _: &CredentialKey,
        ) -> Result<(), eyre::Report> {
            panic!("a queued reboot must not reset the BMC")
        }
    }
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    for (name, drivers, use_ipmi) in [
        ("libredfish IPMI workaround", false, true),
        ("Drivers IPMI workaround", true, true),
        ("Drivers HTTP lost response", true, false),
    ] {
        let host = create_managed_host(&env).await;
        let instance = create_instance(&env, &host, false, segment).await;
        let address = machine(&env, *host.id).await.status.bmc_info.ip.unwrap();
        let mut txn = env.pool.begin().await.unwrap();
        let endpoint = db::explored_endpoints::find_by_ips(txn.as_mut(), vec![address])
            .await
            .unwrap()
            .pop()
            .unwrap();
        let mut report = endpoint.report;
        report.vendor = Some(bmc_vendor::BMCVendor::Lenovo);
        let model = if use_ipmi { "SR650 V4" } else { "SR670" };
        report.model = Some(model.into());
        report
            .service_root
            .get_or_insert_with(Default::default)
            .vendor = Some("Lenovo".into());
        report.systems = vec![model::site_explorer::ComputerSystem {
            id: "1".into(),
            model: Some(model.into()),
            ..Default::default()
        }];
        assert_eq!(
            db::explored_endpoints::try_update(
                address,
                endpoint.report_version,
                &report,
                endpoint.waiting_for_explorer_refresh,
                txn.as_mut()
            )
            .await
            .unwrap(),
            db::ConditionalWrite::Applied(())
        );
        txn.commit().await.unwrap();
        env.api
            .invoke_instance_power(request(instance.id))
            .await
            .unwrap();
        let ipmi = Arc::new(LostReplyIpmi::default());
        let http_calls = Arc::new(AtomicUsize::new(0));
        let (connector, mock) = if drivers {
            // Reuse real discovery; the saved model selects IPMI or standard Redfish.
            let router = axum::Router::new()
                .route(
                    "/redfish/v1/Systems/1/Actions/ComputerSystem.Reset",
                    axum::routing::post(lost_reset_reply),
                )
                .with_state(http_calls.clone())
                .fallback_service(bmc_mock::test_support::lenovo_thinksystem_sr670_router());
            let (server, url) = bmc_mock::test_support::serve_https("queued-driver-reboot", router);
            let pool = carbide_redfish::nv_redfish::new_pool(Arc::new(
                arc_swap::ArcSwap::from_pointee(Some(carbide_utils::HostPortPair::HostAndPort(
                    "127.0.0.1".into(),
                    url.port().unwrap(),
                ))),
            ));
            let credentials = Arc::new(
                carbide_secrets::test_support::credentials::TestCredentialManager::new(
                    carbide_secrets::credentials::Credentials::UsernamePassword {
                        username: "root".into(),
                        password: "password".into(),
                    },
                ),
            );
            let connector = bmc_runtime::DriverConnector::new(
                bmc_runtime::DriverPools {
                    routed: pool.clone(),
                    direct: pool,
                },
                credentials,
                "queued-reboot-test",
                bmc_drivers::built_in_rules(),
                ipmi.clone(),
                carbide_instrument::Instrumentation::noop(),
            )
            .unwrap();
            (
                Some(BmcConnector::Drivers(Arc::new(connector))),
                Some(server),
            )
        } else {
            (None, None)
        };
        let transport = Arc::new(Pool {
            sim: env.redfish_sim.clone(),
            evidence: Arc::new(Evidence {
                ipmi: Some(ipmi.clone()),
                connector,
                ..Default::default()
            }),
        });
        let actions = env.redfish_sim.timepoint();
        assert!(iterate(&env, &host, transport.clone()).await.is_err());
        let attempted = dispatch(&env, *host.id).await;
        assert!(attempted.is_pending());
        iterate(&env, &host, transport.clone()).await.unwrap();
        assert_eq!(
            ipmi.calls.load(Ordering::SeqCst),
            usize::from(use_ipmi),
            "{name}"
        );
        assert_eq!(
            http_calls.load(Ordering::SeqCst),
            usize::from(!use_ipmi),
            "{name}"
        );
        if use_ipmi {
            let bmc_mac_address = machine(&env, *host.id).await.status.bmc_info.mac.unwrap();
            assert_eq!(
                *ipmi.targets.lock().unwrap(),
                vec![(SocketAddr::new(address, 623), bmc_mac_address)],
                "{name}: preserve the captured host and credential identity"
            );
        }
        assert_eq!(dispatch(&env, *host.id).await, attempted);
        assert!(
            env.redfish_sim
                .actions_since(&actions)
                .all_hosts()
                .is_empty()
        );
        if drivers && !use_ipmi {
            // The Drivers mutation backend still uses the controller's existing readonly
            // libredfish pool for supported restart observations.
            let mut retryable = attempted.clone();
            let HardwareDispatchOwner::InstanceReboot {
                last_attempt_at, ..
            } = &mut retryable.owner
            else {
                panic!("reboot owner")
            };
            *last_attempt_at = Some(Utc::now() - chrono::Duration::hours(2));
            sqlx::query("UPDATE machines SET hardware_dispatch = $2 WHERE id = $1")
                .bind(*host.id)
                .bind(sqlx::types::Json(retryable))
                .execute(&env.pool)
                .await
                .unwrap();
            *transport.evidence.log.lock().unwrap() = Some(vec![
                serde_json::from_value(serde_json::json!({
                    "@odata.id": "/reset", "@odata.type": "#LogEntry.v1_15_0.LogEntry",
                    "Created": Utc::now().to_rfc3339(), "EntryType": "Event", "Id": "reset",
                    "Message": "Server reset.", "Name": "Reset", "Severity": "OK"
                }))
                .unwrap(),
            ]);
            iterate(&env, &host, transport).await.unwrap();
            let settled = dispatch(&env, *host.id).await;
            assert_eq!(settled.token, attempted.token);
            assert!(!settled.is_pending());
            assert_eq!(http_calls.load(Ordering::SeqCst), 1);
        }
        if let Some(mut mock) = mock {
            mock.stop().await.unwrap();
        }
    }
}

/// Queued acceptance survives temporary BMC preparation failure and needs no tenant Scout.
#[crate::sqlx_test]
async fn instance_reboot_rpc_queues_and_controller_accepts_without_scout(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    let host = create_managed_host(&env).await;
    let instance = create_instance(&env, &host, false, segment).await;
    let before = machine(&env, *host.id).await;
    assert!(before.status.mlx_device_observation.is_none());
    let actions = env.redfish_sim.timepoint();
    env.redfish_sim
        .set_create_client_error("temporary credential failure");

    env.api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap();
    let queued = dispatch(&env, *host.id).await;
    assert!(queued.is_pending());
    assert!(matches!(
        queued.owner,
        HardwareDispatchOwner::InstanceReboot {
            last_attempt_at: None,
            ..
        }
    ));
    assert_eq!(
        machine(&env, *host.id).await.status.last_reboot_requested,
        before.status.last_reboot_requested
    );

    // A saved request excludes a competing reboot before any controller iteration.
    let error = env
        .api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(dispatch(&env, *host.id).await, queued);

    assert!(
        env.redfish_sim
            .actions_since(&actions)
            .all_hosts()
            .is_empty()
    );
    let preparations = env.redfish_sim.create_client_calls().len();
    env.run_machine_state_controller_iteration().await;
    assert!(env.redfish_sim.create_client_calls().len() > preparations);
    assert_eq!(
        dispatch(&env, *host.id).await,
        queued,
        "preparation failure must leave the request unsent"
    );
    assert!(
        matches!(
            &machine(&env, *host.id).await.controller_state_outcome,
            Some(model::controller_outcome::PersistentStateHandlerOutcome::Error { err, .. })
                if err.contains("temporary credential failure")
        ),
        "the controller must record the injected preparation failure before retrying it"
    );
    env.redfish_sim.clear_create_client_error();
    env.run_machine_state_controller_iteration().await;
    assert_eq!(
        env.redfish_sim.actions_since(&actions).all_hosts(),
        vec![RedfishSimAction::Power(SystemPowerControl::ForceRestart)]
    );
    let after = machine(&env, *host.id).await;
    let completed = after.status.hardware_dispatch.as_ref().unwrap();
    assert_eq!(completed.token, queued.token);
    assert!(!completed.is_pending());
    assert!(matches!(
        completed.owner,
        HardwareDispatchOwner::InstanceReboot {
            last_attempt_at: Some(_),
            ..
        }
    ));
    let HardwareDispatchOwner::InstanceReboot {
        last_attempt_at: Some(attempted_at),
        ..
    } = completed.owner
    else {
        panic!("accepted reboot must retain its committed attempt time");
    };
    let request = after.status.last_reboot_requested.unwrap();
    assert_eq!(request.time, attempted_at);
    assert_eq!(
        request.mode,
        model::machine::MachineLastRebootRequestedMode::Reboot
    );
    assert_eq!(request.restart_verified, None);
    assert_eq!(request.verification_attempts, Some(0));
    assert_eq!(
        after.status.last_reboot_time, before.status.last_reboot_time,
        "no Scout reboot report was sent"
    );
    assert_eq!(
        after.state.value, before.state.value,
        "ordinary reboot must not enter the PXE flow"
    );
    // Unsupported restart logs must not turn an accepted ordinary reboot into another
    // send through the generic restart verifier on subsequent controller passes.
    for _ in 0..3 {
        env.run_machine_state_controller_iteration().await;
    }
    assert_eq!(
        env.redfish_sim.actions_since(&actions).all_hosts(),
        vec![RedfishSimAction::Power(SystemPowerControl::ForceRestart)]
    );
    assert_eq!(
        machine(&env, *host.id).await.status.last_reboot_requested,
        Some(request)
    );
}

/// A target changed during unlocked preparation must never receive the saved reboot.
#[crate::sqlx_test]
async fn instance_reboot_rechecks_target_after_preparation(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    for (name, release_allocation, already_attempted, state_change) in [
        ("unsent target change", false, false, false),
        ("allocation released during preparation", true, false, false),
        ("target changed after a lost reply", false, true, false),
        ("state version changed", false, false, true),
    ] {
        let host = create_managed_host(&env).await;
        let instance = create_instance(&env, &host, false, segment).await;
        env.api
            .invoke_instance_power(request(instance.id))
            .await
            .unwrap();
        let evidence = Arc::new(Evidence::default());
        let transport = Arc::new(Pool {
            sim: env.redfish_sim.clone(),
            evidence: evidence.clone(),
        });
        if already_attempted {
            // A lost reply authorizes no automatic cancellation when routing later changes.
            evidence.lose_reply.store(true, Ordering::SeqCst);
            assert!(iterate(&env, &host, transport.clone()).await.is_err());
            let mut attempted = dispatch(&env, *host.id).await;
            let HardwareDispatchOwner::InstanceReboot {
                last_attempt_at, ..
            } = &mut attempted.owner
            else {
                panic!("expected the queued reboot");
            };
            *last_attempt_at = Some(Utc::now() - chrono::Duration::days(1));
            sqlx::query("UPDATE machines SET hardware_dispatch = $2 WHERE id = $1")
                .bind(*host.id)
                .bind(sqlx::types::Json(attempted))
                .execute(&env.pool)
                .await
                .unwrap();
        }
        let saved = dispatch(&env, *host.id).await;
        let power_before = env.redfish_sim.timepoint();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        *evidence.preparation.lock().unwrap() = Some((entered_tx, release_rx));
        let change_target = async {
            entered_rx.await.unwrap();
            // Credential preparation holds neither row lock, while durable ownership remains.
            let mut txn = env.pool.begin().await.unwrap();
            sqlx::query("SELECT id FROM instances WHERE id = $1 FOR UPDATE NOWAIT")
                .bind(instance.id)
                .fetch_one(txn.as_mut())
                .await
                .unwrap();
            sqlx::query("SELECT id FROM machines WHERE id = $1 FOR NO KEY UPDATE NOWAIT")
                .bind(*host.id)
                .fetch_one(txn.as_mut())
                .await
                .unwrap();
            assert!(
                db::hardware_dispatch::has_pending(&mut *txn, *host.id)
                    .await
                    .unwrap()
            );
            if release_allocation {
                txn.commit().await.unwrap();
                // Release changes the allocation's eligibility independently of BMC routing.
                env.api
                    .release_instance(Request::new(rpc::forge::InstanceReleaseRequest {
                        id: Some(instance.id),
                        ..Default::default()
                    }))
                    .await
                    .unwrap();
            } else if state_change {
                db::machine::update_state(
                    txn.as_mut(),
                    &host.id,
                    &ManagedHostState::Assigned {
                        instance_state: InstanceState::Ready,
                    },
                )
                .await
                .unwrap();
                txn.commit().await.unwrap();
            } else {
                // An inventory update can change credentials while client preparation is pending.
                let replacement_mac: mac_address::MacAddress = if already_attempted {
                    "02:00:00:00:00:fd"
                } else {
                    "02:00:00:00:00:fe"
                }
                .parse()
                .unwrap();
                sqlx::query("UPDATE machine_interfaces SET mac_address = $2 WHERE machine_id = $1 AND interface_type = 'Bmc'")
                    .bind(*host.id).bind(replacement_mac).execute(txn.as_mut()).await.unwrap();
                txn.commit().await.unwrap();
            }
            release_tx.send(()).unwrap();
        };
        tokio::time::timeout(Duration::from_secs(30), async {
            let (result, ()) = tokio::join!(iterate(&env, &host, transport), change_target);
            if already_attempted {
                assert!(
                    matches!(
                        result,
                        Err(StateHandlerError::ManualInterventionRequired(_))
                    ),
                    "{name}"
                );
            } else {
                result.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(
            evidence.calls.load(Ordering::SeqCst),
            usize::from(already_attempted)
        );
        assert!(
            env.redfish_sim
                .actions_since(&power_before)
                .all_hosts()
                .is_empty(),
            "a changed target must not receive a power action through any client"
        );
        let retained = dispatch(&env, *host.id).await;
        if already_attempted || state_change {
            assert_eq!(
                retained, saved,
                "uncertain or refreshed work must retain the exact owner"
            );
        } else {
            assert!(
                !retained.is_pending(),
                "only proven-unsent work may be canceled"
            );
        }
    }
}

/// An unreachable former endpoint cannot prevent cancellation of a stale unsent request.
#[crate::sqlx_test]
async fn instance_reboot_cancels_stale_target_before_preparation(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    let host = create_managed_host(&env).await;
    let instance = create_instance(&env, &host, false, segment).await;
    env.api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap();
    let saved = dispatch(&env, *host.id).await;

    // Inventory moves while the old target can no longer prepare any client.
    let replacement: mac_address::MacAddress = "02:00:00:00:00:fc".parse().unwrap();
    sqlx::query("UPDATE machine_interfaces SET mac_address = $2 WHERE machine_id = $1 AND interface_type = 'Bmc'")
        .bind(*host.id).bind(replacement).execute(&env.pool).await.unwrap();
    env.redfish_sim
        .set_create_client_error("former BMC is unreachable");
    let preparations = env.redfish_sim.create_client_calls().len();
    let actions = env.redfish_sim.timepoint();
    env.run_machine_state_controller_iteration().await;

    let canceled = dispatch(&env, *host.id).await;
    assert_eq!(canceled.token, saved.token);
    assert_eq!(canceled.owner, saved.owner);
    assert!(!canceled.is_pending());
    assert_eq!(env.redfish_sim.create_client_calls().len(), preparations);
    assert!(
        env.redfish_sim
            .actions_since(&actions)
            .all_hosts()
            .is_empty()
    );
}

/// A reboot saved before Ready must run before the automatic provisioning retry.
#[crate::sqlx_test]
async fn instance_reboot_precedes_provisioning_retry(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    let host = create_managed_host(&env).await;
    let instance = create_instance(&env, &host, false, segment).await;
    let mut txn = env.pool.begin().await.unwrap();
    db::machine::update_state(
        txn.as_mut(),
        &host.id,
        &ManagedHostState::Assigned {
            instance_state: InstanceState::HostPlatformConfiguration {
                platform_config_state: HostPlatformConfigurationState::CheckHostConfig,
            },
        },
    )
    .await
    .unwrap();
    // These are the persisted conditions that otherwise select provisioning retry.
    sqlx::query("UPDATE instances SET os_phone_home_enabled = true, phone_home_last_contact = NULL, use_custom_pxe_on_boot = true WHERE id = $1")
        .bind(instance.id).execute(txn.as_mut()).await.unwrap();
    txn.commit().await.unwrap();
    env.api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap();
    let saved = dispatch(&env, *host.id).await;

    let mut txn = env.pool.begin().await.unwrap();
    db::machine::update_state(
        txn.as_mut(),
        &host.id,
        &ManagedHostState::Assigned {
            instance_state: InstanceState::Ready,
        },
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    let actions = env.redfish_sim.timepoint();
    env.run_machine_state_controller_iteration().await;
    let after = machine(&env, *host.id).await;
    let completed = after.status.hardware_dispatch.unwrap();
    assert_eq!(completed.token, saved.token);
    assert!(!completed.is_pending());
    assert!(matches!(
        completed.owner,
        HardwareDispatchOwner::InstanceReboot {
            last_attempt_at: Some(_),
            ..
        }
    ));
    assert_eq!(
        after.state.value,
        ManagedHostState::Assigned {
            instance_state: InstanceState::Ready
        }
    );
    assert_eq!(
        env.redfish_sim.actions_since(&actions).all_hosts(),
        vec![RedfishSimAction::Power(SystemPowerControl::ForceRestart)]
    );
}

/// Custom PXE uses its existing Ready flow; pre-Ready requests cannot queue a second reset.
#[crate::sqlx_test]
async fn instance_reboot_custom_pxe_does_not_queue_an_extra_reboot(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    for (name, always_pxe, explicit_pxe) in [
        ("explicit custom PXE", false, true),
        ("always custom PXE", true, false),
    ] {
        let host = create_managed_host(&env).await;
        let instance = create_instance(&env, &host, always_pxe, segment).await;
        let actions = env.redfish_sim.timepoint();
        let mut reboot = request(instance.id).into_inner();
        reboot.boot_with_custom_ipxe = explicit_pxe;
        env.api
            .invoke_instance_power(Request::new(reboot.clone()))
            .await
            .unwrap();
        let mut txn = env.pool.begin().await.unwrap();
        assert!(
            instance
                .db_instance(&mut txn)
                .await
                .custom_pxe_reboot_requested,
            "{name}"
        );
        txn.commit().await.unwrap();
        assert!(
            machine(&env, *host.id)
                .await
                .status
                .hardware_dispatch
                .is_none(),
            "{name}"
        );
        assert!(
            env.redfish_sim
                .actions_since(&actions)
                .all_hosts()
                .is_empty(),
            "{name}: API must leave the reboot to the controller"
        );

        // These states have a boot already in progress or may belong to release/reprovision.
        // Neither may accept another PXE intent or reserve a later ordinary reset.
        for state in [
            InstanceState::WaitingForDpusToUp,
            InstanceState::WaitingForRebootToReady,
        ] {
            let mut txn = env.pool.begin().await.unwrap();
            db::instance::set_custom_pxe_reboot_requested(&host.id, false, &mut txn)
                .await
                .unwrap();
            db::machine::update_state(
                txn.as_mut(),
                &host.id,
                &ManagedHostState::Assigned {
                    instance_state: state,
                },
            )
            .await
            .unwrap();
            txn.commit().await.unwrap();
            let error = env
                .api
                .invoke_instance_power(Request::new(reboot.clone()))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{name}");
            let mut txn = env.pool.begin().await.unwrap();
            assert!(
                !instance
                    .db_instance(&mut txn)
                    .await
                    .custom_pxe_reboot_requested
            );
            txn.commit().await.unwrap();
            assert!(
                machine(&env, *host.id)
                    .await
                    .status
                    .hardware_dispatch
                    .is_none()
            );
            assert!(
                env.redfish_sim
                    .actions_since(&actions)
                    .all_hosts()
                    .is_empty()
            );
        }
    }
}

/// A lost BMC reply retains the request until an observation or a later retry reconciles it.
#[crate::sqlx_test]
async fn instance_reboot_recovers_lost_reply_without_scout(pool: sqlx::PgPool) {
    let mut config = get_config();
    config.machine_state_controller.failure_retry_time = chrono::Duration::minutes(5);
    config
        .machine_state_controller
        .instance_reboot_queue_enabled = true;
    let env = create_test_env_with_overrides(pool, TestEnvOverrides::with_config(config)).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    let host = create_managed_host(&env).await;
    let instance = create_instance(&env, &host, false, segment).await;
    // Both observations settle the same durable request: recognized reboot logs avoid a
    // repeated reset; unsupported logs preserve the existing controller's later retry policy.
    let matching: LogEntry = serde_json::from_value(serde_json::json!({
        "@odata.id": "/redfish/v1/Systems/System/LogServices/EventLog/Entries/test",
        "@odata.type": "#LogEntry.v1_15_0.LogEntry",
        "Created": Utc::now().to_rfc3339(), "EntryType": "Event", "Id": "test",
        "Message": "Server reset.", "Name": "Reset", "Severity": "OK"
    }))
    .unwrap();
    let mut power_fault = matching.clone();
    power_fault.message = "Power supply redundancy lost".to_string();
    let mut stale = matching.clone();
    stale.created = (Utc::now() - chrono::Duration::days(2)).to_rfc3339();
    for (name, logs, expected_calls, preserve_newer) in [
        ("matching restart", Some(vec![matching.clone()]), 1, false),
        ("unavailable log", None, 2, false),
        ("stale matching restart", Some(vec![stale]), 2, false),
        ("unrelated power event", Some(vec![power_fault]), 2, false),
        (
            "preserve newer reboot bookkeeping",
            Some(vec![matching]),
            1,
            true,
        ),
    ] {
        let observed_restart = expected_calls == 1;
        env.api
            .invoke_instance_power(request(instance.id))
            .await
            .unwrap();
        let evidence = Arc::new(Evidence::default());
        evidence.lose_reply.store(true, Ordering::SeqCst);
        let transport = Arc::new(Pool {
            sim: env.redfish_sim.clone(),
            evidence: evidence.clone(),
        });
        let before = machine(&env, *host.id).await;
        assert!(iterate(&env, &host, transport.clone()).await.is_err());
        let attempted = dispatch(&env, *host.id).await;
        assert!(attempted.is_pending());
        assert!(matches!(
            attempted.owner,
            HardwareDispatchOwner::InstanceReboot {
                last_attempt_at: Some(_),
                ..
            }
        ));
        assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
        iterate(&env, &host, transport.clone()).await.unwrap();
        assert_eq!(
            evidence.calls.load(Ordering::SeqCst),
            1,
            "a lost reply does not immediately repeat the command"
        );
        assert_eq!(dispatch(&env, *host.id).await, attempted);

        if name == "matching restart" {
            // Two minutes exceeds the hard minimum but not this configured interval.
            // Protect the configured policy once, without repeating it for every log case.
            let mut waiting = attempted.clone();
            let HardwareDispatchOwner::InstanceReboot {
                last_attempt_at, ..
            } = &mut waiting.owner
            else {
                panic!("expected an ordinary reboot");
            };
            *last_attempt_at = Some(Utc::now() - chrono::Duration::minutes(2));
            sqlx::query("UPDATE machines SET hardware_dispatch = $2 WHERE id = $1")
                .bind(*host.id)
                .bind(sqlx::types::Json(&waiting))
                .execute(&env.pool)
                .await
                .unwrap();
            iterate(&env, &host, transport.clone()).await.unwrap();
            assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
            assert_eq!(dispatch(&env, *host.id).await, waiting);
        }

        // Advance the persisted retry boundary without a wall-clock sleep. A fresh handler
        // invocation reconstructs everything from the database, with no prior sender state.
        let mut retryable = attempted.clone();
        let HardwareDispatchOwner::InstanceReboot {
            last_attempt_at, ..
        } = &mut retryable.owner
        else {
            panic!("expected the saved instance reboot");
        };
        let earlier_attempt = Utc::now() - chrono::Duration::days(1);
        *last_attempt_at = Some(earlier_attempt);
        // Preserve the production JSON encoding used by conditional owner comparisons.
        sqlx::query("UPDATE machines SET hardware_dispatch = $2 WHERE id = $1")
            .bind(*host.id)
            .bind(sqlx::types::Json(retryable))
            .execute(&env.pool)
            .await
            .unwrap();
        let previous_reboot = model::machine::MachineLastRebootRequested {
            time: if preserve_newer {
                earlier_attempt + chrono::Duration::hours(1)
            } else {
                earlier_attempt - chrono::Duration::days(1)
            },
            mode: model::machine::MachineLastRebootRequestedMode::Reboot,
            restart_verified: Some(false),
            verification_attempts: Some(0),
        };
        if observed_restart {
            // Existing bookkeeping can contain an older unverified reboot or
            // a newer attempt from another controller flow.
            // Only the queued request may query/retry while it owns the host; a
            // generic verification write must not invalidate this settlement.
            let mut txn = env.pool.begin().await.unwrap();
            db::machine::record_reboot_request(&host.id.into(), txn.as_mut(), &previous_reboot)
                .await
                .unwrap();
            txn.commit().await.unwrap();
        }
        *evidence.log.lock().unwrap() = logs;
        if name == "matching restart" {
            // Rebuild the production processor, including its database scan, with new
            // requests disabled. It must recover solely from the persisted owner.
            let mut services = env.machine_state_handler_services();
            let mut config = env.api.runtime_config.machine_state_handler_site_config();
            config
                .machine_state_controller
                .instance_reboot_queue_enabled = false;
            services.site_config = Arc::new(config);
            services.redfish_client_pool = transport.clone();
            services.bmc_connector = BmcConnector::Libredfish(transport);
            let cancel = tokio_util::sync::CancellationToken::new();
            let mut processor = state_controller::controller::StateController::<
                carbide_machine_controller::io::MachineStateControllerIO,
            >::builder()
            .database(env.pool.clone(), env.api.work_lock_manager_handle.clone())
            .meter("restarted_reboot_processor", env.test_meter.meter())
            .processor_id(uuid::Uuid::new_v4().to_string())
            .services(services.into())
            .state_handler(Arc::new(env.machine_state_handler.clone()))
            .io(Arc::new(
                carbide_machine_controller::io::MachineStateControllerIO {
                    host_health: env.api.runtime_config.host_health,
                    sla_config: model::machine::slas::MachineSlaConfig::new(
                        chrono::Duration::minutes(5),
                    ),
                },
            ))
            .build_for_manual_iterations(cancel.clone())
            .unwrap();
            processor.run_single_iteration().await;
            cancel.cancel();
        } else {
            iterate(&env, &host, transport).await.unwrap();
        }
        let after = machine(&env, *host.id).await;
        let completed = after.status.hardware_dispatch.unwrap();
        assert_eq!(completed.token, attempted.token);
        assert!(!completed.is_pending());
        if observed_restart {
            let request = after.status.last_reboot_requested.unwrap();
            let expected = if preserve_newer {
                previous_reboot
            } else {
                model::machine::MachineLastRebootRequested {
                    time: earlier_attempt,
                    mode: model::machine::MachineLastRebootRequestedMode::Reboot,
                    restart_verified: Some(true),
                    verification_attempts: Some(0),
                }
            };
            assert_eq!(request, expected, "{name}");
        }
        assert_eq!(
            evidence.calls.load(Ordering::SeqCst),
            expected_calls,
            "{name}"
        );
        assert_eq!(evidence.nonretry_clients.load(Ordering::SeqCst), 2);
        assert_eq!(
            *evidence.log_requests.lock().unwrap(),
            vec![Some(earlier_attempt)],
            "{name}: query from the actual saved attempt"
        );
        assert_eq!(
            after.status.last_reboot_time,
            before.status.last_reboot_time
        );
        assert_eq!(after.state.value, before.state.value);
    }
}

/// An operator hold and the existing bounded recovery window stop resends, not observation.
#[crate::sqlx_test]
async fn instance_reboot_bounds_automatic_retries(pool: sqlx::PgPool) {
    let mut config = get_config();
    config
        .machine_state_controller
        .instance_reboot_queue_enabled = true;
    config.machine_state_controller.failure_retry_time = chrono::Duration::seconds(10);
    let env = create_test_env_with_overrides(pool, TestEnvOverrides::with_config(config)).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    for held in [true, false] {
        let host = create_managed_host(&env).await;
        let instance = create_instance(&env, &host, false, segment).await;
        if held {
            let report = health_report::HealthReport {
                alerts: vec![health_report::HealthProbeAlert {
                    id: "Maintenance".parse().unwrap(), target: None, in_alert_since: None,
                    message: "operator reboot hold".into(), tenant_message: None,
                    classifications: vec![health_report::HealthAlertClassification::stop_reboot_for_automatic_recovery_from_state_machine()],
                }],
                ..health_report::HealthReport::empty("reboot-test".into())
            };
            let mut txn = env.pool.begin().await.unwrap();
            db::machine::insert_health_report(
                &mut txn,
                &host.id.into(),
                health_report::HealthReportApplyMode::Merge,
                &report,
                false,
            )
            .await
            .unwrap();
            txn.commit().await.unwrap();
        }
        env.api
            .invoke_instance_power(request(instance.id))
            .await
            .unwrap();
        let evidence = Arc::new(Evidence::default());
        evidence.lose_reply.store(true, Ordering::SeqCst);
        let transport = Arc::new(Pool {
            sim: env.redfish_sim.clone(),
            evidence: evidence.clone(),
        });
        // The first attempt is tenant-authorized even under the automatic recovery hold.
        assert!(iterate(&env, &host, transport.clone()).await.is_err());
        assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
        let mut pending = dispatch(&env, *host.id).await;
        let HardwareDispatchOwner::InstanceReboot {
            last_attempt_at, ..
        } = &mut pending.owner
        else {
            panic!("reboot owner")
        };
        // Thirty seconds exceeds the configured ten seconds, but not the one-minute floor.
        *last_attempt_at = Some(Utc::now() - chrono::Duration::seconds(30));
        sqlx::query("UPDATE machines SET hardware_dispatch = $2 WHERE id = $1")
            .bind(*host.id)
            .bind(sqlx::types::Json(&pending))
            .execute(&env.pool)
            .await
            .unwrap();
        iterate(&env, &host, transport.clone()).await.unwrap();
        assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
        assert_eq!(dispatch(&env, *host.id).await, pending);
        let HardwareDispatchOwner::InstanceReboot {
            last_attempt_at, ..
        } = &mut pending.owner
        else {
            panic!("reboot owner")
        };
        *last_attempt_at = Some(Utc::now() - chrono::Duration::minutes(2));
        if !held {
            pending.started_at = Utc::now() - chrono::Duration::minutes(16);
        }
        sqlx::query("UPDATE machines SET hardware_dispatch = $2 WHERE id = $1")
            .bind(*host.id)
            .bind(sqlx::types::Json(&pending))
            .execute(&env.pool)
            .await
            .unwrap();
        let outcome = iterate(&env, &host, transport.clone()).await;
        if held {
            outcome.unwrap();
        } else {
            assert!(matches!(
                outcome,
                Err(StateHandlerError::ManualInterventionRequired(_))
            ));
        }
        assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
        assert_eq!(dispatch(&env, *host.id).await, pending);
        // Neither policy may prevent settlement when a fresh supported restart event arrives.
        *evidence.log.lock().unwrap() = Some(vec![
            serde_json::from_value(serde_json::json!({
                "@odata.id": "/reset", "@odata.type": "#LogEntry.v1_15_0.LogEntry",
                "Created": Utc::now().to_rfc3339(), "EntryType": "Event", "Id": "reset",
                "Message": "Server reset.", "Name": "Reset", "Severity": "OK"
            }))
            .unwrap(),
        ]);
        iterate(&env, &host, transport).await.unwrap();
        let settled = dispatch(&env, *host.id).await;
        assert_eq!(settled.token, pending.token);
        assert!(!settled.is_pending());
        assert_eq!(evidence.calls.load(Ordering::SeqCst), 1);
    }
}

/// Existing NIC work and an ordinary reboot must not issue overlapping hardware commands.
#[crate::sqlx_test]
async fn instance_reboot_and_nic_work_exclude_each_other(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    let host = create_managed_host(&env).await;
    let instance = create_instance(&env, &host, false, segment).await;
    env.api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap();
    let queued = dispatch(&env, *host.id).await;
    // A deferred boot-mode request must not bypass ordinary reboot ownership.
    let mut custom_pxe = request(instance.id);
    custom_pxe.get_mut().boot_with_custom_ipxe = true;
    let error = env.api.invoke_instance_power(custom_pxe).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(dispatch(&env, *host.id).await, queued);
    let mut txn = env.pool.begin().await.unwrap();
    assert!(
        !instance
            .db_instance(&mut txn)
            .await
            .custom_pxe_reboot_requested
    );
    txn.rollback().await.unwrap();
    let mut operation = model::test_support::nic_firmware::operation();
    operation.purpose = model::machine::nic_firmware::NicFirmwarePurpose::Allocation {
        instance_id: instance.id,
    };
    let nic_state = ManagedHostState::NicFirmwareUpdating { operation };
    let actions = env.redfish_sim.timepoint();
    // Production admission is tracked in https://github.com/dsx-ai-factory/infra-controller/issues/8943.
    // The owning handler must retain an earlier pending command if it encounters one.
    sqlx::query("UPDATE machines SET controller_state = $2 WHERE id = $1")
        .bind(*host.id)
        .bind(sqlx::types::Json(nic_state.clone()))
        .execute(&env.pool)
        .await
        .unwrap();
    env.run_machine_state_controller_iteration().await;
    assert_eq!(dispatch(&env, *host.id).await, queued);
    let after = machine(&env, *host.id).await;
    assert_eq!(after.state.value, nic_state);
    assert!(matches!(after.controller_state_outcome,
        Some(model::controller_outcome::PersistentStateHandlerOutcome::Wait { reason, .. })
            if reason == "an earlier hardware command is still awaiting its result"
    ));
    assert!(
        env.redfish_sim
            .actions_since(&actions)
            .all_hosts()
            .is_empty()
    );
    let error = env
        .api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("NIC firmware"));
    assert_eq!(dispatch(&env, *host.id).await, queued);
}

/// Releasing an allocation cancels a request that never reached a sender.
#[crate::sqlx_test]
async fn instance_reboot_cancels_unsent_request_after_release(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    let host = create_managed_host(&env).await;
    let instance = create_instance(&env, &host, false, segment).await;
    env.api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap();
    let queued = dispatch(&env, *host.id).await;
    let actions = env.redfish_sim.timepoint();
    env.api
        .release_instance(Request::new(rpc::forge::InstanceReleaseRequest {
            id: Some(instance.id),
            ..Default::default()
        }))
        .await
        .unwrap();
    env.run_machine_state_controller_iteration().await;
    let canceled = dispatch(&env, *host.id).await;
    assert_eq!(canceled.token, queued.token);
    assert!(!canceled.is_pending());
    assert!(matches!(
        canceled.owner,
        HardwareDispatchOwner::InstanceReboot {
            last_attempt_at: None,
            ..
        }
    ));
    // Settlement removes the pending-request guard, so this rejection proves
    // the ordinary request's terminating-allocation check itself.
    let error = env
        .api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("terminating instance"));
    assert_eq!(dispatch(&env, *host.id).await, canceled);
    assert!(
        env.redfish_sim
            .actions_since(&actions)
            .all_hosts()
            .is_empty()
    );
}

/// An uncertain reboot keeps its exclusion record without stopping instance release.
#[crate::sqlx_test]
async fn instance_reboot_preserves_uncertainty_while_release_progresses(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    let host = create_managed_host(&env).await;
    let instance = create_instance(&env, &host, false, segment).await;
    env.api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap();
    let evidence = Arc::new(Evidence::default());
    evidence.lose_reply.store(true, Ordering::SeqCst);
    let transport = Arc::new(Pool {
        sim: env.redfish_sim.clone(),
        evidence,
    });
    assert!(iterate(&env, &host, transport).await.is_err());
    let attempted = dispatch(&env, *host.id).await;

    // Release takes precedence over retrying a request for the previous allocation.
    env.api
        .release_instance(Request::new(rpc::forge::InstanceReleaseRequest {
            id: Some(instance.id),
            ..Default::default()
        }))
        .await
        .unwrap();
    let actions = env.redfish_sim.timepoint();
    env.run_machine_state_controller_iteration_no_requeue()
        .await;
    let after = machine(&env, *host.id).await;
    assert_eq!(after.status.hardware_dispatch.as_ref(), Some(&attempted));
    assert!(attempted.is_pending());
    assert_eq!(
        after.state.value,
        ManagedHostState::Assigned {
            instance_state: InstanceState::HostPlatformConfiguration {
                platform_config_state: HostPlatformConfigurationState::FactoryResetBmc {
                    reset_state: FactoryResetBmcState::CheckPreconditions,
                },
            },
        }
    );
    assert!(
        env.redfish_sim
            .actions_since(&actions)
            .all_hosts()
            .is_empty()
    );
}

/// An ordinary reboot does not require a managed NIC or a tenant Scout agent.
#[crate::sqlx_test]
async fn instance_reboot_without_managed_nics(pool: sqlx::PgPool) {
    use crate::test_support::fixture_config::ManagedHostConfigExt;
    let mut config = get_config();
    config
        .machine_state_controller
        .instance_reboot_queue_enabled = true;
    let env = crate::tests::machine_states::create_zero_dpu_test_env_with_overrides(
        pool,
        TestEnvOverrides::with_config(config),
    )
    .await;
    let host = crate::tests::common::api_fixtures::create_managed_host_with_config(
        &env,
        model::test_support::ManagedHostConfig::zero_dpu(),
    )
    .await;
    assert!(host.dpu_ids.is_empty());
    // Reuse the zero-DPU lifecycle fixture's valid empty service configuration.
    let mut txn = env.pool.begin().await.unwrap();
    let instance: InstanceId = sqlx::query_scalar("INSERT INTO instances (machine_id, network_config, nvlink_config) VALUES ($1, '{\"interfaces\": []}'::jsonb, '{\"gpu_configs\": []}'::jsonb) RETURNING id")
        .bind(*host.id).fetch_one(txn.as_mut()).await.unwrap();
    db::machine::update_state(
        txn.as_mut(),
        &host.id,
        &ManagedHostState::Assigned {
            instance_state: InstanceState::Ready,
        },
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    let observed = sqlx::query("UPDATE machine_boot_interfaces SET observed_at = CURRENT_TIMESTAMP - INTERVAL '1 day' WHERE machine_id = $1 AND desired_version = verified_version")
        .bind(*host.id).execute(&env.pool).await.unwrap();
    assert_eq!(
        observed.rows_affected(),
        1,
        "fixture needs verified boot intent for the periodic observer"
    );
    env.redfish_sim.set_is_bios_setup(true);
    env.redfish_sim.set_is_boot_order_setup(true);
    let actions = env.redfish_sim.timepoint();
    env.api
        .invoke_instance_power(request(instance))
        .await
        .unwrap();
    let queued = dispatch(&env, *host.id).await;
    assert!(queued.is_pending());
    assert!(
        env.redfish_sim
            .actions_since(&actions)
            .all_hosts()
            .is_empty()
    );
    env.run_machine_state_controller_iteration().await;
    let settled = dispatch(&env, *host.id).await;
    assert_eq!(settled.token, queued.token);
    assert!(!settled.is_pending());
    assert_eq!(
        env.redfish_sim.actions_since(&actions).all_hosts(),
        vec![RedfishSimAction::Power(SystemPowerControl::ForceRestart)]
    );
}

/// The reset API can commit without changing the state version while BMC preparation waits.
#[crate::sqlx_test]
async fn instance_reboot_rechecks_reset_after_preparation(pool: sqlx::PgPool) {
    use crate::tests::managed_host_reset::{
        dpf_ingested_host, dpf_test_env, reset_request_allowing_instance,
    };
    let env = dpf_test_env(pool).await;
    let host = dpf_ingested_host(&env).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    let instance = create_instance(&env, &host, false, segment).await;
    let before = machine(&env, *host.id).await;
    let mut txn = env.pool.begin().await.unwrap();
    db::hardware_dispatch::begin(
        &mut txn,
        uuid::Uuid::new_v4(),
        &HardwareDispatchOwner::InstanceReboot {
            machine_id: *host.id,
            instance_id: instance.id,
            bmc: before.status.bmc_info.clone(),
            last_attempt_at: None,
        },
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    let saved = dispatch(&env, *host.id).await;
    let (entered, started) = oneshot::channel();
    let (resume, released) = oneshot::channel();
    let evidence = Arc::new(Evidence {
        preparation: Mutex::new(Some((entered, released))),
        ..Default::default()
    });
    let transport = Arc::new(Pool {
        sim: env.redfish_sim.clone(),
        evidence: evidence.clone(),
    });
    tokio::time::timeout(Duration::from_secs(30), async {
        let reset = async {
            started.await.unwrap();
            host.mark_machine_for_updates().await;
            env.api
                .trigger_managed_host_reset(reset_request_allowing_instance(host.id.into()))
                .await
                .unwrap();
            resume.send(()).unwrap();
        };
        let (outcome, ()) = tokio::join!(iterate(&env, &host, transport), reset);
        outcome.unwrap();
    })
    .await
    .unwrap();
    let after = machine(&env, *host.id).await;
    assert_eq!(after.state.version, before.state.version);
    assert!(after.reset_requested.unwrap().started_at.is_none());
    assert_eq!(after.status.hardware_dispatch, Some(saved));
    assert_eq!(evidence.calls.load(Ordering::SeqCst), 0);
}

/// Update approval owns the reboot even when tenant PXE was requested first.
#[crate::sqlx_test]
async fn instance_reboot_preserves_reprovision_precedence(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    let host = create_managed_host(&env).await;
    let instance = create_instance(&env, &host, true, segment).await;
    let actions = env.redfish_sim.timepoint();
    env.api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap();
    host.mark_machine_for_updates().await;
    host.dpu()
        .trigger_dpu_reprovisioning(rpc::forge::dpu_reprovisioning_request::Mode::Set, false)
        .await;
    let mut approval = request(instance.id);
    approval.get_mut().apply_updates_on_reboot = true;
    approval.get_mut().boot_with_custom_ipxe = true;
    env.api.invoke_instance_power(approval).await.unwrap();
    let mut txn = env.pool.begin().await.unwrap();
    let approved = host
        .dpu()
        .db_machine(&mut txn)
        .await
        .reprovision_requested
        .unwrap();
    assert!(approved.user_approval_received);
    assert!(approved.started_at.is_none());
    assert!(
        !instance
            .db_instance(&mut txn)
            .await
            .custom_pxe_reboot_requested
    );
    txn.commit().await.unwrap();
    assert!(
        machine(&env, *host.id)
            .await
            .status
            .hardware_dispatch
            .is_none()
    );
    assert!(
        env.redfish_sim
            .actions_since(&actions)
            .all_hosts()
            .is_empty()
    );
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let after = machine(&env, *host.id).await;
    assert!(matches!(
        after.state.value,
        ManagedHostState::Assigned {
            instance_state: InstanceState::HostPlatformConfiguration { .. }
        }
    ));
    let mut txn = env.pool.begin().await.unwrap();
    assert!(
        host.dpu()
            .db_machine(&mut txn)
            .await
            .reprovision_requested
            .unwrap()
            .started_at
            .is_some()
    );
    assert!(
        !instance
            .db_instance(&mut txn)
            .await
            .custom_pxe_reboot_requested
    );
    txn.commit().await.unwrap();
    assert!(after.status.hardware_dispatch.is_none());
}

/// A pending host reboot must not hide the DPU verifier's persisted result.
#[crate::sqlx_test]
async fn instance_reboot_preserves_dpu_restart_verification(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    let host = create_managed_host(&env).await;
    let instance = create_instance(&env, &host, false, segment).await;
    env.api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap();
    let queued = dispatch(&env, *host.id).await;
    let mut txn = env.pool.begin().await.unwrap();
    let pending =
        db::hardware_dispatch::record_instance_reboot_attempt(&mut txn, *host.id, &queued)
            .await
            .unwrap()
            .unwrap();
    let dpu_request = model::machine::MachineLastRebootRequested {
        time: Utc::now(),
        mode: model::machine::MachineLastRebootRequestedMode::Reboot,
        restart_verified: Some(false),
        verification_attempts: Some(0),
    };
    db::machine::record_reboot_request(&host.dpu().id, &mut txn, &dpu_request)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    let actions = env.redfish_sim.timepoint();
    env.run_machine_state_controller_iteration().await;
    assert_eq!(dispatch(&env, *host.id).await, pending);
    let mut txn = env.pool.begin().await.unwrap();
    let verified = host
        .dpu()
        .db_machine(&mut txn)
        .await
        .status
        .last_reboot_requested
        .unwrap();
    assert_eq!(verified.time, dpu_request.time);
    assert_eq!(
        verified.restart_verified, None,
        "unsupported DPU logs must still be reconciled"
    );
    txn.commit().await.unwrap();
    assert!(
        env.redfish_sim
            .actions_since(&actions)
            .all_hosts()
            .is_empty()
    );
}

/// A busy BMC interface must leave the request unsent without reversing cleanup's lock order.
#[crate::sqlx_test]
async fn instance_reboot_retries_after_bmc_address_assignment(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    let host = create_managed_host(&env).await;
    let instance = create_instance(&env, &host, false, segment).await;
    env.api
        .invoke_instance_power(request(instance.id))
        .await
        .unwrap();
    let queued = dispatch(&env, *host.id).await;
    let mut writer = env.pool.begin().await.unwrap();
    let interface_id: carbide_uuid::machine::MachineInterfaceId = sqlx::query_scalar(
        "SELECT id FROM machine_interfaces WHERE machine_id = $1 AND interface_type = 'Bmc'",
    )
    .bind(*host.id)
    .fetch_one(writer.as_mut())
    .await
    .unwrap();
    db::machine_interface::lock_for_address_assignment(&mut writer, interface_id)
        .await
        .unwrap();
    // The writer has changed the address, but it is invisible until its commit.
    sqlx::query("UPDATE machine_interface_addresses SET address = '192.0.2.254' WHERE interface_id = $1 AND family(address) = 4")
        .bind(interface_id).execute(writer.as_mut()).await.unwrap();
    let evidence = Arc::new(Evidence::default());
    let transport = Arc::new(Pool {
        sim: env.redfish_sim.clone(),
        evidence: evidence.clone(),
    });
    // Keep the writer locked until the iteration returns. Waiting here would
    // deadlock against a cleanup transaction that next needs the Machine lock.
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        iterate(&env, &host, transport.clone()),
    )
    .await
    .unwrap()
    .unwrap_err();
    let StateHandlerError::DBError(error) = error else {
        panic!("expected a database lock error: {error}");
    };
    let db::DatabaseError::Sqlx(error) = *error else {
        panic!("expected a SQL error: {error}");
    };
    assert_eq!(
        error.source.as_database_error().unwrap().code().as_deref(),
        Some("55P03"),
    );
    assert_eq!(dispatch(&env, *host.id).await, queued);
    assert_eq!(evidence.calls.load(Ordering::SeqCst), 0);
    writer.commit().await.unwrap();

    // The next pass sees the changed address and cancels the original unsent request.
    iterate(&env, &host, transport).await.unwrap();
    let canceled = dispatch(&env, *host.id).await;
    assert_eq!(canceled.token, queued.token);
    assert!(!canceled.is_pending());
    assert_eq!(evidence.calls.load(Ordering::SeqCst), 0);
}
