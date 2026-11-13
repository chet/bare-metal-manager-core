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
    FactoryResetBmcState, HostMachine, HostPlatformConfigurationState, InstanceState,
    ManagedHostState,
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
        // The BMC accepted the POST, but its response cannot be read completely.
        calls.fetch_add(1, Ordering::SeqCst);
        Response::builder()
            .header("content-length", "10")
            .body(Body::from_stream(futures_util::stream::once(async {
                Err::<Bytes, _>(std::io::Error::other("lost reset response"))
            })))
            .unwrap()
    }

    #[derive(Default)]
    struct LostReplyIpmi(AtomicUsize);
    #[async_trait]
    impl carbide_ipmi::IPMITool for LostReplyIpmi {
        async fn restart_once(&self, _: SocketAddr, _: &CredentialKey) -> Result<(), eyre::Report> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(eyre::eyre!("reply lost after sending the chassis reset"))
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
        iterate(&env, &host, transport).await.unwrap();
        assert_eq!(
            ipmi.0.load(Ordering::SeqCst),
            usize::from(use_ipmi),
            "{name}"
        );
        assert_eq!(
            http_calls.load(Ordering::SeqCst),
            usize::from(!use_ipmi),
            "{name}"
        );
        assert_eq!(dispatch(&env, *host.id).await, attempted);
        assert!(
            env.redfish_sim
                .actions_since(&actions)
                .all_hosts()
                .is_empty()
        );
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
    // Empty restart logs must not turn an accepted ordinary reboot into another
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
    for (name, release_allocation, already_attempted) in [
        ("unsent target change", false, false),
        ("allocation released during preparation", true, false),
        ("target changed after a lost reply", false, true),
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
        if already_attempted {
            assert_eq!(
                retained, saved,
                "uncertain work must retain the exact owner"
            );
        } else {
            assert!(
                !retained.is_pending(),
                "only proven-unsent work may be canceled"
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
    for (name, logs, expected_calls, preserve_newer) in [
        ("matching restart", Some(vec![matching.clone()]), 1, false),
        ("unavailable log", None, 2, false),
        ("empty successful log", Some(vec![]), 2, false),
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

        if observed_restart && !preserve_newer {
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
        iterate(&env, &host, transport).await.unwrap();
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
    assert!(!instance.db_instance(&mut txn).await.use_custom_pxe_on_boot);
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
