// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Ordinary reboot requests cross the RPC, durable ownership and existing controller boundary.

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
use state_controller::state_handler::{StateHandler, StateHandlerContext, StateHandlerError};
use tokio::sync::oneshot;
use tonic::Request;

use crate::tests::common::api_fixtures::{
    TestEnv, TestManagedHost, create_managed_host, create_test_env,
};
use crate::tests::instance_ipxe_behaviors::create_instance;

/// Controls BMC preparation and responses. An absent gate proceeds immediately;
/// an absent log models a BMC that cannot report restart events.
#[derive(Default)]
struct Evidence {
    preparation: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
    lose_reply: AtomicBool,
    calls: AtomicUsize,
    nonretry_clients: AtomicUsize,
    log: Mutex<Option<LogEntry>>,
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
        self.sim.create_client(host, port, auth, vendor).await
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
        self.inner.get_power_state()
    }

    fn power(&self, action: SystemPowerControl) -> RedfishFuture<'_, Result<(), RedfishError>> {
        Box::pin(async move {
            self.evidence.calls.fetch_add(1, Ordering::SeqCst);
            self.inner.power(action).await?;
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
            let log = self.evidence.log.lock().unwrap().clone();
            match log {
                Some(log) => {
                    assert!(from.is_some());
                    assert!(DateTime::parse_from_rfc3339(&log.created).unwrap() >= from.unwrap());
                    Ok(vec![log])
                }
                None => Err(RedfishError::GenericError {
                    error: "event log unsupported".into(),
                }),
            }
        })
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

/// Run the production handler with a controlled transport, including its returned transaction
/// and pending writes. The success test below separately exercises the complete processor.
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
    services.bmc_connector = BmcConnector::Libredfish(pool.clone());
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
    let mut txn = match outcome.take_transaction() {
        Some(txn) => txn,
        None => env.pool.begin().await.unwrap(),
    };
    writes.apply_all(&mut txn).await?;
    txn.commit().await.unwrap();
    Ok(())
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
    env.run_machine_state_controller_iteration().await;
    assert_eq!(
        dispatch(&env, *host.id).await,
        queued,
        "preparation failure must leave the request unsent"
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
    assert_ne!(
        after.status.last_reboot_requested,
        before.status.last_reboot_requested
    );
    assert_eq!(
        after.status.last_reboot_time, before.status.last_reboot_time,
        "no Scout reboot report was sent"
    );
    assert_eq!(
        after.state.value, before.state.value,
        "ordinary reboot must not enter the PXE flow"
    );
}

/// A target changed during unlocked preparation must never receive the saved reboot.
#[crate::sqlx_test]
async fn instance_reboot_rechecks_target_after_preparation(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
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
        sqlx::query("UPDATE machine_interfaces SET mac_address = '02:00:00:00:00:fe' WHERE machine_id = $1 AND interface_type = 'Bmc'").bind(*host.id).execute(txn.as_mut()).await.unwrap();
        txn.commit().await.unwrap();
        release_tx.send(()).unwrap();
    };
    tokio::time::timeout(Duration::from_secs(30), async {
        let (result, ()) = tokio::join!(iterate(&env, &host, transport), change_target);
        result.unwrap();
    })
    .await
    .unwrap();
    assert_eq!(evidence.calls.load(Ordering::SeqCst), 0);
    assert!(
        !dispatch(&env, *host.id).await.is_pending(),
        "only the proven-unsent request may be canceled"
    );
}

/// A lost BMC reply retains the request until an observation or a later retry reconciles it.
#[crate::sqlx_test]
async fn instance_reboot_recovers_lost_reply_without_scout(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    let host = create_managed_host(&env).await;
    let instance = create_instance(&env, &host, false, segment).await;
    // Both observations settle the same durable request: recognized reboot logs avoid a
    // repeated reset; unsupported logs preserve the existing controller's later retry policy.
    for observed_restart in [true, false] {
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

        // Advance the persisted retry boundary without a wall-clock sleep. A fresh handler
        // invocation reconstructs everything from the database, with no prior sender state.
        let mut retryable = attempted.clone();
        let HardwareDispatchOwner::InstanceReboot {
            last_attempt_at, ..
        } = &mut retryable.owner
        else {
            panic!("expected the saved instance reboot");
        };
        *last_attempt_at = Some(Utc::now() - chrono::Duration::days(1));
        // Preserve the production JSON encoding used by conditional owner comparisons.
        sqlx::query("UPDATE machines SET hardware_dispatch = $2 WHERE id = $1")
            .bind(*host.id)
            .bind(sqlx::types::Json(retryable))
            .execute(&env.pool)
            .await
            .unwrap();
        if observed_restart {
            *evidence.log.lock().unwrap() = Some(serde_json::from_value(serde_json::json!({
                "@odata.id": "/redfish/v1/Systems/System/LogServices/EventLog/Entries/test",
                "@odata.type": "#LogEntry.v1_15_0.LogEntry", "Created": Utc::now().to_rfc3339(),
                "EntryType": "Event", "Id": "test", "Message": "Server reset.", "Name": "Reset", "Severity": "OK"
            })).unwrap());
        }
        iterate(&env, &host, transport).await.unwrap();
        let after = machine(&env, *host.id).await;
        let completed = after.status.hardware_dispatch.unwrap();
        assert_eq!(completed.token, attempted.token);
        assert!(!completed.is_pending());
        assert_eq!(
            evidence.calls.load(Ordering::SeqCst),
            if observed_restart { 1 } else { 2 }
        );
        assert_eq!(evidence.nonretry_clients.load(Ordering::SeqCst), 2);
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
    let nic_state = ManagedHostState::NicFirmwareUpdating {
        operation: model::test_support::nic_firmware::operation(),
    };
    // Production NIC admission is a later task. Exercise the existing owning handler's
    // exclusion even if it encounters a NIC operation alongside this earlier command.
    sqlx::query("UPDATE machines SET controller_state = $2 WHERE id = $1")
        .bind(*host.id)
        .bind(sqlx::types::Json(nic_state.clone()))
        .execute(&env.pool)
        .await
        .unwrap();
    env.run_machine_state_controller_iteration().await;
    assert_eq!(dispatch(&env, *host.id).await, queued);
    assert_eq!(machine(&env, *host.id).await.state.value, nic_state);
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
