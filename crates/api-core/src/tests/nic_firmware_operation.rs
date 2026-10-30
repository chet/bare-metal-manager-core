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

//! Exercise admitted NIC work through authenticated reporting, Scout delivery,
//! and persisted controller iterations. Admission itself remains disabled.

use carbide_libmlx_model::device::info::MlxDeviceInfo;
use carbide_libmlx_model::firmware::operation::{
    FirmwareOperation, FirmwareOperationPhase, FirmwareOperationState,
};
use carbide_libmlx_model::firmware::result::FirmwareFlashReport;
use carbide_machine_controller::metrics::MachineMetrics;
use carbide_redfish::libredfish::test_support::RedfishSimAction;
use carbide_secrets::credentials::{CredentialKey, CredentialWriter, Credentials, NicLockdownIkm};
use carbide_uuid::machine::{HostMachineId, MachineId};
use carbide_uuid::network::NetworkSegmentId;
use chrono::Utc;
use libredfish::SystemPowerControl;
use model::controller_outcome::PersistentStateHandlerOutcome;
use model::dpa_interface::{DpaInterfaceType, NewDpaInterface};
use model::machine::machine_search_config::MachineSearchConfig;
use model::machine::nic_firmware::{
    NicFirmwareAccessPolicy, NicFirmwareAccessResult, NicFirmwareAccessStep,
    NicFirmwareBaselineContinuation, NicFirmwareDeviceObservation, NicFirmwareOperation,
    NicFirmwareOutcome, NicFirmwarePhase, NicFirmwarePurpose, NicFirmwareTarget,
};
use model::machine::{
    CleanupContext, CleanupState, HostMachine, InstanceState, MachineLastRebootRequested,
    MachineLastRebootRequestedMode, MachineState, ManagedHostState, ResetRequest,
};
use model::nic_firmware::{NicFirmwareArtifact, NicFirmwareCardSelector};
use model::test_support::nic_firmware as fixtures;
use rpc::forge::forge_server::Forge;
use rpc::forge::{ForgeAgentControlCapability, ForgeAgentControlRequest};
use rpc::forge_agent_control_response as fac;
use rpc::forge_agent_control_response::mlx_device_reconcile_firmware_access::Request as AccessRequest;
use rpc::protos::mlx_device as pb;
use rpc::protos::mlx_device::mlx_device_firmware_access_result::Result as AccessResult;
use rpc::protos::mlx_device::mlx_device_update_firmware_result::Result as FirmwareResult;
use state_controller::db_write_batch::DbWriteBatch;
use state_controller::state_handler::{
    StateHandler, StateHandlerContext, StateHandlerError, StateHandlerOutcome,
};
use tonic::{Code, Request};

use crate::auth::AuthContext;
use crate::tests::common::api_fixtures::instance::single_interface_network_config;
use crate::tests::common::api_fixtures::rpc_instance::RpcInstance;
use crate::tests::common::api_fixtures::{
    TestEnv, TestEnvOverrides, TestManagedHost, create_managed_host,
    create_test_env_with_overrides, get_config, remove_health_report_entry,
    send_health_report_entry,
};
use crate::tests::common::postgres::wait_for_blocked_query;
use crate::tests::common::rpc_builder::{
    InstanceAllocationRequest, InstanceConfig, InstanceConfigExt,
};

fn authenticated<T>(message: T, machine_id: MachineId) -> Request<T> {
    let mut request = Request::new(message);
    let mut auth = AuthContext::default();
    auth.principals.push(
        carbide_authn::middleware::Principal::SpiffeMachineIdentifier(machine_id.to_string()),
    );
    request.extensions_mut().insert(auth);
    request
}

// Generic delivery and persistence must operate with legacy SVPC disabled.
async fn environment(pool: sqlx::PgPool) -> TestEnv {
    let mut config = get_config();
    config.ewethers_config = None;
    create_test_env_with_overrides(
        pool,
        TestEnvOverrides {
            config: Some(config),
            ..Default::default()
        },
    )
    .await
}

// Persist a real allocation owner without advancing the unrelated DPA-enabled
// provisioning fixture; these stories enter NIC recovery with SVPC disabled.
async fn allocate_owner(
    env: &TestEnv,
    host: &TestManagedHost,
    segment: NetworkSegmentId,
) -> RpcInstance {
    let config =
        InstanceConfig::default_tenant_and_os().network(single_interface_network_config(segment));
    let instance = env
        .api
        .allocate_instance(
            InstanceAllocationRequest::builder(false)
                .machine_id(host.host().id)
                .config(config)
                .tonic_request(),
        )
        .await
        .expect("allocate lifecycle owner")
        .into_inner();
    env.one_instance(instance.id.expect("allocated owner has an ID"))
        .await
}

async fn machine(env: &TestEnv, host_id: HostMachineId) -> HostMachine {
    db::machine::find_one(&env.pool, &host_id, Default::default())
        .await
        .expect("read host")
        .expect("host exists")
}

// Read the actual work queue so unchanged state alone cannot hide a redundant
// controller wakeup from an ignored observation.
async fn queued(env: &TestEnv, host_id: HostMachineId) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM machine_state_controller_queued_objects WHERE object_id = $1)",
    )
    .bind(host_id.to_string())
    .fetch_one(&env.pool)
    .await
    .expect("read machine queue")
}

async fn operation(env: &TestEnv, host_id: HostMachineId) -> NicFirmwareOperation {
    let ManagedHostState::NicFirmwareUpdating { operation } =
        machine(env, host_id).await.state.value
    else {
        panic!("expected active NIC operation")
    };
    operation
}

// Tests seed the same durable state that admission will eventually create.
// The real controller and RPC handlers perform every subsequent transition.
async fn seed(env: &TestEnv, host_id: HostMachineId, operation: NicFirmwareOperation) {
    operation.validate().expect("valid admitted fixture");
    let mut txn = env.pool.begin().await.expect("begin fixture transaction");
    db::machine::update_state(
        &mut txn,
        &host_id,
        &ManagedHostState::NicFirmwareUpdating { operation },
    )
    .await
    .expect("seed admitted operation");
    txn.commit().await.expect("commit admitted operation");
}

fn observation(target: &NicFirmwareTarget, result: FirmwareResult) -> pb::MlxObservation {
    pb::MlxObservation {
        firmware_update_result: Some(pb::MlxDeviceUpdateFirmwareResult {
            operation_id: target.operation_id.clone(),
            phase_id: target.phase_id.clone(),
            pci_name: target.pci_name.clone(),
            result: Some(result),
        }),
        ..Default::default()
    }
}

// Only requested phases have success evidence; an omitted optional phase must
// remain absent instead of asserting that an unrequested hardware step ran.
fn complete(target: &NicFirmwareTarget) -> FirmwareOperation {
    FirmwareOperation {
        operation_id: target.operation_id.clone(),
        state: FirmwareOperationState::Complete,
        phase: None,
        report: FirmwareFlashReport {
            flashed: true,
            reset: target.options.reset.then_some(true),
            verified_image: target.options.verify_image.then_some(true),
            verified_version: target.options.verify_version.then_some(true),
            expected_version: Some(target.resolution.entry.firmware.version.clone()),
            observed_version: target
                .options
                .verify_version
                .then(|| target.resolution.entry.firmware.version.clone()),
        },
        diagnostic: None,
    }
}

fn publication(
    host_id: HostMachineId,
    observation: pb::MlxObservation,
) -> pb::PublishMlxObservationReportRequest {
    pb::PublishMlxObservationReportRequest {
        report: Some(pb::MlxObservationReport {
            machine_id: Some(host_id.into()),
            observations: vec![observation],
            ..Default::default()
        }),
    }
}

async fn publish(env: &TestEnv, host_id: HostMachineId, observation: pb::MlxObservation) {
    env.api
        .publish_mlx_observation_report(authenticated(
            publication(host_id, observation),
            host_id.into(),
        ))
        .await
        .expect("publish correlated firmware observation");
}

async fn action(env: &TestEnv, host_id: HostMachineId) -> fac::Action {
    env.api
        .forge_agent_control(authenticated(
            ForgeAgentControlRequest {
                machine_id: Some(host_id.into()),
                capabilities: vec![
                    ForgeAgentControlCapability::NicFirmwareUpdateV1.into(),
                    ForgeAgentControlCapability::NicFirmwareUpdateV2.into(),
                ],
                ..Default::default()
            },
            host_id.into(),
        ))
        .await
        .expect("poll firmware action")
        .into_inner()
        .action
        .expect("action")
}

fn command(action: fac::Action) -> fac::mlx_device_action::Command {
    let fac::Action::MlxAction(mut action) = action else {
        panic!("expected MLX action")
    };
    assert_eq!(action.device_actions.len(), 1);
    action
        .device_actions
        .remove(0)
        .command
        .expect("device command")
}

fn missing(target: &NicFirmwareTarget, instance: &str) -> pb::MlxObservation {
    observation(
        target,
        FirmwareResult::MissingReceipt(pb::FirmwareOperationMissingReceipt {
            supervisor_instance_id: instance.into(),
        }),
    )
}

// Ordinary inventory confirms card identity and version without an operation
// identifier. Correlation remains a separate execution-receipt contract.
fn device(target: &NicFirmwareTarget) -> pb::MlxDeviceInfo {
    let NicFirmwareCardSelector::Mac(mac) = target.selector else {
        panic!("fixture uses MAC")
    };
    pb::MlxDeviceInfo {
        pci_name: target.pci_name.clone(),
        base_mac: mac.to_string(),
        part_number: target.resolution.entry.firmware.part_number.clone(),
        psid: target.resolution.entry.firmware.psid.clone(),
        fw_version_current: target.resolution.entry.firmware.version.clone(),
        device_type: "ConnectX".into(),
        ..Default::default()
    }
}

// Echo the committed request revision exactly as Scout does. A later controller
// step must not consume a delayed reply from an earlier read or mutation.
fn access_observation(
    request: &fac::MlxDeviceReconcileFirmwareAccess,
    target: &NicFirmwareTarget,
    result: AccessResult,
) -> pb::MlxObservation {
    pb::MlxObservation {
        device_info: Some(device(target)),
        firmware_access_result: Some(pb::MlxDeviceFirmwareAccessResult {
            controller_state_version: request.controller_state_version.clone(),
            pci_name: target.pci_name.clone(),
            expected_device: request.expected_device.clone(),
            result: Some(result),
        }),
        ..Default::default()
    }
}

fn status(target: &NicFirmwareTarget, locked: bool) -> AccessResult {
    AccessResult::StatusReport(pb::StatusReport {
        device_id: target.pci_name.clone(),
        status: if locked {
            pb::LockStatus::Locked
        } else {
            pb::LockStatus::Unlocked
        }
        .into(),
        timestamp: Utc::now().to_rfc3339(),
    })
}

async fn access_action(
    env: &TestEnv,
    host_id: HostMachineId,
) -> fac::MlxDeviceReconcileFirmwareAccess {
    let fac::mlx_device_action::Command::ReconcileFirmwareAccess(request) =
        command(action(env, host_id).await)
    else {
        panic!("expected access reconciliation")
    };
    request
}

// Poll and publication may use a different Core replica from the controller;
// the database, not a registered process-local stream, connects those steps.
async fn report_access(
    env: &TestEnv,
    host_id: HostMachineId,
    target: &NicFirmwareTarget,
    result: AccessResult,
) -> fac::MlxDeviceReconcileFirmwareAccess {
    let request = access_action(env, host_id).await;
    publish(env, host_id, access_observation(&request, target, result)).await;
    request
}

async fn inventory(
    env: &TestEnv,
    host_id: HostMachineId,
    devices: Vec<pb::MlxDeviceInfo>,
) -> chrono::DateTime<Utc> {
    let observed_at = Utc::now();
    env.api
        .publish_mlx_device_report(authenticated(
            pb::PublishMlxDeviceReportRequest {
                report: Some(pb::MlxDeviceReport {
                    machine_id: Some(host_id.into()),
                    timestamp: Some(observed_at.into()),
                    devices,
                    ..Default::default()
                }),
            },
            host_id.into(),
        ))
        .await
        .expect("publish ordinary inventory");
    observed_at
}

// Drive authenticated reports into actual control delivery with DPA disabled.
// Submission permission must come from saved evidence and survive helper loss.
#[crate::sqlx_test]
async fn generic_delivery_freezes_helper_before_submission_and_never_replaces_it(
    pool: sqlx::PgPool,
) {
    let env = environment(pool).await;
    let host = create_managed_host(&env).await;
    let host_id = *host.id;
    let mut initial = fixtures::operation();
    initial.reboot_host = false;
    initial.targets[0].options.reset = true;
    initial.targets[0].options.verify_image = true;
    initial.targets[0].options.verify_version = true;
    initial.targets[0].options.reset_level = 4;
    initial.targets[0]
        .options
        .post_reset_verify_retry_interval_seconds = Some(17);
    initial.targets[0].options.post_reset_verify_timeout_seconds = Some(241);
    initial.targets[0].resolution.entry.device_config = Some(NicFirmwareArtifact {
        url: "https://firmware.example.test/device.conf".parse().unwrap(),
        sha256: "cd".repeat(32),
    });
    let target = &initial.targets[0];
    // A late report on an idle host has no active operation to advance. Clear
    // fixture scheduling first so only this RPC can account for a new queue row.
    sqlx::query("DELETE FROM machine_state_controller_queued_objects WHERE object_id = $1")
        .bind(host_id.to_string())
        .execute(&env.pool)
        .await
        .unwrap();
    publish(&env, host_id, missing(target, "first-helper")).await;
    assert!(!queued(&env, host_id).await);
    seed(&env, host_id, initial.clone()).await;
    assert!(!env.api.runtime_config.is_svpc_enabled());

    // An older Scout must not receive a command it cannot execute, and polling
    // cannot consume the initial observation permission.
    let before = machine(&env, host_id).await;
    let error = env
        .api
        .forge_agent_control(authenticated(
            ForgeAgentControlRequest {
                machine_id: Some(host_id.into()),
                capabilities: vec![ForgeAgentControlCapability::NicFirmwareUpdateV1.into()],
                ..Default::default()
            },
            host_id.into(),
        ))
        .await
        .expect_err("V1 cannot execute guarded workflow");
    assert_eq!(error.code(), Code::FailedPrecondition);
    let after = machine(&env, host_id).await;
    assert_eq!(after.state.value, before.state.value);
    assert_eq!(after.state.version, before.state.version);
    let fac::mlx_device_action::Command::ObserveFirmwareUpdate(observe) =
        command(action(&env, host_id).await)
    else {
        panic!("expected checked observation")
    };

    publish(&env, host_id, missing(target, "first-helper")).await;
    assert!(queued(&env, host_id).await);
    let observed = operation(&env, host_id).await;
    assert_eq!(
        observed.targets[0].supervisor_instance_id.as_deref(),
        Some("first-helper")
    );
    assert_eq!(observed.phase, initial.phase);
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    assert_eq!(
        operation(&env, host_id).await.phase,
        NicFirmwarePhase::Unlocking { target_index: 0 }
    );

    report_access(&env, host_id, target, status(target, false)).await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    assert_eq!(
        operation(&env, host_id).await.phase,
        NicFirmwarePhase::UpdatingFirmware { target_index: 0 }
    );
    let submitted_action = action(&env, host_id).await;
    let fac::Action::MlxAction(ref mlx) = submitted_action else {
        panic!("expected MLX delivery")
    };
    assert_eq!(mlx.device_actions[0].pci_name, "0000:01:00.0");
    let fac::mlx_device_action::Command::UpdateFirmware(submit) = command(submitted_action) else {
        panic!("expected guarded update")
    };
    // Assert the complete public payload, including nondefault options. This
    // catches a dropped URL, digest, timing choice or selector at Core delivery.
    let expected_profile = pb::FirmwareFlasherProfile {
        firmware_spec: Some(pb::FirmwareSpec {
            part_number: "test-card".into(),
            psid: "test-psid".into(),
            version: "1.2.3".into(),
        }),
        flash_spec: Some(pb::FlashSpec {
            firmware_url: "https://firmware.example.test/image.bin".into(),
            device_conf_url: Some("https://firmware.example.test/device.conf".into()),
            ..Default::default()
        }),
        flash_options: Some(pb::FlashOptions {
            reset: true,
            verify_image: true,
            verify_version: true,
            reset_level: 4,
            post_reset_verify_retry_interval_seconds: Some(17),
            post_reset_verify_timeout_seconds: Some(241),
        }),
    };
    assert_eq!(
        submit,
        fac::MlxDeviceUpdateFirmware {
            operation_id: "nic-card-0".into(),
            phase_id: "nic-phase-0".into(),
            supervisor_instance_id: Some("first-helper".into()),
            profile: Some(expected_profile),
            expected_digests: Some(pb::FirmwareArtifactDigests {
                image_sha256: "ab".repeat(32),
                configuration_sha256: Some("cd".repeat(32)),
            }),
            expected_device: Some(pb::MlxDeviceSelector {
                identity: Some(pb::mlx_device_selector::Identity::BaseMac(
                    "02:00:00:00:00:01".into()
                )),
            }),
        }
    );
    assert_eq!(observe.operation_id, submit.operation_id);
    assert_eq!(observe.phase_id, submit.phase_id);
    assert_eq!(observe.profile, submit.profile);
    assert_eq!(observe.expected_digests, submit.expected_digests);
    assert_eq!(observe.expected_device, submit.expected_device);

    // Delivery errors are persisted independently of the missing receipt. They
    // remain visible to reconciliation and disappear only when evidence arrives.
    let delivery_error = observation(
        target,
        FirmwareResult::Error(pb::MlxDeviceUpdateFirmwareError {
            code: pb::MlxDeviceUpdateFirmwareErrorCode::Unavailable.into(),
            diagnostic: "helper connection interrupted".into(),
        }),
    );
    publish(&env, host_id, delivery_error.clone()).await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let failed_delivery = operation(&env, host_id).await;
    assert_eq!(
        failed_delivery.phase,
        NicFirmwarePhase::UpdatingFirmware { target_index: 0 }
    );
    assert!(failed_delivery.targets[0].receipt.is_none());
    assert_eq!(
        failed_delivery.targets[0].supervisor_instance_id.as_deref(),
        Some("first-helper")
    );
    assert!(
        failed_delivery.targets[0]
            .diagnostic
            .as_deref()
            .unwrap()
            .contains("helper connection interrupted")
    );

    // Repeating the same delivery failure adds neither evidence nor work, and
    // must leave the frozen helper authorization and controller revision alone.
    let before_repeat = machine(&env, host_id).await;
    sqlx::query("DELETE FROM machine_state_controller_queued_objects WHERE object_id = $1")
        .bind(host_id.to_string())
        .execute(&env.pool)
        .await
        .expect("clear this host's queued work");
    publish(&env, host_id, delivery_error.clone()).await;
    let repeated = machine(&env, host_id).await;
    assert_eq!(repeated.state.value, before_repeat.state.value);
    assert_eq!(repeated.state.version, before_repeat.state.version);
    assert!(!queued(&env, host_id).await);

    // A lost reply may repeat the same request, but a replacement helper's
    // missing receipt cannot change the authorization saved by Core.
    publish(&env, host_id, missing(target, "replacement-helper")).await;
    let uncertain = operation(&env, host_id).await;
    assert_eq!(
        uncertain.targets[0].supervisor_instance_id.as_deref(),
        Some("first-helper")
    );
    assert!(
        uncertain.targets[0]
            .diagnostic
            .as_deref()
            .is_some_and(|diagnostic| diagnostic.contains("instance changed"))
    );
    let fac::mlx_device_action::Command::UpdateFirmware(retry) =
        command(action(&env, host_id).await)
    else {
        panic!("expected same guarded update")
    };
    assert_eq!(
        retry.supervisor_instance_id.as_deref(),
        Some("first-helper")
    );
    let receipt = complete(target);
    publish(
        &env,
        host_id,
        observation(target, FirmwareResult::Receipt(receipt.clone().into())),
    )
    .await;
    let completed = machine(&env, host_id).await;
    assert_eq!(
        operation(&env, host_id).await.targets[0].receipt.as_ref(),
        Some(&receipt)
    );
    assert!(
        operation(&env, host_id).await.targets[0]
            .diagnostic
            .is_none()
    );
    assert!(matches!(
        command(action(&env, host_id).await),
        fac::mlx_device_action::Command::ObserveFirmwareUpdate(_)
    ));
    // Completion is already committed. Duplicate and delayed reports must not
    // schedule another iteration when they cannot change that evidence.
    sqlx::query("DELETE FROM machine_state_controller_queued_objects WHERE object_id = $1")
        .bind(host_id.to_string())
        .execute(&env.pool)
        .await
        .unwrap();
    publish(
        &env,
        host_id,
        observation(target, FirmwareResult::Receipt(receipt.clone().into())),
    )
    .await;
    assert!(!queued(&env, host_id).await);
    publish(&env, host_id, delivery_error).await;
    let after_error = machine(&env, host_id).await;
    assert_eq!(after_error.state.value, completed.state.value);
    assert_eq!(after_error.state.version, completed.state.version);

    // A delayed progress report must not replace already accepted completion.
    let mut delayed = receipt;
    delayed.state = FirmwareOperationState::Running;
    delayed.phase = Some(FirmwareOperationPhase::Prepare);
    delayed.report = FirmwareFlashReport::default();
    publish(
        &env,
        host_id,
        observation(target, FirmwareResult::Receipt(delayed.into())),
    )
    .await;
    let after = machine(&env, host_id).await;
    assert_eq!(after.state.value, completed.state.value);
    assert_eq!(after.state.version, completed.state.version);
    assert!(!queued(&env, host_id).await);
    assert!(
        db::dpa_interface::find_by_machine_id(&env.pool, host_id, Default::default())
            .await
            .unwrap()
            .is_empty()
    );

    // Core can encounter a receipt on its first observation, before it saved a
    // helper token. That running operation may hold the card claim, so recovery
    // must skip access changes and continue observing without submission.
    seed(&env, host_id, initial.clone()).await;
    let mut existing = complete(target);
    existing.state = FirmwareOperationState::Running;
    existing.phase = Some(FirmwareOperationPhase::Prepare);
    existing.report = FirmwareFlashReport::default();
    publish(
        &env,
        host_id,
        observation(target, FirmwareResult::Receipt(existing.clone().into())),
    )
    .await;
    assert_eq!(operation(&env, host_id).await.phase, initial.phase);
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let resumed = operation(&env, host_id).await;
    assert_eq!(
        resumed.phase,
        NicFirmwarePhase::UpdatingFirmware { target_index: 0 }
    );
    assert_eq!(resumed.targets[0].receipt.as_ref(), Some(&existing));
    assert!(resumed.targets[0].supervisor_instance_id.is_none());
    assert!(matches!(
        command(action(&env, host_id).await),
        fac::mlx_device_action::Command::ObserveFirmwareUpdate(_)
    ));
    publish(
        &env,
        host_id,
        missing(target, "replacement-without-journal"),
    )
    .await;
    let missing_after_receipt = operation(&env, host_id).await;
    assert_eq!(
        missing_after_receipt.targets[0].receipt.as_ref(),
        Some(&existing)
    );
    assert!(
        missing_after_receipt.targets[0]
            .supervisor_instance_id
            .is_none()
    );
    assert!(matches!(
        command(action(&env, host_id).await),
        fac::mlx_device_action::Command::ObserveFirmwareUpdate(_)
    ));

    // A retained Running receipt must not hide the missing-journal diagnostic
    // from the persisted controller outcome that operators inspect.
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    assert_eq!(operation(&env, host_id).await, missing_after_receipt);
    let diagnostic = missing_after_receipt.targets[0]
        .diagnostic
        .as_deref()
        .expect("missing receipt diagnostic");
    assert!(
        matches!(machine(&env, host_id).await.controller_state_outcome,
        Some(PersistentStateHandlerOutcome::Wait { reason, .. })
            if reason.contains("Running") && reason.contains(diagnostic))
    );
}

// A report that loses its state CAS must be retried from fresh state. The retry
// must commit its receipt before waking a controller that will read that state.
#[crate::sqlx_test]
async fn retried_firmware_report_commits_before_queueing(pool: sqlx::PgPool) {
    let env = environment(pool).await;
    let host = create_managed_host(&env).await;
    let host_id = *host.id;
    let mut admitted = fixtures::operation();
    admitted.phase = NicFirmwarePhase::UpdatingFirmware { target_index: 0 };
    admitted.targets[0].supervisor_instance_id = Some("observed-helper".into());
    seed(&env, host_id, admitted.clone()).await;
    sqlx::query("DELETE FROM machine_state_controller_queued_objects WHERE object_id = $1")
        .bind(host_id.to_string())
        .execute(&env.pool)
        .await
        .expect("clear this host's queued work");
    let before = machine(&env, host_id).await;
    let receipt = complete(&admitted.targets[0]);
    let report = publication(
        host_id,
        observation(
            &admitted.targets[0],
            FirmwareResult::Receipt(receipt.clone().into()),
        ),
    );

    // Leave a competing revision uncommitted so ingestion reads the old one,
    // then wait until its state-history insert reaches the held lock.
    let mut newer = admitted.clone();
    newer.targets[0].diagnostic = Some("newer controller diagnostic".into());
    let winner_version = before.state.version.increment();
    let mut winner = env.pool.begin().await.unwrap();
    let winner_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(winner.as_mut())
        .await
        .unwrap();
    assert_eq!(
        db::machine::try_update_controller_state(
            &mut winner,
            &host_id,
            before.state.version,
            winner_version,
            &ManagedHostState::NicFirmwareUpdating {
                operation: newer.clone(),
            },
        )
        .await
        .unwrap(),
        db::ConditionalWrite::Applied(())
    );
    let stale_report = env
        .api
        .publish_mlx_observation_report(authenticated(report.clone(), host_id.into()));
    let commit_winner = async {
        wait_for_blocked_query(&env.pool, winner_pid, "INSERT INTO machine_state_history").await;
        assert_eq!(operation(&env, host_id).await, admitted);
        assert!(!queued(&env, host_id).await);
        winner.commit().await.unwrap();
    };
    let (rejected, ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(stale_report, commit_winner)
    })
    .await
    .expect("competing report and state commit finish");
    let error = rejected.expect_err("stale report must be retried");
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("machine changed"));
    assert_eq!(operation(&env, host_id).await, newer);
    assert_eq!(machine(&env, host_id).await.state.version, winner_version);
    assert!(!queued(&env, host_id).await);

    // This fixture has its own database. Pause only its queue insert so an
    // independent connection can prove the retry's receipt already committed.
    let mut queue_gate = env.pool.begin().await.unwrap();
    let queue_gate_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(queue_gate.as_mut())
        .await
        .unwrap();
    sqlx::query("LOCK TABLE machine_state_controller_queued_objects IN SHARE MODE")
        .execute(queue_gate.as_mut())
        .await
        .unwrap();
    let mut accepted = newer;
    accepted.targets[0].receipt = Some(receipt);
    accepted.targets[0].diagnostic = None;
    let retry = env
        .api
        .publish_mlx_observation_report(authenticated(report, host_id.into()));
    let observe_commit = async {
        wait_for_blocked_query(
            &env.pool,
            queue_gate_pid,
            "INSERT INTO machine_state_controller_queued_objects",
        )
        .await;
        assert_eq!(operation(&env, host_id).await, accepted);
        assert_eq!(
            machine(&env, host_id).await.state.version.version_nr(),
            winner_version.version_nr() + 1
        );
        assert!(!queued(&env, host_id).await);
        queue_gate.commit().await.unwrap();
    };
    let (retried, ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(retry, observe_commit)
    })
    .await
    .expect("committed report and queued wakeup finish");
    retried.expect("retry stores the receipt");
    assert_eq!(operation(&env, host_id).await, accepted);
    assert!(queued(&env, host_id).await);
}

// These guards must apply at authenticated ingress before changing the saved
// operation, not merely when constructing or validating local model values.
#[crate::sqlx_test]
async fn generic_reports_reject_foreign_or_contradictory_evidence_without_state_changes(
    pool: sqlx::PgPool,
) {
    let env = environment(pool).await;
    let host = create_managed_host(&env).await;
    let host_id = *host.id;
    let mut admitted = fixtures::operation();
    admitted.phase = NicFirmwarePhase::UpdatingFirmware { target_index: 0 };
    admitted.targets[0].supervisor_instance_id = Some("first-helper".into());
    seed(&env, host_id, admitted.clone()).await;
    let target = &admitted.targets[0];
    let valid = observation(target, FirmwareResult::Receipt(complete(target).into()));

    struct Case {
        name: &'static str,
        change: fn(&mut pb::MlxObservation),
        authenticated_as_host: bool,
        expected_error: Option<Code>,
    }
    let cases = [
        // A valid but delayed phase is ignored, not turned into newer evidence.
        Case {
            name: "stale phase",
            change: |report| {
                report.firmware_update_result.as_mut().unwrap().phase_id = "earlier-phase".into();
            },
            authenticated_as_host: true,
            expected_error: None,
        },
        // The operation token cannot override contradictory device identity.
        Case {
            name: "wrong card",
            change: |report| {
                report.device_info = Some(pb::MlxDeviceInfo {
                    pci_name: "0000:01:00.0".into(),
                    base_mac: "02:00:00:00:00:ff".into(),
                    ..Default::default()
                });
            },
            authenticated_as_host: true,
            expected_error: Some(Code::InvalidArgument),
        },
        // Generic execution must not acquire meaning from legacy access fields.
        Case {
            name: "mixed legacy fields",
            change: |report| {
                report.lock_status = Some(pb::LockStatus::Unlocked.into());
            },
            authenticated_as_host: true,
            expected_error: Some(Code::InvalidArgument),
        },
        // A DPU principal cannot publish a receipt on behalf of its host.
        Case {
            name: "different authenticated machine",
            change: |_| {},
            authenticated_as_host: false,
            expected_error: Some(Code::PermissionDenied),
        },
    ];
    let before = machine(&env, host_id).await;
    for case in cases {
        let mut report = valid.clone();
        (case.change)(&mut report);
        let source = if case.authenticated_as_host {
            host_id.into()
        } else {
            host.dpu().id.into()
        };
        let result = env
            .api
            .publish_mlx_observation_report(authenticated(publication(host_id, report), source))
            .await;
        assert_eq!(
            result.err().map(|error| error.code()),
            case.expected_error,
            "{}",
            case.name
        );
        let after = machine(&env, host_id).await;
        assert_eq!(after.state.value, before.state.value, "{}", case.name);
        assert_eq!(after.state.version, before.state.version, "{}", case.name);
    }
    let error = env
        .api
        .publish_mlx_observation_report(Request::new(publication(host_id, valid)))
        .await
        .expect_err("generic evidence needs an authenticated host");
    assert_eq!(error.code(), Code::PermissionDenied);
    let after = machine(&env, host_id).await;
    assert_eq!(after.state.value, before.state.value);
    assert_eq!(after.state.version, before.state.version);

    // Polling is an admission boundary too: neither an unauthenticated caller
    // nor the attached DPU may obtain the host's guarded firmware command.
    for principal in [None, Some(host.dpu().id.into())] {
        let poll = ForgeAgentControlRequest {
            machine_id: Some(host_id.into()),
            capabilities: vec![ForgeAgentControlCapability::NicFirmwareUpdateV2.into()],
            ..Default::default()
        };
        let request = principal.map_or_else(
            || Request::new(poll.clone()),
            |id| authenticated(poll.clone(), id),
        );
        assert_eq!(
            env.api
                .forge_agent_control(request)
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let after_poll = machine(&env, host_id).await;
        assert_eq!(after_poll.state.value, before.state.value);
        assert_eq!(after_poll.state.version, before.state.version);
    }
}

// Process the second card only after the first settles, then require fresh
// inventory and independent access readback before saving the baseline.
#[crate::sqlx_test]
async fn cards_converge_sequentially_after_inventory_retry_and_access_readback(pool: sqlx::PgPool) {
    let env = environment(pool.clone()).await;
    let replica =
        create_test_env_with_overrides(pool, TestEnvOverrides::no_network_segments()).await;
    let host = create_managed_host(&env).await;
    let host_id = *host.id;
    let mut admitted = fixtures::operation();
    admitted.reboot_host = false;
    admitted.targets[0].options.reset = true;
    admitted.targets[0].options.verify_version = true;
    let mut second = admitted.targets[0].clone();
    second.selector = NicFirmwareCardSelector::Mac("02:00:00:00:00:02".parse().unwrap());
    second.pci_name = "0000:02:00.0".into();
    second.operation_id = "nic-card-1".into();
    second.supervisor_instance_id = None;
    admitted.targets.push(second);
    let NicFirmwareCardSelector::Mac(mac_address) = admitted.targets[0].selector else {
        unreachable!()
    };
    admitted.targets[0].original_access = NicFirmwareAccessPolicy::Locked {
        ikm_version: 7,
        mac_address,
    };
    assert!(replica.api.runtime_config.is_svpc_enabled());
    for core in [&env, &replica] {
        core.test_credential_manager
            .set_credentials(
                &CredentialKey::NicLockdownIkm {
                    credential_type: NicLockdownIkm::SiteWide { version: 7 },
                },
                &Credentials::UsernamePassword {
                    username: String::new(),
                    password: "test-ikm".into(),
                },
            )
            .await
            .unwrap();
    }
    let lock_key = crate::dpa::lockdown::build_resolved_lockdown_key(
        host_id,
        mac_address,
        &*env.test_credential_manager,
        7,
    )
    .await
    .unwrap();
    // Updating these two cards must retain a different card's accepted baseline.
    let mut previous_baseline = fixtures::baseline();
    let retained = &mut previous_baseline.targets[0];
    retained.selector = NicFirmwareCardSelector::Mac("02:00:00:00:00:03".parse().unwrap());
    retained.pci_name = "0000:03:00.0".into();
    retained.observation.device.pci_name = retained.pci_name.clone();
    retained.observation.device.base_mac = Some("02:00:00:00:00:03".parse().unwrap());
    sqlx::query("UPDATE machines SET nic_firmware_baseline = $1 WHERE id = $2")
        .bind(sqlx::types::Json(&previous_baseline))
        .bind(host_id)
        .execute(&env.pool)
        .await
        .unwrap();
    seed(&env, host_id, admitted.clone()).await;
    let power_before = env.redfish_sim.timepoint();

    // Start with the first card, including a failed unlock. A command failure
    // may have changed hardware, so the next committed step is another read.
    // A mixed batch has two independent destinations: generic progress stays
    // on the machine, while the legacy profile observation stays on its DPA.
    let mut txn = env.pool.begin().await.unwrap();
    let legacy = db::dpa_interface::persist(
        NewDpaInterface {
            machine_id: host_id,
            mac_address: "02:00:00:00:00:99".parse().unwrap(),
            device_type: "SuperNIC".into(),
            pci_name: "0000:99:00.0".into(),
            device_description: None,
            interface_type: DpaInterfaceType::Svpc,
        },
        &mut txn,
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    let legacy_observation = pb::MlxObservation {
        device_info: Some(pb::MlxDeviceInfo {
            base_mac: legacy.mac_address.to_string(),
            pci_name: legacy.pci_name.clone(),
            ..Default::default()
        }),
        profile_name: Some("legacy-profile".into()),
        ..Default::default()
    };
    // Authentication also protects legacy-only batches. Moving this check
    // inside generic-result routing would let the DPU change its host's card.
    let error = replica
        .api
        .publish_mlx_observation_report(authenticated(
            publication(host_id, legacy_observation.clone()),
            host.dpu().id.into(),
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
    let unchanged = db::dpa_interface::find_by_machine_id(&env.pool, host_id, Default::default())
        .await
        .unwrap();
    assert_eq!(unchanged.len(), 1);
    assert_eq!(unchanged[0].id, legacy.id);
    assert_eq!(
        unchanged[0].controller_state.value,
        legacy.controller_state.value
    );
    assert_eq!(
        unchanged[0].controller_state.version,
        legacy.controller_state.version
    );
    assert_eq!(
        serde_json::to_value(&unchanged[0].card_state).unwrap(),
        serde_json::to_value(&legacy.card_state).unwrap()
    );
    replica
        .api
        .publish_mlx_observation_report(authenticated(
            pb::PublishMlxObservationReportRequest {
                report: Some(pb::MlxObservationReport {
                    machine_id: Some(host_id.into()),
                    observations: vec![missing(&admitted.targets[0], "helper"), legacy_observation],
                    ..Default::default()
                }),
            },
            host_id.into(),
        ))
        .await
        .unwrap();
    let cards = db::dpa_interface::find_by_machine_id(&env.pool, host_id, Default::default())
        .await
        .unwrap();
    assert_eq!(cards.len(), 1);
    assert_eq!(cards[0].id, legacy.id);
    let legacy_state = cards[0].card_state.as_ref().unwrap();
    assert_eq!(legacy_state.profile.as_deref(), Some("legacy-profile"));
    assert!(legacy_state.firmware_operation.is_none());
    assert_eq!(
        operation(&env, host_id).await.targets[0]
            .supervisor_instance_id
            .as_deref(),
        Some("helper")
    );
    env.run_machine_state_controller_iteration_without_requeue()
        .await;

    // A failed status read stays in Observe, but its result must be consumed
    // so the next poll can request status again instead of remaining Noop.
    report_access(
        &replica,
        host_id,
        &admitted.targets[0],
        AccessResult::Error(pb::MlxDeviceStreamError {
            message: "status temporarily unavailable".into(),
            ..Default::default()
        }),
    )
    .await;
    let failed_read = operation(&env, host_id).await;
    assert_eq!(failed_read.access.step, NicFirmwareAccessStep::Observe);
    assert_eq!(
        failed_read.access.result,
        Some(NicFirmwareAccessResult::Error {
            diagnostic: "status temporarily unavailable".into(),
        })
    );
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let retrying_read = operation(&env, host_id).await;
    assert_eq!(retrying_read.phase, failed_read.phase);
    assert_eq!(retrying_read.access.step, NicFirmwareAccessStep::Observe);
    assert!(retrying_read.access.result.is_none());
    let stale_read = access_action(&replica, host_id).await;
    assert!(matches!(
        &stale_read.request,
        Some(AccessRequest::Status(_))
    ));
    publish(
        &replica,
        host_id,
        access_observation(
            &stale_read,
            &admitted.targets[0],
            status(&admitted.targets[0], true),
        ),
    )
    .await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    assert_eq!(
        operation(&env, host_id).await.access.step,
        NicFirmwareAccessStep::Change
    );
    let unlock = access_action(&replica, host_id).await;
    assert_eq!(
        unlock.request,
        Some(AccessRequest::Unlock(pb::MlxDeviceLockdownUnlockRequest {
            device_id: admitted.targets[0].pci_name.clone(),
            key: lock_key.clone(),
        }))
    );
    publish(
        &replica,
        host_id,
        access_observation(
            &unlock,
            &admitted.targets[0],
            AccessResult::Error(pb::MlxDeviceStreamError {
                message: "unlock completion uncertain".into(),
                ..Default::default()
            }),
        ),
    )
    .await;
    assert_eq!(action(&replica, host_id).await, fac::Action::noop());
    assert_eq!(
        operation(&env, host_id).await.access.result,
        Some(NicFirmwareAccessResult::Error {
            diagnostic: "unlock completion uncertain".into(),
        })
    );
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let checking_unlock = machine(&env, host_id).await;
    assert_eq!(
        operation(&env, host_id).await.access.step,
        NicFirmwareAccessStep::Observe
    );
    assert!(operation(&env, host_id).await.access.result.is_none());

    // This pre-mutation status has the right device but the wrong revision.
    // Accepting it would bypass the independent read after the uncertain unlock.
    publish(
        &replica,
        host_id,
        access_observation(
            &stale_read,
            &admitted.targets[0],
            status(&admitted.targets[0], false),
        ),
    )
    .await;
    let after_stale = machine(&env, host_id).await;
    assert_eq!(after_stale.state.value, checking_unlock.state.value);
    assert_eq!(after_stale.state.version, checking_unlock.state.version);
    report_access(
        &replica,
        host_id,
        &admitted.targets[0],
        status(&admitted.targets[0], false),
    )
    .await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    assert_eq!(
        operation(&env, host_id).await.phase,
        NicFirmwarePhase::UpdatingFirmware { target_index: 0 }
    );
    assert!(matches!(
        command(action(&replica, host_id).await),
        fac::mlx_device_action::Command::UpdateFirmware(_)
    ));

    publish(
        &env,
        host_id,
        observation(
            &admitted.targets[0],
            FirmwareResult::Receipt(complete(&admitted.targets[0]).into()),
        ),
    )
    .await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let next = operation(&env, host_id).await;
    assert_eq!(
        next.phase,
        NicFirmwarePhase::ObservingSupervisor { target_index: 1 }
    );
    assert!(next.targets[0].is_complete());
    assert!(next.targets[1].receipt.is_none());

    publish(&replica, host_id, missing(&admitted.targets[1], "helper")).await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    report_access(
        &replica,
        host_id,
        &admitted.targets[1],
        status(&admitted.targets[1], false),
    )
    .await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    assert_eq!(
        operation(&env, host_id).await.phase,
        NicFirmwarePhase::UpdatingFirmware { target_index: 1 }
    );
    publish(
        &env,
        host_id,
        observation(
            &admitted.targets[1],
            FirmwareResult::Receipt(complete(&admitted.targets[1]).into()),
        ),
    )
    .await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let awaiting_inventory = operation(&env, host_id).await;
    assert!(matches!(
        awaiting_inventory.phase,
        NicFirmwarePhase::Reobserving { .. }
    ));
    assert!(
        awaiting_inventory
            .targets
            .iter()
            .all(NicFirmwareTarget::is_complete)
    );

    // A fresh report is not enough: its version must match every frozen target.
    // FAC keeps requesting ordinary inventory until a later report converges.
    assert_eq!(
        action(&replica, host_id).await,
        fac::Action::MlxDeviceInfoReport(pb::MlxDeviceInfoReportRequest { filters: None })
    );
    let mut wrong_versions: Vec<_> = admitted.targets.iter().map(device).collect();
    wrong_versions[0].fw_version_current = "0.9.0".into();
    inventory(&replica, host_id, wrong_versions).await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    assert_eq!(operation(&env, host_id).await, awaiting_inventory);
    assert!(
        matches!(machine(&env, host_id).await.controller_state_outcome,
        Some(PersistentStateHandlerOutcome::Wait { reason, .. }) if reason.contains("exact identity and firmware version"))
    );
    assert!(matches!(
        action(&replica, host_id).await,
        fac::Action::MlxDeviceInfoReport(_)
    ));
    let observed_at = inventory(
        &replica,
        host_id,
        admitted.targets.iter().map(device).collect(),
    )
    .await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let restoring = operation(&env, host_id).await;
    assert_eq!(
        restoring.phase,
        NicFirmwarePhase::RestoringAccess {
            target_index: 0,
            outcome: NicFirmwareOutcome::Succeeded
        }
    );
    assert!(restoring.targets.iter().all(|target| {
        target
            .observation
            .as_ref()
            .is_some_and(|o| o.observed_at == observed_at)
    }));

    // A successful lock command is not independent readback. The baseline and
    // Ready continuation must remain absent until a later status request agrees.
    report_access(
        &replica,
        host_id,
        &admitted.targets[0],
        status(&admitted.targets[0], false),
    )
    .await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let lock = report_access(
        &replica,
        host_id,
        &admitted.targets[0],
        AccessResult::Changed(pb::mlx_device_firmware_access_result::Changed {}),
    )
    .await;
    assert_eq!(
        lock.request,
        Some(AccessRequest::Lock(pb::MlxDeviceLockdownLockRequest {
            device_id: admitted.targets[0].pci_name.clone(),
            key: lock_key,
        }))
    );
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let waiting_readback = operation(&env, host_id).await;
    assert_eq!(waiting_readback.phase, restoring.phase);
    assert_eq!(waiting_readback.access.step, NicFirmwareAccessStep::Observe);
    assert!(waiting_readback.access.result.is_none());
    assert_eq!(
        machine(&env, host_id).await.status.nic_firmware_baseline,
        Some(previous_baseline.clone())
    );
    report_access(
        &replica,
        host_id,
        &admitted.targets[0],
        status(&admitted.targets[0], true),
    )
    .await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    assert_eq!(
        operation(&env, host_id).await.phase,
        NicFirmwarePhase::RestoringAccess {
            target_index: 1,
            outcome: NicFirmwareOutcome::Succeeded
        }
    );
    report_access(
        &replica,
        host_id,
        &admitted.targets[1],
        status(&admitted.targets[1], false),
    )
    .await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let finished = machine(&env, host_id).await;
    assert_eq!(finished.state.value, ManagedHostState::Ready);
    let baseline = finished
        .status
        .nic_firmware_baseline
        .expect("baseline committed with continuation");
    assert_eq!(baseline.operation_id, admitted.operation_id);
    assert_eq!(baseline.targets.len(), 3);
    assert_eq!(baseline.targets[0], previous_baseline.targets[0]);
    for (saved, target) in baseline.targets[1..].iter().zip(admitted.targets) {
        assert_eq!(saved.resolution, target.resolution);
        assert_eq!(saved.access, target.original_access);
        assert_eq!(
            saved.observation.device,
            MlxDeviceInfo::try_from(device(&target)).unwrap()
        );
    }
    assert!(
        env.redfish_sim
            .actions_since(&power_before)
            .all_hosts()
            .is_empty(),
        "supervisor reset must not add a Core reset"
    );
}

// All completed cards share one host activation. Retaining both its request
// and confirmation prevents ordinary restart verification from repeating it.
#[crate::sqlx_test]
async fn completed_burns_are_retained_before_one_planned_host_reboot(pool: sqlx::PgPool) {
    let mut config = get_config();
    config.ewethers_config = None;
    config.machine_state_controller.failure_retry_time = chrono::Duration::seconds(1);
    let env = create_test_env_with_overrides(
        pool,
        TestEnvOverrides {
            config: Some(config),
            ..Default::default()
        },
    )
    .await;
    let host = create_managed_host(&env).await;
    let host_id = *host.id;
    let mut admitted = fixtures::operation();
    let mut second = fixtures::target();
    second.selector = NicFirmwareCardSelector::Mac("02:00:00:00:00:02".parse().unwrap());
    second.pci_name = "0000:02:00.0".into();
    second.operation_id = "nic-card-1".into();
    admitted.targets.push(second);
    for target in &mut admitted.targets {
        target.supervisor_instance_id = Some("pre-reboot-helper".into());
        target.receipt = Some(complete(target));
    }
    admitted.reboot_host = true;
    admitted.phase = NicFirmwarePhase::UpdatingFirmware { target_index: 1 };
    let before = env.redfish_sim.timepoint();

    // Host activation must wait for the next card even when the first card is
    // complete. Its receipt survives progression to observing that next helper.
    let mut first_done = admitted.clone();
    first_done.phase = NicFirmwarePhase::UpdatingFirmware { target_index: 0 };
    first_done.targets[1].receipt = None;
    first_done.targets[1].supervisor_instance_id = None;
    seed(&env, host_id, first_done.clone()).await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    first_done.phase = NicFirmwarePhase::ObservingSupervisor { target_index: 1 };
    assert_eq!(operation(&env, host_id).await, first_done);
    assert!(
        env.redfish_sim
            .actions_since(&before)
            .all_hosts()
            .is_empty()
    );

    // The final card has not settled yet. Even the earlier completed burn must
    // remain on this host without triggering activation ahead of its peer.
    let mut pending = admitted.clone();
    pending.targets[1].receipt = None;
    seed(&env, host_id, pending.clone()).await;
    sqlx::query("UPDATE machines SET last_reboot_time = NULL WHERE id = $1")
        .bind(host_id)
        .execute(&env.pool)
        .await
        .expect("clear simulated reboot feedback");
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    assert_eq!(operation(&env, host_id).await, pending);
    assert!(
        env.redfish_sim
            .actions_since(&before)
            .all_hosts()
            .is_empty()
    );

    publish(
        &env,
        host_id,
        observation(
            &admitted.targets[1],
            FirmwareResult::Receipt(complete(&admitted.targets[1]).into()),
        ),
    )
    .await;
    // Fail an actual simulated Redfish request, then reload its persisted
    // attempt marker. An immediate later iteration must observe the cooldown.
    env.redfish_sim.fail_next_power_action(
        SystemPowerControl::ForceRestart,
        "BMC temporarily unavailable",
    );
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let ready_to_reboot = operation(&env, host_id).await;
    let NicFirmwarePhase::Rebooting {
        started_at,
        last_attempt_at: None,
    } = ready_to_reboot.phase
    else {
        panic!("activation intent must commit before reboot")
    };
    assert_eq!(ready_to_reboot.targets, admitted.targets);
    assert!(
        env.redfish_sim
            .actions_since(&before)
            .all_hosts()
            .is_empty()
    );

    // A newer state version revokes this snapshot's reboot authorization even
    // when its operation is unchanged. Reject it before issuing a power action.
    let mut txn = env.db_txn().await;
    let mut stale_snapshot = host.snapshot(&mut txn).await;
    txn.commit().await.unwrap();
    let stale_state = stale_snapshot.host_snapshot.state.value.clone();
    seed(&env, host_id, ready_to_reboot.clone()).await;
    let winner = machine(&env, host_id).await;
    assert_ne!(
        winner.state.version,
        stale_snapshot.host_snapshot.state.version
    );
    let mut services = env.machine_state_handler_services();
    let mut metrics = MachineMetrics::default();
    let mut pending_db_writes = DbWriteBatch::new();
    let mut ctx = StateHandlerContext {
        services: &mut services,
        metrics: &mut metrics,
        pending_db_writes: &mut pending_db_writes,
    };
    let rejected = env
        .machine_state_handler
        .handle_object_state(&host_id, &mut stale_snapshot, &stale_state, &mut ctx)
        .await;
    assert!(matches!(
        rejected,
        Err(StateHandlerError::IterationInvalidated { .. })
    ));
    let after_rejection = machine(&env, host_id).await;
    assert_eq!(after_rejection.state.value, winner.state.value);
    assert_eq!(after_rejection.state.version, winner.state.version);
    assert_eq!(operation(&env, host_id).await, ready_to_reboot);
    assert!(
        env.redfish_sim
            .actions_since(&before)
            .all_hosts()
            .is_empty()
    );

    // An otherwise eligible activation must honor the existing health override
    // without consuming an attempt or the retained successful burn receipts.
    let mut block_reboot = health_report::HealthReport::empty("test-nic-reboot-block".into());
    block_reboot.alerts.push(health_report::HealthProbeAlert {
        id: "TestNicRebootBlock".parse().unwrap(),
        target: None,
        in_alert_since: None,
        message: "hold firmware activation".into(),
        tenant_message: None,
        classifications: vec![
            health_report::HealthAlertClassification::stop_reboot_for_automatic_recovery_from_state_machine(),
        ],
    });
    send_health_report_entry(
        &env,
        &host_id.into(),
        (block_reboot, health_report::HealthReportApplyMode::Merge),
    )
    .await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let blocked = machine(&env, host_id).await;
    assert_eq!(operation(&env, host_id).await, ready_to_reboot);
    assert_eq!(blocked.state.version, after_rejection.state.version);
    assert_eq!(
        blocked.status.last_reboot_requested,
        after_rejection.status.last_reboot_requested
    );
    assert!(matches!(
        blocked.controller_state_outcome,
        Some(PersistentStateHandlerOutcome::Wait { reason, .. })
            if reason == "NIC firmware activation reboot is blocked by a health override"
    ));
    assert!(
        env.redfish_sim
            .actions_since(&before)
            .all_hosts()
            .is_empty()
    );
    remove_health_report_entry(&env, &host_id.into(), "test-nic-reboot-block".into()).await;

    // Missing DPU snapshots must not strand already-authorized firmware work.
    // Keep the association, omit its snapshot, and reach the actual reboot
    // attempt so the persisted marker proves recovery passed that guard.
    let mut txn = env.db_txn().await;
    let mut recovery_snapshot = host.snapshot(&mut txn).await;
    txn.commit().await.unwrap();
    assert!(
        !recovery_snapshot
            .host_snapshot
            .associated_dpu_machine_ids()
            .is_empty()
    );
    recovery_snapshot.dpu_snapshots.clear();
    let recovery_state = recovery_snapshot.host_snapshot.state.value.clone();
    let mut recovery_services = env.machine_state_handler_services();
    let mut recovery_metrics = MachineMetrics::default();
    let mut recovery_writes = DbWriteBatch::new();
    let mut recovery_ctx = StateHandlerContext {
        services: &mut recovery_services,
        metrics: &mut recovery_metrics,
        pending_db_writes: &mut recovery_writes,
    };
    let recovery = env
        .machine_state_handler
        .handle_object_state(
            &host_id,
            &mut recovery_snapshot,
            &recovery_state,
            &mut recovery_ctx,
        )
        .await;
    assert!(matches!(
        recovery,
        Ok(StateHandlerOutcome::Wait { reason, .. })
            if reason.contains("waiting for Scout to report")
    ));
    let mut txn = env.db_txn().await;
    recovery_writes.apply_all(&mut txn).await.unwrap();
    txn.commit().await.unwrap();
    let reboot_requested = operation(&env, host_id).await;
    assert!(matches!(
        reboot_requested.phase,
        NicFirmwarePhase::Rebooting {
            last_attempt_at: Some(_),
            ..
        }
    ));
    assert_eq!(reboot_requested.targets, admitted.targets);
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    assert_eq!(operation(&env, host_id).await, reboot_requested);
    assert_eq!(
        env.redfish_sim.actions_since(&before).all_hosts(),
        vec![RedfishSimAction::PowerFailed(
            SystemPowerControl::ForceRestart
        )]
    );

    // Advance only the persisted clock boundary, not the receipt or owner. The
    // next iteration must retry the failed request and retain every burn receipt.
    let mut retry_due = reboot_requested;
    retry_due.phase = NicFirmwarePhase::Rebooting {
        started_at,
        last_attempt_at: Some(Utc::now() - chrono::Duration::minutes(4)),
    };
    seed(&env, host_id, retry_due).await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let retried = operation(&env, host_id).await;
    let NicFirmwarePhase::Rebooting {
        last_attempt_at: Some(latest_attempt),
        ..
    } = retried.phase
    else {
        panic!("retry attempt must remain durable")
    };
    assert!(latest_attempt > started_at);
    assert_eq!(retried.targets, admitted.targets);
    assert_eq!(
        env.redfish_sim.actions_since(&before).all_hosts(),
        vec![
            RedfishSimAction::PowerFailed(SystemPowerControl::ForceRestart),
            RedfishSimAction::Power(SystemPowerControl::ForceRestart),
        ]
    );

    let pending_reboot = machine(&env, host_id)
        .await
        .status
        .last_reboot_requested
        .expect("activation reboot request persisted");
    assert_eq!(pending_reboot.mode, MachineLastRebootRequestedMode::Reboot);
    assert_eq!(pending_reboot.restart_verified, Some(false));
    // Neither another host's credential nor the unbound simulator service can
    // provide this host's activation evidence. Rejection must preserve the
    // durable timestamp and request, not merely the visible controller phase.
    let before_report = machine(&env, host_id).await;
    let foreign_host: MachineId = "fm100hseddco33hvlofuqvg543p6p9aj60g76q5cq491g9m9tgtf2dk0530"
        .parse()
        .unwrap();
    assert_ne!(foreign_host, host_id.into());
    let report = rpc::forge::MachineRebootCompletedRequest {
        machine_id: Some(host_id.into()),
    };
    for principal in [
        carbide_authn::middleware::Principal::SpiffeMachineIdentifier(foreign_host.to_string()),
        carbide_authn::middleware::Principal::SpiffeServiceIdentifier("machine-a-tron".into()),
    ] {
        let mut request = Request::new(report.clone());
        let mut auth = AuthContext::default();
        auth.principals.push(principal);
        request.extensions_mut().insert(auth);
        assert_eq!(
            env.api.reboot_completed(request).await.unwrap_err().code(),
            Code::PermissionDenied
        );
        let rejected = machine(&env, host_id).await;
        assert_eq!(
            rejected.status.last_reboot_time,
            before_report.status.last_reboot_time
        );
        assert_eq!(
            rejected.status.last_reboot_requested,
            before_report.status.last_reboot_requested
        );
        assert_eq!(rejected.state.value, before_report.state.value);
        assert_eq!(rejected.state.version, before_report.state.version);
    }
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    assert_eq!(operation(&env, host_id).await, retried);

    env.api
        .reboot_completed(authenticated(report, host_id.into()))
        .await
        .unwrap();
    let reported_at = machine(&env, host_id)
        .await
        .status
        .last_reboot_time
        .unwrap();
    assert!(reported_at > started_at);
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let activated = operation(&env, host_id).await;
    assert_eq!(
        activated.phase,
        NicFirmwarePhase::Reobserving {
            not_before: reported_at
        }
    );
    assert_eq!(activated.targets, admitted.targets);
    assert!(matches!(
        action(&env, host_id).await,
        fac::Action::MlxDeviceInfoReport(_)
    ));
    // Verify the exact recorded request, preserving its timestamp and mode.
    // Otherwise the generic verifier can retry it after this workflow returns.
    assert_eq!(
        machine(&env, host_id).await.status.last_reboot_requested,
        Some(MachineLastRebootRequested {
            restart_verified: Some(true),
            verification_attempts: Some(0),
            ..pending_reboot
        }),
    );
    assert_eq!(
        env.redfish_sim.actions_since(&before).all_hosts(),
        vec![
            RedfishSimAction::PowerFailed(SystemPowerControl::ForceRestart),
            RedfishSimAction::Power(SystemPowerControl::ForceRestart)
        ],
    );
}

// A response returned after a newer recovery decision cannot replace that
// decision, even when it reports the correct card and firmware version.
#[crate::sqlx_test]
async fn delayed_inventory_cannot_overwrite_a_newer_controller_state(pool: sqlx::PgPool) {
    let env = environment(pool).await;
    let host = create_managed_host(&env).await;
    let host_id = *host.id;
    let mut admitted = fixtures::operation();
    admitted.phase = NicFirmwarePhase::Reobserving {
        not_before: Utc::now() - chrono::Duration::seconds(1),
    };
    admitted.targets[0].receipt = Some(complete(&admitted.targets[0]));
    seed(&env, host_id, admitted.clone()).await;
    let mut newer = admitted.clone();
    newer.phase = NicFirmwarePhase::Failed {
        reason: "newer recovery decision".into(),
    };

    // Delivery reads one committed state; before its reply arrives, recovery
    // commits another. Ordinary inventory still updates, but cannot resurrect
    // the superseded operation or save a baseline for it.
    assert!(matches!(
        action(&env, host_id).await,
        fac::Action::MlxDeviceInfoReport(_)
    ));
    seed(&env, host_id, newer.clone()).await;
    let observed_at = inventory(&env, host_id, vec![device(&admitted.targets[0])]).await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    assert_eq!(operation(&env, host_id).await, newer);
    assert_eq!(
        machine(&env, host_id)
            .await
            .status
            .mlx_device_observation
            .unwrap()
            .observed_at,
        observed_at
    );
    assert!(
        machine(&env, host_id)
            .await
            .status
            .nic_firmware_baseline
            .is_none()
    );
}

// Matching ordinary inventory cannot satisfy a required verification phase
// that failed after a burn; uncertain hardware effects also forbid restoration.
#[crate::sqlx_test]
async fn matching_inventory_does_not_erase_required_verification_failure(pool: sqlx::PgPool) {
    let env = environment(pool).await;
    let host = create_managed_host(&env).await;
    let host_id = *host.id;
    let mut admitted = fixtures::operation();
    admitted.phase = NicFirmwarePhase::UpdatingFirmware { target_index: 0 };
    admitted.targets[0].supervisor_instance_id = Some("helper".into());
    admitted.targets[0].options.verify_version = true;
    seed(&env, host_id, admitted.clone()).await;
    let target = &admitted.targets[0];
    let mut receipt = complete(target);
    receipt.state = FirmwareOperationState::ManualIntervention;
    receipt.phase = Some(FirmwareOperationPhase::VerifyVersion);
    receipt.diagnostic = Some("version query timed out".into());
    receipt.report.verified_version = Some(false);
    receipt.report.observed_version = None;
    publish(
        &env,
        host_id,
        observation(target, FirmwareResult::Receipt(receipt.clone().into())),
    )
    .await;
    inventory(&env, host_id, vec![device(target)]).await;

    // An operator's ordinary host-reset request must not overtake unsettled
    // firmware work. Keep it pending until recovery establishes a safe exit.
    let reset_request = ResetRequest {
        requested_at: Utc::now(),
        initiator: "test-operator".into(),
        started_at: None,
        ignore_cleanup: false,
    };
    sqlx::query("UPDATE machines SET reset_requested = $1 WHERE id = $2")
        .bind(sqlx::types::Json(&reset_request))
        .bind(host_id)
        .execute(&env.pool)
        .await
        .unwrap();
    let power_before = env.redfish_sim.timepoint();

    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let uncertain = operation(&env, host_id).await;
    assert_eq!(uncertain.phase, admitted.phase);
    assert_eq!(uncertain.targets[0].receipt, Some(receipt));
    assert!(uncertain.targets[0].observation.is_none());
    assert!(matches!(
        command(action(&env, host_id).await),
        fac::mlx_device_action::Command::ObserveFirmwareUpdate(_)
    ));
    assert!(
        matches!(machine(&env, host_id).await.controller_state_outcome,
        Some(PersistentStateHandlerOutcome::Error { err, .. }) if err.contains("without successful requested-phase evidence"))
    );
    let retained_reset = machine(&env, host_id).await.reset_requested.unwrap();
    assert_eq!(retained_reset.requested_at, reset_request.requested_at);
    assert!(retained_reset.started_at.is_none());
    assert!(
        env.redfish_sim
            .actions_since(&power_before)
            .all_hosts()
            .is_empty()
    );
    assert!(
        machine(&env, host_id)
            .await
            .status
            .nic_firmware_baseline
            .is_none()
    );
}

// Allocation deletion stops submission, but release must wait for execution
// evidence and independent access restoration before resuming its normal path.
#[crate::sqlx_test]
async fn lost_allocation_owner_stops_submission_but_retains_pending_execution(pool: sqlx::PgPool) {
    let env = environment(pool).await;
    let host = create_managed_host(&env).await;
    let host_id = *host.id;
    let segment = env.create_vpc_and_tenant_segment().await;
    let owner = allocate_owner(&env, &host, segment).await;
    let mut admitted = fixtures::operation();
    admitted.purpose = NicFirmwarePurpose::Allocation {
        instance_id: owner.id(),
        config_version: owner.config_version(),
    };
    admitted.phase = NicFirmwarePhase::UpdatingFirmware { target_index: 0 };
    admitted.targets[0].supervisor_instance_id = Some("helper".into());
    seed(&env, host_id, admitted.clone()).await;
    // The live attached owner permits guarded submission; deletion below must
    // change delivery to observation without losing the already authorized work.
    assert!(matches!(
        command(action(&env, host_id).await),
        fac::mlx_device_action::Command::UpdateFirmware(update)
            if update.operation_id == admitted.targets[0].operation_id
                && update.supervisor_instance_id == admitted.targets[0].supervisor_instance_id
    ));

    // Losing the owner before submission also revokes a pending unlock. Use a
    // valid locked-card context so the ownership guard, not missing key data,
    // is what suppresses that mutation.
    let mut before_submission = admitted.clone();
    before_submission.phase = NicFirmwarePhase::Unlocking { target_index: 0 };
    before_submission.access.step = NicFirmwareAccessStep::Change;
    let NicFirmwareCardSelector::Mac(mac_address) = before_submission.targets[0].selector else {
        unreachable!()
    };
    before_submission.targets[0].original_access = NicFirmwareAccessPolicy::Locked {
        ikm_version: 7,
        mac_address,
    };
    env.test_credential_manager
        .set_credentials(
            &CredentialKey::NicLockdownIkm {
                credential_type: NicLockdownIkm::SiteWide { version: 7 },
            },
            &Credentials::UsernamePassword {
                username: String::new(),
                password: "test-ikm".into(),
            },
        )
        .await
        .unwrap();
    seed(&env, host_id, before_submission.clone()).await;
    let mut txn = env.pool.begin().await.unwrap();
    db::instance::mark_as_deleted(owner.id(), txn.as_mut())
        .await
        .unwrap();
    txn.commit().await.unwrap();

    assert_eq!(action(&env, host_id).await, fac::Action::noop());
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let stopped_before_submission = operation(&env, host_id).await;
    assert!(matches!(
        stopped_before_submission.phase,
        NicFirmwarePhase::RestoringAccess {
            target_index: 0,
            outcome: NicFirmwareOutcome::Failed { reason },
        } if reason.contains("lifecycle owner changed")
    ));
    assert_eq!(stopped_before_submission.targets, before_submission.targets);
    assert_eq!(stopped_before_submission.purpose, admitted.purpose);
    assert_eq!(
        stopped_before_submission.access.step,
        NicFirmwareAccessStep::Observe
    );
    assert!(stopped_before_submission.access.result.is_none());

    // Exercise the later boundary separately: submission may already have
    // happened, so the same deleted owner no longer permits immediate recovery.
    seed(&env, host_id, admitted.clone()).await;
    // The allocated owner is gone, but Core cannot assume the already
    // authorized submit had no effect. Poll for evidence without another submit.
    assert!(matches!(
        command(action(&env, host_id).await),
        fac::mlx_device_action::Command::ObserveFirmwareUpdate(_)
    ));
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    assert_eq!(operation(&env, host_id).await, admitted);
    assert!(
        matches!(machine(&env, host_id).await.controller_state_outcome,
        Some(PersistentStateHandlerOutcome::Wait { reason, .. })
            if reason.contains("waiting for already authorized NIC firmware effects to settle"))
    );

    // This receipt establishes that preparation failed before mutation. It is
    // the evidence that permits access restoration after allocation deletion.
    let failed_prepare = FirmwareOperation {
        operation_id: admitted.targets[0].operation_id.clone(),
        state: FirmwareOperationState::Failed,
        phase: Some(FirmwareOperationPhase::Prepare),
        report: FirmwareFlashReport::default(),
        diagnostic: Some("image unavailable before mutation".into()),
    };
    publish(
        &env,
        host_id,
        observation(
            &admitted.targets[0],
            FirmwareResult::Receipt(failed_prepare.clone().into()),
        ),
    )
    .await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let restoring = operation(&env, host_id).await;
    assert_eq!(restoring.targets[0].receipt, Some(failed_prepare.clone()));
    assert_eq!(restoring.purpose, admitted.purpose);
    assert!(
        matches!(restoring.phase, NicFirmwarePhase::RestoringAccess {
        outcome: NicFirmwareOutcome::Failed { reason }, ..
    } if reason.contains("lifecycle owner changed"))
    );
    assert!(
        machine(&env, host_id)
            .await
            .status
            .nic_firmware_baseline
            .is_none()
    );
    report_access(
        &env,
        host_id,
        &admitted.targets[0],
        status(&admitted.targets[0], false),
    )
    .await;
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let safe_failure = operation(&env, host_id).await;
    assert!(matches!(
        safe_failure.phase,
        NicFirmwarePhase::Failed { .. }
    ));
    assert_eq!(safe_failure.purpose, admitted.purpose);
    assert_eq!(safe_failure.targets[0].receipt, Some(failed_prepare));
    env.run_machine_state_controller_iteration_without_requeue()
        .await;
    let released = db::machine::find_one(
        &env.pool,
        &host_id,
        MachineSearchConfig {
            include_history: true,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        released.state.value,
        ManagedHostState::Assigned {
            instance_state: InstanceState::WaitingForRebootToReady
        }
    );
    assert!(released.history.iter().any(|record| {
        serde_json::from_str::<ManagedHostState>(&record.state).is_ok_and(|state| {
            matches!(state, ManagedHostState::NicFirmwareUpdating { operation } if operation == safe_failure)
        })
    }));
    let mut txn = env.pool.begin().await.unwrap();
    let retained_owner = db::instance::find_by_id(txn.as_mut(), owner.id())
        .await
        .unwrap()
        .unwrap();
    assert!(retained_owner.deleted.is_some());
    let NicFirmwarePurpose::Allocation { config_version, .. } = admitted.purpose else {
        unreachable!()
    };
    assert_eq!(retained_owner.config_version, config_version);
}

// Each lifecycle resumes its own persisted continuation after independent
// access readback. Only baseline work may replace the retained baseline.
#[crate::sqlx_test]
async fn safe_completion_resumes_the_frozen_lifecycle_owner(pool: sqlx::PgPool) {
    enum Continuation {
        InitialPreparation,
        Allocation,
        RestoreBaseline,
    }
    let env = environment(pool).await;
    let segment = env.create_vpc_and_tenant_segment().await;
    for scenario in [
        // Initial preparation preserves the discovered host's reboot-wait choice.
        Continuation::InitialPreparation,
        // Allocation keeps the same live instance and its admitted generation.
        Continuation::Allocation,
        // Release keeps the deleted instance and original cleanup reason.
        Continuation::RestoreBaseline,
    ] {
        let host = create_managed_host(&env).await;
        let host_id = *host.id;
        let mut admitted = fixtures::operation();
        let (purpose, expected, retains_baseline) = match scenario {
            Continuation::InitialPreparation => (
                NicFirmwarePurpose::Baseline {
                    continuation: NicFirmwareBaselineContinuation::InitialPreparation {
                        skip_reboot_wait: true,
                    },
                },
                ManagedHostState::HostInit {
                    machine_state: MachineState::Discovered {
                        skip_reboot_wait: true,
                    },
                },
                true,
            ),
            Continuation::Allocation | Continuation::RestoreBaseline => {
                let owner = allocate_owner(&env, &host, segment).await;
                if matches!(scenario, Continuation::Allocation) {
                    (
                        NicFirmwarePurpose::Allocation {
                            instance_id: owner.id(),
                            config_version: owner.config_version(),
                        },
                        ManagedHostState::Assigned {
                            instance_state: InstanceState::DpaProvisioning,
                        },
                        false,
                    )
                } else {
                    let mut txn = env.pool.begin().await.unwrap();
                    db::instance::mark_as_deleted(owner.id(), txn.as_mut())
                        .await
                        .unwrap();
                    txn.commit().await.unwrap();
                    (
                        NicFirmwarePurpose::RestoreBaseline {
                            instance_id: owner.id(),
                            cleanup_context: CleanupContext::Reset,
                        },
                        ManagedHostState::WaitingForCleanup {
                            cleanup_state: CleanupState::Init,
                            cleanup_context: CleanupContext::Reset,
                        },
                        false,
                    )
                }
            }
        };
        admitted.purpose = purpose.clone();
        admitted.phase = NicFirmwarePhase::RestoringAccess {
            target_index: 0,
            outcome: NicFirmwareOutcome::Succeeded,
        };
        admitted.targets[0].receipt = Some(complete(&admitted.targets[0]));
        admitted.targets[0].observation = Some(NicFirmwareDeviceObservation {
            observed_at: Utc::now(),
            received_at: Utc::now(),
            device: device(&admitted.targets[0]).try_into().unwrap(),
        });
        let previous = fixtures::baseline();
        sqlx::query("UPDATE machines SET nic_firmware_baseline = $1 WHERE id = $2")
            .bind(sqlx::types::Json(&previous))
            .bind(host_id)
            .execute(&env.pool)
            .await
            .unwrap();
        seed(&env, host_id, admitted.clone()).await;
        report_access(
            &env,
            host_id,
            &admitted.targets[0],
            status(&admitted.targets[0], false),
        )
        .await;
        let before = machine(&env, host_id).await;
        env.run_machine_state_controller_iteration_without_requeue()
            .await;
        let finished = machine(&env, host_id).await;
        assert_eq!(finished.state.value, expected);
        assert_ne!(finished.state.version, before.state.version);
        let baseline = finished.status.nic_firmware_baseline.unwrap();
        if retains_baseline {
            assert_eq!(
                baseline.targets[0].observation,
                admitted.targets[0].observation.clone().unwrap()
            );
        } else {
            assert_eq!(baseline, previous);
            let (NicFirmwarePurpose::Allocation { instance_id, .. }
            | NicFirmwarePurpose::RestoreBaseline { instance_id, .. }) = purpose
            else {
                unreachable!()
            };
            let mut txn = env.pool.begin().await.unwrap();
            let owner = db::instance::find_by_id(txn.as_mut(), instance_id)
                .await
                .unwrap()
                .unwrap();
            assert!(admitted.purpose.matches_owner(Some((
                owner.id,
                owner.config_version,
                owner.deleted.is_some()
            ))));
        }
    }
}
