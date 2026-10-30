/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Reconcile an already admitted NIC operation; lifecycle admission lives elsewhere.
//! Each transition is a committed recovery point. Scout command delivery consumes
//! that state independently, while this handler owns activation and continuation.

use std::time::Duration;

use carbide_libmlx_model::firmware::operation::FirmwareOperationState;
use chrono::{DateTime, Utc};
use libredfish::SystemPowerControl;
use model::machine::machine_search_config::MachineSearchConfig;
use model::machine::nic_firmware::{
    NicFirmwareAccessProgress, NicFirmwareAccessResult, NicFirmwareAccessStep, NicFirmwareBaseline,
    NicFirmwareBaselineContinuation, NicFirmwareBaselineTarget, NicFirmwareDeviceObservation,
    NicFirmwareOperation, NicFirmwareOutcome, NicFirmwarePhase, NicFirmwarePurpose,
    NicFirmwareTarget,
};
use model::machine::status::MlxDeviceObservation;
use model::machine::{
    CleanupState, InstanceState, MachineState, ManagedHostState, ManagedHostStateSnapshot,
};
use state_controller::CheckApplied;
use state_controller::state_handler::{
    StateHandlerContext, StateHandlerError, StateHandlerOutcome,
};

use crate::context::MachineStateHandlerContextObjects;

// One attempt includes client/credential lookup and the power request. A timeout
// leaves activation pending: only Scout's reboot report can advance the phase.
const REBOOT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(120);

pub(super) async fn handle(
    snapshot: &ManagedHostStateSnapshot,
    operation: &NicFirmwareOperation,
    ctx: &mut StateHandlerContext<'_, MachineStateHandlerContextObjects>,
) -> Result<StateHandlerOutcome<ManagedHostState>, StateHandlerError> {
    operation.validate().map_err(|error| {
        StateHandlerError::ManualInterventionRequired(format!(
            "invalid admitted NIC firmware operation: {error}"
        ))
    })?;

    if let Some(reason) = ownership_failure(snapshot, &operation.purpose) {
        let recovering = matches!(
            operation.phase,
            NicFirmwarePhase::Rebooting { .. }
                | NicFirmwarePhase::RestoringAccess {
                    outcome: NicFirmwareOutcome::Failed { .. },
                    ..
                }
                | NicFirmwarePhase::Failed { .. }
        );
        if !recovering {
            if let Some(target) = operation.current_target() {
                let possibly_submitted = target.receipt.is_some()
                    || matches!(operation.phase, NicFirmwarePhase::UpdatingFirmware { .. });
                if possibly_submitted && !target.can_restore_access() {
                    let reason = with_target_diagnostic(
                        target,
                        format!(
                            "{reason}; waiting for already authorized NIC firmware effects to settle"
                        ),
                    );
                    // An absent receipt in `UpdatingFirmware` is not proof that
                    // submission never happened, even on the same helper.
                    if target
                        .receipt
                        .as_ref()
                        .is_none_or(|receipt| execution_pending(receipt.state))
                    {
                        return Ok(StateHandlerOutcome::wait(reason));
                    }
                    return Err(StateHandlerError::ManualInterventionRequired(reason));
                }
            }
            return Ok(transition(
                operation,
                NicFirmwarePhase::RestoringAccess {
                    target_index: 0,
                    outcome: NicFirmwareOutcome::Failed { reason },
                },
            ));
        }
    }

    match &operation.phase {
        NicFirmwarePhase::ObservingSupervisor { target_index } => {
            let target = current_target(operation)?;
            if let Some(receipt) = &target.receipt {
                if !execution_pending(receipt.state) && !target.can_restore_access() {
                    return Err(StateHandlerError::ManualInterventionRequired(
                        with_target_diagnostic(
                            target,
                            format!(
                                "NIC firmware operation {} has unresolved {:?} execution evidence",
                                target.operation_id, receipt.state,
                            ),
                        ),
                    ));
                }
                // A retained receipt proves prior admission, not permission to
                // unlock or submit again. Resume status-only reconciliation.
                return Ok(transition(
                    operation,
                    NicFirmwarePhase::UpdatingFirmware {
                        target_index: *target_index,
                    },
                ));
            }
            if target.supervisor_instance_id.is_none() {
                return Ok(StateHandlerOutcome::wait(
                    target.diagnostic.clone().unwrap_or_else(|| {
                        "waiting for the initial NIC firmware supervisor observation".into()
                    }),
                ));
            }
            Ok(transition(
                operation,
                NicFirmwarePhase::Unlocking {
                    target_index: *target_index,
                },
            ))
        }
        NicFirmwarePhase::Unlocking { target_index } => {
            if let Some(pending) = reconcile_access(operation)? {
                return Ok(pending);
            }
            Ok(transition(
                operation,
                NicFirmwarePhase::UpdatingFirmware {
                    target_index: *target_index,
                },
            ))
        }
        NicFirmwarePhase::UpdatingFirmware { target_index } => {
            let target = current_target(operation)?;
            let Some(receipt) = &target.receipt else {
                return Ok(StateHandlerOutcome::wait(
                    target.diagnostic.clone().unwrap_or_else(|| {
                        "waiting for the correlated NIC firmware receipt".into()
                    }),
                ));
            };
            if execution_pending(receipt.state) {
                return Ok(StateHandlerOutcome::wait(with_target_diagnostic(
                    target,
                    format!(
                        "NIC firmware operation {} is {:?}",
                        target.operation_id, receipt.state
                    ),
                )));
            }
            if !target.is_complete() {
                let reason = with_target_diagnostic(
                    target,
                    format!(
                        "NIC firmware operation {} ended in {:?} without successful requested-phase evidence",
                        target.operation_id, receipt.state,
                    ),
                );
                if !target.can_restore_access() {
                    // Keep the receipt and execution phase visible. Terminal
                    // uncertainty does not permit another mutation or safe exit.
                    return Err(StateHandlerError::ManualInterventionRequired(reason));
                }
                return Ok(transition(
                    operation,
                    NicFirmwarePhase::RestoringAccess {
                        target_index: 0,
                        outcome: NicFirmwareOutcome::Failed { reason },
                    },
                ));
            }
            let phase = if *target_index + 1 < operation.targets.len() {
                NicFirmwarePhase::ObservingSupervisor {
                    target_index: *target_index + 1,
                }
            } else if operation.reboot_host {
                // All burn receipts and this activation intent commit before the
                // first reboot. A lost runtime journal cannot recreate the burns.
                NicFirmwarePhase::Rebooting {
                    started_at: Utc::now(),
                    last_attempt_at: None,
                }
            } else {
                NicFirmwarePhase::Reobserving {
                    not_before: Utc::now(),
                }
            };
            Ok(transition(operation, phase))
        }
        NicFirmwarePhase::Rebooting {
            started_at,
            last_attempt_at,
        } => {
            // Recheck feedback under the state and machine locks before either
            // confirming activation or committing another attempt. A snapshot
            // taken before Scout's report must not cause an avoidable reboot.
            let version = snapshot.host_snapshot.state.version;
            let host_machine_id = snapshot.host_snapshot.id;
            let mut txn = ctx.services.db_pool.begin().await?;
            db::machine::lock_nic_firmware_state(&mut txn, &host_machine_id, version)
                .await?
                .check_applied()?;
            let current = db::machine::find_one(
                txn.as_mut(),
                &host_machine_id,
                MachineSearchConfig::default(),
            )
            .await?
            .ok_or_else(|| StateHandlerError::MissingData {
                object_id: host_machine_id.to_string(),
                missing: "NIC firmware host after state lock",
            })?;
            if let Some(reported_at) = current.status.last_reboot_time
                && reboot_report_covers_attempt(
                    reported_at,
                    *started_at,
                    *last_attempt_at,
                    current
                        .status
                        .last_reboot_requested
                        .map(|request| request.time),
                )
            {
                // `RebootCompleted` is Core receipt ordering, not a hardware
                // boot ID. Inventory still has to arrive after this boundary.
                if let Some(request) = current.status.last_reboot_requested
                    && request.restart_verified == Some(false)
                {
                    db::machine::update_restart_verification_status(
                        &host_machine_id.into(),
                        request,
                        Some(true),
                        0,
                        &mut txn,
                    )
                    .await?
                    .check_applied()?;
                }
                return Ok(transition(
                    operation,
                    NicFirmwarePhase::Reobserving {
                        not_before: reported_at,
                    },
                )
                .with_txn(txn));
            }
            if snapshot
                .aggregate_health
                .is_reboot_blocked_in_state_machine()
            {
                return Ok(StateHandlerOutcome::wait(
                    "NIC firmware activation reboot is blocked by a health override".into(),
                )
                .with_txn(txn));
            }
            // Count from the persisted attempt start. The minimum includes the
            // bounded 120-second call plus a minute for Scout to report back.
            let retry_delay = ctx
                .services
                .site_config
                .machine_state_controller
                .failure_retry_time
                .max(chrono::Duration::seconds(
                    REBOOT_ATTEMPT_TIMEOUT.as_secs() as i64 + 60,
                ));
            let now = Utc::now();
            if last_attempt_at.is_some_and(|attempt| now < attempt + retry_delay) {
                return Ok(StateHandlerOutcome::wait(
                    "waiting for Scout to report the NIC firmware activation reboot".into(),
                )
                .with_txn(txn));
            }
            // Commit the attempt marker before external work so retries retain
            // the same interval across controller crashes. A crash before sending
            // only delays the next attempt by that interval.
            let mut next = operation.clone();
            next.phase = NicFirmwarePhase::Rebooting {
                started_at: *started_at,
                last_attempt_at: Some(now),
            };
            db::machine::try_update_controller_state(
                &mut txn,
                &host_machine_id,
                version,
                version.increment(),
                &ManagedHostState::NicFirmwareUpdating { operation: next },
            )
            .await?
            .check_applied()?;
            txn.commit().await?;

            let attempt = tokio::time::timeout(
                REBOOT_ATTEMPT_TIMEOUT,
                super::handler_host_power_control(snapshot, ctx, SystemPowerControl::ForceRestart),
            )
            .await;
            match attempt {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(
                    host_machine_id = %snapshot.host_snapshot.id,
                    error = %error,
                    "NIC firmware activation reboot attempt failed"
                ),
                Err(error) => tracing::warn!(
                    host_machine_id = %snapshot.host_snapshot.id,
                    error = %error,
                    "NIC firmware activation reboot attempt timed out"
                ),
            }
            // Even an accepted power request is not reboot-completion evidence.
            // The attempt state already committed; do not repeat its CAS in the
            // outer processor. Scout feedback will enqueue the next iteration.
            Ok(StateHandlerOutcome::wait(
                "waiting for Scout to report the NIC firmware activation reboot".into(),
            ))
        }
        NicFirmwarePhase::Reobserving { not_before } => {
            let Some(observation) = &snapshot.host_snapshot.status.mlx_device_observation else {
                return Ok(StateHandlerOutcome::wait(
                    "waiting for ordinary NIC inventory after activation".into(),
                ));
            };
            let mut next = operation.clone();
            if let Err(reason) = accept_inventory(&mut next, observation, *not_before) {
                return Ok(StateHandlerOutcome::wait(reason));
            }
            next.phase = NicFirmwarePhase::RestoringAccess {
                target_index: 0,
                outcome: NicFirmwareOutcome::Succeeded,
            };
            next.access = NicFirmwareAccessProgress::default();
            Ok(StateHandlerOutcome::transition(
                ManagedHostState::NicFirmwareUpdating { operation: next },
            ))
        }
        NicFirmwarePhase::RestoringAccess {
            target_index,
            outcome,
        } => {
            if let Some(pending) = reconcile_access(operation)? {
                return Ok(pending);
            }
            if *target_index + 1 < operation.targets.len() {
                return Ok(transition(
                    operation,
                    NicFirmwarePhase::RestoringAccess {
                        target_index: *target_index + 1,
                        outcome: outcome.clone(),
                    },
                ));
            }
            match outcome {
                NicFirmwareOutcome::Succeeded => complete(snapshot, operation, ctx).await,
                NicFirmwareOutcome::Failed { reason } => Ok(transition(
                    operation,
                    NicFirmwarePhase::Failed {
                        reason: reason.clone(),
                    },
                )),
            }
        }
        NicFirmwarePhase::Failed { reason } => {
            if let NicFirmwarePurpose::Allocation { instance_id, .. } = operation.purpose
                && snapshot.instance.as_ref().is_some_and(|instance| {
                    instance.id == instance_id && instance.deleted.is_some()
                })
            {
                // Only safe, independently restored failure reaches this phase.
                // A deleted original owner must still follow normal network and
                // instance teardown; jumping to cleanup would skip those steps.
                let mut txn = ctx.services.db_pool.begin().await?;
                db::machine::prepare_nic_firmware_owner_release(
                    &mut txn,
                    &snapshot.host_snapshot.id,
                    snapshot.host_snapshot.state.version,
                    &instance_id,
                )
                .await?
                .check_applied()?;
                return Ok(StateHandlerOutcome::transition(ManagedHostState::Assigned {
                    instance_state: InstanceState::WaitingForRebootToReady,
                })
                .with_txn(txn));
            }
            Err(StateHandlerError::ManualInterventionRequired(
                reason.clone(),
            ))
        }
    }
}

fn current_target(
    operation: &NicFirmwareOperation,
) -> Result<&NicFirmwareTarget, StateHandlerError> {
    operation.current_target().ok_or_else(|| {
        StateHandlerError::ManualInterventionRequired(
            "NIC firmware phase has no valid frozen target".into(),
        )
    })
}

/// Check the lifecycle owner before effects; completion repeats this check
/// transactionally so a snapshot race cannot resume a stale caller.
fn ownership_failure(
    snapshot: &ManagedHostStateSnapshot,
    purpose: &NicFirmwarePurpose,
) -> Option<String> {
    let owner = snapshot.instance.as_ref().map(|instance| {
        (
            instance.id,
            instance.config_version,
            instance.deleted.is_some(),
        )
    });
    (!purpose.matches_owner(owner))
        .then(|| "NIC firmware lifecycle owner changed; no new cards will be submitted".into())
}

fn transition(
    operation: &NicFirmwareOperation,
    phase: NicFirmwarePhase,
) -> StateHandlerOutcome<ManagedHostState> {
    let mut next = operation.clone();
    if let NicFirmwarePhase::Unlocking { target_index }
    | NicFirmwarePhase::RestoringAccess { target_index, .. } = operation.phase
        && let Some(NicFirmwareAccessResult::Status(status)) = &operation.access.result
        && Some(*status) == operation.desired_access()
        && let Some(target) = next.targets.get_mut(target_index)
    {
        target.diagnostic = None;
    }
    next.phase = phase;
    next.access = NicFirmwareAccessProgress::default();
    StateHandlerOutcome::transition(ManagedHostState::NicFirmwareUpdating { operation: next })
}

/// Consume one access report before authorizing the next command. Even a change
/// error returns to observation: the command might have changed the card before
/// its response was lost, so its reply cannot establish the final policy.
fn reconcile_access(
    operation: &NicFirmwareOperation,
) -> Result<Option<StateHandlerOutcome<ManagedHostState>>, StateHandlerError> {
    let desired = operation.desired_access().ok_or_else(|| {
        StateHandlerError::ManualInterventionRequired(
            "NIC firmware access phase has no valid frozen target".into(),
        )
    })?;
    let Some(result) = &operation.access.result else {
        return Ok(Some(StateHandlerOutcome::wait(with_target_diagnostic(
            current_target(operation)?,
            "waiting for the correlated NIC access report".into(),
        ))));
    };
    let (step, diagnostic) = match (operation.access.step, result) {
        (NicFirmwareAccessStep::Observe, NicFirmwareAccessResult::Status(status))
            if *status == desired =>
        {
            return Ok(None);
        }
        (NicFirmwareAccessStep::Observe, NicFirmwareAccessResult::Status(_)) => {
            (NicFirmwareAccessStep::Change, None)
        }
        (NicFirmwareAccessStep::Change, NicFirmwareAccessResult::Changed) => {
            (NicFirmwareAccessStep::Observe, None)
        }
        (_, NicFirmwareAccessResult::Error { diagnostic }) => {
            (NicFirmwareAccessStep::Observe, Some(diagnostic.clone()))
        }
        _ => {
            return Err(StateHandlerError::ManualInterventionRequired(
                "NIC firmware access report does not match its requested step".into(),
            ));
        }
    };
    let mut next = operation.clone();
    next.access = NicFirmwareAccessProgress { step, result: None };
    let target_index = match operation.phase {
        NicFirmwarePhase::Unlocking { target_index }
        | NicFirmwarePhase::RestoringAccess { target_index, .. } => target_index,
        _ => {
            return Err(StateHandlerError::ManualInterventionRequired(
                "NIC firmware access report has no current access phase".into(),
            ));
        }
    };
    let target = next.targets.get_mut(target_index).ok_or_else(|| {
        StateHandlerError::ManualInterventionRequired(
            "NIC firmware access report has no current target".into(),
        )
    })?;
    target.diagnostic = diagnostic;
    Ok(Some(StateHandlerOutcome::transition(
        ManagedHostState::NicFirmwareUpdating { operation: next },
    )))
}

fn execution_pending(state: FirmwareOperationState) -> bool {
    matches!(
        state,
        FirmwareOperationState::Accepted
            | FirmwareOperationState::Running
            | FirmwareOperationState::CancelRequested
    )
}

fn with_target_diagnostic(target: &NicFirmwareTarget, reason: String) -> String {
    match target.diagnostic.as_deref().or_else(|| {
        target
            .receipt
            .as_ref()
            .and_then(|receipt| receipt.diagnostic.as_deref())
    }) {
        Some(diagnostic) => format!("{reason}: {diagnostic}"),
        None => reason,
    }
}

fn reboot_report_covers_attempt(
    reported_at: DateTime<Utc>,
    started_at: DateTime<Utc>,
    last_attempt_at: Option<DateTime<Utc>>,
    requested_at: Option<DateTime<Utc>>,
) -> bool {
    reported_at > started_at
        && last_attempt_at.is_none_or(|attempt| reported_at >= attempt)
        && requested_at.is_none_or(|request| reported_at >= request)
}

/// Accept the whole frozen target set from one post-activation inventory. A
/// partial, stale, ambiguous or mismatched report remains retryable and does not
/// replace previously accepted evidence.
fn accept_inventory(
    operation: &mut NicFirmwareOperation,
    observation: &MlxDeviceObservation,
    not_before: DateTime<Utc>,
) -> Result<(), String> {
    let received_at = observation
        .received_at
        .filter(|received_at| *received_at > not_before)
        .ok_or_else(|| "waiting for NIC inventory received after activation".to_string())?;
    let accepted = operation
        .targets
        .iter()
        .map(|target| {
            let index = target
                .selector
                .device_index(&observation.devices)
                .map_err(|error| error.to_string())?;
            let device = &observation.devices[index];
            let firmware = &target.resolution.entry.firmware;
            if device.pci_name != target.pci_name
                || device.part_number.as_deref() != Some(firmware.part_number.as_str())
                || device.psid.as_deref() != Some(firmware.psid.as_str())
                || device.fw_version_current.as_deref() != Some(firmware.version.as_str())
            {
                return Err(format!(
                    "waiting for exact identity and firmware version {} on NIC {}",
                    firmware.version, target.pci_name,
                ));
            }
            Ok(NicFirmwareDeviceObservation {
                observed_at: observation.observed_at,
                received_at,
                device: device.clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    for (target, observed) in operation.targets.iter_mut().zip(accepted) {
        target.observation = Some(observed);
    }
    Ok(())
}

/// Resume the frozen lifecycle purpose only after all cards have converged and
/// their original access policies have been independently observed.
async fn complete(
    snapshot: &ManagedHostStateSnapshot,
    operation: &NicFirmwareOperation,
    ctx: &mut StateHandlerContext<'_, MachineStateHandlerContextObjects>,
) -> Result<StateHandlerOutcome<ManagedHostState>, StateHandlerError> {
    let (continuation, retain_baseline) = match &operation.purpose {
        NicFirmwarePurpose::Baseline { continuation } => (
            match continuation {
                NicFirmwareBaselineContinuation::InitialPreparation { skip_reboot_wait } => {
                    ManagedHostState::HostInit {
                        machine_state: MachineState::Discovered {
                            skip_reboot_wait: *skip_reboot_wait,
                        },
                    }
                }
                NicFirmwareBaselineContinuation::Ready => ManagedHostState::Ready,
            },
            true,
        ),
        NicFirmwarePurpose::Allocation { .. } => (
            ManagedHostState::Assigned {
                instance_state: InstanceState::DpaProvisioning,
            },
            false,
        ),
        NicFirmwarePurpose::RestoreBaseline {
            cleanup_context, ..
        } => (
            ManagedHostState::WaitingForCleanup {
                cleanup_state: CleanupState::Init,
                cleanup_context: *cleanup_context,
            },
            false,
        ),
    };
    let baseline = if retain_baseline {
        let updated_targets = operation
            .targets
            .iter()
            .map(|target| {
                let observation = target.observation.clone().ok_or_else(|| {
                    StateHandlerError::ManualInterventionRequired(format!(
                        "NIC {} has no accepted baseline observation",
                        target.pci_name,
                    ))
                })?;
                Ok(NicFirmwareBaselineTarget {
                    selector: target.selector.clone(),
                    pci_name: target.pci_name.clone(),
                    resolution: target.resolution.clone(),
                    access: target.original_access.clone(),
                    observation,
                })
            })
            .collect::<Result<Vec<_>, StateHandlerError>>()?;
        let mut targets: Vec<_> = snapshot
            .host_snapshot
            .status
            .nic_firmware_baseline
            .as_ref()
            .into_iter()
            .flat_map(|baseline| &baseline.targets)
            .filter(|previous| {
                !updated_targets
                    .iter()
                    .any(|updated| updated.selector == previous.selector)
            })
            .cloned()
            .collect();
        targets.extend(updated_targets);
        Some(NicFirmwareBaseline {
            operation_id: operation.operation_id.clone(),
            targets,
        })
    } else {
        None
    };
    // Keep the owner check and baseline write in the same transaction as the
    // normal controller transition, including its lifecycle event.
    let version = snapshot.host_snapshot.state.version;
    let mut txn = ctx.services.db_pool.begin().await?;
    db::machine::prepare_nic_firmware_completion(
        &mut txn,
        &snapshot.host_snapshot.id,
        version,
        &operation.purpose,
        baseline.as_ref(),
    )
    .await?
    .check_applied()?;
    Ok(StateHandlerOutcome::transition(continuation).with_txn(txn))
}

#[cfg(test)]
mod tests {
    use carbide_libmlx_model::device::info::MlxDeviceInfo;
    use carbide_test_support::{Case, Check, Outcome, check_cases, check_values};
    use model::nic_firmware::NicFirmwareCardSelector;
    use model::test_support::nic_firmware::{baseline, operation};

    use super::*;

    #[test]
    fn inventory_must_be_newer_and_match_every_frozen_hardware_fact() {
        // These independent rejection boundaries must preserve the previously
        // accepted evidence, not merely defer the phase transition.
        let accepted = baseline().targets.remove(0).observation;
        let barrier = accepted.received_at;
        let fresh = barrier + chrono::Duration::seconds(1);
        let device = accepted.device.clone();
        let mut original = operation();
        original.targets[0].observation = Some(accepted);

        check_cases(
            [
                Case {
                    scenario: "pre-activation inventory cannot confirm the new image",
                    input: MlxDeviceObservation {
                        observed_at: barrier,
                        received_at: Some(barrier),
                        devices: vec![device.clone()],
                    },
                    expect: Outcome::Fails,
                },
                Case {
                    scenario: "legacy inventory has no Core reception boundary",
                    input: MlxDeviceObservation {
                        observed_at: fresh,
                        received_at: None,
                        devices: vec![device.clone()],
                    },
                    expect: Outcome::Fails,
                },
                Case {
                    scenario: "a reused selector at another PCI function is not the frozen target",
                    input: MlxDeviceObservation {
                        observed_at: fresh,
                        received_at: Some(fresh),
                        devices: vec![MlxDeviceInfo {
                            pci_name: "0000:02:00.0".into(),
                            ..device.clone()
                        }],
                    },
                    expect: Outcome::Fails,
                },
                Case {
                    scenario: "an incomplete hardware query cannot confirm compatibility",
                    input: MlxDeviceObservation {
                        observed_at: fresh,
                        received_at: Some(fresh),
                        devices: vec![MlxDeviceInfo {
                            part_number: None,
                            ..device.clone()
                        }],
                    },
                    expect: Outcome::Fails,
                },
                Case {
                    scenario: "the same model and version with a different PSID is not confirmation",
                    input: MlxDeviceObservation {
                        observed_at: fresh,
                        received_at: Some(fresh),
                        devices: vec![MlxDeviceInfo {
                            psid: Some("different-psid".into()),
                            ..device
                        }],
                    },
                    expect: Outcome::Fails,
                },
            ],
            |report| {
                let mut candidate = original.clone();
                let result = accept_inventory(&mut candidate, &report, barrier);
                assert_eq!(
                    candidate, original,
                    "rejected inventory must retain accepted evidence"
                );
                result
            },
        );
    }

    #[test]
    fn one_mismatched_card_preserves_all_evidence_until_a_later_complete_report() {
        // One host report is accepted atomically across the frozen target set;
        // a later complete report must still recover after a partial mismatch.
        let first = baseline().targets.remove(0).observation;
        let second_mac = "02:00:00:00:00:02".parse().expect("fixture MAC");
        let second_device = MlxDeviceInfo {
            pci_name: "0000:02:00.0".into(),
            base_mac: Some(second_mac),
            ..first.device.clone()
        };
        let mut original = operation();
        original.targets[0].observation = Some(first.clone());
        let mut second_target = original.targets[0].clone();
        second_target.selector = NicFirmwareCardSelector::Mac(second_mac);
        second_target.pci_name = second_device.pci_name.clone();
        second_target.operation_id = "nic-card-1".into();
        second_target.observation = Some(NicFirmwareDeviceObservation {
            observed_at: first.observed_at,
            received_at: first.received_at,
            device: second_device.clone(),
        });
        original.targets.push(second_target);
        let barrier = first.received_at;
        // Scout's clock is deliberately behind Core's activation boundary.
        // The activation check uses receipt time, not Scout's collection clock.
        let observed_at = first.observed_at - chrono::Duration::hours(1);
        let received_at = barrier + chrono::Duration::seconds(1);
        let mut candidate = original.clone();

        // If acceptance mutates cards one at a time, the first card's newer
        // observation leaks despite the second card still reporting the old image.
        let partial = MlxDeviceObservation {
            observed_at,
            received_at: Some(received_at),
            devices: vec![
                first.device.clone(),
                MlxDeviceInfo {
                    fw_version_current: Some("old-version".into()),
                    ..second_device.clone()
                },
            ],
        };
        assert!(accept_inventory(&mut candidate, &partial, barrier).is_err());
        assert_eq!(candidate, original);

        // Report ordering is not card ordering; the frozen selectors own mapping.
        accept_inventory(
            &mut candidate,
            &MlxDeviceObservation {
                observed_at,
                received_at: Some(received_at),
                devices: vec![second_device.clone(), first.device.clone()],
            },
            barrier,
        )
        .expect("later exact inventory must converge");
        assert_eq!(
            candidate.targets[0].observation,
            Some(NicFirmwareDeviceObservation {
                observed_at,
                received_at,
                device: first.device,
            })
        );
        assert_eq!(
            candidate.targets[1].observation,
            Some(NicFirmwareDeviceObservation {
                observed_at,
                received_at,
                device: second_device,
            })
        );
    }

    #[test]
    fn reboot_report_must_cover_activation_latest_attempt_and_latest_request() {
        // A delayed report from an earlier retry must not verify a newer
        // request. All values use Core time; this is report receipt ordering.
        let started_at = baseline().targets.remove(0).observation.received_at;
        let earlier = started_at - chrono::Duration::seconds(1);
        let later = started_at + chrono::Duration::seconds(1);
        check_values(
            [
                Check {
                    scenario: "report at activation boundary is stale",
                    input: (started_at, None, None),
                    expect: false,
                },
                Check {
                    scenario: "report before latest committed attempt",
                    input: (later, Some(later + chrono::Duration::seconds(1)), None),
                    expect: false,
                },
                Check {
                    scenario: "report before newer power request",
                    input: (
                        later,
                        Some(started_at),
                        Some(later + chrono::Duration::seconds(1)),
                    ),
                    expect: false,
                },
                Check {
                    scenario: "report covers every persisted boundary",
                    input: (later, Some(started_at), Some(started_at)),
                    expect: true,
                },
                Check {
                    scenario: "report after activation covers an older request",
                    input: (later, None, Some(earlier)),
                    expect: true,
                },
            ],
            |(reported_at, last_attempt_at, requested_at)| {
                reboot_report_covers_attempt(reported_at, started_at, last_attempt_at, requested_at)
            },
        );
    }
}
