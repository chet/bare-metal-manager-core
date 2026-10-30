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

use carbide_libmlx_model::firmware::result::FirmwareFlashReport;
use carbide_test_support::{Case, Check, Outcome, check_cases, check_values};

use super::*;
use crate::machine::ManagedHostState;
use crate::test_support::nic_firmware::{baseline, operation, target};

/// Burn evidence for the fixture's exact version and caller-selected lifecycle
/// state; optional phase evidence stays absent unless the case requests it.
fn receipt(state: FirmwareOperationState) -> FirmwareOperation {
    FirmwareOperation {
        operation_id: "nic-card-0".to_string(),
        state,
        phase: None,
        report: FirmwareFlashReport {
            flashed: true,
            expected_version: Some("1.2.3".into()),
            ..Default::default()
        },
        diagnostic: None,
    }
}

#[test]
fn missing_receipt_freezes_helper_identity_without_regrant() {
    let mut operation = operation();
    assert!(operation.accept_missing_receipt(
        "nic-card-0",
        "nic-phase-0",
        "0000:01:00.0",
        "helper-a".into()
    ));

    // A process replacement must not reopen the admission gate, even while the
    // controller is still persisting its initial observation phase.
    assert!(operation.accept_missing_receipt(
        "nic-card-0",
        "nic-phase-0",
        "0000:01:00.0",
        "helper-b".into()
    ));
    operation.phase = NicFirmwarePhase::UpdatingFirmware { target_index: 0 };
    assert!(!operation.accept_missing_receipt(
        "nic-card-0",
        "nic-phase-0",
        "0000:01:00.0",
        "helper-b".into()
    ));
    assert_eq!(
        operation.targets[0].supervisor_instance_id.as_deref(),
        Some("helper-a")
    );
    assert!(
        operation.targets[0]
            .diagnostic
            .as_deref()
            .expect("replacement diagnostic")
            .contains("not renewed")
    );

    let encoded = serde_json::to_value(ManagedHostState::NicFirmwareUpdating {
        operation: operation.clone(),
    })
    .expect("encode active operation");
    assert_eq!(encoded["state"], "nicfirmwareupdating");
    let decoded: ManagedHostState =
        serde_json::from_value(encoded).expect("decode active operation");
    assert_eq!(decoded, ManagedHostState::NicFirmwareUpdating { operation });
}

#[test]
fn omitted_access_progress_requires_a_new_observation() {
    // Persisted operations without access progress cannot assume an unlock or
    // a completed restoration; their first command must read the card.
    let operation = operation();
    let mut encoded = serde_json::to_value(&operation).expect("encode operation");
    encoded
        .as_object_mut()
        .expect("operation object")
        .remove("access");
    let decoded: NicFirmwareOperation =
        serde_json::from_value(encoded).expect("decode operation without access");
    assert_eq!(decoded, operation);
    assert_eq!(decoded.access.step, NicFirmwareAccessStep::Observe);
    assert_eq!(decoded.access.result, None);
}

#[test]
fn receipt_intake_matches_the_whole_target_and_preserves_terminal_evidence() {
    let cases = [
        Check {
            scenario: "different operation",
            input: ("other", "nic-phase-0", "0000:01:00.0", None),
            expect: false,
        },
        Check {
            scenario: "different workflow token",
            input: ("nic-card-0", "other", "0000:01:00.0", None),
            expect: false,
        },
        Check {
            scenario: "different journal target",
            input: ("nic-card-0", "nic-phase-0", "0000:02:00.0", None),
            expect: false,
        },
        Check {
            scenario: "first exact receipt",
            input: ("nic-card-0", "nic-phase-0", "0000:01:00.0", None),
            expect: true,
        },
        Check {
            scenario: "repeated running receipt is unchanged without terminal protection",
            input: (
                "nic-card-0",
                "nic-phase-0",
                "0000:01:00.0",
                Some(FirmwareOperationState::Running),
            ),
            expect: false,
        },
        Check {
            scenario: "late progress after completion",
            input: (
                "nic-card-0",
                "nic-phase-0",
                "0000:01:00.0",
                Some(FirmwareOperationState::Complete),
            ),
            expect: false,
        },
        Check {
            scenario: "late progress after failure",
            input: (
                "nic-card-0",
                "nic-phase-0",
                "0000:01:00.0",
                Some(FirmwareOperationState::Failed),
            ),
            expect: false,
        },
    ];
    check_values(cases, |(operation_id, phase_id, pci_name, previous)| {
        let mut operation = operation();
        operation.phase = NicFirmwarePhase::UpdatingFirmware { target_index: 0 };
        operation.targets[0].receipt = previous.map(receipt);
        let before = operation.clone();
        let accepted = operation.accept_receipt(
            operation_id,
            phase_id,
            pci_name,
            receipt(FirmwareOperationState::Running),
        );
        if !accepted {
            assert_eq!(
                operation, before,
                "ignored evidence must not modify the operation"
            );
        }
        accepted
    });
}

#[test]
fn unrelated_delivery_error_preserves_active_evidence() {
    // Delivery errors have a separate intake path; an unrelated request must
    // not overwrite the active receipt, helper identity or diagnostic.
    let mut operation = operation();
    operation.phase = NicFirmwarePhase::UpdatingFirmware { target_index: 0 };
    operation.targets[0].receipt = Some(receipt(FirmwareOperationState::Running));
    operation.targets[0].supervisor_instance_id = Some("helper-a".into());
    operation.targets[0].diagnostic = Some("waiting for the active request".into());
    let before = operation.clone();

    assert!(!operation.accept_error(
        "another-operation",
        "nic-phase-0",
        "0000:01:00.0",
        "unrelated delivery failure".into(),
    ));
    assert_eq!(operation, before);
}

#[test]
fn initial_observation_accepts_existing_receipt_without_granting_submission() {
    let mut operation = operation();
    let completed = receipt(FirmwareOperationState::Complete);
    assert!(operation.accept_receipt(
        "nic-card-0",
        "nic-phase-0",
        "0000:01:00.0",
        completed.clone(),
    ));
    assert_eq!(operation.targets[0].receipt, Some(completed));
    assert_eq!(operation.targets[0].supervisor_instance_id, None);
    assert_eq!(
        operation.phase,
        NicFirmwarePhase::ObservingSupervisor { target_index: 0 }
    );

    // Observation is not new admission: neither a later missing read nor a late
    // progress receipt can reopen submission or replace this terminal evidence.
    assert!(!operation.accept_missing_receipt(
        "nic-card-0",
        "nic-phase-0",
        "0000:01:00.0",
        "replacement-helper".into(),
    ));
    assert!(!operation.accept_receipt(
        "nic-card-0",
        "nic-phase-0",
        "0000:01:00.0",
        receipt(FirmwareOperationState::Running),
    ));
}

#[test]
fn completion_requires_the_frozen_phase_choices() {
    // Optional reset evidence must match the frozen choice exactly, including
    // absence when host activation was selected instead.
    check_values(
        [
            Check {
                scenario: "unrequested reset absent",
                input: (false, None),
                expect: true,
            },
            Check {
                scenario: "requested reset proven",
                input: (true, Some(true)),
                expect: true,
            },
            Check {
                scenario: "requested reset omitted",
                input: (true, None),
                expect: false,
            },
            Check {
                scenario: "unexpected reset evidence",
                input: (false, Some(true)),
                expect: false,
            },
        ],
        |(reset, reset_result)| {
            let mut target = target();
            target.options.reset = reset;
            let mut receipt = receipt(FirmwareOperationState::Complete);
            receipt.report.reset = reset_result;
            target.receipt = Some(receipt);
            target.is_complete()
        },
    );
}

#[test]
fn completion_rejects_missing_or_contradictory_execution_evidence() {
    // Each row isolates a different request/report field. Validating only the
    // terminal label must not erase a requested phase or a mismatched version.
    type Mutation = fn(&mut NicFirmwareTarget, &mut FirmwareOperation);
    check_values(
        [
            Check {
                scenario: "receipt belongs to another request",
                input: (|_, receipt: &mut FirmwareOperation| {
                    receipt.operation_id = "another-card".into();
                }) as Mutation,
                expect: false,
            },
            Check {
                scenario: "running receipt is not completion",
                input: |_, receipt| receipt.state = FirmwareOperationState::Running,
                expect: false,
            },
            Check {
                scenario: "completed no-work receipt is not a burn",
                input: |_, receipt| receipt.report.flashed = false,
                expect: false,
            },
            Check {
                scenario: "reported target version is missing",
                input: |_, receipt| receipt.report.expected_version = None,
                expect: false,
            },
            Check {
                scenario: "reported target version is different",
                input: |_, receipt| receipt.report.expected_version = Some("other-version".into()),
                expect: false,
            },
            Check {
                scenario: "required image check is missing",
                input: |target, _| target.options.verify_image = true,
                expect: false,
            },
            Check {
                scenario: "unrequested image check is present",
                input: |_, receipt| receipt.report.verified_image = Some(true),
                expect: false,
            },
            Check {
                scenario: "required image check succeeded",
                input: |target, receipt| {
                    target.options.verify_image = true;
                    receipt.report.verified_image = Some(true);
                },
                expect: true,
            },
            Check {
                scenario: "required image check failed",
                input: |target, receipt| {
                    target.options.verify_image = true;
                    receipt.report.verified_image = Some(false);
                },
                expect: false,
            },
            Check {
                scenario: "required version check is missing",
                input: |target, _| target.options.verify_version = true,
                expect: false,
            },
            Check {
                scenario: "unrequested version check is present",
                input: |_, receipt| receipt.report.verified_version = Some(true),
                expect: false,
            },
            Check {
                scenario: "successful version flag has no version",
                input: |target, receipt| {
                    target.options.verify_version = true;
                    receipt.report.verified_version = Some(true);
                },
                expect: false,
            },
            Check {
                scenario: "successful version flag contradicts observed version",
                input: |target, receipt| {
                    target.options.verify_version = true;
                    receipt.report.verified_version = Some(true);
                    receipt.report.observed_version = Some("old-version".into());
                },
                expect: false,
            },
            Check {
                scenario: "required exact version was observed",
                input: |target, receipt| {
                    target.options.verify_version = true;
                    receipt.report.verified_version = Some(true);
                    receipt.report.observed_version = Some("1.2.3".into());
                },
                expect: true,
            },
        ],
        |mutate| {
            let mut target = target();
            let mut completed = receipt(FirmwareOperationState::Complete);
            mutate(&mut target, &mut completed);
            target.receipt = Some(completed);
            target.is_complete()
        },
    );
}

#[test]
fn access_restoration_requires_evidence_that_mutation_has_settled_safely() {
    // Missing completion checkpoints can leave effects unknown even when
    // `flashed` is false; the recorded phase intent must also be pre-mutation.
    check_values(
        [
            Check {
                scenario: "completed executor released its work",
                input: (
                    FirmwareOperationState::Complete,
                    Some(FirmwareOperationPhase::VerifyVersion),
                    true,
                ),
                expect: true,
            },
            Check {
                scenario: "failed preparation cannot have mutated the card",
                input: (
                    FirmwareOperationState::Failed,
                    Some(FirmwareOperationPhase::Prepare),
                    false,
                ),
                expect: true,
            },
            Check {
                scenario: "cancel before phase selection is safe",
                input: (FirmwareOperationState::Canceled, None, false),
                expect: true,
            },
            Check {
                scenario: "configuration failure may have changed the card",
                input: (
                    FirmwareOperationState::Failed,
                    Some(FirmwareOperationPhase::Configure),
                    false,
                ),
                expect: false,
            },
            Check {
                scenario: "burn intent without a checkpoint is uncertain",
                input: (
                    FirmwareOperationState::Failed,
                    Some(FirmwareOperationPhase::Burn),
                    false,
                ),
                expect: false,
            },
            Check {
                scenario: "cancel after burn does not authorize reset or restoration",
                input: (
                    FirmwareOperationState::Canceled,
                    Some(FirmwareOperationPhase::Activate),
                    true,
                ),
                expect: false,
            },
            Check {
                scenario: "interruption is not safe completion",
                input: (
                    FirmwareOperationState::Interrupted,
                    Some(FirmwareOperationPhase::Prepare),
                    false,
                ),
                expect: false,
            },
            Check {
                scenario: "manual intervention remains explicit even before burn",
                input: (
                    FirmwareOperationState::ManualIntervention,
                    Some(FirmwareOperationPhase::Inspect),
                    false,
                ),
                expect: false,
            },
        ],
        |(state, phase, flashed)| {
            let mut target = target();
            target.receipt = Some(FirmwareOperation {
                operation_id: target.operation_id.clone(),
                state,
                phase,
                report: FirmwareFlashReport {
                    flashed,
                    ..Default::default()
                },
                diagnostic: None,
            });
            target.can_restore_access()
        },
    );
}

#[test]
fn validation_rejects_unsafe_requests() {
    // Reject malformed persisted request inputs before the controller can
    // authorize device effects; each mutation isolates one independent guard.
    type Mutation = fn(&mut NicFirmwareOperation);
    let mutations: [(&str, Mutation); 7] = [
        ("short PCI locator", |operation| {
            operation.targets[0].pci_name = "01:00.0".into()
        }),
        ("reserved legacy identity", |operation| {
            operation.targets[0].operation_id = "dpa-firmware:old".into()
        }),
        ("zero retry interval", |operation| {
            operation.targets[0]
                .options
                .post_reset_verify_retry_interval_seconds = Some(0)
        }),
        ("excessive verification budget", |operation| {
            operation.targets[0]
                .options
                .post_reset_verify_timeout_seconds = Some(1801)
        }),
        ("two activation owners", |operation| {
            operation.reboot_host = true;
            operation.targets[0].options.reset = true;
        }),
        (
            "persisted activation lacks completed receipts",
            |operation| {
                operation.phase = NicFirmwarePhase::Rebooting {
                    started_at: baseline().targets.remove(0).observation.received_at,
                    last_attempt_at: None,
                };
            },
        ),
        (
            "persisted restoration has uncertain execution",
            |operation| {
                operation.phase = NicFirmwarePhase::RestoringAccess {
                    target_index: 0,
                    outcome: NicFirmwareOutcome::Failed {
                        reason: "interrupted burn".into(),
                    },
                };
                operation.targets[0].receipt = Some(receipt(FirmwareOperationState::Interrupted));
            },
        ),
    ];
    check_cases(
        mutations.map(|(scenario, input)| Case {
            scenario,
            input,
            expect: Outcome::Fails,
        }),
        |mutate| {
            let mut operation = operation();
            mutate(&mut operation);
            operation.validate()
        },
    );
}

#[test]
fn validation_requires_exactly_one_activation_owner() {
    // Core needs a way to activate every burn; standalone libmlx may still run
    // burn-only requests without claiming host workflow completion.
    check_values(
        [
            Check {
                scenario: "host activates burn-only requests",
                input: (true, false),
                expect: true,
            },
            Check {
                scenario: "supervisor activates each card",
                input: (false, true),
                expect: true,
            },
            Check {
                scenario: "no activation owner",
                input: (false, false),
                expect: false,
            },
        ],
        |(reboot_host, reset)| {
            let mut operation = operation();
            operation.reboot_host = reboot_host;
            operation.targets[0].options.reset = reset;
            operation.validate().is_ok()
        },
    );
}

#[test]
fn target_uniqueness_rejects_each_identity_independently() {
    // Two otherwise distinct cards isolate each uniqueness rule; duplicating
    // the entire first target would let any one guard hide the missing others.
    type Mutation = fn(&mut NicFirmwareTarget, &NicFirmwareTarget);
    check_values(
        [
            Check {
                scenario: "different physical cards and journals",
                input: (|_, _| {}) as Mutation,
                expect: true,
            },
            Check {
                scenario: "same physical selector",
                input: |second, first| second.selector = first.selector.clone(),
                expect: false,
            },
            Check {
                scenario: "same PCI locator",
                input: |second, first| second.pci_name = first.pci_name.clone(),
                expect: false,
            },
            Check {
                scenario: "same journal operation ID",
                input: |second, first| second.operation_id = first.operation_id.clone(),
                expect: false,
            },
        ],
        |mutate| {
            let mut operation = operation();
            let first = &operation.targets[0];
            let mut second = first.clone();
            second.selector =
                NicFirmwareCardSelector::Mac("02:00:00:00:00:02".parse().expect("fixture MAC"));
            second.pci_name = "0000:02:00.0".into();
            second.operation_id = "nic-card-1".into();
            mutate(&mut second, first);
            operation.targets.push(second);
            operation.validate().is_ok()
        },
    );
}

#[test]
fn purpose_checks_current_allocation_generation_and_release() {
    let instance_id = InstanceId::new();
    let version = ConfigVersion::initial();
    let allocation = NicFirmwarePurpose::Allocation {
        instance_id,
        config_version: version,
    };
    check_values(
        [
            Check {
                scenario: "same live generation",
                input: Some((instance_id, version, false)),
                expect: true,
            },
            Check {
                scenario: "edited allocation",
                input: Some((instance_id, version.increment(), false)),
                expect: false,
            },
            Check {
                scenario: "released allocation",
                input: Some((instance_id, version, true)),
                expect: false,
            },
            Check {
                scenario: "missing allocation",
                input: None,
                expect: false,
            },
        ],
        |owner| allocation.matches_owner(owner),
    );
    let cleanup = NicFirmwarePurpose::RestoreBaseline {
        instance_id,
        cleanup_context: CleanupContext::Deprovision,
    };
    assert!(cleanup.matches_owner(None));
    assert!(!cleanup.matches_owner(Some((InstanceId::new(), version, true))));
}

#[test]
fn old_machine_snapshot_omission_is_absent_baseline() {
    use crate::machine::AnyMachine;
    use crate::machine::json::MachineSnapshotPgJson;
    use crate::test_support::machine_snapshot::{host_machine_id, machine_snapshot_pg_json};

    let accepted = baseline();
    let mut snapshot = machine_snapshot_pg_json(host_machine_id());
    snapshot.nic_firmware_baseline = Some(accepted.clone());
    let mut encoded = serde_json::to_value(snapshot).expect("encode machine snapshot");
    let decoded: MachineSnapshotPgJson =
        serde_json::from_value(encoded.clone()).expect("decode retained baseline");
    assert_eq!(
        AnyMachine::try_from(decoded)
            .expect("convert machine")
            .status
            .nic_firmware_baseline,
        Some(accepted)
    );

    // Predecessor rows and DPU projections have no baseline member, which must
    // stay genuine absence instead of invented successful firmware evidence.
    encoded
        .as_object_mut()
        .expect("snapshot object")
        .remove("nic_firmware_baseline");
    let decoded: MachineSnapshotPgJson =
        serde_json::from_value(encoded).expect("decode old machine snapshot");
    assert_eq!(
        AnyMachine::try_from(decoded)
            .expect("convert old machine")
            .status
            .nic_firmware_baseline,
        None
    );
}

#[test]
fn phase_display_is_bounded_and_nic_work_has_no_sla() {
    // Displayed phases exclude card positions and diagnostics; IO separately
    // checks the labels used by the controller's metrics.
    check_values(
        [
            Check {
                scenario: "card index is not a label",
                input: NicFirmwarePhase::UpdatingFirmware { target_index: 17 },
                expect: "NicFirmwareUpdating/UpdatingFirmware".to_string(),
            },
            Check {
                scenario: "failure detail is not a label",
                input: NicFirmwarePhase::Failed {
                    reason: "unique hardware failure".into(),
                },
                expect: "NicFirmwareUpdating/Failed".to_string(),
            },
        ],
        |phase| {
            let mut operation = operation();
            operation.phase = phase;
            ManagedHostState::NicFirmwareUpdating { operation }.to_string()
        },
    );

    // Epoch-old state without health exclusions still has no expiry. Delayed
    // firmware convergence must not acquire a generic state-machine deadline.
    let machine_id = "fm100htes3rn1npvbtm5qd57dkilaag7ljugl1llmm7rfuq1ov50i0rpl30"
        .parse()
        .expect("fixture host ID");
    let state = ManagedHostState::NicFirmwareUpdating {
        operation: operation(),
    };
    let sla = crate::machine::state_sla(
        &machine_id,
        &state,
        &ConfigVersion::invalid(),
        &health_report::HealthReport::empty("nic-firmware-test".into()),
        &crate::machine::slas::MachineSlaConfig::default(),
    );
    assert_eq!((sla.sla, sla.time_in_state_above_sla), (None, false));
}
