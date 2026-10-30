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

//! Durable generic NIC firmware progress owned by the managed-host controller.
//! An admitted operation freezes its target identity, artifacts and execution
//! policy. Receipts describe execution; fresh inventory establishes convergence.

use carbide_libmlx_model::device::info::MlxDeviceInfo;
use carbide_libmlx_model::firmware::operation::{
    FirmwareOperation, FirmwareOperationPhase, FirmwareOperationState,
};
use carbide_uuid::instance::InstanceId;
use chrono::{DateTime, Utc};
use config_version::ConfigVersion;
use mac_address::MacAddress;
use serde::{Deserialize, Serialize};

use super::CleanupContext;
use crate::ConfigValidationError;
use crate::nic_firmware::{
    NicFirmwareCardSelector, NicFirmwareProfileConfig, NicFirmwareResolution,
};
use crate::pci::PciBdfOrderingKey;

#[cfg(test)]
mod tests;

/// One admitted host workflow, processing its frozen targets in order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NicFirmwareOperation {
    /// Stable host workflow identity: 1..=256 printable nonspace ASCII bytes,
    /// independent of controller revisions.
    pub operation_id: String,
    /// Owner and permitted continuation after successful access restoration.
    pub purpose: NicFirmwarePurpose,
    /// Nonempty ordered physical targets supplied by admission; never re-resolved.
    pub targets: Vec<NicFirmwareTarget>,
    /// Durable recovery point; external work follows its committed intent.
    pub phase: NicFirmwarePhase,
    /// Request one host reboot after all burns, instead of supervisor card resets.
    /// Exactly one of this flag and each target's `options.reset` must be true.
    pub reboot_host: bool,
    /// Current access request and its correlated report. Omission starts with a
    /// read; phase and card transitions discard the previous report.
    #[serde(default)]
    pub access: NicFirmwareAccessProgress,
}

/// The lifecycle that admitted the workflow and owns its successful continuation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NicFirmwarePurpose {
    /// Establish or update the tenant-free baseline.
    Baseline {
        /// Existing host preparation or idle state to resume.
        continuation: NicFirmwareBaselineContinuation,
    },
    /// Apply the firmware requirement frozen for one allocation generation, then
    /// resume `InstanceState::DpaProvisioning`.
    Allocation {
        /// Allocation that must still own the host at completion.
        instance_id: InstanceId,
        /// Frozen allocation configuration version, not controller-state version.
        config_version: ConfigVersion,
    },
    /// Restore the retained baseline while releasing one allocation, then resume
    /// `CleanupState::Init` with the original cleanup context.
    RestoreBaseline {
        /// Allocation whose cleanup owns this work.
        instance_id: InstanceId,
        /// Existing cleanup reason, preserved across firmware work.
        cleanup_context: CleanupContext,
    },
}

/// Supported tenant-free continuation points, without arbitrary host states.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NicFirmwareBaselineContinuation {
    /// Resume the discovered host's existing boot configuration and preparation.
    InitialPreparation {
        /// Preserve the discovered host's existing reboot-wait choice.
        skip_reboot_wait: bool,
    },
    /// Return an idle, already prepared host to the allocation pool.
    Ready,
}

/// A frozen card request and the evidence accumulated for that exact request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NicFirmwareTarget {
    /// Stable card identity used to check ordinary inventory after activation.
    pub selector: NicFirmwareCardSelector,
    /// Full lowercase PCI BDF retained for the supervisor journal and all retries.
    pub pci_name: String,
    /// Exact selected profile revision, hardware pair, source URLs and digests.
    pub resolution: NicFirmwareResolution,
    /// Frozen phase choices; later configuration changes cannot alter a request.
    pub options: NicFirmwareExecutionOptions,
    /// Access state to restore, including the original nonsecret key context.
    pub original_access: NicFirmwareAccessPolicy,
    /// Stable supervisor identity, unique among this workflow's cards: 1..=256
    /// printable nonspace ASCII bytes, without the reserved `dpa-firmware:` prefix.
    pub operation_id: String,
    /// Immutable whole-workflow correlation token, not a supervisor step name;
    /// 1..=256 printable nonspace ASCII bytes.
    pub phase_id: String,
    /// Helper instance first observed without this receipt. Never refreshed after
    /// it is set; missing state from another helper cannot authorize a new burn.
    /// Present tokens contain 1..=256 printable nonspace ASCII bytes.
    pub supervisor_instance_id: Option<String>,
    /// Last accepted correlated receipt; a terminal receipt cannot regress.
    pub receipt: Option<FirmwareOperation>,
    /// Latest correlated command diagnostic while execution or access remains
    /// unresolved; a successful access read clears its prior command error.
    pub diagnostic: Option<String>,
    /// Accepted ordinary inventory after the requested activation boundary.
    pub observation: Option<NicFirmwareDeviceObservation>,
}

/// Core's frozen execution choices, translated to libmlx options at delivery.
/// Optional timing remains absent when omitted, preserving request identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NicFirmwareExecutionOptions {
    /// Request comparison against the staged firmware image.
    pub verify_image: bool,
    /// Request exact-version verification by the supervisor.
    pub verify_version: bool,
    /// Request a supervisor-owned reset after burn.
    pub reset: bool,
    /// MFT reset level, used only when reset is requested.
    pub reset_level: u8,
    /// Post-reset delay between verification attempts, in 1..=1800 seconds;
    /// omission uses 10 seconds and never enables a verification phase.
    pub post_reset_verify_retry_interval_seconds: Option<u32>,
    /// Supervisor post-reset verification budget, in 1..=1800 seconds; omission
    /// uses 1800 seconds. This is not a deadline for host reconciliation.
    pub post_reset_verify_timeout_seconds: Option<u32>,
}

/// Access policy captured before changing the card; credentials are never saved.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NicFirmwareAccessPolicy {
    /// Leave a card that was already unlocked unlocked.
    Unlocked,
    /// Restore the exact key identity under which the card was locked.
    Locked {
        /// Version of the site-wide lockdown input-key material.
        ikm_version: u32,
        /// MAC used with the owning host ID by the existing lockdown KDF.
        mac_address: MacAddress,
    },
}

/// A successful live access read; unsupported or unknown status is not unlocked.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NicFirmwareLockdownState {
    /// Hardware access is locked.
    Locked,
    /// Hardware access is unlocked.
    Unlocked,
}

/// `NicFirmwareAccessProgress` separates access changes from their readback.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NicFirmwareAccessProgress {
    /// One command selected by the controller for the current state revision.
    pub step: NicFirmwareAccessStep,
    /// Authenticated report accepted for that exact revision, or absence while
    /// delivery remains pending. A stored report suppresses further delivery.
    pub result: Option<NicFirmwareAccessResult>,
}

/// `NicFirmwareAccessStep` identifies a read or an idempotent access change.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NicFirmwareAccessStep {
    /// Read the selected card's access state without changing it.
    #[default]
    Observe,
    /// Apply the access policy required by the current controller phase.
    Change,
}

/// `NicFirmwareAccessResult` preserves command success without inventing readback.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NicFirmwareAccessResult {
    /// Independently read access state of the exact selected physical card.
    Status(NicFirmwareLockdownState),
    /// The change command completed; a later status request is still required.
    Changed,
    /// The command failed or its effect is uncertain; observe before retrying.
    Error {
        /// Nonsecret explanation retained until the next controller iteration.
        diagnostic: String,
    },
}

/// The controller's recovery point, separate from supervisor execution phases.
/// Display uses only the phase name, excluding per-operation values from labels.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NicFirmwarePhase {
    /// Observe the receipt before granting this helper permission to submit.
    ObservingSupervisor {
        /// Index of the sole card being processed.
        target_index: usize,
    },
    /// Obtain live unlocked status before submitting firmware work.
    Unlocking {
        /// Index of the sole card being processed.
        target_index: usize,
    },
    /// Deliver or observe the immutable request until its execution settles.
    UpdatingFirmware {
        /// Index of the sole card being processed.
        target_index: usize,
    },
    /// Activate all completed burns through one planned host reboot.
    Rebooting {
        /// Committed start of the activation stage. Reboot reports must arrive
        /// after this boundary and cover the latest attempt and power request;
        /// their receipt time does not identify a hardware boot.
        started_at: DateTime<Utc>,
        /// Last bounded reboot attempt, or absence before the first attempt.
        last_attempt_at: Option<DateTime<Utc>>,
    },
    /// Wait across bounded iterations for fresh, exact-version ordinary inventory.
    Reobserving {
        /// Inventory received by Core at or before this activation boundary
        /// cannot establish success; Scout's collection clock is not compared.
        not_before: DateTime<Utc>,
    },
    /// Restore and independently read back each card's original access policy.
    RestoringAccess {
        /// Index of the sole card whose access is being restored.
        target_index: usize,
        /// Execution outcome retained while access is restored.
        outcome: NicFirmwareOutcome,
    },
    /// Safe access restoration finished, but firmware convergence did not succeed.
    Failed {
        /// Nonsecret actionable explanation; this state does not permit a reburn.
        reason: String,
    },
}

impl std::fmt::Display for NicFirmwarePhase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::ObservingSupervisor { .. } => "ObservingSupervisor",
            Self::Unlocking { .. } => "Unlocking",
            Self::UpdatingFirmware { .. } => "UpdatingFirmware",
            Self::Rebooting { .. } => "Rebooting",
            Self::Reobserving { .. } => "Reobserving",
            Self::RestoringAccess { .. } => "RestoringAccess",
            Self::Failed { .. } => "Failed",
        })
    }
}

/// Firmware outcome carried through access restoration without losing failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NicFirmwareOutcome {
    /// Every required phase and the final ordinary observation succeeded.
    Succeeded,
    /// A known execution failure prevents successful continuation.
    Failed {
        /// Nonsecret explanation retained through restoration.
        reason: String,
    },
}

/// One selected device from an ordinary, unfiltered Scout inventory report.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NicFirmwareDeviceObservation {
    /// Scout's observation timestamp, not Core receipt time or an operation token.
    pub observed_at: DateTime<Utc>,
    /// Core received this ordinary report after the activation boundary. This
    /// establishes report ordering, not a hardware boot or collection timestamp.
    pub received_at: DateTime<Utc>,
    /// Exact reported hardware and installed-version evidence accepted by Core.
    pub device: MlxDeviceInfo,
}

/// Successfully established baseline retained after the active workflow ends.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NicFirmwareBaseline {
    /// Latest host operation that updated this baseline; untouched card recipes
    /// and their accepted observations remain from earlier operations.
    pub operation_id: String,
    /// Confirmed cards, preserving their recipe and access policy for restoration.
    pub targets: Vec<NicFirmwareBaselineTarget>,
}

/// Accepted baseline evidence for one card, without transient execution progress.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NicFirmwareBaselineTarget {
    /// Stable identity of the card whose baseline was confirmed.
    pub selector: NicFirmwareCardSelector,
    /// PCI locator associated with that accepted identity.
    pub pci_name: String,
    /// Exact profile revision and artifact digests needed to restore this baseline.
    pub resolution: NicFirmwareResolution,
    /// Original access policy confirmed after baseline preparation.
    pub access: NicFirmwareAccessPolicy,
    /// Ordinary inventory evidence establishing the accepted exact version.
    pub observation: NicFirmwareDeviceObservation,
}

impl NicFirmwareOperation {
    /// Return the access policy requested by an access phase, or absence while
    /// firmware execution, activation or inventory reconciliation is in progress.
    pub fn desired_access(&self) -> Option<NicFirmwareLockdownState> {
        match self.phase {
            NicFirmwarePhase::Unlocking { .. } => Some(NicFirmwareLockdownState::Unlocked),
            NicFirmwarePhase::RestoringAccess { .. } => {
                self.current_target()
                    .map(|target| match target.original_access {
                        NicFirmwareAccessPolicy::Unlocked => NicFirmwareLockdownState::Unlocked,
                        NicFirmwareAccessPolicy::Locked { .. } => NicFirmwareLockdownState::Locked,
                    })
            }
            _ => None,
        }
    }

    /// Return the card owned by a card-local phase, or absence for host-wide work.
    fn current_target_index(&self) -> Option<usize> {
        match self.phase {
            NicFirmwarePhase::ObservingSupervisor { target_index }
            | NicFirmwarePhase::Unlocking { target_index }
            | NicFirmwarePhase::UpdatingFirmware { target_index }
            | NicFirmwarePhase::RestoringAccess { target_index, .. } => Some(target_index),
            NicFirmwarePhase::Rebooting { .. }
            | NicFirmwarePhase::Reobserving { .. }
            | NicFirmwarePhase::Failed { .. } => None,
        }
    }

    /// Resolve the current card without panicking on malformed persisted indices.
    pub fn current_target(&self) -> Option<&NicFirmwareTarget> {
        self.targets.get(self.current_target_index()?)
    }

    /// Accept only the current card's exact correlated execution receipt during
    /// initial observation or execution monitoring; no submission token is granted.
    /// Stale selectors, duplicate values and updates after a terminal receipt are
    /// ignored. The return value says whether the saved operation changed.
    pub fn accept_receipt(
        &mut self,
        operation_id: &str,
        phase_id: &str,
        pci_name: &str,
        receipt: FirmwareOperation,
    ) -> bool {
        let target_index = match self.phase {
            NicFirmwarePhase::ObservingSupervisor { target_index }
            | NicFirmwarePhase::UpdatingFirmware { target_index } => target_index,
            _ => return false,
        };
        let Some(target) = self.targets.get_mut(target_index) else {
            return false;
        };
        if !target.matches_result(operation_id, phase_id, pci_name)
            || receipt.operation_id != operation_id
            || target
                .receipt
                .as_ref()
                .is_some_and(|previous| is_terminal(previous.state) || previous == &receipt)
        {
            return false;
        }
        target.receipt = Some(receipt);
        target.diagnostic = None;
        true
    }

    /// Freeze the initial helper identity from a correlated missing-receipt read.
    /// Later missing evidence records uncertainty without replacing that token
    /// or a terminal receipt. Malformed tokens and unrelated results are ignored.
    pub fn accept_missing_receipt(
        &mut self,
        operation_id: &str,
        phase_id: &str,
        pci_name: &str,
        supervisor_instance_id: String,
    ) -> bool {
        let target_index = match self.phase {
            NicFirmwarePhase::ObservingSupervisor { target_index }
            | NicFirmwarePhase::UpdatingFirmware { target_index } => target_index,
            _ => return false,
        };
        let Some(target) = self.targets.get_mut(target_index) else {
            return false;
        };
        if !target.matches_result(operation_id, phase_id, pci_name)
            || !valid_token(&supervisor_instance_id)
            || target
                .receipt
                .as_ref()
                .is_some_and(|receipt| is_terminal(receipt.state))
        {
            return false;
        }
        if target.supervisor_instance_id.is_none()
            && target.receipt.is_none()
            && matches!(self.phase, NicFirmwarePhase::ObservingSupervisor { .. })
        {
            target.supervisor_instance_id = Some(supervisor_instance_id);
            target.diagnostic = None;
            return true;
        }
        let diagnostic = if target.receipt.is_some() {
            "previously accepted firmware receipt is unavailable; observing without resubmitting"
        } else if target.supervisor_instance_id.as_ref() != Some(&supervisor_instance_id) {
            "firmware supervisor instance changed without a receipt; initial submission permission is not renewed"
        } else {
            return false;
        };
        if target.diagnostic.as_deref() == Some(diagnostic) {
            return false;
        }
        target.diagnostic = Some(diagnostic.to_string());
        true
    }

    /// Retain a correlated delivery failure without replacing settled execution
    /// evidence. This diagnostic never authorizes another helper instance.
    pub fn accept_error(
        &mut self,
        operation_id: &str,
        phase_id: &str,
        pci_name: &str,
        diagnostic: String,
    ) -> bool {
        let target_index = match self.phase {
            NicFirmwarePhase::ObservingSupervisor { target_index }
            | NicFirmwarePhase::UpdatingFirmware { target_index } => target_index,
            _ => return false,
        };
        let Some(target) = self.targets.get_mut(target_index) else {
            return false;
        };
        if !target.matches_result(operation_id, phase_id, pci_name)
            || target
                .receipt
                .as_ref()
                .is_some_and(|receipt| is_terminal(receipt.state))
            || target.diagnostic.as_ref() == Some(&diagnostic)
        {
            return false;
        }
        target.diagnostic = Some(diagnostic);
        true
    }

    /// Validate operation identity, card uniqueness and activation ownership.
    /// This does not qualify hardware or authorize production admission.
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        if !valid_token(&self.operation_id) || self.targets.is_empty() {
            return Err(ConfigValidationError::InvalidValue(
                "NIC firmware operation requires a valid identity and at least one target".into(),
            ));
        }
        if self
            .current_target_index()
            .is_some_and(|index| index >= self.targets.len())
        {
            return Err(ConfigValidationError::InvalidValue(
                "NIC firmware phase references a missing target".into(),
            ));
        }
        for (index, target) in self.targets.iter().enumerate() {
            target.selector.clone().validate()?;
            if !valid_token(&target.operation_id)
                || target.operation_id.starts_with("dpa-firmware:")
                || !valid_token(&target.phase_id)
                || target
                    .supervisor_instance_id
                    .as_ref()
                    .is_some_and(|token| !valid_token(token))
            {
                return Err(ConfigValidationError::InvalidValue(
                    "NIC firmware target requires valid operation and phase identities".into(),
                ));
            }
            let pci = target.pci_name.as_bytes();
            if pci.len() != 12
                || pci[4] != b':'
                || pci[7] != b':'
                || pci[10] != b'.'
                || pci.iter().any(u8::is_ascii_uppercase)
                || PciBdfOrderingKey::from_bdf(&target.pci_name).is_err()
            {
                return Err(ConfigValidationError::InvalidValue(
                    "NIC firmware target requires a full lowercase PCI BDF".into(),
                ));
            }
            NicFirmwareProfileConfig {
                entries: vec![target.resolution.entry.clone()],
            }
            .validate_and_normalize()?;
            for seconds in [
                target.options.post_reset_verify_retry_interval_seconds,
                target.options.post_reset_verify_timeout_seconds,
            ]
            .into_iter()
            .flatten()
            {
                if !(1..=1800).contains(&seconds) {
                    return Err(ConfigValidationError::InvalidValue(
                        "NIC firmware post-reset verification timing must be in 1..=1800 seconds"
                            .into(),
                    ));
                }
            }
            if let NicFirmwareAccessPolicy::Locked { mac_address, .. } = target.original_access {
                NicFirmwareCardSelector::Mac(mac_address).validate()?;
                if matches!(target.selector, NicFirmwareCardSelector::Mac(selected) if selected != mac_address)
                {
                    return Err(ConfigValidationError::InvalidValue(
                        "NIC firmware lockdown context does not match the selected card".into(),
                    ));
                }
            }
            if self.reboot_host == target.options.reset {
                return Err(ConfigValidationError::InvalidValue(
                    "NIC firmware activation requires exactly one of card reset or host reboot"
                        .into(),
                ));
            }
            if self.targets[..index].iter().any(|previous| {
                previous.selector == target.selector
                    || previous.pci_name == target.pci_name
                    || previous.operation_id == target.operation_id
            }) {
                return Err(ConfigValidationError::InvalidValue(
                    "NIC firmware targets must have distinct identities, PCI locators and operation IDs".into(),
                ));
            }
        }
        // Both controller reconciliation and Scout command delivery validate
        // persisted phases. Neither may bypass the receipts normally required
        // by the predecessor before activation or access restoration.
        if matches!(
            self.phase,
            NicFirmwarePhase::Rebooting { .. }
                | NicFirmwarePhase::Reobserving { .. }
                | NicFirmwarePhase::RestoringAccess {
                    outcome: NicFirmwareOutcome::Succeeded,
                    ..
                }
        ) && self.targets.iter().any(|target| !target.is_complete())
        {
            return Err(ConfigValidationError::InvalidValue(
                "NIC firmware activation and continuation require successful requested-phase evidence".into(),
            ));
        }
        if matches!(self.phase, NicFirmwarePhase::RestoringAccess { .. })
            && self
                .targets
                .iter()
                .any(|target| target.receipt.is_some() && !target.can_restore_access())
        {
            return Err(ConfigValidationError::InvalidValue(
                "NIC firmware access cannot be restored while execution effects remain uncertain"
                    .into(),
            ));
        }
        Ok(())
    }
}

impl NicFirmwarePurpose {
    /// Check current allocation ownership before delivery or continuation.
    /// The tuple contains instance ID, configuration version and deletion status.
    /// Cleanup may continue after its deleted instance has been removed, but not
    /// after a different allocation attaches to the host.
    pub fn matches_owner(&self, owner: Option<(InstanceId, ConfigVersion, bool)>) -> bool {
        match (self, owner) {
            (Self::Baseline { .. }, None) | (Self::RestoreBaseline { .. }, None) => true,
            (
                Self::Allocation {
                    instance_id,
                    config_version,
                    ..
                },
                Some((current_id, current_version, deleted)),
            ) => *instance_id == current_id && *config_version == current_version && !deleted,
            (Self::RestoreBaseline { instance_id, .. }, Some((current_id, _, deleted))) => {
                *instance_id == current_id && deleted
            }
            _ => false,
        }
    }
}

impl NicFirmwareTarget {
    /// Require completed execution or failure before configuration/burn before
    /// restoring access. A terminal label alone does not settle uncertain effects.
    /// Missing receipts return false; callers may separately know that delivery
    /// never reached the firmware-submission phase.
    pub fn can_restore_access(&self) -> bool {
        self.receipt.as_ref().is_some_and(|receipt| {
            receipt.operation_id == self.operation_id
                && match receipt.state {
                    FirmwareOperationState::Complete => true,
                    FirmwareOperationState::Failed | FirmwareOperationState::Canceled => {
                        matches!(
                            receipt.phase,
                            None | Some(
                                FirmwareOperationPhase::Inspect | FirmwareOperationPhase::Prepare
                            )
                        ) && !receipt.report.flashed
                            && receipt.report.reset != Some(true)
                            && receipt.report.verified_image != Some(true)
                            && receipt.report.verified_version != Some(true)
                    }
                    _ => false,
                }
        })
    }

    /// Match every immutable selector carried by a generic execution result.
    pub fn matches_result(&self, operation_id: &str, phase_id: &str, pci_name: &str) -> bool {
        self.operation_id == operation_id && self.phase_id == phase_id && self.pci_name == pci_name
    }

    /// Require a completed burn and explicit success for exactly the requested
    /// phases. Missing required evidence cannot be repaired by later inventory.
    pub fn is_complete(&self) -> bool {
        self.receipt.as_ref().is_some_and(|receipt| {
            receipt.operation_id == self.operation_id
                && receipt.is_complete()
                && receipt.report.flashed
                && receipt.report.expected_version.as_deref()
                    == Some(self.resolution.entry.firmware.version.as_str())
                && receipt.report.reset == self.options.reset.then_some(true)
                && receipt.report.verified_image == self.options.verify_image.then_some(true)
                && receipt.report.verified_version == self.options.verify_version.then_some(true)
                && (!self.options.verify_version
                    || receipt.report.observed_version.as_deref()
                        == Some(self.resolution.entry.firmware.version.as_str()))
        })
    }
}

fn valid_token(value: &str) -> bool {
    (1..=256).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn is_terminal(state: FirmwareOperationState) -> bool {
    !matches!(
        state,
        FirmwareOperationState::Accepted
            | FirmwareOperationState::Running
            | FirmwareOperationState::CancelRequested
    )
}
