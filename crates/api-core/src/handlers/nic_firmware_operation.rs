/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Delivery and receipt ingestion for the machine controller's frozen NIC work.
//! This module cannot admit an operation or select a new firmware target.

use carbide_libmlx_model::device::identity::MlxDeviceIdentity;
use carbide_libmlx_model::device::info::MlxDeviceInfo;
use carbide_libmlx_model::firmware::operation::FirmwareOperation;
use carbide_uuid::machine::{HostMachineId, MachineId};
use db::ConditionalWrite;
use model::machine::ManagedHostState;
use model::machine::nic_firmware::{
    NicFirmwareAccessResult, NicFirmwareAccessStep, NicFirmwareLockdownState, NicFirmwareOperation,
    NicFirmwarePhase, NicFirmwareTarget,
};
use rpc::forge_agent_control_response as fac;
use rpc::protos::mlx_device as pb;
use rpc::protos::mlx_device::mlx_device_update_firmware_result::Result as WireResult;

use super::utils::{StateHandlerWakeupFailed, WakeupTrigger};
use crate::api::Api;
use crate::{CarbideError, CarbideResult};

/// Build only the command authorized by the already committed machine state.
/// Once a receipt is known, polling cannot submit even if the helper disappears.
pub(super) async fn control_action(
    api: &Api,
    host: HostMachineId,
    version: config_version::ConfigVersion,
    operation: &NicFirmwareOperation,
    owner_is_current: bool,
) -> CarbideResult<fac::Action> {
    operation.validate()?;
    if matches!(operation.phase, NicFirmwarePhase::Reobserving { .. }) {
        return Ok(fac::Action::MlxDeviceInfoReport(
            pb::MlxDeviceInfoReportRequest { filters: None },
        ));
    }
    let Some(target) = operation.current_target() else {
        return Ok(fac::Action::noop());
    };
    let command = match operation.phase {
        NicFirmwarePhase::Unlocking { .. } | NicFirmwarePhase::RestoringAccess { .. } => {
            if operation.access.result.is_some()
                || (matches!(operation.phase, NicFirmwarePhase::Unlocking { .. })
                    && operation.access.step == NicFirmwareAccessStep::Change
                    && !owner_is_current)
            {
                return Ok(fac::Action::noop());
            }
            let desired = operation.desired_access().ok_or_else(|| {
                CarbideError::FailedPrecondition(
                    "NIC firmware access phase has no desired policy".into(),
                )
            })?;
            fac::mlx_device_action::Command::ReconcileFirmwareAccess(
                crate::nic_firmware::access_request(
                    host,
                    version,
                    target,
                    operation.access.step,
                    desired,
                    &*api.credential_manager,
                )
                .await?,
            )
        }
        NicFirmwarePhase::ObservingSupervisor { .. } => {
            fac::mlx_device_action::Command::ObserveFirmwareUpdate(observe_request(target))
        }
        NicFirmwarePhase::UpdatingFirmware { .. } => {
            if target.receipt.is_some() || !owner_is_current {
                fac::mlx_device_action::Command::ObserveFirmwareUpdate(observe_request(target))
            } else {
                let instance = target.supervisor_instance_id.as_ref().ok_or_else(|| {
                    CarbideError::FailedPrecondition(
                        "NIC firmware submission has no saved supervisor instance".into(),
                    )
                })?;
                fac::mlx_device_action::Command::UpdateFirmware(fac::MlxDeviceUpdateFirmware {
                    operation_id: target.operation_id.clone(),
                    phase_id: target.phase_id.clone(),
                    supervisor_instance_id: Some(instance.clone()),
                    profile: Some(frozen_profile(target)),
                    expected_digests: Some(frozen_digests(target)),
                    expected_device: Some(target.selector.clone().into()),
                })
            }
        }
        _ => return Ok(fac::Action::noop()),
    };
    Ok(fac::Action::MlxAction(fac::MlxAction {
        device_actions: vec![fac::MlxDeviceAction {
            pci_name: target.pci_name.clone(),
            command: Some(command),
        }],
    }))
}

/// Observation carries precisely the same immutable inputs as submission. The
/// supervisor compares its saved request before returning an existing receipt.
fn observe_request(target: &NicFirmwareTarget) -> fac::MlxDeviceObserveFirmwareUpdate {
    fac::MlxDeviceObserveFirmwareUpdate {
        operation_id: target.operation_id.clone(),
        phase_id: target.phase_id.clone(),
        profile: Some(frozen_profile(target)),
        expected_digests: Some(frozen_digests(target)),
        expected_device: Some(target.selector.clone().into()),
    }
}

fn frozen_profile(target: &NicFirmwareTarget) -> pb::FirmwareFlasherProfile {
    let entry = &target.resolution.entry;
    let options = &target.options;
    pb::FirmwareFlasherProfile {
        firmware_spec: Some(entry.firmware.clone().into()),
        flash_spec: Some(pb::FlashSpec {
            firmware_url: entry.image.url.to_string(),
            device_conf_url: entry
                .device_config
                .as_ref()
                .map(|artifact| artifact.url.to_string()),
            ..Default::default()
        }),
        flash_options: Some(pb::FlashOptions {
            verify_image: options.verify_image,
            verify_version: options.verify_version,
            reset: options.reset,
            reset_level: u32::from(options.reset_level),
            post_reset_verify_retry_interval_seconds: options
                .post_reset_verify_retry_interval_seconds,
            post_reset_verify_timeout_seconds: options.post_reset_verify_timeout_seconds,
        }),
    }
}

fn frozen_digests(target: &NicFirmwareTarget) -> pb::FirmwareArtifactDigests {
    let entry = &target.resolution.entry;
    pb::FirmwareArtifactDigests {
        image_sha256: entry.image.sha256.clone(),
        configuration_sha256: entry
            .device_config
            .as_ref()
            .map(|artifact| artifact.sha256.clone()),
    }
}

struct FirmwareUpdate {
    operation_id: String,
    phase_id: String,
    pci_name: String,
    device_info: Option<MlxDeviceInfo>,
    result: FirmwareUpdateResult,
}

enum FirmwareUpdateResult {
    Receipt(FirmwareOperation),
    MissingReceipt(String),
    Error(pb::MlxDeviceUpdateFirmwareErrorCode, String),
}

/// Access replies are evidence for one machine-state revision, not reusable
/// lock state. In particular a successful change cannot stand in for readback.
struct FirmwareAccess {
    version: String,
    pci_name: String,
    expected_device: MlxDeviceIdentity,
    device_info: Option<MlxDeviceInfo>,
    result: NicFirmwareAccessResult,
}

impl TryFrom<&pb::MlxObservation> for FirmwareAccess {
    type Error = CarbideError;

    fn try_from(observation: &pb::MlxObservation) -> CarbideResult<Self> {
        use pb::mlx_device_firmware_access_result::Result as AccessResult;

        if observation.lock_status.is_some()
            || observation.profile_name.is_some()
            || observation.profile_synced.is_some()
            || observation.firmware_report.is_some()
            || observation.firmware_operation.is_some()
            || observation.firmware_update_result.is_some()
        {
            return Err(CarbideError::InvalidArgument(
                "generic NIC access results cannot include firmware or legacy fields".into(),
            ));
        }
        let access = observation.firmware_access_result.as_ref().ok_or_else(|| {
            CarbideError::InvalidArgument("NIC firmware access result is required".into())
        })?;
        validate_token(&access.controller_state_version)?;
        let expected_device = access
            .expected_device
            .clone()
            .ok_or_else(|| {
                CarbideError::InvalidArgument("NIC firmware access selector is required".into())
            })?
            .try_into()
            .map_err(
                |error: carbide_libmlx_model::device::identity::MlxDeviceIdentityError| {
                    CarbideError::InvalidArgument(error.to_string())
                },
            )?;
        let device_info = observation
            .device_info
            .clone()
            .map(TryInto::try_into)
            .transpose()
            .map_err(CarbideError::InvalidArgument)?;
        let result = match access.result.as_ref() {
            Some(AccessResult::StatusReport(status)) => {
                if status.device_id != access.pci_name {
                    return Err(CarbideError::InvalidArgument(
                        "NIC firmware access status target contradicts its result".into(),
                    ));
                }
                chrono::DateTime::parse_from_rfc3339(&status.timestamp).map_err(|_| {
                    CarbideError::InvalidArgument(
                        "NIC firmware access status timestamp is invalid".into(),
                    )
                })?;
                let state = match pb::LockStatus::try_from(status.status) {
                    Ok(pb::LockStatus::Locked) => NicFirmwareLockdownState::Locked,
                    Ok(pb::LockStatus::Unlocked) => NicFirmwareLockdownState::Unlocked,
                    _ => {
                        return Err(CarbideError::InvalidArgument(
                            "NIC firmware access status must be known".into(),
                        ));
                    }
                };
                NicFirmwareAccessResult::Status(state)
            }
            Some(AccessResult::Changed(_)) => NicFirmwareAccessResult::Changed,
            Some(AccessResult::Error(error)) => {
                validate_diagnostic(&error.message)?;
                NicFirmwareAccessResult::Error {
                    diagnostic: error.message.clone(),
                }
            }
            None => {
                return Err(CarbideError::InvalidArgument(
                    "NIC firmware access result is required".into(),
                ));
            }
        };
        if !matches!(result, NicFirmwareAccessResult::Error { .. }) && device_info.is_none() {
            return Err(CarbideError::InvalidArgument(
                "successful NIC firmware access result requires observed device identity".into(),
            ));
        }
        Ok(Self {
            version: access.controller_state_version.clone(),
            pci_name: access.pci_name.clone(),
            expected_device,
            device_info,
            result,
        })
    }
}

impl FirmwareAccess {
    /// A later controller revision has consumed or superseded this evidence.
    /// Ignore it without overwriting the result awaiting the current iteration.
    fn apply(
        self,
        operation: &mut NicFirmwareOperation,
        version: config_version::ConfigVersion,
    ) -> CarbideResult<bool> {
        if self.version != version.to_string()
            || operation.desired_access().is_none()
            || operation.access.result.is_some()
        {
            return Ok(false);
        }
        let Some(target) = operation.current_target() else {
            return Ok(false);
        };
        if self.pci_name != target.pci_name || self.expected_device != target.selector {
            return Ok(false);
        }
        if self.device_info.as_ref().is_some_and(|device| {
            device.pci_name != target.pci_name || !target.selector.matches(device)
        }) {
            return Err(CarbideError::InvalidArgument(
                "NIC firmware access result device identity contradicts its frozen target".into(),
            ));
        }
        if !matches!(
            (&self.result, operation.access.step),
            (
                NicFirmwareAccessResult::Status(_),
                NicFirmwareAccessStep::Observe
            ) | (
                NicFirmwareAccessResult::Changed,
                NicFirmwareAccessStep::Change
            ) | (NicFirmwareAccessResult::Error { .. }, _)
        ) {
            return Err(CarbideError::InvalidArgument(
                "NIC firmware access result does not match the requested step".into(),
            ));
        }
        operation.access.result = Some(self.result);
        Ok(true)
    }
}

impl TryFrom<&pb::MlxObservation> for FirmwareUpdate {
    type Error = CarbideError;

    fn try_from(observation: &pb::MlxObservation) -> CarbideResult<Self> {
        if observation.lock_status.is_some()
            || observation.profile_name.is_some()
            || observation.profile_synced.is_some()
            || observation.firmware_report.is_some()
            || observation.firmware_operation.is_some()
            || observation.firmware_access_result.is_some()
        {
            return Err(CarbideError::InvalidArgument(
                "generic NIC firmware results cannot include legacy operation or access fields"
                    .into(),
            ));
        }
        let update = observation.firmware_update_result.as_ref().ok_or_else(|| {
            CarbideError::InvalidArgument("NIC firmware update result is required".into())
        })?;
        validate_token(&update.operation_id)?;
        validate_token(&update.phase_id)?;
        let result = match update.result.as_ref() {
            Some(WireResult::Receipt(receipt)) => {
                if receipt.operation_id != update.operation_id {
                    return Err(CarbideError::InvalidArgument(
                        "NIC firmware receipt operation ID contradicts its result".into(),
                    ));
                }
                if let Some(diagnostic) = &receipt.diagnostic {
                    validate_diagnostic(diagnostic)?;
                }
                FirmwareUpdateResult::Receipt(
                    receipt
                        .clone()
                        .try_into()
                        .map_err(CarbideError::InvalidArgument)?,
                )
            }
            Some(WireResult::MissingReceipt(missing)) => {
                validate_token(&missing.supervisor_instance_id)?;
                FirmwareUpdateResult::MissingReceipt(missing.supervisor_instance_id.clone())
            }
            Some(WireResult::Error(error)) => {
                let code =
                    pb::MlxDeviceUpdateFirmwareErrorCode::try_from(error.code).map_err(|_| {
                        CarbideError::InvalidArgument("unknown firmware request error".into())
                    })?;
                if code == pb::MlxDeviceUpdateFirmwareErrorCode::Unspecified {
                    return Err(CarbideError::InvalidArgument(
                        "firmware request error code is required".into(),
                    ));
                }
                validate_diagnostic(&error.diagnostic)?;
                FirmwareUpdateResult::Error(code, error.diagnostic.clone())
            }
            None => {
                return Err(CarbideError::InvalidArgument(
                    "firmware result is required".into(),
                ));
            }
        };
        Ok(Self {
            operation_id: update.operation_id.clone(),
            phase_id: update.phase_id.clone(),
            pci_name: update.pci_name.clone(),
            device_info: observation
                .device_info
                .clone()
                .map(TryInto::try_into)
                .transpose()
                .map_err(CarbideError::InvalidArgument)?,
            result,
        })
    }
}

fn validate_token(token: &str) -> CarbideResult<()> {
    if !(1..=256).contains(&token.len()) || !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(CarbideError::InvalidArgument(
            "firmware identifiers must contain 1-256 printable nonspace ASCII bytes".into(),
        ));
    }
    Ok(())
}

fn validate_diagnostic(diagnostic: &str) -> CarbideResult<()> {
    if diagnostic.is_empty() || diagnostic.len() > 2048 {
        return Err(CarbideError::InvalidArgument(
            "firmware diagnostic must contain 1-2048 UTF-8 bytes".into(),
        ));
    }
    Ok(())
}

/// Store authenticated generic results without consulting DPA feature flags.
/// Validate the whole generic batch before writing, then condition the state
/// change on its original revision so a delayed response cannot overwrite a
/// newer controller iteration. No hardware command is sent from ingestion.
pub(super) async fn persist(
    api: &Api,
    authenticated_machine_id: MachineId,
    report: &pb::MlxObservationReport,
) -> CarbideResult<()> {
    let host_id = HostMachineId::try_from(authenticated_machine_id)?;
    if report.machine_id != Some(authenticated_machine_id) {
        return Err(CarbideError::PermissionDeniedError(
            "firmware report machine ID does not match the authenticated host".into(),
        ));
    }
    let updates: Vec<_> = report
        .observations
        .iter()
        .filter(|observation| observation.firmware_update_result.is_some())
        .map(FirmwareUpdate::try_from)
        .collect::<CarbideResult<_>>()?;
    let access_results: Vec<_> = report
        .observations
        .iter()
        .filter(|observation| observation.firmware_access_result.is_some())
        .map(FirmwareAccess::try_from)
        .collect::<CarbideResult<_>>()?;
    // Bound this database attempt, not the firmware operation's lifetime.
    let persisted = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let machine = db::machine::find_one(api.pg_pool(), &host_id, Default::default())
            .await?
            .ok_or_else(|| CarbideError::NotFoundError {
                kind: "machine",
                id: host_id.to_string(),
            })?;
        let ManagedHostState::NicFirmwareUpdating { mut operation } =
            machine.current_state().clone()
        else {
            return Ok(false);
        };
        let mut changed = false;
        for result in access_results {
            changed |= result.apply(&mut operation, machine.current_version())?;
        }
        for update in updates {
            if let Some(target) = operation.current_target()
                && target.matches_result(&update.operation_id, &update.phase_id, &update.pci_name)
                && let Some(device) = &update.device_info
                && (!target.selector.matches(device) || device.pci_name != target.pci_name)
            {
                return Err(CarbideError::InvalidArgument(
                    "firmware result device identity contradicts its frozen target".into(),
                ));
            }
            changed |= match update.result {
                FirmwareUpdateResult::Receipt(receipt) => operation.accept_receipt(
                    &update.operation_id,
                    &update.phase_id,
                    &update.pci_name,
                    receipt,
                ),
                FirmwareUpdateResult::MissingReceipt(instance_id) => operation
                    .accept_missing_receipt(
                        &update.operation_id,
                        &update.phase_id,
                        &update.pci_name,
                        instance_id,
                    ),
                FirmwareUpdateResult::Error(code, diagnostic) => operation.accept_error(
                    &update.operation_id,
                    &update.phase_id,
                    &update.pci_name,
                    format!("{}: {diagnostic}", code.as_str_name()),
                ),
            };
        }
        if !changed {
            return Ok(false);
        }
        let version = machine.current_version();
        let state = ManagedHostState::NicFirmwareUpdating { operation };
        let mut txn = api.txn_begin().await?;
        match db::machine::try_update_controller_state(
            &mut txn,
            &host_id,
            version,
            version.increment(),
            &state,
        )
        .await?
        {
            ConditionalWrite::Applied(()) => txn.commit().await?,
            ConditionalWrite::NotApplied(_) => {
                return Err(CarbideError::FailedPrecondition(
                    "machine changed before the firmware report could be stored".into(),
                ));
            }
        }
        Ok(true)
    })
    .await
    .map_err(|_| {
        CarbideError::FailedPrecondition(
            "firmware report persistence exceeded its 30-second deadline; retry reporting".into(),
        )
    })??;
    // Polling can repeat settled or stale evidence indefinitely. Only a committed
    // change gives the controller new work; ignored reports need no wakeup.
    if !persisted {
        return Ok(());
    }
    if let Err(error) = api
        .machine_state_handler_enqueuer
        .enqueue_object(&host_id)
        .await
    {
        carbide_instrument::emit(StateHandlerWakeupFailed {
            trigger: WakeupTrigger::NicFirmwareObservation,
            machine_id: authenticated_machine_id,
            err: error.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use carbide_test_support::Outcome::*;
    use carbide_test_support::scenarios;
    use pb::mlx_device_firmware_access_result::Result as AccessResult;

    use super::*;

    const DEVICE: &str = "0000:01:00.0";

    /// Delivery failures need a known code and bounded diagnostic before they
    /// can become operator-visible evidence in the persisted operation.
    #[test]
    fn firmware_error_decoder_preserves_valid_details_and_rejects_invalid_fields() {
        scenarios!(
            run = |(code, diagnostic): (i32, String)| {
                let observation = pb::MlxObservation {
                    firmware_update_result: Some(pb::MlxDeviceUpdateFirmwareResult {
                        operation_id: "nic-card-0".into(),
                        phase_id: "nic-phase-0".into(),
                        pci_name: DEVICE.into(),
                        result: Some(WireResult::Error(pb::MlxDeviceUpdateFirmwareError {
                            code,
                            diagnostic,
                        })),
                    }),
                    ..Default::default()
                };
                FirmwareUpdate::try_from(&observation).map(|update| {
                    let FirmwareUpdateResult::Error(code, diagnostic) = update.result else {
                        panic!("delivery error must remain an error result")
                    };
                    (code, diagnostic)
                }).map_err(drop)
            };
            "valid errors preserve their code and diagnostic" {
                (pb::MlxDeviceUpdateFirmwareErrorCode::Unavailable.into(), "helper unavailable".into())
                    => Yields((pb::MlxDeviceUpdateFirmwareErrorCode::Unavailable, "helper unavailable".into())),
            }
            "unknown enum values cannot become meaningful failures" {
                (i32::MAX, "helper unavailable".into()) => Fails,
            }
            "the unspecified sentinel is not an error classification" {
                (pb::MlxDeviceUpdateFirmwareErrorCode::Unspecified.into(), "helper unavailable".into()) => Fails,
            }
            "diagnostics cannot exceed the persistence bound" {
                (pb::MlxDeviceUpdateFirmwareErrorCode::Unavailable.into(), "x".repeat(2049)) => Fails,
            }
        );
    }

    /// One valid envelope makes each rejected field independently observable.
    fn access_observation(edit: impl FnOnce(&mut pb::MlxObservation)) -> pb::MlxObservation {
        let mut observation = pb::MlxObservation {
            device_info: Some(pb::MlxDeviceInfo {
                pci_name: DEVICE.into(),
                base_mac: "00:11:22:33:44:55".into(),
                ..Default::default()
            }),
            firmware_access_result: Some(pb::MlxDeviceFirmwareAccessResult {
                controller_state_version: config_version::ConfigVersion::initial().to_string(),
                pci_name: DEVICE.into(),
                expected_device: Some(
                    MlxDeviceIdentity::Mac("00:11:22:33:44:55".parse().unwrap()).into(),
                ),
                result: Some(AccessResult::StatusReport(pb::StatusReport {
                    device_id: DEVICE.into(),
                    status: pb::LockStatus::Unlocked.into(),
                    timestamp: "2026-10-27T12:00:00Z".into(),
                })),
            }),
            ..Default::default()
        };
        edit(&mut observation);
        observation
    }

    /// Decode only usable typed evidence; persistence/version fencing is proved
    /// through the authenticated API integration tests, not repeated here.
    #[test]
    fn access_envelopes_preserve_result_kind_and_reject_incomplete_evidence() {
        scenarios!(
            run = |observation| FirmwareAccess::try_from(&observation)
                .map(|access| access.result).map_err(drop);
            "observed status and mutation acknowledgment remain distinct" {
                access_observation(|_| {}) => Yields(NicFirmwareAccessResult::Status(NicFirmwareLockdownState::Unlocked)),
                access_observation(|observation| {
                    observation.firmware_access_result.as_mut().unwrap().result = Some(AccessResult::Changed(Default::default()));
                }) => Yields(NicFirmwareAccessResult::Changed),
            }
            "a failed identity query can report failure without fabricating device facts" {
                access_observation(|observation| {
                    observation.device_info = None;
                    observation.firmware_access_result.as_mut().unwrap().result = Some(AccessResult::Error(pb::MlxDeviceStreamError {
                        status: pb::MlxDeviceStreamErrorStatus::Internal.into(),
                        message: "device identity unavailable".into(),
                    }));
                }) => Yields(NicFirmwareAccessResult::Error { diagnostic: "device identity unavailable".into() }),
            }
            "successful access requires independently observed device identity" {
                access_observation(|observation| observation.device_info = None) => Fails,
            }
            "generic access cannot be mixed with legacy or firmware evidence" {
                access_observation(|observation| observation.lock_status = Some(pb::LockStatus::Unlocked.into())) => Fails,
                access_observation(|observation| observation.firmware_update_result = Some(Default::default())) => Fails,
            }
            "unknown status and malformed timestamp are not observed access" {
                access_observation(|observation| {
                    let Some(AccessResult::StatusReport(status)) = &mut observation.firmware_access_result.as_mut().unwrap().result else { unreachable!() };
                    status.status = pb::LockStatus::Unknown.into();
                }) => Fails,
                access_observation(|observation| {
                    let Some(AccessResult::StatusReport(status)) = &mut observation.firmware_access_result.as_mut().unwrap().result else { unreachable!() };
                    status.timestamp = "invalid".into();
                }) => Fails,
            }
            "the status payload cannot name a different device from its envelope" {
                access_observation(|observation| {
                    let Some(AccessResult::StatusReport(status)) = &mut observation.firmware_access_result.as_mut().unwrap().result else { unreachable!() };
                    status.device_id = "0000:02:00.0".into();
                }) => Fails,
            }
        );
    }
}
