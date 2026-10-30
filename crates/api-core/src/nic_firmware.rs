/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Resolve frozen NIC access policy only when delivering its committed action.
//! Credentials stay out of controller state and reports; any Core replica can
//! render the action from shared persistence without owning a Scout stream.

use std::time::Duration;

use carbide_secrets::credentials::CredentialManager;
use carbide_uuid::machine::HostMachineId;
use config_version::ConfigVersion;
use model::machine::nic_firmware::{
    NicFirmwareAccessPolicy, NicFirmwareAccessStep, NicFirmwareLockdownState, NicFirmwareTarget,
};
use rpc::forge_agent_control_response::MlxDeviceReconcileFirmwareAccess;
use rpc::forge_agent_control_response::mlx_device_reconcile_firmware_access::Request;
use rpc::protos::mlx_device;

use crate::dpa::lockdown::build_resolved_lockdown_key;
use crate::{CarbideError, CarbideResult};

/// Build a status request or derive the original key for a committed change.
/// The caller has already checked the active phase, result, and lifecycle owner.
pub(crate) async fn access_request(
    host: HostMachineId,
    version: ConfigVersion,
    target: &NicFirmwareTarget,
    step: NicFirmwareAccessStep,
    desired: NicFirmwareLockdownState,
    credentials: &dyn CredentialManager,
) -> CarbideResult<MlxDeviceReconcileFirmwareAccess> {
    let request = match step {
        NicFirmwareAccessStep::Observe => {
            Request::Status(mlx_device::MlxDeviceLockdownStatusRequest {
                device_id: target.pci_name.clone(),
            })
        }
        NicFirmwareAccessStep::Change => {
            let NicFirmwareAccessPolicy::Locked {
                ikm_version,
                mac_address,
            } = target.original_access
            else {
                return Err(CarbideError::FailedPrecondition(
                    "cannot change NIC lockdown without its frozen key identity".into(),
                ));
            };
            let key = tokio::time::timeout(
                Duration::from_secs(30),
                build_resolved_lockdown_key(host, mac_address, credentials, ikm_version),
            )
            .await
            .map_err(|_| {
                CarbideError::FailedPrecondition("NIC lockdown credential lookup timed out".into())
            })?
            .map_err(|error| {
                CarbideError::internal(format!("NIC lockdown credential lookup failed: {error}"))
            })?;
            match desired {
                NicFirmwareLockdownState::Locked => {
                    Request::Lock(mlx_device::MlxDeviceLockdownLockRequest {
                        device_id: target.pci_name.clone(),
                        key,
                    })
                }
                NicFirmwareLockdownState::Unlocked => {
                    Request::Unlock(mlx_device::MlxDeviceLockdownUnlockRequest {
                        device_id: target.pci_name.clone(),
                        key,
                    })
                }
            }
        }
    };
    Ok(MlxDeviceReconcileFirmwareAccess {
        controller_state_version: version.to_string(),
        expected_device: Some(target.selector.clone().into()),
        request: Some(request),
    })
}
