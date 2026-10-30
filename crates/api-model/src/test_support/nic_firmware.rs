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

//! Admitted generic firmware fixtures, without physical hardware qualification.

use carbide_libmlx_model::device::info::MlxDeviceInfo;
use carbide_libmlx_model::firmware::FirmwareSpec;
use config_version::ConfigVersion;
use mac_address::MacAddress;

use crate::machine::nic_firmware::{
    NicFirmwareAccessPolicy, NicFirmwareBaseline, NicFirmwareBaselineContinuation,
    NicFirmwareBaselineTarget, NicFirmwareDeviceObservation, NicFirmwareExecutionOptions,
    NicFirmwareOperation, NicFirmwarePhase, NicFirmwarePurpose, NicFirmwareTarget,
};
use crate::nic_firmware::{
    NicFirmwareApproach, NicFirmwareArtifact, NicFirmwareCardSelector, NicFirmwareProfileEntry,
    NicFirmwareResolution, NicFirmwareSelectionScope,
};

/// A single immutable request; tests change the specific identity or policy they exercise.
pub fn target() -> NicFirmwareTarget {
    NicFirmwareTarget {
        selector: NicFirmwareCardSelector::Mac(MacAddress::new([2, 0, 0, 0, 0, 1])),
        pci_name: "0000:01:00.0".to_string(),
        resolution: NicFirmwareResolution {
            scope: NicFirmwareSelectionScope::Site,
            profile_id: "baseline".parse().expect("fixture profile ID"),
            profile_version: ConfigVersion::initial(),
            entry: NicFirmwareProfileEntry {
                firmware: FirmwareSpec {
                    part_number: "test-card".to_string(),
                    psid: "test-psid".to_string(),
                    version: "1.2.3".to_string(),
                },
                image: NicFirmwareArtifact {
                    url: "https://firmware.example.test/image.bin"
                        .parse()
                        .expect("fixture URL"),
                    sha256: "ab".repeat(32),
                },
                device_config: None,
                approach: NicFirmwareApproach::Scout,
            },
        },
        options: NicFirmwareExecutionOptions {
            verify_image: false,
            verify_version: false,
            reset: false,
            reset_level: 3,
            post_reset_verify_retry_interval_seconds: None,
            post_reset_verify_timeout_seconds: None,
        },
        original_access: NicFirmwareAccessPolicy::Unlocked,
        operation_id: "nic-card-0".to_string(),
        phase_id: "nic-phase-0".to_string(),
        supervisor_instance_id: None,
        receipt: None,
        diagnostic: None,
        observation: None,
    }
}

/// One tenant-free workflow before helper observation or any hardware effect.
pub fn operation() -> NicFirmwareOperation {
    NicFirmwareOperation {
        operation_id: "nic-workflow".to_string(),
        purpose: NicFirmwarePurpose::Baseline {
            continuation: NicFirmwareBaselineContinuation::Ready,
        },
        targets: vec![target()],
        phase: NicFirmwarePhase::ObservingSupervisor { target_index: 0 },
        reboot_host: true,
        access: Default::default(),
    }
}

/// One accepted baseline with explicit identity/version evidence for storage tests.
pub fn baseline() -> NicFirmwareBaseline {
    let target = target();
    NicFirmwareBaseline {
        operation_id: "nic-workflow".to_string(),
        targets: vec![NicFirmwareBaselineTarget {
            observation: NicFirmwareDeviceObservation {
                observed_at: "2026-10-27T00:00:00Z"
                    .parse()
                    .expect("fixture observation time"),
                received_at: "2026-10-27T00:00:01Z"
                    .parse()
                    .expect("fixture receipt time"),
                device: MlxDeviceInfo {
                    pci_name: target.pci_name.clone(),
                    device_type: "ConnectX".to_string(),
                    psid: Some(target.resolution.entry.firmware.psid.clone()),
                    device_description: None,
                    part_number: Some(target.resolution.entry.firmware.part_number.clone()),
                    fw_version_current: Some(target.resolution.entry.firmware.version.clone()),
                    pxe_version_current: None,
                    uefi_version_current: None,
                    uefi_version_virtio_blk_current: None,
                    uefi_version_virtio_net_current: None,
                    base_mac: Some(MacAddress::new([2, 0, 0, 0, 0, 1])),
                    base_guid: None,
                    status: None,
                },
            },
            selector: target.selector,
            pci_name: target.pci_name,
            resolution: target.resolution,
            access: target.original_access,
        }],
    }
}
