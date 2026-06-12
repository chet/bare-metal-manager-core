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

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use carbide_site_explorer::SiteExplorer;
use carbide_site_explorer::config::SiteExplorerConfig;
use carbide_site_explorer::test_support::{MockEndpointExplorer, TestSiteExplorer};
use carbide_test_harness::network::segment::TestNetworkSegment;
use carbide_test_harness::prelude::*;
use carbide_test_harness::test_support::fixture_config::FixtureDefault as _;
use mac_address::MacAddress;
use model::expected_machine::{DpuMode, ExpectedMachine, ExpectedMachineData};
use model::network_segment::NetworkSegmentType;
use model::test_support::ManagedHostConfig;

struct ZeroDpuEnv {
    pool: PgPool,
    test_harness: TestHarness,
    underlay_segment: TestNetworkSegment,
    admin_segment: TestNetworkSegment,
    host_inband_segment: TestNetworkSegment,
    site_explorer: TestSiteExplorer,
}

impl ZeroDpuEnv {
    fn api(&self) -> &Api {
        self.test_harness.api()
    }
}

async fn init(pool: PgPool) -> ZeroDpuEnv {
    init_with_site_dpu_mode(pool, None).await
}

/// `init` with the explorer's site-wide `dpu_mode` set (the fallback when
/// an ExpectedMachine leaves `dpu_mode` at its default).
async fn init_with_site_dpu_mode(pool: PgPool, site_dpu_mode: Option<DpuMode>) -> ZeroDpuEnv {
    // Three vlan-backed segments (underlay, admin, host-inband); the
    // default vlan/vni pools only cover two.
    let int_range_pool = |start: u32, end: u32| model::resource_pool::ResourcePoolDef {
        pool_type: model::resource_pool::ResourcePoolType::Integer,
        ranges: vec![model::resource_pool::Range {
            start: start.to_string(),
            end: end.to_string(),
            auto_assign: true,
        }],
        prefix: None,
        delegate_prefix_len: None,
    };
    let mut resource_pools = ResourcePoolBuilder::default().build();
    resource_pools.insert(
        model::resource_pool::common::VLANID.to_string(),
        int_range_pool(1, 3),
    );
    resource_pools.insert(
        model::resource_pool::common::VNI.to_string(),
        int_range_pool(10001, 10003),
    );

    let test_harness = TestHarness::builder(pool.clone())
        .with_resource_pools(resource_pools)
        .build()
        .await;
    let domain = test_harness.test_domain().await;
    let network_controller = test_harness.network_controller();
    let underlay_segment = network_controller.create_underlay_segment(&domain).await;
    let admin_segment = network_controller.create_admin_segment(&domain).await;
    let host_inband_segment = network_controller.create_host_inband_segment(&domain).await;
    let endpoint_explorer = Arc::new(MockEndpointExplorer::default());
    let api = test_harness.api();
    let site_explorer = TestSiteExplorer::new(
        SiteExplorer::new(
            api.database_connection.clone(),
            SiteExplorerConfig {
                enabled: Arc::new(true.into()),
                retained_boot_interface_window: None,
                explorations_per_run: 1,
                concurrent_explorations: 1,
                run_interval: Duration::from_secs(1),
                create_machines: Arc::new(true.into()),
                dpu_mode: site_dpu_mode,
                ..Default::default()
            },
            test_harness.test_meter.meter(),
            endpoint_explorer.clone(),
            Arc::new(api.runtime_config.get_firmware_config()),
            api.common_pools().clone(),
            api.work_lock_manager_handle(),
            None,
            api.credential_manager().clone(),
        ),
        endpoint_explorer,
    );

    ZeroDpuEnv {
        pool,
        test_harness,
        underlay_segment,
        admin_segment,
        host_inband_segment,
        site_explorer,
    }
}

async fn register_zero_dpu_expected_machine(
    env: &ZeroDpuEnv,
    managed_host: &ManagedHostConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    register_expected_machine_with_data(
        env,
        managed_host,
        ExpectedMachineData {
            dpu_mode: DpuMode::NoDpu,
            ..Default::default()
        },
    )
    .await
}

/// Register the host's expected machine with the given data (the serial
/// number is filled in from the fixture).
async fn register_expected_machine_with_data(
    env: &ZeroDpuEnv,
    managed_host: &ManagedHostConfig,
    mut data: ExpectedMachineData,
) -> Result<(), Box<dyn std::error::Error>> {
    data.serial_number = managed_host.serial.clone();
    let mut txn = env.pool.begin().await?;
    db::expected_machine::create(
        &mut txn,
        ExpectedMachine {
            id: None,
            bmc_mac_address: managed_host.bmc_mac_address,
            data,
        },
    )
    .await?;
    txn.commit().await?;

    Ok(())
}

/// Drive a host through BMC discovery and registration: BMC DHCP on the
/// underlay, exploration recorded, preingestion marked complete, and a
/// second explorer pass to register the machine.
async fn explore_and_ingest(
    env: &ZeroDpuEnv,
    mock_host: &ManagedHostConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let host_bmc_response = env
        .api()
        .discover_dhcp(
            rpc::forge::DhcpDiscovery::builder(
                mock_host.bmc_mac_address,
                env.underlay_segment.relay_address,
            )
            .vendor_string("SomeVendor")
            .tonic_request(),
        )
        .await?
        .into_inner();
    let host_bmc_ip = host_bmc_response.address.parse()?;

    env.site_explorer.insert_endpoints(
        mock_host
            .exploration_results(Some(host_bmc_ip), &[])?
            .into_endpoints(),
    );
    env.site_explorer.run_single_iteration().await?;
    let mut txn = env.pool.begin().await?;
    db::explored_endpoints::set_preingestion_complete(host_bmc_ip, &mut txn).await?;
    txn.commit().await?;
    env.site_explorer.run_single_iteration().await?;

    Ok(())
}

fn zero_dpu_host() -> ManagedHostConfig {
    ManagedHostConfig {
        dpus: vec![],
        ..ManagedHostConfig::default()
    }
}

/// A zero-DPU host whose only NIC is a plain (non-DPU) host NIC.
/// We expect to walk over the report ethernet interfaces and record
/// the NIC's Redfish-reported interface id onto its machine_interface
/// row, matched/paired with its MAC address.
#[sqlx_test]
async fn test_site_explorer_records_boot_interface_id_onto_non_dpu_nic(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = init(pool).await;
    let non_dpu_mac = MacAddress::from_str("d4:04:e6:84:13:98").unwrap();
    let mock_host = ManagedHostConfig {
        dpus: vec![],
        non_dpu_macs: vec![non_dpu_mac],
        ..ManagedHostConfig::default()
    };
    register_zero_dpu_expected_machine(&env, &mock_host).await?;

    let host_bmc_response = env
        .api()
        .discover_dhcp(
            rpc::forge::DhcpDiscovery::builder(
                mock_host.bmc_mac_address,
                env.underlay_segment.relay_address,
            )
            .vendor_string("SomeVendor")
            .tonic_request(),
        )
        .await?
        .into_inner();
    let host_bmc_ip = host_bmc_response.address.parse()?;

    env.site_explorer.insert_endpoints(
        mock_host
            .exploration_results(Some(host_bmc_ip), &[])?
            .into_endpoints(),
    );
    env.site_explorer.run_single_iteration().await?;
    let mut txn = env.pool.begin().await?;
    db::explored_endpoints::set_preingestion_complete(host_bmc_ip, &mut txn).await?;
    txn.commit().await?;
    env.site_explorer.run_single_iteration().await?;

    let host_dhcp_response = env
        .api()
        .discover_dhcp(
            rpc::forge::DhcpDiscovery::builder(non_dpu_mac, env.host_inband_segment.relay_address)
                .vendor_string("Bluefield")
                .tonic_request(),
        )
        .await?
        .into_inner();
    let machine_id = host_dhcp_response
        .machine_id
        .expect("the in-band NIC DHCP should promote the zero-DPU host prediction");

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_machine_ids(&mut txn, &[machine_id]).await?;
    let nic = interfaces
        .get(&machine_id)
        .into_iter()
        .flatten()
        .find(|i| i.mac_address == non_dpu_mac)
        .expect("the non-DPU host NIC should have a machine_interface row");
    assert_eq!(
        nic.boot_interface_id.as_deref(),
        Some("NIC.Embedded.1-1-1"),
        "exploration should record a non-DPU NIC's Redfish interface id on its row",
    );
    txn.rollback().await?;

    Ok(())
}

/// Site-explorer hands a boot interface id to the predicted interfaces it
/// mints for zero-DPU hosts (from the live report here), and DHCP promotion
/// passes it on to the machine_interfaces row -- so the host's boot
/// target is a full MAC + Redfish-id pair from its first owned interface.
#[sqlx_test]
async fn test_predicted_interface_hands_boot_interface_id_to_real_row(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = init(pool).await;
    let mock_host = zero_dpu_host();
    let inband_mac = *mock_host.non_dpu_macs.first().unwrap();
    register_zero_dpu_expected_machine(&env, &mock_host).await?;

    let host_bmc_response = env
        .api()
        .discover_dhcp(
            rpc::forge::DhcpDiscovery::builder(
                mock_host.bmc_mac_address,
                env.underlay_segment.relay_address,
            )
            .vendor_string("SomeVendor")
            .tonic_request(),
        )
        .await?
        .into_inner();
    let host_bmc_ip = host_bmc_response.address.parse()?;

    // Site-explorer runs BEFORE the in-band NIC ever DHCPs, so ingestion
    // mints a predicted interface for it.
    env.site_explorer.insert_endpoints(
        mock_host
            .exploration_results(Some(host_bmc_ip), &[])?
            .into_endpoints(),
    );
    env.site_explorer.run_single_iteration().await?;
    let mut txn = env.pool.begin().await?;
    db::explored_endpoints::set_preingestion_complete(host_bmc_ip, &mut txn).await?;
    txn.commit().await?;
    env.site_explorer.run_single_iteration().await?;

    let mut txn = env.pool.begin().await?;
    let predicted = db::predicted_machine_interface::find_by_mac_address(&mut txn, inband_mac)
        .await?
        .expect("zero-DPU ingest should have minted a predicted interface");
    // The fixture report names the embedded NIC's Redfish id.
    assert_eq!(
        predicted.boot_interface_id.as_deref(),
        Some("NIC.Embedded.1-1-1"),
        "predicted interface should hold the report-derived boot interface id"
    );
    txn.rollback().await?;

    // The in-band NIC's first DHCP promotes the prediction into a
    // machine_interfaces row.
    let host_dhcp_response = env
        .api()
        .discover_dhcp(
            rpc::forge::DhcpDiscovery::builder(inband_mac, env.host_inband_segment.relay_address)
                .vendor_string("Bluefield")
                .tonic_request(),
        )
        .await?
        .into_inner();
    assert!(host_dhcp_response.machine_id.is_some());

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(txn.as_mut(), inband_mac).await?;
    assert_eq!(interfaces.len(), 1);
    assert_eq!(
        interfaces[0].boot_interface_id.as_deref(),
        Some("NIC.Embedded.1-1-1"),
        "promotion should land the predicted boot interface id on the promoted row"
    );
    assert!(
        db::predicted_machine_interface::find_by_mac_address(&mut txn, inband_mac)
            .await?
            .is_none(),
        "the prediction should be consumed by promotion"
    );
    txn.rollback().await?;

    Ok(())
}

/// When a retained boot interface id AND a prediction with a live-report
/// id both exist for a MAC, DHCP promotion lands the LIVE id on the
/// promoted row -- the prediction is refreshed every exploration cycle,
/// while the retained id predates the deletion that recorded it. The
/// retention record is consumed either way.
#[sqlx_test]
async fn test_predicted_live_boot_interface_id_outranks_retained_at_promotion(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = init(pool).await;
    let mock_host = zero_dpu_host();
    let inband_mac = *mock_host.non_dpu_macs.first().unwrap();
    register_zero_dpu_expected_machine(&env, &mock_host).await?;

    // A prior row for this MAC was deleted with its boot pair retained --
    // the id names a slot the NIC occupied before the migration.
    let mut txn = env.pool.begin().await?;
    db::retained_boot_interface::upsert(txn.as_mut(), inband_mac, "NIC.Old.9-9-9").await?;
    txn.commit().await?;

    let host_bmc_response = env
        .api()
        .discover_dhcp(
            rpc::forge::DhcpDiscovery::builder(
                mock_host.bmc_mac_address,
                env.underlay_segment.relay_address,
            )
            .vendor_string("SomeVendor")
            .tonic_request(),
        )
        .await?
        .into_inner();
    let host_bmc_ip = host_bmc_response.address.parse()?;

    // Ingestion mints a predicted interface holding the CURRENT id
    // from the live report.
    env.site_explorer.insert_endpoints(
        mock_host
            .exploration_results(Some(host_bmc_ip), &[])?
            .into_endpoints(),
    );
    env.site_explorer.run_single_iteration().await?;
    let mut txn = env.pool.begin().await?;
    db::explored_endpoints::set_preingestion_complete(host_bmc_ip, &mut txn).await?;
    txn.commit().await?;
    env.site_explorer.run_single_iteration().await?;

    let mut txn = env.pool.begin().await?;
    let predicted = db::predicted_machine_interface::find_by_mac_address(&mut txn, inband_mac)
        .await?
        .expect("zero-DPU ingest should have minted a predicted interface");
    assert_eq!(
        predicted.boot_interface_id.as_deref(),
        Some("NIC.Embedded.1-1-1"),
        "the prediction holds the live-report id, never the retained one"
    );
    txn.rollback().await?;

    // The in-band NIC's first DHCP promotes the prediction. Creation
    // recovers the retained id onto the brand-new row first -- the
    // prediction's live id must still win.
    env.api()
        .discover_dhcp(
            rpc::forge::DhcpDiscovery::builder(inband_mac, env.host_inband_segment.relay_address)
                .vendor_string("Bluefield")
                .tonic_request(),
        )
        .await?;

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(txn.as_mut(), inband_mac).await?;
    assert_eq!(interfaces.len(), 1);
    assert_eq!(
        interfaces[0].boot_interface_id.as_deref(),
        Some("NIC.Embedded.1-1-1"),
        "the live predicted id outranks the retained id on the promoted row"
    );
    assert!(
        db::retained_boot_interface::find_by_mac(txn.as_mut(), inband_mac, None)
            .await?
            .is_none(),
        "the retention record is consumed by promotion regardless"
    );
    txn.rollback().await?;

    Ok(())
}

/// If a static preallocation creates the machine_interfaces row while a
/// prediction is still pending (an ExpectedMachine `fixed_ip` recorded in
/// between), the prediction's live-report id still outranks the retained
/// id the preallocated row recovered.
#[sqlx_test]
async fn test_predicted_live_boot_interface_id_outranks_preallocated_retained_row_at_promotion(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = init(pool).await;
    let mock_host = zero_dpu_host();
    let inband_mac = *mock_host.non_dpu_macs.first().unwrap();
    register_zero_dpu_expected_machine(&env, &mock_host).await?;

    // A prior row retained an obsolete boot interface id for this MAC.
    let mut txn = env.pool.begin().await?;
    db::retained_boot_interface::upsert(txn.as_mut(), inband_mac, "NIC.Old.9-9-9").await?;
    txn.commit().await?;

    let host_bmc_response = env
        .api()
        .discover_dhcp(
            rpc::forge::DhcpDiscovery::builder(
                mock_host.bmc_mac_address,
                env.underlay_segment.relay_address,
            )
            .vendor_string("SomeVendor")
            .tonic_request(),
        )
        .await?
        .into_inner();
    let host_bmc_ip = host_bmc_response.address.parse()?;

    // Site-explorer mints a pending prediction with the current Redfish id.
    env.site_explorer.insert_endpoints(
        mock_host
            .exploration_results(Some(host_bmc_ip), &[])?
            .into_endpoints(),
    );
    env.site_explorer.run_single_iteration().await?;
    let mut txn = env.pool.begin().await?;
    db::explored_endpoints::set_preingestion_complete(host_bmc_ip, &mut txn).await?;
    txn.commit().await?;
    env.site_explorer.run_single_iteration().await?;

    let static_ip: std::net::IpAddr = "192.0.3.77".parse()?;
    let mut txn = env.pool.begin().await?;

    // A `fixed_ip` declaration creates the row on the same
    // HostInband segment before the NIC ever DHCPs; creation
    // recovers the retained (obsolete) id onto it.
    db::machine_interface::preallocate_machine_interface(txn.as_mut(), inband_mac, static_ip, None)
        .await?;

    let interfaces = db::machine_interface::find_by_mac_address(txn.as_mut(), inband_mac).await?;
    assert_eq!(interfaces.len(), 1);
    assert_eq!(
        interfaces[0].boot_interface_id.as_deref(),
        Some("NIC.Old.9-9-9")
    );
    txn.commit().await?;

    // DHCP promotion must overwrite the preallocated row with the live id.
    env.api()
        .discover_dhcp(
            rpc::forge::DhcpDiscovery::builder(inband_mac, env.host_inband_segment.relay_address)
                .vendor_string("Bluefield")
                .tonic_request(),
        )
        .await?;

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(txn.as_mut(), inband_mac).await?;
    assert_eq!(interfaces.len(), 1);
    assert_eq!(
        interfaces[0].boot_interface_id.as_deref(),
        Some("NIC.Embedded.1-1-1"),
        "the live predicted id outranks the preallocation-recovered retained id"
    );
    txn.rollback().await?;

    Ok(())
}

/// A newly DHCP-created machine_interface recovers a retained boot
/// interface id -- recorded when a prior row for its MAC was deleted (e.g.
/// admin force-delete during a DPU-to-NIC mode migration) -- and consumes
/// the retention record.
#[sqlx_test]
async fn test_dhcp_created_interface_recovers_retained_boot_interface_id(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = init(pool).await;
    let mock_host = zero_dpu_host();
    let inband_mac = *mock_host.non_dpu_macs.first().unwrap();
    register_zero_dpu_expected_machine(&env, &mock_host).await?;

    // A prior interface row for this MAC was deleted with its boot pair
    // retained; seed that record directly.
    let mut txn = env.pool.begin().await?;
    db::retained_boot_interface::upsert(txn.as_mut(), inband_mac, "NIC.Retained.7-1-1").await?;
    txn.commit().await?;

    // DHCP arrives before site-explorer ever runs: the brand-new row
    // recovers the retained boot interface id on creation.
    env.api()
        .discover_dhcp(
            rpc::forge::DhcpDiscovery::builder(inband_mac, env.host_inband_segment.relay_address)
                .vendor_string("Bluefield")
                .tonic_request(),
        )
        .await?;

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(txn.as_mut(), inband_mac).await?;
    assert_eq!(interfaces.len(), 1);
    assert_eq!(
        interfaces[0].boot_interface_id.as_deref(),
        Some("NIC.Retained.7-1-1"),
        "the new row should recover the retained boot interface id"
    );
    assert!(
        db::retained_boot_interface::find_by_mac(txn.as_mut(), inband_mac, None)
            .await?
            .is_none(),
        "the retention record should be consumed once applied"
    );
    txn.rollback().await?;

    Ok(())
}

/// A prediction minted before the BMC report resolved the NIC's Redfish id
/// is refreshed by the next exploration that does resolve it -- pending
/// predictions stay as current as the live report until DHCP promotes them.
#[sqlx_test]
async fn test_exploration_refreshes_pending_predicted_boot_interface_id(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = init(pool).await;
    let mock_host = zero_dpu_host();
    let inband_mac = *mock_host.non_dpu_macs.first().unwrap();
    register_zero_dpu_expected_machine(&env, &mock_host).await?;

    // First exploration: the BMC reports the NIC's MAC but no Redfish id
    // yet, so the minted prediction has no boot interface id.
    let mut id_less_report: model::site_explorer::EndpointExplorationReport =
        mock_host.clone().into();
    for system in id_less_report.systems.iter_mut() {
        for iface in system.ethernet_interfaces.iter_mut() {
            iface.id = None;
        }
    }

    let host_bmc_response = env
        .api()
        .discover_dhcp(
            rpc::forge::DhcpDiscovery::builder(
                mock_host.bmc_mac_address,
                env.underlay_segment.relay_address,
            )
            .vendor_string("SomeVendor")
            .tonic_request(),
        )
        .await?
        .into_inner();
    let host_bmc_ip = host_bmc_response.address.parse()?;
    env.site_explorer
        .insert_endpoint_result(host_bmc_ip, Ok(id_less_report));

    env.site_explorer.run_single_iteration().await?;
    let mut txn = env.pool.begin().await?;
    db::explored_endpoints::set_preingestion_complete(host_bmc_ip, &mut txn).await?;
    txn.commit().await?;
    env.site_explorer.run_single_iteration().await?;

    let mut txn = env.pool.begin().await?;
    let predicted = db::predicted_machine_interface::find_by_mac_address(&mut txn, inband_mac)
        .await?
        .expect("zero-DPU ingest should have minted a predicted interface");
    assert!(
        predicted.boot_interface_id.is_none(),
        "an id-less report can't give the prediction a boot interface id"
    );
    txn.rollback().await?;

    // Second exploration: the BMC now resolves the id; the pending
    // prediction picks it up.
    env.site_explorer
        .insert_endpoint_result(host_bmc_ip, Ok(mock_host.clone().into()));
    env.site_explorer.run_single_iteration().await?;

    let mut txn = env.pool.begin().await?;
    let predicted = db::predicted_machine_interface::find_by_mac_address(&mut txn, inband_mac)
        .await?
        .expect("the prediction should still be pending");
    assert_eq!(
        predicted.boot_interface_id.as_deref(),
        Some("NIC.Embedded.1-1-1"),
        "the next exploration that resolves the id refreshes the prediction"
    );
    txn.rollback().await?;

    Ok(())
}

/// The core of this change: a NIC-mode host's data port can carry an
/// unowned `Admin`-segment interface left from when its DPU managed it.
/// Ingest deletes that stale interface and predicts the NIC on
/// `HostInband` instead, so its first DHCP lands on the right network.
#[sqlx_test]
async fn test_nic_mode_ingest_deletes_stale_admin_interface(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = init(pool).await;
    let mock_host = zero_dpu_host();
    let inband_mac = *mock_host.non_dpu_macs.first().unwrap();
    register_expected_machine_with_data(
        &env,
        &mock_host,
        ExpectedMachineData {
            dpu_mode: DpuMode::NicMode,
            ..Default::default()
        },
    )
    .await?;

    // The port's DPU-managed past: an unowned interface on the `Admin`
    // segment.
    let mut txn = env.pool.begin().await?;
    let stale = db::machine_interface::validate_existing_mac_and_create(
        txn.as_mut(),
        inband_mac,
        &[env.admin_segment.relay_address],
        None,
        None,
    )
    .await?;
    assert_eq!(stale.network_segment_type, Some(NetworkSegmentType::Admin));
    txn.commit().await?;

    explore_and_ingest(&env, &mock_host).await?;

    let mut txn = env.pool.begin().await?;
    // The stale `Admin` interface is gone...
    assert!(
        db::machine_interface::find_by_mac_address(txn.as_mut(), inband_mac)
            .await?
            .is_empty(),
        "the stale Admin interface should be deleted, not adopted"
    );
    // ...and a `HostInband` prediction replaces it.
    let predicted = db::predicted_machine_interface::find_by_mac_address(&mut txn, inband_mac)
        .await?
        .expect("a HostInband prediction should replace the deleted interface");
    assert_eq!(
        predicted.expected_network_segment_type,
        NetworkSegmentType::HostInband
    );
    txn.rollback().await?;

    // The NIC's first DHCP on `HostInband` promotes the prediction cleanly
    // (no leftover Admin interface to collide with).
    let response = env
        .api()
        .discover_dhcp(
            rpc::forge::DhcpDiscovery::builder(inband_mac, env.host_inband_segment.relay_address)
                .vendor_string("Bluefield")
                .tonic_request(),
        )
        .await?
        .into_inner();
    assert!(response.machine_id.is_some());

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(txn.as_mut(), inband_mac).await?;
    assert_eq!(interfaces.len(), 1);
    assert_eq!(
        interfaces[0].network_segment_type,
        Some(NetworkSegmentType::HostInband)
    );
    assert!(interfaces[0].machine_id.is_some());
    txn.rollback().await?;

    Ok(())
}

/// The deletion keys on the host's effective mode, so a host made NIC-mode
/// by the site-wide `dpu_mode` setting alone (no per-host declaration)
/// deletes its stale `Admin` interface just the same.
#[sqlx_test]
async fn test_site_wide_nic_mode_deletes_stale_admin_interface(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = init_with_site_dpu_mode(pool, Some(DpuMode::NicMode)).await;
    let mock_host = zero_dpu_host();
    let inband_mac = *mock_host.non_dpu_macs.first().unwrap();
    // No per-host declaration: the site-wide setting alone makes it NIC-mode.
    register_expected_machine_with_data(&env, &mock_host, ExpectedMachineData::default()).await?;

    let mut txn = env.pool.begin().await?;
    let stale = db::machine_interface::validate_existing_mac_and_create(
        txn.as_mut(),
        inband_mac,
        &[env.admin_segment.relay_address],
        None,
        None,
    )
    .await?;
    assert_eq!(stale.network_segment_type, Some(NetworkSegmentType::Admin));
    txn.commit().await?;

    explore_and_ingest(&env, &mock_host).await?;

    let mut txn = env.pool.begin().await?;
    assert!(
        db::machine_interface::find_by_mac_address(txn.as_mut(), inband_mac)
            .await?
            .is_empty(),
        "site-wide NIC mode should delete the stale Admin interface too"
    );
    let predicted = db::predicted_machine_interface::find_by_mac_address(&mut txn, inband_mac)
        .await?
        .expect("a HostInband prediction should replace the deleted interface");
    assert_eq!(
        predicted.expected_network_segment_type,
        NetworkSegmentType::HostInband
    );
    txn.rollback().await?;

    Ok(())
}

/// A host flipped from DpuMode to NoDpu gets the same treatment as a
/// NIC-mode flip: the deletion is scoped to hosts with no managed DPU, not
/// to NIC mode specifically, so its stale `Admin` interface is deleted too.
#[sqlx_test]
async fn test_no_dpu_ingest_deletes_stale_admin_interface(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = init(pool).await;
    let mock_host = zero_dpu_host();
    let inband_mac = *mock_host.non_dpu_macs.first().unwrap();
    register_zero_dpu_expected_machine(&env, &mock_host).await?;

    let mut txn = env.pool.begin().await?;
    let stale = db::machine_interface::validate_existing_mac_and_create(
        txn.as_mut(),
        inband_mac,
        &[env.admin_segment.relay_address],
        None,
        None,
    )
    .await?;
    assert_eq!(stale.network_segment_type, Some(NetworkSegmentType::Admin));
    txn.commit().await?;

    explore_and_ingest(&env, &mock_host).await?;

    let mut txn = env.pool.begin().await?;
    assert!(
        db::machine_interface::find_by_mac_address(txn.as_mut(), inband_mac)
            .await?
            .is_empty(),
        "a NoDpu host deletes its stale Admin interface too"
    );
    let predicted = db::predicted_machine_interface::find_by_mac_address(&mut txn, inband_mac)
        .await?
        .expect("a HostInband prediction should replace the deleted interface");
    assert_eq!(
        predicted.expected_network_segment_type,
        NetworkSegmentType::HostInband
    );
    txn.rollback().await?;

    Ok(())
}

/// The deletion only targets `Admin` interfaces: an unowned interface
/// already on `HostInband` is adopted, not deleted, even for a host with no
/// managed DPU.
#[sqlx_test]
async fn test_zero_dpu_ingest_adopts_host_inband_interface(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = init(pool).await;
    let mock_host = zero_dpu_host();
    let inband_mac = *mock_host.non_dpu_macs.first().unwrap();
    register_zero_dpu_expected_machine(&env, &mock_host).await?;

    // The port already DHCP'd on HostInband before re-ingest.
    let mut txn = env.pool.begin().await?;
    let row = db::machine_interface::validate_existing_mac_and_create(
        txn.as_mut(),
        inband_mac,
        &[env.host_inband_segment.relay_address],
        None,
        None,
    )
    .await?;
    assert_eq!(
        row.network_segment_type,
        Some(NetworkSegmentType::HostInband)
    );
    txn.commit().await?;

    explore_and_ingest(&env, &mock_host).await?;

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(txn.as_mut(), inband_mac).await?;
    assert_eq!(
        interfaces.len(),
        1,
        "the HostInband interface should survive"
    );
    assert_eq!(
        interfaces[0].network_segment_type,
        Some(NetworkSegmentType::HostInband)
    );
    assert!(
        interfaces[0].machine_id.is_some(),
        "an existing HostInband interface is adopted, not deleted"
    );
    txn.rollback().await?;

    Ok(())
}
