-- Add boot_interface_id alongside boot_interface_mac on explored_endpoints.
-- Captures the Redfish EthernetInterface.Id of the boot interface so it
-- can be passed back to libredfish via BootInterfaceRef::InterfaceId when
-- the MAC-based call needs a fallback (some BMCs wipe the MAC for NIC
-- partitions that aren't currently bound for boot).
ALTER TABLE explored_endpoints
    ADD COLUMN boot_interface_id TEXT;
