-- Retain accepted tenant-free firmware recipes after the active operation ends.
ALTER TABLE machines ADD COLUMN nic_firmware_baseline JSONB;
