CREATE TABLE IF NOT EXISTS ib_partition_state_history (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    partition_id uuid NOT NULL,
    state jsonb NOT NULL,
    state_version VARCHAR(64) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION ib_partition_state_history_keep_limit()
RETURNS TRIGGER AS
$body$
BEGIN
    DELETE FROM ib_partition_state_history WHERE partition_id=NEW.partition_id AND id NOT IN (SELECT id from ib_partition_state_history where partition_id=NEW.partition_id ORDER BY id DESC LIMIT 250);
    RETURN NULL;
END;
$body$
LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER t_ib_partition_state_history_keep_limit
  AFTER INSERT ON ib_partition_state_history
  FOR EACH ROW EXECUTE PROCEDURE ib_partition_state_history_keep_limit();
