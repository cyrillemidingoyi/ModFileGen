-- Replace the empty legacy IrrigationFOperations table in
-- tests/dssatsuccessive/MasterInput.db with the shared LowInput schema.
--
-- The legacy fixture used the misspelled column IrrrigationAmount and did not
-- contain any rows when this migration was applied. No data are seeded here.

BEGIN;

DROP TABLE IrrigationFOperations;

CREATE TABLE IrrigationFOperations (
    idIrrigation          VARCHAR(30) PRIMARY KEY,
    IrrigationPolicyCode  VARCHAR(30) NOT NULL,
    IrrigationNumber      SMALLINT(5) NOT NULL,
    DIrrigation           INTEGER(10) NOT NULL,
    IrrigationAmount      DOUBLE(53) NOT NULL
);

CREATE UNIQUE INDEX uq_irrigation_operation_policy_number
ON IrrigationFOperations (IrrigationPolicyCode, IrrigationNumber);

CREATE INDEX idx_irrigation_operation_policy
ON IrrigationFOperations (IrrigationPolicyCode);

COMMIT;
