-- Add dated irrigation operations to a MasterInput database.
--
-- DIrrigation is a signed day offset relative to sowing, like Dferti:
-- negative values are before sowing and positive values are after sowing.
-- IrrigationAmount is the applied water dose. Its unit is defined by the
-- consumers of the MasterInput schema.

CREATE TABLE IF NOT EXISTS IrrigationFOperations (
    idIrrigation          VARCHAR(30) PRIMARY KEY,
    IrrigationPolicyCode  VARCHAR(30) NOT NULL,
    IrrigationNumber      SMALLINT(5) NOT NULL,
    DIrrigation           INTEGER(10) NOT NULL,
    IrrigationAmount      DOUBLE(53) NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_irrigation_operation_policy_number
ON IrrigationFOperations (IrrigationPolicyCode, IrrigationNumber);

CREATE INDEX IF NOT EXISTS idx_irrigation_operation_policy
ON IrrigationFOperations (IrrigationPolicyCode);
