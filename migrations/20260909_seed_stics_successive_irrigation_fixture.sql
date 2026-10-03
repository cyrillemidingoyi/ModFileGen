-- Seed the STICS successive test fixture with one irrigated rotation season.
--
-- Target fixture: tests/stics_successive/MasterInput.db
-- Season 2 uses three sowing-relative water applications. Seasons 1 and 3
-- retain IrrigationPolicyCode = '0' to cover irrigated and rainfed seasons in
-- the same successive simulation.

INSERT OR REPLACE INTO IrrigationPolicy (
    IrrigationPolicyCode,
    NumIrrig
) VALUES (
    'IRR_ROT_TEST',
    3
);

INSERT OR REPLACE INTO IrrigationFOperations (
    idIrrigation,
    IrrigationPolicyCode,
    IrrigationNumber,
    DIrrigation,
    IrrigationAmount
) VALUES
    ('IRR_ROT_TEST_1', 'IRR_ROT_TEST', 1, -10, 20.0),
    ('IRR_ROT_TEST_2', 'IRR_ROT_TEST', 2,  20, 30.0),
    ('IRR_ROT_TEST_3', 'IRR_ROT_TEST', 3,  50, 30.0);

UPDATE CropManagement
SET IrrigationPolicyCode = 'IRR_ROT_TEST'
WHERE idMangt = 'ROT_MAIZE_PEANUT_3Y'
  AND SeasonOrder = 2
  AND PlantOrder = 1;
