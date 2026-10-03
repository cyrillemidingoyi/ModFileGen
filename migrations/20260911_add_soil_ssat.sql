-- Add soil-level saturated water content for DSSAT.
--
-- Unit: percent volumetric water content, consistent with Soil.Wfc and
-- Soil.Wwp. DssatSoilConverter divides this value by 100 when writing SSAT.
-- NULL keeps the legacy fallback: Soil.Wfc * 1.01 / 100.
--
-- Apply this statement only when PRAGMA table_info(Soil) confirms that the
-- column is absent. SQLite does not support ADD COLUMN IF NOT EXISTS.
ALTER TABLE Soil ADD COLUMN Ssat REAL;
