-- Align a MasterInput database with the reference input schema of
-- LowInput/blindphase_corrected/MasterInput.db (2026-09-28).
--
-- Prerequisite: 20260911_add_soil_ssat.sql (Soil.Ssat).
--
-- Apply each block only when PRAGMA table_info confirms that the change is
-- missing: SQLite has no ADD COLUMN IF NOT EXISTS and no conditional RENAME.
-- Requires SQLite >= 3.25 for RENAME COLUMN.

-- 1. CropManagement: SowingYearOffset is renamed SeasonYearOffset.
--    The converters still read SowingYearOffset when SeasonYearOffset is
--    absent, so older databases keep working without this step.
ALTER TABLE CropManagement RENAME COLUMN SowingYearOffset TO SeasonYearOffset;

-- 2. InitialConditions: initialisation option and initial ammonium.
--    option = 'simple' keeps the previous behaviour. Once this column exists,
--    STICS v11 requires the InitialConditionsLayers table (step 3).
ALTER TABLE InitialConditions ADD COLUMN option TEXT DEFAULT 'simple';
ALTER TABLE InitialConditions ADD COLUMN NH4initf REAL;
CREATE INDEX IF NOT EXISTS idx_initialconditions_idini
ON InitialConditions (idIni);

-- 3. Layered initial conditions, read when Soil.SoilOption and
--    InitialConditions.option are both 'detailed'.
CREATE TABLE IF NOT EXISTS InitialConditionsLayers (
    idIni TEXT,
    NumLayer INTEGER,
    Lup REAL,
    Ldown REAL,
    WStockinit REAL,
    Ninit REAL,
    NH4initf REAL
);
CREATE INDEX IF NOT EXISTS idx_initialconditionslayers_idini
ON InitialConditionsLayers (idIni);

-- 4. RAclimateD: rhum becomes REAL; vapour pressure and CO2 are added.
--    SQLite cannot change a column type, so the table is rebuilt in rowid
--    order. Numeric text values of rhum are converted by the REAL affinity.
CREATE TABLE RAclimateD_new (
    "idPoint" TEXT,
    "w_date" TEXT,
    "year" INTEGER,
    "DOY" INTEGER,
    "Nmonth" INTEGER,
    "NdayM" INTEGER,
    "srad" REAL,
    "tmax" REAL,
    "tmin" REAL,
    "tmoy" REAL,
    "rain" REAL,
    "wind" REAL,
    "rhum" REAL,
    "Etppm" REAL,
    "Tdewmin" REAL,
    "Tdewmax" REAL,
    "Surfpress" REAL,
    vapeurp REAL,
    co2 REAL
);
INSERT INTO RAclimateD_new (
    idPoint, w_date, year, DOY, Nmonth, NdayM, srad, tmax, tmin, tmoy,
    rain, wind, rhum, Etppm, Tdewmin, Tdewmax, Surfpress
)
SELECT
    idPoint, w_date, year, DOY, Nmonth, NdayM, srad, tmax, tmin, tmoy,
    rain, wind, rhum, Etppm, Tdewmin, Tdewmax, Surfpress
FROM RAclimateD
ORDER BY rowid;
DROP TABLE RAclimateD;
ALTER TABLE RAclimateD_new RENAME TO RAclimateD;
CREATE INDEX IF NOT EXISTS idx_idPoint ON RAclimateD (idPoint);
CREATE INDEX IF NOT EXISTS idx_idPoint_year ON RAclimateD (idPoint, year);
CREATE INDEX IF NOT EXISTS idx_raclimate_idpoint_date ON RAclimateD (idPoint, w_date);

-- 5. Daily observations used for calibration (for example maturity dates).
CREATE TABLE IF NOT EXISTS dailyobs (
    idsim TEXT,
    maturitydate INTEGER,
    maturitydate_calendar TEXT
);
CREATE INDEX IF NOT EXISTS idx_dailyobs_idsim ON dailyobs (idsim);
