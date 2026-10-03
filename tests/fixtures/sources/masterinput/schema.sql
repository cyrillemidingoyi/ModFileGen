CREATE TABLE "Coordinate_years" (
[idPoint] VARCHAR(255),
[year] INTEGER);

CREATE TABLE [Coordinates] (
[altitude] SMALLINT(5),
[latitudeDD] DOUBLE(53),
[longitudeDD] DOUBLE(53),
[codeSWstation] VARCHAR(10),
[idPoint] VARCHAR(25),
[startRain] SMALLINT(5),
[EndRain] SMALLINT(5)
);

CREATE TABLE "CropManagement" (
"idMangt" TEXT,
  "Idcultivar" TEXT,
  "sdens" REAL,
  "OFertiPolicyCode" TEXT,
  "InoFertiPolicyCode" TEXT,
  "IrrigationPolicyCode" TEXT,
  "SoilTillPolicyCode" TEXT,
  "DHarvest" INTEGER,
  "sowingdate" INTEGER
, PlantOrder INTEGER DEFAULT 1, SeasonOrder INTEGER NOT NULL DEFAULT 1, SeasonYearOffset INTEGER NOT NULL DEFAULT 0);

CREATE TABLE CropManagementParameterOverrides (
    idMangt       TEXT NOT NULL,
    SeasonOrder   INTEGER NOT NULL DEFAULT 1,
    PlantOrder    INTEGER NOT NULL DEFAULT 1,
    Model         TEXT NOT NULL,
    TargetTable   TEXT NOT NULL,
    Parameter     TEXT NOT NULL,
    Value         TEXT NOT NULL,
    PRIMARY KEY (
        Model, TargetTable, Parameter,
        idMangt, SeasonOrder, PlantOrder
    ),
    FOREIGN KEY (idMangt, SeasonOrder, PlantOrder)
        REFERENCES CropManagement (idMangt, SeasonOrder, PlantOrder)
);

CREATE TABLE [InitialConditions] (
[idIni] VARCHAR(70),
[WStockinit] DOUBLE(53),
[Ninit] DOUBLE(53)
, option TEXT DEFAULT 'simple', NH4initf REAL);

CREATE TABLE InitialConditionsLayers (
    idIni TEXT,
    NumLayer INTEGER,
    Lup REAL,
    Ldown REAL,
    WStockinit REAL,
    Ninit REAL,
    NH4initf REAL
);

CREATE TABLE [InorganicFOperations] (
[idFertInorg] VARCHAR(30),
[InorgFertiPolicyCode] VARCHAR(30),
[IFNumber] SMALLINT(5),
[Dferti] INTEGER(10),
[N] DOUBLE(53),
[P] DOUBLE(53),
[K] DOUBLE(53)
);

CREATE TABLE [InorganicFertilizationPolicy] (
[InorgFertiPolicyCode] VARCHAR(30),
[NumInorganicFerti] SMALLINT(5)
);

CREATE TABLE IrrigationFOperations (idIrrigation VARCHAR(30) PRIMARY KEY, IrrigationPolicyCode VARCHAR(30) NOT NULL, IrrigationNumber SMALLINT(5) NOT NULL, DIrrigation INTEGER(10) NOT NULL, IrrigationAmount DOUBLE(53) NOT NULL);

CREATE TABLE [IrrigationPolicy] (
[IrrigationPolicyCode] VARCHAR(30),
[NumIrrig] SMALLINT(5)
);

CREATE TABLE [ListCultOption] (
[CodePSpecies] VARCHAR(255),
[PRCROP] VARCHAR(255),
[CG] VARCHAR(255),
[FicPlt] VARCHAR(255)
, DSCROP TEXT);

CREATE TABLE [ListCultivars] (
[CodePSpecies] VARCHAR(20),
[SpeciesName] VARCHAR(250),
[IdCultivar] VARCHAR(20),
[CodCultivar] VARCHAR(20),
[CycleDuration] SMALLINT(5),
[IdcultivarCelsius] VARCHAR(30),
[idcultivarStics] VARCHAR(30),
[IdcultivarDssat] VARCHAR(30)
);

CREATE TABLE [ListResidues] (
[TypeResidues] VARCHAR(30),
[IdResidueCelsius] VARCHAR(30),
[NomResidueCelsius] VARCHAR(50),
[idresidueStics] VARCHAR(30),
[NomResidueStics] VARCHAR(50),
[idresidueDssat] VARCHAR(30),
[NomResidueDssat] VARCHAR(50)
);

CREATE TABLE [Models] (
[N°] COUNTER(10),
[Model] VARCHAR(30),
[Order] SMALLINT(5)
);

CREATE TABLE [OrganicFOperations] (
[idFertOrga] VARCHAR(30),
[OFertiPolicyCode] VARCHAR(30),
[OFNumber] SMALLINT(5),
[Dferti] INTEGER(10),
[CNferti] DOUBLE(53),
[NFerti] DOUBLE(53),
[PFerti] DOUBLE(53),
[Qmanure] DOUBLE(53),
[In_OnManure] VARCHAR(30),
[TypeResidues] VARCHAR(30)
);

CREATE TABLE [OrganicFertilizationPolicy] (
[OFertiPolicyCode] VARCHAR(30),
[NumOrganicFerti] SMALLINT(5)
);

CREATE TABLE PointParameterOverrides (
    idPoint      TEXT NOT NULL,
    Model        TEXT NOT NULL,
    TargetTable  TEXT NOT NULL,
    Parameter    TEXT NOT NULL,
    Value        TEXT NOT NULL,
    PRIMARY KEY (Model, TargetTable, Parameter, idPoint),
    FOREIGN KEY (idPoint) REFERENCES Coordinates (idPoint)
);

CREATE TABLE "RAclimateD" (
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

CREATE TABLE [RunoffTypes] (
[RunoffType] VARCHAR(30),
[param1] DOUBLE(53),
[RunoffText] LONGCHAR(1073741823),
[CodRunoffCelsius] SMALLINT(5),
[RunoffCoefBSoil] DOUBLE(53),
[CurveNumber] SMALLINT(5)
);

CREATE TABLE "SimUnitList" (
[idsim] VARCHAR(70),
[StartYear] INTEGER,
[StartDay] SMALLINT(5),
[EndYear] SMALLINT(5),
[EndDay] SMALLINT(5),
[idPoint] VARCHAR(20),
[idMangt] VARCHAR(50),
[idsoil] VARCHAR(50),
[idIni] VARCHAR(50),
[idOption] SMALLINT(5)
);

CREATE TABLE [SimulationOptions] (
[IdOptions] SMALLINT(5),
[StressW_YN] BIT(1),
[StressN_YN] BIT(1),
[StressP_YN] BIT(1),
[StressK_YN] BIT(1)
);

CREATE TABLE "Soil" (
"location" INTEGER,
  "OrganicNStock" REAL,
  "extp" REAL,
  "totp" INTEGER,
  "SoilTotalDepth" REAL,
  "bd" REAL,
  "clay" REAL,
  "OrganicC" REAL,
  "pH" REAL,
  "silt" REAL,
  "sand" REAL,
  "awcpf" REAL,
  "cf" REAL,
  "Wwp" REAL,
  "Wfc" REAL,
  "SoilRDepth" REAL,
  "IdSoil" TEXT,
  "SoilTextureType" TEXT,
  "SoilOption" TEXT,
  "Slope" TEXT,
  "RunoffType" INTEGER,
  "albedo" REAL
, Ssat REAL);

CREATE TABLE [SoilLayers] (
[idsoil] VARCHAR(30),
[DepthCm] SMALLINT(5),
[NumLayer] SMALLINT(5),
[Lup] SMALLINT(5),
[Ldown] SMALLINT(5),
[bd] DOUBLE(53),
[Wwp] DOUBLE(53),
[Wfc] DOUBLE(53),
[cf] SMALLINT(5),
[TotalN] DOUBLE(53),
[pH] DOUBLE(53),
[OrganicC] DOUBLE(53),
[Clay] DOUBLE(53),
[Silt] DOUBLE(53)
);

CREATE TABLE SoilParameterOverrides (
    IdSoil          TEXT NOT NULL,
    Model           TEXT NOT NULL,
    TargetTable     TEXT NOT NULL,
    Parameter       TEXT NOT NULL,
    Value           TEXT NOT NULL,
    PRIMARY KEY (IdSoil, Model, TargetTable, Parameter),
    FOREIGN KEY (IdSoil) REFERENCES Soil(IdSoil)
);

CREATE TABLE [SoilTillPolicy] (
[SoilTillPolicyCode] VARCHAR(30),
[NumTillOperations] SMALLINT(5)
);

CREATE TABLE [SoilTillageOperations] (
[IdTillOp] VARCHAR(30),
[SoilTillPolicyCode] VARCHAR(30),
[STNumber] SMALLINT(5),
[DSTill] SMALLINT(5),
[DepthResUp] SMALLINT(5),
[DepthResLow] SMALLINT(5)
);

CREATE TABLE [SoilTypes] (
[SoilTextureType] VARCHAR(30),
[Silt] DOUBLE(53),
[Sand] DOUBLE(53),
[Clay] DOUBLE(53),
[USDAcode] VARCHAR(30),
[IBSNATCode] VARCHAR(30)
);

CREATE TABLE dailyobs (
    idsim TEXT,
    maturitydate INTEGER,
    maturitydate_calendar TEXT
);

CREATE INDEX idx_idCoord ON Coordinates (idPoint);

CREATE UNIQUE INDEX uq_coordinates_idpoint
ON Coordinates (idPoint);

CREATE INDEX idx_idMangt ON CropManagement (idMangt);

CREATE UNIQUE INDEX uq_crop_management_season_plant
ON CropManagement (idMangt, SeasonOrder, PlantOrder);

CREATE INDEX idx_management_parameter_override_lookup
ON CropManagementParameterOverrides (
    Model, TargetTable, idMangt, SeasonOrder, PlantOrder
);

CREATE INDEX idx_initialconditions_idini
ON InitialConditions (idIni);

CREATE INDEX idx_initialconditionslayers_idini
ON InitialConditionsLayers (idIni);

CREATE INDEX idx_irrigation_operation_policy ON IrrigationFOperations (IrrigationPolicyCode);

CREATE UNIQUE INDEX uq_irrigation_operation_policy_number ON IrrigationFOperations (IrrigationPolicyCode, IrrigationNumber);

CREATE INDEX idx_cultoptspec ON ListCultOption (CodePSpecies);

CREATE INDEX idx_cultivars ON ListCultivars (idCultivar);

CREATE INDEX idx_cultopt ON ListCultivars (CodePSpecies);

CREATE INDEX idx_res ON ListResidues (TypeResidues);

CREATE INDEX idx_orga ON OrganicFOperations (idFertOrga);

CREATE INDEX idx_orga_res ON OrganicFOperations (TypeResidues);

CREATE INDEX idx_point_parameter_override_lookup
ON PointParameterOverrides (Model, TargetTable, idPoint);

CREATE INDEX idx_idPoint ON RAclimateD (idPoint);

CREATE INDEX idx_idPoint_year ON RAclimateD (idPoint, year);

CREATE INDEX idx_raclimate_idpoint_date ON RAclimateD (idPoint, w_date);

CREATE INDEX idx_idsim ON SimUnitList (idsim);

CREATE INDEX idx_idoption ON SimulationOptions (idOptions);

CREATE INDEX idx_idsoil ON Soil (IdSoil);

CREATE INDEX idx_idsoill ON Soil (Lower(IdSoil));

CREATE UNIQUE INDEX uq_soil_idsoil
ON Soil(IdSoil);

CREATE INDEX idx_idsoiltl ON SoilTypes (Lower(SoilTextureType));

CREATE INDEX idx_dailyobs_idsim ON dailyobs (idsim);
