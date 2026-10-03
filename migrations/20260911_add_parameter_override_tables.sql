-- Add the optional model-specific parameter extension tables.
--
-- Initially applied to tests/data_Maroc/MasterInput.db. The tables are empty:
-- converters therefore continue to use ModelsDictionary defaults until an
-- override is inserted for a soil, crop-management row, or point.

CREATE UNIQUE INDEX IF NOT EXISTS uq_soil_idsoil
ON Soil (IdSoil);

CREATE TABLE IF NOT EXISTS SoilParameterOverrides (
    IdSoil      TEXT NOT NULL,
    Model       TEXT NOT NULL,
    TargetTable TEXT NOT NULL,
    Parameter   TEXT NOT NULL,
    Value       TEXT NOT NULL,
    PRIMARY KEY (IdSoil, Model, TargetTable, Parameter),
    FOREIGN KEY (IdSoil) REFERENCES Soil (IdSoil)
);

CREATE INDEX IF NOT EXISTS idx_soil_parameter_override_lookup
ON SoilParameterOverrides (Model, TargetTable, IdSoil);

CREATE UNIQUE INDEX IF NOT EXISTS uq_crop_management_season_plant
ON CropManagement (idMangt, SeasonOrder, PlantOrder);

CREATE TABLE IF NOT EXISTS CropManagementParameterOverrides (
    idMangt      TEXT NOT NULL,
    SeasonOrder  INTEGER NOT NULL DEFAULT 1,
    PlantOrder   INTEGER NOT NULL DEFAULT 1,
    Model        TEXT NOT NULL,
    TargetTable  TEXT NOT NULL,
    Parameter    TEXT NOT NULL,
    Value        TEXT NOT NULL,
    PRIMARY KEY (
        Model, TargetTable, Parameter,
        idMangt, SeasonOrder, PlantOrder
    ),
    FOREIGN KEY (idMangt, SeasonOrder, PlantOrder)
        REFERENCES CropManagement (idMangt, SeasonOrder, PlantOrder)
);

CREATE INDEX IF NOT EXISTS idx_management_parameter_override_lookup
ON CropManagementParameterOverrides (
    Model, TargetTable, idMangt, SeasonOrder, PlantOrder
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_coordinates_idpoint
ON Coordinates (idPoint);

CREATE TABLE IF NOT EXISTS PointParameterOverrides (
    idPoint      TEXT NOT NULL,
    Model        TEXT NOT NULL,
    TargetTable  TEXT NOT NULL,
    Parameter    TEXT NOT NULL,
    Value        TEXT NOT NULL,
    PRIMARY KEY (Model, TargetTable, Parameter, idPoint),
    FOREIGN KEY (idPoint) REFERENCES Coordinates (idPoint)
);

CREATE INDEX IF NOT EXISTS idx_point_parameter_override_lookup
ON PointParameterOverrides (Model, TargetTable, idPoint);
