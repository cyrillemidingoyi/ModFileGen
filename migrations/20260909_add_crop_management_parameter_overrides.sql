-- Add model-specific parameter overrides scoped to one CropManagement row.
--
-- Target databases: MasterInput databases used by model converters.
-- The unique index makes the logical CropManagement key referenceable by the
-- composite foreign key without rebuilding the existing SQLite table.

CREATE UNIQUE INDEX IF NOT EXISTS uq_crop_management_season_plant
ON CropManagement (idMangt, SeasonOrder, PlantOrder);

CREATE TABLE IF NOT EXISTS CropManagementParameterOverrides (
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

CREATE INDEX IF NOT EXISTS idx_management_parameter_override_lookup
ON CropManagementParameterOverrides (
    Model, TargetTable, idMangt, SeasonOrder, PlantOrder
);
