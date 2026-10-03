-- Add model-specific parameter overrides scoped to a shared idPoint.
-- Target databases: MasterInput databases used by model converters.

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
