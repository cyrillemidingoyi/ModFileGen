-- Seed one model-specific CropManagement override in the STICS successive
-- fixture: enable automatic irrigation for season 3 of the test rotation.
-- All other automatic-irrigation settings continue to use ModelsDictionary.

INSERT OR REPLACE INTO CropManagementParameterOverrides (
    idMangt,
    SeasonOrder,
    PlantOrder,
    Model,
    TargetTable,
    Parameter,
    Value
) VALUES (
    'ROT_MAIZE_PEANUT_3Y',
    3,
    1,
    'sticsv11',
    'fictec1',
    'codecalirrig',
    '1'
);
