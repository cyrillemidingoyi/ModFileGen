import sqlite3

from modfilegen.soil_repository import SoilDataRepository


def test_prefetch_loads_soil_and_ordered_layers():
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        """
        CREATE TABLE Soil (
            IdSoil TEXT, SoilOption TEXT, OrganicC REAL,
            OrganicNStock REAL, SoilRDepth REAL, SoilTotalDepth REAL,
            SoilTextureType TEXT, Wwp REAL, Wfc REAL, bd REAL,
            albedo REAL, Ph REAL, cf REAL, RunoffType TEXT, Clay REAL,
            sand REAL
        );
        CREATE TABLE RunoffTypes (RunoffType TEXT, RunoffCoefBSoil REAL);
        CREATE TABLE SoilLayers (
            idsoil TEXT, NumLayer INTEGER, Lup REAL, Ldown REAL,
            Wwp REAL, Wfc REAL, bd REAL
        );
        INSERT INTO RunoffTypes VALUES ('standard', 0.25);
        INSERT INTO Soil VALUES (
            'Soil-1', 'detailed', 1.2, 0.12, 100, 120,
            'loam', 10, 30, 1.3, 0.2, 6.5, 0, 'standard', 20, 40
        );
        INSERT INTO SoilLayers VALUES ('Soil-1', 2, 20, 50, 11, 31, 1.4);
        INSERT INTO SoilLayers VALUES ('Soil-1', 1, 0, 20, 10, 30, 1.3);
        """
    )

    repository = SoilDataRepository(connection)
    repository.prefetch({"soil-1"})

    soil = repository.get_soil("SOIL-1")
    layers = repository.get_layers("soil-1")

    assert soil["RunoffCoefBSoil"] == 0.25
    assert soil["Sand"] == 40
    assert [layer["NumLayer"] for layer in layers] == [1, 2]
