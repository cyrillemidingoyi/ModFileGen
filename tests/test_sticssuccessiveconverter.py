import sqlite3

from modfilegen.Converter.SticsV11Converter import sticssuccessiveconverter as converter


def pattern_connection():
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        """
        CREATE TABLE CropManagement (
            idMangt TEXT,
            SeasonOrder INTEGER,
            PlantOrder INTEGER,
            SowingYearOffset INTEGER,
            sowingdate INTEGER,
            DHarvest INTEGER,
            Idcultivar TEXT
        );
        CREATE TABLE ListCultivars (IdCultivar TEXT, SpeciesName TEXT);
        INSERT INTO ListCultivars VALUES ('maize', 'maize');
        INSERT INTO ListCultivars VALUES ('bean', 'bean');
        INSERT INTO ListCultivars VALUES ('legume', 'legume');
        INSERT INTO CropManagement VALUES ('rotation', 1, 1, 0, 100, 80, 'maize');
        INSERT INTO CropManagement VALUES ('rotation', 1, 2, 0, 100, 90, 'bean');
        INSERT INTO CropManagement VALUES ('rotation', 2, 1, 0, 250, 70, 'legume');
        """
    )
    return connection


def test_management_pattern_repeats_with_seasons_and_plant_orders():
    simulation = {
        "idMangt": "rotation",
        "StartYear": 2000,
        "StartDay": 1,
        "EndYear": 2002,
        "EndDay": 365,
    }
    with pattern_connection() as connection:
        seasons = converter.fetch_rotation_seasons(connection, simulation)

    assert [season["SeasonOrder"] for season in seasons] == [1, 2, 3, 4, 5, 6]
    assert [season["ManagementSeasonOrder"] for season in seasons] == [1, 2, 1, 2, 1, 2]
    assert [season["SeasonYearOffset"] for season in seasons] == [0, 0, 1, 1, 2, 2]
    assert [plant["PlantOrder"] for plant in seasons[0]["Plants"]] == [1, 2]
    assert [plant["ManagementPlantOrder"] for plant in seasons[0]["Plants"]] == [1, 2]
    assert seasons[-1]["EndDate"] == converter.julian_date(2002, 320)
    assert max(
        plant["HarvestDate"] for plant in seasons[-1]["Plants"]
    ) == converter.julian_date(2002, 320)


def test_single_offset_one_pattern_repeats_annually_from_following_year():
    simulation = {
        "idMangt": "annual",
        "StartYear": 1992,
        "StartDay": 305,
        "EndYear": 1995,
        "EndDay": 181,
    }
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        """
        CREATE TABLE CropManagement (
            idMangt TEXT,
            SeasonOrder INTEGER,
            PlantOrder INTEGER,
            SowingYearOffset INTEGER,
            sowingdate INTEGER,
            DHarvest INTEGER,
            Idcultivar TEXT
        );
        CREATE TABLE ListCultivars (IdCultivar TEXT, SpeciesName TEXT);
        INSERT INTO ListCultivars VALUES ('wheat', 'wheat');
        INSERT INTO CropManagement
        VALUES ('annual', 1, 1, 1, 15, 180, 'wheat');
        """
    )
    try:
        seasons = converter.fetch_rotation_seasons(connection, simulation)
    finally:
        connection.close()

    assert [plant["SowingDate"] for season in seasons for plant in season["Plants"]] == [
        converter.julian_date(1993, 15),
        converter.julian_date(1994, 15),
        converter.julian_date(1995, 15),
    ]
    assert [season["EndDate"] for season in seasons] == [
        converter.julian_date(1993, 15) + converter.timedelta(days=180),
        converter.julian_date(1994, 15) + converter.timedelta(days=180),
        converter.julian_date(1995, 181),
    ]
    assert all(
        converter.stics_datefin(
            season["StartDate"].year,
            season["EndDate"].year,
            season["EndDate"].timetuple().tm_yday,
        ) <= 731
        for season in seasons
    )
