import sqlite3

import pytest

from modfilegen.Converter.SticsV11Converter.sticsficiniconverter import (
    SticsFicIniConverter,
)
from modfilegen.Converter.SticsV11Converter.sticsfictec1converter import (
    _season_year_offset_days,
    _irecbutoir,
    _simulation_end_day,
    _stics_date,
)


DEFAULT_FIELDS = [
    "stade0", "lai0", "magrain0", "zrac0", "code_acti_reserve",
    "maperenne0", "QNperenne0", "masecnp0", "QNplantenp0", "masec0",
    "QNplante0", "restemp0", "densinitial", "stade0_2", "lai0_2",
    "masec0_2", "zrac0_2", "code_acti_reserve_2", "maperenne0_2",
    "QNperenne0_2", "masecnp0_2", "QNplantenp0_2", "masec0_2",
    "QNplante0_2", "restemp0_2", "densinitial_2", "NH4initf",
]


class MemoryConverter(SticsFicIniConverter):
    def write_file(self, *_args, **_kwargs):
        pass


def model_dictionary():
    connection = sqlite3.connect(":memory:")
    connection.execute(
        """CREATE TABLE Variables (
            Champ TEXT, Default_Value_Datamill TEXT,
            defaultValueOtherSource TEXT, model TEXT, [Table] TEXT
        )"""
    )
    for field in dict.fromkeys(DEFAULT_FIELDS):
        value = "1" if field != "NH4initf" else "2"
        connection.execute(
            "INSERT INTO Variables VALUES (?, ?, NULL, 'sticsv11', 'ficini')",
            (field, value),
        )
    return connection


def master_input(with_option=False, with_initial_layers=False):
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        """
        CREATE TABLE Soil (
            IdSoil TEXT, SoilOption TEXT, Wwp REAL, Wfc REAL, bd REAL
        );
        CREATE TABLE SoilLayers (
            idsoil TEXT, NumLayer INTEGER, Lup REAL, Ldown REAL,
            Wwp REAL, Wfc REAL, bd REAL
        );
        CREATE TABLE SimUnitList (idsim TEXT, idIni TEXT, idsoil TEXT, idMangt TEXT);
        CREATE TABLE CropManagement (idMangt TEXT, PlantOrder INTEGER, SeasonOrder INTEGER);
        INSERT INTO Soil VALUES ('soil-1', 'detailed', 10, 30, 1.2);
        INSERT INTO SoilLayers VALUES ('soil-1', 1, 0, 20, 10, 30, 1.0);
        INSERT INTO SoilLayers VALUES ('soil-1', 2, 20, 50, 20, 40, 2.0);
        INSERT INTO SimUnitList VALUES ('sim-1', 'ini-1', 'soil-1', 'mangt-1');
        INSERT INTO CropManagement VALUES ('mangt-1', 1, 1);
        """
    )
    option_sql = ", option TEXT" if with_option else ""
    connection.execute(
        f"""CREATE TABLE InitialConditions (
            idIni TEXT, WStockinit REAL, Ninit REAL, NH4initf REAL{option_sql}
        )"""
    )
    if with_option:
        connection.execute(
            "INSERT INTO InitialConditions VALUES ('ini-1', 50, 30, 6, 'detailed')"
        )
    else:
        connection.execute(
            "INSERT INTO InitialConditions VALUES ('ini-1', 50, 30, 6)"
        )
    if with_initial_layers:
        connection.executescript(
            """
            CREATE TABLE InitialConditionsLayers (
                idIni TEXT, NumLayer INTEGER, Lup REAL, Ldown REAL,
                WStockinit REAL, Ninit REAL, NH4initf REAL
            );
            INSERT INTO InitialConditionsLayers
                VALUES ('ini-1', 1, 0, 20, 25, 10, 1);
            INSERT INTO InitialConditionsLayers
                VALUES ('ini-1', 2, 20, 50, 75, 20, 5);
            """
        )
    return connection


def output_values(content, label):
    lines = content.splitlines()
    return [float(value) for value in lines[lines.index(label) + 1].split()]


def export(master):
    return MemoryConverter().export(
        "/tmp/sim-1/season/output",
        model_dictionary(),
        master,
        "/tmp",
    )


def test_legacy_detailed_initial_conditions_are_distributed_uniformly():
    content = export(master_input())

    assert output_values(content, ":NO3init:") == [15, 15, 0, 0, 0]
    assert output_values(content, ":NH4initf:") == [3, 3, 0, 0, 0]
    assert output_values(content, ":Hinitf:") == [20, 15, 0, 0, 0]


def test_new_detailed_initial_conditions_use_layer_values():
    content = export(master_input(with_option=True, with_initial_layers=True))

    assert output_values(content, ":NO3init:") == [10, 20, 0, 0, 0]
    assert output_values(content, ":NH4initf:") == [1, 5, 0, 0, 0]
    assert output_values(content, ":Hinitf:") == [15, 17.5, 0, 0, 0]


def test_option_column_requires_initial_conditions_layers_table():
    with pytest.raises(ValueError, match="InitialConditionsLayers table is mandatory"):
        export(master_input(with_option=True, with_initial_layers=False))


def test_season_year_offset_uses_actual_calendar_year_lengths():
    assert _season_year_offset_days(1985, 0) == 0
    assert _season_year_offset_days(1985, 1) == 365
    assert _season_year_offset_days(1988, 1) == 366
    assert _season_year_offset_days(1987, 2) == 365 + 366


def test_stics_date_keeps_current_season_coordinate_system():
    assert _stics_date(730) == 730
    assert _stics_date(731) == 731
    assert _stics_date(700, 31) == 731


def test_irecbutoir_equals_effective_simulation_end_day():
    assert _irecbutoir(350) == 350
    assert _irecbutoir(716) == 716
    assert _irecbutoir(731) == 731


def test_standard_simulation_end_day_uses_actual_year_lengths():
    assert _simulation_end_day(2000, 2000, 350) == 350
    assert _simulation_end_day(2000, 2001, 350) == 716
    assert _simulation_end_day(2001, 2002, 350) == 715
