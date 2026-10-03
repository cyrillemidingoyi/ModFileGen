import sqlite3

from modfilegen.Converter.DssatConverter.dssatxconverter import (
    writeBlockAutomaticIrrigation,
)
from modfilegen.Converter.DssatConverter.dssatweatherconverter import (
    format_dssat_weather_date,
    mean_dewpoint,
)


def test_weather_values_from_sqlite_are_formatted_for_dssat():
    assert format_dssat_weather_date(1993.0, 1.0) == "93001"
    assert mean_dewpoint(-2.0, 4.0) == 1.0


def test_automatic_irrigation_preserves_fractional_efficiency():
    connection = sqlite3.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE Variables (
            model TEXT,
            [Table] TEXT,
            Champ TEXT,
            Default_Value_Datamill TEXT,
            defaultValueOtherSource TEXT
        )
        """
    )
    values = {
        "LNSIM": "1",
        "TITIRR": "IR",
        "DSOIL": "30",
        "THETAC": "55",
        "IEPT": "100",
        "IOFF": "GS000",
        "IAME": "IR003",
        "AIRAMT": "10",
        "EFFIRR": "0.60",
    }
    connection.executemany(
        "INSERT INTO Variables VALUES ('dssat', 'dssat_x_automatic_irrigation', ?, ?, NULL)",
        values.items(),
    )

    block = writeBlockAutomaticIrrigation(
        "dssat_x_automatic_irrigation", "dssat_x_exp_id", "simulation", connection
    )

    assert block.splitlines()[-1].endswith("  0.60")
