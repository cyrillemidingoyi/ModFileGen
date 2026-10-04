import csv
import sqlite3
from datetime import date

from modfilegen.weather_coverage import (
    check_simulations,
    keep_simulations_with_weather,
    weather_periods,
)


def _simulations(path):
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in connection.execute(
            "SELECT * FROM SimUnitList ORDER BY idsim"
        )]
    finally:
        connection.close()


def _ids(rows):
    return sorted(str(row["idsim"]) for row in rows)


def _execute(path, sql, params=()):
    with sqlite3.connect(path) as connection:
        connection.execute(sql, params)


def test_fixture_weather_is_one_period_per_point(masterinput_db):
    with sqlite3.connect(masterinput_db) as connection:
        periods = weather_periods(connection)
    assert periods == {"5.925_6.025": [(date(2000, 1, 1), date(2002, 12, 31))]}


def test_all_fixture_simulations_are_covered(masterinput_db):
    rows = _simulations(masterinput_db)
    with sqlite3.connect(masterinput_db) as connection:
        kept, skipped = check_simulations(rows, connection)
    assert _ids(kept) == _ids(rows)
    assert skipped == []


def test_missing_final_year_skips_only_the_simulations_that_need_it(
    masterinput_copy, tmp_path
):
    _execute(masterinput_copy, "DELETE FROM RAclimateD WHERE year = 2002")
    rows = _simulations(masterinput_copy)

    kept = keep_simulations_with_weather(rows, masterinput_copy, "Test", tmp_path)

    assert _ids(kept) == [
        "5.925_6.025_2000_Mgt1M0_150_2",
        "5.925_6.025_2001_Mgt1M0_150_2",
    ]
    with open(tmp_path / "skipped_simulations_test.csv", encoding="utf-8") as handle:
        report = list(csv.DictReader(handle))
    assert sorted(item["idsim"] for item in report) == [
        "5.925_6.025_2000_ROT_MAIZE_PEANUT_3Y_2",
        "5.925_6.025_2002_Mgt1M0_150_2",
    ]
    assert all("2000-01-01 to 2001-12-31" in item["reason"] for item in report)
    assert len(_simulations(masterinput_copy)) == len(rows)


def test_gap_inside_the_period_skips_the_simulation(masterinput_copy):
    _execute(masterinput_copy, "DELETE FROM RAclimateD WHERE year = 2001 AND DOY = 200")
    rows = _simulations(masterinput_copy)
    with sqlite3.connect(masterinput_copy) as connection:
        kept, skipped = check_simulations(rows, connection)

    assert "5.925_6.025_2001_Mgt1M0_150_2" not in _ids(kept)
    assert sorted(item.idsim for item in skipped) == [
        "5.925_6.025_2000_ROT_MAIZE_PEANUT_3Y_2",
        "5.925_6.025_2001_Mgt1M0_150_2",
    ]


def test_point_without_weather_and_invalid_dates_are_skipped(masterinput_copy):
    rows = _simulations(masterinput_copy)
    rows[0] = dict(rows[0], idPoint="unknown_point")
    rows[1] = dict(rows[1], EndDay=None)
    with sqlite3.connect(masterinput_copy) as connection:
        kept, skipped = check_simulations(rows, connection)

    reasons = {item.idsim: item.reason for item in skipped}
    assert reasons[str(rows[0]["idsim"])] == "no weather for this idPoint"
    assert "invalid" in reasons[str(rows[1]["idsim"])]
    assert len(kept) == len(rows) - 2


def test_celsius_v32_conversion_does_not_convert_uncovered_simulations(
    masterinput_copy, celsius_v32_template_db, tmp_path
):
    import shutil

    from modfilegen.Converter.CelsiusV32Converter.core import convert_database

    _execute(masterinput_copy, "DELETE FROM RAclimateD WHERE year = 2002")
    target = tmp_path / "celsius.db"
    shutil.copy2(celsius_v32_template_db, target)

    convert_database(
        masterinput_copy, target, mode="successive", report_directory=tmp_path
    )

    with sqlite3.connect(target) as connection:
        situations = [
            row[0] for row in connection.execute("SELECT Situation FROM SimUnitList")
        ]
    assert situations and not [s for s in situations if "2002" in s or "ROT_" in s]
    assert (tmp_path / "skipped_simulations_celsiusv32.csv").exists()


SIMULATION = {"StartYear": 2000, "StartDay": 120, "EndYear": 2002, "EndDay": 350}


def test_daily_rows_outside_the_simulation_period_are_dropped():
    import pandas as pd

    from modfilegen.weather_coverage import keep_rows_in_simulation_period

    daily = pd.DataFrame({
        "YEAR": [2000, 2000, 2002, 2002, 2003, 2004],
        "DOY": [119, 120, 350, 351, 1, 60],
        "value": [1, 2, 3, 4, 5, 6],
    })

    kept = keep_rows_in_simulation_period(daily, SIMULATION)

    assert kept[["YEAR", "DOY"]].values.tolist() == [[2000, 120], [2002, 350]]


def test_dssat_successive_daily_outputs_stop_at_simulation_end(tmp_path):
    converter = __import__(
        "modfilegen.Converter.DssatConverter.dssatsuccessiveconverter",
        fromlist=["read_sequence_daily"],
    )
    lines = ["*WEATHER MODULE DAILY OUTPUT FILE\n", "@YEAR DOY   DAS   SRAA\n"]
    for year, doy in [(2002, 349), (2002, 350), (2002, 351), (2003, 100), (2003, 118)]:
        lines.append(f" {year} {doy:3d}   900  15.0\n")
    (tmp_path / "Weather.OUT").write_text("".join(lines), encoding="utf-8")

    daily = converter.read_sequence_daily(tmp_path, "rotation", SIMULATION)

    assert daily[["YEAR", "DOY"]].values.tolist() == [[2002, 349], [2002, 350]]
    assert set(daily["Idsim"]) == {"rotation"}


def test_dssat_standard_daily_outputs_stop_at_simulation_end(tmp_path):
    converter = __import__(
        "modfilegen.Converter.DssatConverter.dssatconverter",
        fromlist=["create_df_daily"],
    )
    lines = ["*WEATHER MODULE DAILY OUTPUT FILE\n", "@YEAR DOY   DAS   SRAA\n"]
    for year, doy in [(2000, 119), (2000, 120), (2002, 350), (2002, 351)]:
        lines.append(f" {year} {doy:3d}   900  15.0\n")
    path = tmp_path / "Weather.OUT"
    path.write_text("".join(lines), encoding="utf-8")

    daily = converter.create_df_daily({"Weather": str(path)}, "standard", SIMULATION)

    assert daily[["YEAR", "DOY"]].values.tolist() == [[2000, 120], [2002, 350]]
