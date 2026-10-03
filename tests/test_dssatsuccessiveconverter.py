import sqlite3
from types import SimpleNamespace

import pandas as pd

from modfilegen.output_configuration import OutputConfiguration
from modfilegen.Converter.DssatConverter.summary_output import (
    transform_summary_dataframe,
)
from modfilegen.coordinate_resolver import CoordinateValues
from modfilegen.Converter.DssatConverter import dssatsuccessiveconverter as converter



# Three-season rotation (one crop per season) from the text MasterInput fixture.
ROTATION_MANAGEMENT = "ROT_MAIZE_PEANUT_3Y"


def simulation_row(connection):
    connection.row_factory = sqlite3.Row
    return dict(
        connection.execute(
            "SELECT * FROM SimUnitList WHERE idMangt = ?", (ROTATION_MANAGEMENT,)
        ).fetchone()
    )


def test_new_schema_expands_one_simunit_into_ordered_managements(masterinput_db):
    with sqlite3.connect(masterinput_db) as connection:
        row = simulation_row(connection)
        managements = converter.successive_managements(row, connection)

    assert [item["SeasonOrder"] for item in managements] == [1, 2, 3]
    assert [item["Idcultivar"] for item in managements] == [
        "testcult", "testcult2", "testcult"
    ]


def test_two_digit_section_level_does_not_shift_fixed_width_columns():
    original = " 1 00270   -99   7.0"

    replaced = converter.replace_first_int(original, 10)

    assert replaced == "10 00270   -99   7.0"
    assert replaced.index("00270") == original.index("00270")


def test_harvest_date_can_be_replaced_without_shifting_columns():
    original = " 1 01154 GS000   -99"

    replaced = converter.replace_second_token(original, "00365")

    assert replaced == " 1 00365 GS000   -99"
    assert replaced.index("GS000") == original.index("GS000")


def test_rendered_sections_keep_replaced_harvest_date():
    sections = converter.parse_sections(
        "*HARVEST DETAILS\n@H HDATE HSTG\n 1 01154 GS000\n"
    )
    converter.set_section_date(sections, "*HARVEST", "00365")

    assert " 1 00365 GS000" in converter.render_sections(sections)


def test_successive_dates_use_sowing_year_offset_in_legacy_sections():
    sections = converter.parse_sections(
        "*PLANTING DETAILS\n@P PDATE EDATE\n 1 92015 -99\n"
        "*RESIDUES AND ORGANIC FERTILIZER\n@R RDATE RCOD\n 1 92016 -99\n"
        "*TILLAGE AND ROTATIONS\n@T TDATE TIMPL\n 1 92015 TI007\n"
        "*IRRIGATION AND WATER MANAGEMENT\n@I IDATE IROP\n 1 00001 IR001\n"
        "*HARVEST DETAILS\n@H HDATE HSTG\n 1 92195 GS000\n"
    )
    simunit = {"StartYear": 1992}
    season = {"StartYear": 1992}
    management = {"SowingYearOffset": 1, "sowingdate": 15}

    converter.apply_successive_management_dates(
        sections, simunit, season, management
    )

    rendered = converter.render_sections(sections)
    assert " 1 93015 -99" in rendered
    assert " 1 93016 -99" in rendered
    assert " 1 93015 TI007" in rendered
    assert " 1 00001 IR001" in rendered
    # Harvest has its own season-end rewrite and is not shifted here.
    assert " 1 92195 GS000" in rendered


def test_standard_date_writer_is_not_changed_by_successive_date_helper():
    sections = converter.parse_sections(
        "*PLANTING DETAILS\n@P PDATE EDATE\n 1 92015 -99\n"
    )

    assert " 1 92015 -99" in converter.render_sections(sections)


def test_textual_management_policy_codes_are_enabled():
    assert converter.policy_code_enabled("MA_IA55")
    assert converter.policy_code_enabled("MA_NPK225")


def test_legacy_zero_management_policy_codes_are_disabled():
    for value in (None, "", "0", 0, "0.0", 0.0):
        assert not converter.policy_code_enabled(value)


def test_rotation_workdirs_are_removed_after_sequence_assembly(tmp_path):
    rotations = [SimpleNamespace(index=1), SimpleNamespace(index=2)]
    for rotation in rotations:
        rotation_dir = tmp_path / f"_rotation_{rotation.index}"
        rotation_dir.mkdir()
        (rotation_dir / "ITSA1301.MZX").write_text("temporary")
    final_sequence = tmp_path / "ITSA1301.SQX"
    final_sequence.write_text("final")

    converter.remove_rotation_workdirs(tmp_path, rotations)

    assert final_sequence.exists()
    assert not (tmp_path / "_rotation_1").exists()
    assert not (tmp_path / "_rotation_2").exists()




def test_season_dates_use_sowing_year_offset_without_fallow_rows(masterinput_db):
    with sqlite3.connect(masterinput_db) as connection:
        row = simulation_row(connection)
        managements = converter.successive_managements(row, connection)

    seasons = converter.successive_season_rows(row, managements)

    assert [(item["StartYear"], item["StartDay"]) for item in seasons] == [
        (2000, 120), (2000, 351), (2001, 351)
    ]
    assert len(seasons) == len(managements) == 3


def test_in_memory_database_is_reused_across_seasons(masterinput_db):
    with sqlite3.connect(masterinput_db) as source:
        row = simulation_row(source)
        managements = converter.successive_managements(row, source)
        connection = converter.successive_group_connection(source)
        try:
            for management in managements:
                configured = converter.configure_season_connection(
                    connection, row, management
                )
                assert configured is connection
                selected = connection.execute(
                    "SELECT SeasonOrder FROM CropManagement"
                ).fetchall()
                assert selected == [(management["SeasonOrder"],)]
        finally:
            connection.close()


def test_last_season_is_truncated_at_simunit_end():
    row = {
        "idsim": "rotation",
        "StartYear": 1991,
        "StartDay": 1,
        "EndYear": 2000,
        "EndDay": 365,
    }
    management = {
        "SeasonOrder": 10,
        "SowingYearOffset": 9,
        "sowingdate": 270,
        "DHarvest": 250,
    }

    season = converter.season_simunit_row(
        row,
        management,
        is_last_season=True,
        previous_season_end=converter.julian_date(2000, 155),
    )

    assert (season["StartYear"], season["StartDay"]) == (2000, 156)
    assert (season["EndYear"], season["EndDay"]) == (2000, 365)


def test_non_final_season_cannot_exceed_simunit_end():
    row = {
        "idsim": "rotation",
        "StartYear": 1991,
        "StartDay": 1,
        "EndYear": 2000,
        "EndDay": 365,
    }
    management = {
        "SeasonOrder": 9,
        "SowingYearOffset": 9,
        "sowingdate": 270,
        "DHarvest": 250,
    }

    try:
        converter.season_simunit_row(row, management)
    except ValueError as error:
        assert "is outside simulation" in str(error)
    else:
        raise AssertionError("An overflowing non-final season must be rejected")


def test_simunit_rows_are_not_grouped_as_successive_seasons_anymore(masterinput_db):
    with sqlite3.connect(masterinput_db) as connection:
        row = simulation_row(connection)

    assert converter.build_successive_groups([row]) == [[row]]
    assert converter.group_key([row]) == row["idsim"]


def test_weather_covers_the_complete_dssat_nyers_interval(masterinput_db):
    with sqlite3.connect(masterinput_db) as connection:
        row = simulation_row(connection)

    assert converter.dssat_sequence_years([row]) == 3
    # SDATE is day 120: DSSAT also reads the year after the NYERS boundary.
    assert converter.sequence_weather_years([row]) == [2000, 2001, 2002, 2003, 2004]


def test_weather_includes_exclusive_boundary_year_for_january_start():
    row = {
        "StartYear": 1991,
        "StartDay": 1,
        "EndYear": 2000,
        "EndDay": 365,
    }

    assert converter.dssat_sequence_years([row]) == 10
    assert converter.sequence_weather_years([row]) == list(range(1991, 2002))


def test_weather_includes_following_calendar_year_for_late_year_start():
    row = {
        "StartYear": 1992,
        "StartDay": 305,
        "EndYear": 2021,
        "EndDay": 181,
    }

    assert converter.dssat_sequence_years([row]) == 29
    assert converter.sequence_weather_years([row]) == list(range(1992, 2023))


def test_summary_keeps_season_order_and_uses_sdat_for_time_and_dates():
    dataframe = pd.DataFrame(
        {
            "Model": ["Dssat"],
            "Idsim": ["rotation"],
            "Texte": [""],
            "SeasonOrder": [2],
            "SDAT": [1992180],
            "time": [1992],
            "PDAT": [1992324],
            "EDAT": [1992329],
            "ADAT": [1993010],
            "MDAT": [1993059],
            "HDAT": [1993065],
            "CWAM": [6500],
            "HWAM": [2400],
            "H#AM": [500],
            "LAIX": [4.2],
            "NLCM": [1.5],
            "NIAM": [18],
            "CNAM": [75],
            "ESCP": [90],
            "EPCP": [210],
        }
    )

    result = transform_summary_dataframe(
        dataframe, OutputConfiguration.from_files(), mode="successive"
    )

    assert result.loc[0, "SeasonOrder"] == 2
    assert result.loc[0, "time"] == 1992
    assert result.loc[0, "Planting"] == 324
    assert result.loc[0, "Mat"] == 425
    assert result.loc[0, "Harvest"] == 431


def test_one_management_builds_one_sequence_rotation_for_full_nyers():
    row = {
        "idsim": "single-management-sequence",
        "StartYear": 1991,
        "StartDay": 1,
        "EndYear": 2000,
        "EndDay": 365,
    }
    rotations = [SimpleNamespace(index=1)]

    batch = converter.build_batch_file(rotations)

    assert converter.dssat_sequence_years([row]) == 10
    assert batch.count("ITSA1301.SQX") == 1
    assert converter.batch_line(converter.SEQ_FILE_NAME, 1) in batch


def test_one_management_keeps_all_automatic_cycles(tmp_path):
    summary = tmp_path / "Summary.OUT"
    summary.write_text(
        "*SUMMARY\n\n! identifiers\n"
        "@ A B C D E F G H I J K L M SDAT PDAT CWAM\n"
        "1 1 1 0 1 0 0 0 0 0 0 0 0 1991200 1991270 10\n"
        "2 1 1 0 1 0 0 0 0 0 0 0 0 1992200 1992270 20\n"
    )
    rotation = SimpleNamespace(
        index=1,
        row={
            "idsim": "single-management-sequence",
            "StartYear": 1991,
        },
        management={"SeasonOrder": 1},
    )

    result = converter.transform_sequence(
        summary,
        [rotation],
        CoordinateValues(latitude=-11.725, longitude=36.175),
    )

    assert len(result) == 2
    assert result["SeasonOrder"].tolist() == [1, 2]
    assert result["time"].tolist() == [1991, 1992]
    assert result["lat"].tolist() == [-11.725, -11.725]
    assert result["lon"].tolist() == [36.175, 36.175]
