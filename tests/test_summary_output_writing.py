"""SummaryOutput writing: create or extend the table, replace one model's rows."""

import sqlite3

import pandas as pd
import pytest

from modfilegen.output_configuration import OutputConfiguration


def _rows(path, sql):
    with sqlite3.connect(path) as connection:
        return connection.execute(sql).fetchall()


def _columns(path):
    return {row[1].lower() for row in _rows(path, "PRAGMA table_info(SummaryOutput)")}


def _summary(model, idsims):
    return pd.DataFrame({
        "Model": [model] * len(idsims),
        "Idsim": idsims,
        "SeasonOrder": [1] * len(idsims),
        "Yield": [1.5] * len(idsims),
    })


def test_creates_summary_table_when_missing(masterinput_copy):
    configuration = OutputConfiguration.from_files()
    with sqlite3.connect(masterinput_copy) as connection:
        configuration.replace_model_summary_rows(
            connection, _summary("Stics", ["a", "b"]), "Stics"
        )

    assert _rows(masterinput_copy, "SELECT COUNT(*) FROM SummaryOutput")[0][0] == 2
    expected = {name.lower() for name in configuration.summary_columns("legacy")}
    assert expected <= _columns(masterinput_copy)


def test_replaces_only_rows_of_the_same_model(masterinput_copy):
    configuration = OutputConfiguration.from_files()
    with sqlite3.connect(masterinput_copy) as connection:
        configuration.replace_model_summary_rows(
            connection, _summary("Dssat", ["d1"]), "Dssat"
        )
        configuration.replace_model_summary_rows(
            connection, _summary("Stics", ["old1", "old2"]), "Stics"
        )
        configuration.replace_model_summary_rows(
            connection, _summary("STICS", ["new"]), "stics"
        )

    rows = _rows(
        masterinput_copy, "SELECT Model, IdSim FROM SummaryOutput ORDER BY Model"
    )
    assert rows == [("Dssat", "d1"), ("STICS", "new")]


def test_adds_dataframe_columns_missing_from_the_table(masterinput_copy):
    configuration = OutputConfiguration.from_files()
    dataframe = _summary("Stics", ["a"]).assign(ExtraCount=[3], ExtraLabel=["x"])
    with sqlite3.connect(masterinput_copy) as connection:
        added = configuration.replace_model_summary_rows(
            connection, dataframe, "Stics"
        )

    assert {"ExtraCount", "ExtraLabel"} <= set(added)
    types = {
        row[1]: row[2] for row in _rows(masterinput_copy, "PRAGMA table_info(SummaryOutput)")
    }
    assert types["ExtraCount"] == "INTEGER"
    assert types["ExtraLabel"] == "TEXT"


def test_keeps_existing_table_and_columns(masterinput_copy):
    with sqlite3.connect(masterinput_copy) as connection:
        connection.execute(
            "CREATE TABLE SummaryOutput (Model TEXT, IdSim TEXT, LocalNote TEXT)"
        )
        connection.execute(
            "INSERT INTO SummaryOutput VALUES ('Celsius', 'c1', 'kept')"
        )
    configuration = OutputConfiguration.from_files()
    with sqlite3.connect(masterinput_copy) as connection:
        configuration.replace_model_summary_rows(
            connection, _summary("Stics", ["s1"]), "Stics"
        )

    assert "localnote" in _columns(masterinput_copy)
    assert _rows(
        masterinput_copy,
        "SELECT LocalNote FROM SummaryOutput WHERE Model = 'Celsius'",
    ) == [("kept",)]


def test_stics_v11_writes_summary_without_existing_table(masterinput_copy, tmp_path):
    sticsconverter = pytest.importorskip(
        "modfilegen.Converter.SticsV11Converter.sticsconverter"
    )
    result_path = tmp_path / "result.csv"
    pd.DataFrame({
        "Model": ["Stics", "Stics"],
        "Idsim": ["s1", "s2"],
        "Texte": ["", ""],
        "SeasonOrder": [1, 2],
        "Yield": [2.0, 3.0],
    }).to_csv(result_path, index=False)

    sticsconverter.save_summary_output(result_path, masterinput_copy)
    sticsconverter.save_summary_output(result_path, masterinput_copy)

    assert _rows(
        masterinput_copy,
        "SELECT IdSim, SeasonOrder FROM SummaryOutput ORDER BY IdSim",
    ) == [("s1", 1), ("s2", 2)]


def test_dssat_successive_writes_summary_without_existing_table(masterinput_copy):
    converter = pytest.importorskip(
        "modfilegen.Converter.DssatConverter.dssatsuccessiveconverter"
    )

    converter.save_successive_outputs(
        masterinput_copy, _summary("Dssat", ["d1", "d2"]), [], True, False
    )
    converter.save_successive_outputs(
        masterinput_copy, _summary("Dssat", ["d3"]), [], True, False
    )

    assert _rows(
        masterinput_copy, "SELECT IdSim FROM SummaryOutput WHERE Model = 'Dssat'"
    ) == [("d3",)]
