import math
import sqlite3

import pytest

from modfilegen.output_configuration import OutputConfiguration, OutputConfigurationError


def test_default_catalog_loads_and_resolves_model_mappings():
    config = OutputConfiguration.from_files()

    assert "legacy" in config.selection_names()
    assert config.model_mapping("yield", "DSSAT")["field"] == "HWAM"
    assert config.model_mapping("potential_irrigation_requirement", "stics")["field"] == "totir"
    unavailable = config.selected_variables("legacy", "celsius", include_unavailable=False)
    assert "potential_irrigation_requirement" not in {item.key for item in unavailable}


def test_common_scale_offset_and_missing_model_conversions():
    config = OutputConfiguration.from_files()

    assert config.convert_value("yield", "dssat", 4250) == pytest.approx(4.25)
    assert config.convert_value("maximum_root_depth", "dssat", 1.35) == pytest.approx(135.0)
    assert config.convert_value("potential_irrigation_requirement", "celsius", 20) is None


def test_dssat_dates_support_standard_and_successive_contexts():
    config = OutputConfiguration.from_files()

    assert config.convert_value(
        "maturity", "dssat", 1992030,
        {"reference_year": 1991, "mode": "standard"},
    ) == 396
    assert config.convert_value(
        "maturity", "dssat", 1993030,
        {"reference_year": 1991, "mode": "successive"},
    ) == 761
    assert math.isnan(config.convert_value(
        "maturity", "dssat", -99,
        {"reference_year": 1991, "mode": "standard"},
    ))


def test_summary_schema_is_created_then_extended_without_losing_rows():
    config = OutputConfiguration.from_files()
    connection = sqlite3.connect(":memory:")

    created = config.ensure_summary_output_schema(connection, "minimal")
    assert "Yield" in created
    connection.execute(
        'INSERT INTO SummaryOutput (Model, IdSim, Yield) VALUES (?, ?, ?)',
        ("Dssat", "sim-1", 2.5),
    )
    added = config.ensure_summary_output_schema(connection, "legacy")

    assert "PotentialIrrigationRequirement" in added
    assert connection.execute("SELECT Yield FROM SummaryOutput").fetchone()[0] == 2.5
    columns = {
        row[1].lower(): row[2].upper()
        for row in connection.execute("PRAGMA table_info(SummaryOutput)")
    }
    assert columns["potentialirrigationrequirement"] == "REAL"


def test_unknown_selection_has_an_explicit_error():
    config = OutputConfiguration.from_files()
    with pytest.raises(OutputConfigurationError, match="Selection inconnue"):
        config.selected_keys("missing")
