import pandas as pd
import pytest

from modfilegen.Converter.CelsiusConverter.summary_output import (
    add_spatial_time_columns,
    append_canonical_summary_csv,
    transform_summary_dataframe,
)
from modfilegen.output_configuration import OutputConfiguration


def raw_output():
    return pd.DataFrame(
        {
            "Idsim": ["CEL-1"],
            "iplt": [10],
            "JulPheno1_1": [15],
            "JulPheno1_4": [70],
            "JulPheno1_6": [120],
            "Biom(nrec)": [8.5],
            "Grain(nrec)": [3.25],
            "Ngrain": [12500],
            "LAI": [4.2],
            "stockNsol": [18.0],
            "SigmaSimEsol": [91.0],
            "SigmaTranspiMC": [244.5],
        }
    )


@pytest.mark.parametrize(
    ("model", "model_label"),
    [("celsius", "Celsius"), ("celsiusv32", "CelsiusV32")],
)
def test_celsius_summary_uses_shared_columns_and_conversions(model, model_label):
    configuration = OutputConfiguration.from_files()
    result = transform_summary_dataframe(raw_output(), configuration, model=model)

    assert list(result.columns) == list(configuration.summary_columns("legacy"))
    assert result.loc[0, "Model"] == model_label
    assert result.loc[0, "IdSim"] == "CEL-1"
    assert result.loc[0, "Biom_ma"] == pytest.approx(8500.0)
    assert result.loc[0, "Yield"] == pytest.approx(3.25)
    assert result.loc[0, "Transp"] == pytest.approx(244.5)
    assert pd.isna(result.loc[0, "FreshYield"])


def test_celsius_optional_unavailable_outputs_are_null():
    result = transform_summary_dataframe(
        raw_output(), OutputConfiguration.from_files(), model="celsius"
    )

    assert pd.isna(result.loc[0, "RootBiomass"])
    assert pd.isna(result.loc[0, "CumN2OEmissions"])


def test_raw_batches_are_written_as_one_canonical_celsius_csv(tmp_path):
    configuration = OutputConfiguration.from_files()
    result_path = tmp_path / "result_celsius.csv"

    append_canonical_summary_csv(
        raw_output(),
        result_path,
        configuration,
        model="celsius",
        write_header=True,
    )
    append_canonical_summary_csv(
        raw_output().assign(Idsim="CEL-2"),
        result_path,
        configuration,
        model="celsius",
        write_header=False,
    )
    stored = pd.read_csv(result_path)

    assert list(stored.columns) == list(configuration.summary_columns("legacy"))
    assert stored["IdSim"].tolist() == ["CEL-1", "CEL-2"]
    assert "Biom(nrec)" not in stored.columns
    assert stored["Biom_ma"].tolist() == [8500.0, 8500.0]


def test_spatial_coordinates_and_year_come_from_simulation_context():
    raw = raw_output().assign(Idsim="arbitrary-id")

    result = add_spatial_time_columns(
        raw,
        {"arbitrary-id": {"lat": 5.925, "lon": 6.025, "time": 2000}},
    )

    assert result.loc[0, "lat"] == 5.925
    assert result.loc[0, "lon"] == 6.025
    assert result.loc[0, "time"] == 2000


def test_missing_celsius_coordinates_are_null_without_idsim_parsing():
    raw = raw_output().assign(Idsim="5.925_6.025_2000_old-format")

    result = add_spatial_time_columns(raw, {})

    assert pd.isna(result.loc[0, "lat"])
    assert pd.isna(result.loc[0, "lon"])
    assert pd.isna(result.loc[0, "time"])
