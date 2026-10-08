import sqlite3

import pytest

from modfilegen.coordinate_resolver import CoordinateResolver, CoordinateValues
from modfilegen.Converter.DssatConverter import dssatconverter
from modfilegen.Converter.SticsConverter.sticsconverter import (
    create_df_summary as create_stics_v9_summary,
)
from modfilegen.Converter.SticsV11Converter.sticsconverter import (
    create_df_summary as create_stics_v11_summary,
)


def coordinate_database():
    connection = sqlite3.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE Coordinates (
            idPoint TEXT,
            latitudeDD REAL,
            longitudeDD REAL,
            altitude REAL
        )
        """
    )
    connection.execute(
        "INSERT INTO Coordinates VALUES (?, ?, ?, ?)",
        ("POINT_A", -11.725, 36.175, 506.0),
    )
    return connection


def test_resolves_prefetched_coordinates_case_insensitively():
    connection = coordinate_database()
    try:
        resolver = CoordinateResolver(connection)
        resolver.prefetch(["point_a"])

        assert resolver.resolve("POINT_A") == CoordinateValues(
            latitude=-11.725,
            longitude=36.175,
            altitude=506.0,
        )
    finally:
        connection.close()


def test_prefetched_missing_point_resolves_to_null_coordinates():
    connection = coordinate_database()
    try:
        resolver = CoordinateResolver(connection)
        resolver.prefetch(["missing-point"])

        assert resolver.resolve("missing-point") == CoordinateValues()
    finally:
        connection.close()


def test_resolve_requires_prefetch():
    connection = coordinate_database()
    try:
        resolver = CoordinateResolver(connection)
        with pytest.raises(RuntimeError, match="not prefetched"):
            resolver.resolve("POINT_A")
    finally:
        connection.close()


def test_dssat_standard_transform_uses_resolved_coordinates_for_every_dt(tmp_path):
    summary = tmp_path / "Summary_arbitrary_identifier.OUT"
    summary.write_text(
        "*SUMMARY\n\n! identifiers\n"
        "@ A B C D E F G H I J K L M PDAT HWAM\n"
        "1 1 1 0 1 0 0 0 0 0 0 0 0 2000150 2500\n"
    )

    result = dssatconverter.transform(
        summary,
        CoordinateValues(latitude=-11.725, longitude=36.175),
        2000,
    )

    assert result.loc[0, "lat"] == -11.725
    assert result.loc[0, "lon"] == 36.175
    assert result.loc[0, "time"] == 2000


def test_stics_summaries_use_resolved_coordinates_independently_of_idsim(tmp_path):
    coordinates = CoordinateValues(latitude=-11.725, longitude=36.175)
    v9_report = tmp_path / "mod_rapport_arbitrary-id.sti"
    v9_report.write_text("ansemis;iplts;\n1991;270;\n")
    v11_report = tmp_path / "report.sti"
    v11_report.write_text("ansemis;iplts;\n1992;180;\n")

    v9 = create_stics_v9_summary(v9_report, coordinates)
    v11 = create_stics_v11_summary(
        v11_report, coordinates, "identifier-without-coordinates"
    )

    assert (v9.loc[0, "lat"], v9.loc[0, "lon"], v9.loc[0, "time"]) == (
        -11.725, 36.175, 1991,
    )
    assert (v11.loc[0, "lat"], v11.loc[0, "lon"], v11.loc[0, "time"]) == (
        -11.725, 36.175, 1992,
    )
