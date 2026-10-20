"""Check the delivered reference data without network access or APSIM execution."""

from collections import Counter
import csv
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest


ROOT = Path(__file__).resolve().parents[1]
CATALOGUE = ROOT / "docs/catalogues/apsimng"


@pytest.fixture(scope="module")
def catalogue():
    path = CATALOGUE / "Variables_APSIMNG.csv"
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        rows = list(reader)
        columns = reader.fieldnames
    manifest = json.loads((CATALOGUE / "manifest.json").read_text())
    return path, columns, rows, manifest


def test_catalogue_matches_variables_schema_and_manifest(catalogue):
    path, columns, rows, manifest = catalogue
    with sqlite3.connect(":memory:") as connection:
        schema = ROOT / "tests/fixtures/sources/modelsdictionary/schema.sql"
        connection.executescript(schema.read_text())
        expected = [row[1] for row in connection.execute("PRAGMA table_info(Variables)")]
    assert columns == expected == manifest["schema_columns"]
    assert len(rows) == manifest["rows"]
    assert dict(Counter(r["status"] for r in rows)) == manifest["status_counts"]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == manifest["csv_sha256"]
    assert manifest["parser_issues_in_catalogue"] == []
    assert manifest["management_issues"] == []
    assert dict(Counter(r["Table"] for r in rows)) == manifest["table_counts"]
    assert manifest["duplicate_keys"] == []


def test_catalogue_keys_and_value_provenance(catalogue):
    _, columns, rows, manifest = catalogue
    seen = set()
    for row in rows:
        assert list(row) == columns
        assert None not in row.values()  # No truncated CSV records.
        key = row["model"], row["Table"].casefold(), row["Champ"].casefold()
        assert key not in seen
        seen.add(key)
        assert row["model"] == "apsim"
        evidence = json.loads(row["commentaire"])
        assert evidence["category"]
        assert evidence["sources"]
        assert all(source["definition"] for source in evidence["sources"])
        if row["defaultvalueYN"] == "1":
            assert manifest["revision"] in row["SourceDV"]
        else:
            assert row["defaultValueOtherSource"] == ""
        assert row["Default_Value_Datamill"] == ""
        assert row["ACMEinputField"] == ""
        assert row["status"] in {"input", "output"}
        assert (row["Table"] == "outputs") == (row["status"] == "output")


def test_verified_soil_definitions_and_explicit_defaults(catalogue):
    _, _, rows, _ = catalogue
    lookup = {(r["Table"], r["Champ"]): r for r in rows}
    for name in ("ParticleSizeSand", "ParticleSizeSilt", "ParticleSizeClay", "Rocks"):
        assert lookup["physical", name]["unit"] == "%"
        assert lookup["physical", name]["Type"] == "double[]"
    assert lookup["physical", "SAT"]["unit"] == "mm/mm"
    assert lookup["physical", "Texture"]["Type"] == "string[]"
    assert lookup["outputs", "organic.Nitrogen"]["status"] == "output"
    assert lookup["organic", "SoilCNRatio"]["unit"] == "g/g"
    assert lookup["organic", "FOMCNRatio"]["defaultvalueYN"] == "0"
    assert lookup["organic", "FOMCNRatio"]["defaultValueOtherSource"] == ""
    for name, value in (("SummerU", "6"), ("WinterU", "6"),
                        ("SummerCona", "3.5"), ("WinterCona", "2.5")):
        assert lookup["soilwater", name]["defaultValueOtherSource"] == value


def test_functional_categories_exclude_technical_and_crop_paths(catalogue):
    _, _, rows, _ = catalogue
    forbidden = {"models.", "resource:", "tsdml", "ttyped", "maize", "wheat", "sorghum"}
    for row in rows:
        assert row["Table"].islower()
        public_name = (row["Table"] + "." + row["Champ"]).casefold()
        assert not any(term in public_name for term in forbidden)
        assert not row["Champ"].endswith(("Metadata", "()"))
        assert "FixedValue" not in row["Champ"]
        if "VariableName" in row["Champ"]:
            assert row["Table"] == "autofertiliser"  # Genuine threshold-rule input.
            assert all(source.get("management_rule") for source in
                       json.loads(row["commentaire"])["sources"])
    lookup = {(r["Table"], r["Champ"]): r for r in rows}
    lai = lookup["outputs", "leaf.LAI"]
    assert lai["Type"] == "double"
    assert len(json.loads(lai["commentaire"])["sources"]) >= 2
    carbon = lookup["leaf", "CarbonConcentration"]
    assert carbon["status"] == "input"
    assert carbon["defaultvalueYN"] == "0"  # No crop default promoted to generic default.


def test_incompatible_homonyms_are_not_merged(catalogue):
    _, _, rows, manifest = catalogue
    lookup = {(r["Table"], r["Champ"]): r for r in rows}
    assert ("outputs", "organ.N") not in lookup
    assert lookup["outputs", "organ.Organ.N"]["unit"] == "g"
    assert lookup["outputs", "organ.GenericOrgan.N"]["unit"] == "g/m2"
    assert manifest["metadata_conflicts"] == []
    assert any(r["Table"] == "root" and r["Champ"] == "KNO3"
               for r in manifest["disambiguated_homonyms"])


def test_management_parameters_cover_automatic_rules_and_residues(catalogue):
    _, _, rows, manifest = catalogue
    lookup = {(r['Table'], r['Champ']): r for r in rows}
    expected = {
        'autosowing': {'StartDate', 'EndDate', 'MinESW', 'MinRain', 'RainDays', 'MustSow'},
        'autoirrigation': {'AutoIrrigationOn', 'FASWThreshold', 'FASWDepth',
                           'weeks', 'afterSowing', 'seasonsAllocation', 'maximumAmount'},
        'tillage': {'TillageDate', 'Fraction', 'Depth', 'f_incorp', 'tillage_depth', 'cn_red', 'cn_rain'},
        'residuetype': {'fraction_C', 'fr_c', 'fr_n', 'fr_p', 'pot_decomp_rate'},
        'organicfertiliser': {'DryMatterContent', 'ConcTotalN', 'PercentageNH4N', 'PercentageNO3N'},
        'autofertiliser': {'CritNThreshold', 'CritNDepth', 'Threshold', 'TriggerValue'},
    }
    for category, names in expected.items():
        for name in names:
            assert lookup[category, name]['status'] == 'input'
    assert lookup['autosowing', 'MinRain']['unit'] == 'mm'
    assert lookup['autoirrigation', 'FASWDepth']['unit'] == 'mm'
    assert lookup['tillage', 'Depth']['unit'] == 'mm'
    assert len(manifest['management_rules']) >= 20


def test_toolbox_settings_are_not_generic_defaults_and_methods_are_excluded(catalogue):
    _, _, rows, _ = catalogue
    lookup = {(r['Table'], r['Champ']): r for r in rows}
    rain = lookup['autosowing', 'MinRain']
    assert rain['defaultValueOtherSource'] == ''
    assert rain['defaultvalueYN'] == '0'
    source = json.loads(rain['commentaire'])['sources'][0]
    assert source['management_rule'] == 'Sow using a variable rule'
    assert source['toolbox_example'] == '25'
    assert source['definition'].endswith('/ApsimNG/Resources/Toolboxes/ManagementToolbox.apsimx')
    for name in ['autoirrigation.FASW', 'autoirrigation.WaterDeficit', 'autoirrigation.TodayDeficit']:
        assert lookup['outputs', name]['status'] == 'output'
    assert lookup['outputs', 'autoirrigation.TodayDeficit']['unit'] == 'mm'
    for row in rows:
        assert row['Champ'] not in {'Sow', 'Harvest', 'Apply', 'EndCrop', 'Tillage'}
        assert '(' not in row['Champ']


def test_reviewed_runtime_states_are_outputs_despite_public_setters(catalogue):
    _, _, rows, _ = catalogue
    lookup = {(r['Table'], r['Champ']): r for r in rows}
    assert ('structure', 'FinalLeafNo') not in lookup
    assert ('volatilisation', 'pH') not in lookup
    for name in ['structure.FinalLeafNo', 'volatilisation.pH']:
        row = lookup['outputs', name]
        assert row['status'] == 'output'
        assert row['defaultvalueYN'] == '0'
        assert row['defaultValueOtherSource'] == ''
        sources = json.loads(row['commentaire'])['sources']
        reviews = [s['classification_review'] for s in sources if 'classification_review' in s]
        assert reviews and all(r['status'] == 'output' and r['evidence'] for r in reviews)
    # The measured initial soil pH remains an independent input.
    assert lookup['chemical', 'PH']['status'] == 'input'
