import sqlite3
import tempfile
import unittest
from pathlib import Path
import pandas as pd

from modfilegen.output_configuration import OutputConfiguration, OutputConfigurationError
from modfilegen.Converter.SticsConverter.summary_output import (
    build_rap_mod,
    stics_report_fields,
    transform_summary_dataframe,
    write_canonical_summary_csv,
)


class SticsRapConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.configuration = OutputConfiguration.from_files()

    def test_minimal_selection_controls_report_fields(self):
        self.assertEqual(
            stics_report_fields(self.configuration, "minimal"),
            ("iplts", "imats", "masec(n)", "mafruit"),
        )

    def test_legacy_excludes_invalid_and_unavailable_mappings(self):
        fields = stics_report_fields(self.configuration, "legacy")
        self.assertNotIn("QNapp", fields)
        self.assertIn("azomes", fields)
        self.assertIn("ammomes", fields)
        self.assertNotIn("SMN", fields)
        self.assertNotIn("msrac(n)", fields)
        self.assertNotIn("FWAH", fields)
        self.assertIn("QNgrain", fields)
        self.assertIn("N_mineralisation", fields)
        self.assertNotIn("Qmin", fields)
        self.assertIn("Qfix", fields)
        self.assertNotIn("Qfixtot", fields)
        self.assertIn("totir", fields)
        self.assertEqual(len(fields), len({field.lower() for field in fields}))

    def test_template_control_lines_are_preserved(self):
        report = build_rap_mod(
            self.configuration,
            "minimal",
            "9\n8\n7\n6\nrec\nold_field\n",
        )
        self.assertEqual(
            report.splitlines(),
            ["9", "8", "7", "6", "rec", "iplts", "imats", "masec(n)", "mafruit"],
        )

    def test_invalid_template_is_rejected(self):
        with self.assertRaises(OutputConfigurationError):
            build_rap_mod(self.configuration, "minimal", "1\n2\n")

    def test_v9_report_is_transformed_and_stored_dynamically(self):
        raw_values = {}
        for variable in self.configuration.selected_variables(
            "legacy", "stics", include_unavailable=False
        ):
            mapping = variable.mapping
            if str(mapping.get("status", "")).lower() != "invalid":
                for field in self.configuration.source_fields(variable.key, "stics"):
                    raw_values[field] = 1.0
        raw_values.update({
            "masec(n)": 5.449,
            "mafruit": 1.444,
            "N_mineralisation": 42.5,
            "Qfix": 3.25,
            "Qem_N2O": 0.75,
            "totir": 18.0,
            "azomes": 12.5,
            "ammomes": 2.75,
            "Model": "Stics",
            "Idsim": "simulation-1",
            "Texte": "",
            "time": 2000,
        })
        raw = pd.DataFrame([raw_values])

        transformed = transform_summary_dataframe(
            raw, self.configuration, "legacy"
        )

        self.assertEqual(
            list(transformed.columns),
            list(self.configuration.summary_columns("legacy")),
        )
        self.assertNotIn("masec(n)", transformed.columns)
        self.assertNotIn("ces", transformed.columns)
        self.assertNotIn("index", transformed.columns)
        self.assertAlmostEqual(transformed.loc[0, "Biom_ma"], 5449.0)
        self.assertAlmostEqual(transformed.loc[0, "Yield"], 1.444)
        self.assertAlmostEqual(transformed.loc[0, "CumNMineralized"], 42.5)
        self.assertAlmostEqual(transformed.loc[0, "CumNFixed"], 3.25)
        self.assertAlmostEqual(transformed.loc[0, "CumN2OEmissions"], 0.75)
        self.assertAlmostEqual(
            transformed.loc[0, "PotentialIrrigationRequirement"], 18.0
        )
        self.assertAlmostEqual(transformed.loc[0, "SoilN"], 15.25)
        self.assertAlmostEqual(transformed.loc[0, "SoilMineralN"], 15.25)
        self.assertEqual(transformed.loc[0, "SeasonOrder"], 1)
        self.assertEqual(transformed.loc[0, "PlantOrder"], 1)

        connection = sqlite3.connect(":memory:")
        self.configuration.ensure_summary_output_schema(connection, "legacy")
        transformed.to_sql("SummaryOutput", connection, if_exists="append", index=False)
        stored = connection.execute(
            "SELECT CumNMineralized, CumNFixed, PotentialIrrigationRequirement "
            "FROM SummaryOutput"
        ).fetchone()
        self.assertEqual(stored, (42.5, 3.25, 18.0))


    def test_v9_soil_n_is_missing_when_one_component_is_missing(self):
        raw_values = {"Model": "Stics", "Idsim": "simulation-1"}
        for variable in self.configuration.selected_variables(
            "legacy", "stics", include_unavailable=False
        ):
            if str(variable.mapping.get("status", "")).lower() != "invalid":
                for field in self.configuration.source_fields(variable.key, "stics"):
                    raw_values[field] = 1.0
        raw_values.update({"azomes": 12.5, "ammomes": float("nan")})
        raw = pd.DataFrame([raw_values])

        transformed = transform_summary_dataframe(raw, self.configuration, "legacy")

        self.assertTrue(pd.isna(transformed.loc[0, "SoilN"]))
        self.assertTrue(pd.isna(transformed.loc[0, "SoilMineralN"]))

    def test_canonical_csv_is_written_from_raw_output(self):
        raw_values = {}
        for variable in self.configuration.selected_variables(
            "minimal", "stics", include_unavailable=False
        ):
            for field in self.configuration.source_fields(variable.key, "stics"):
                raw_values[field] = 1.0
        raw_values.update(
            {
                "Model": "Stics",
                "Idsim": "simulation-1",
                "masec(n)": 5.0,
                "mafruit": 2.0,
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            raw_path = Path(directory) / "result.csv.raw"
            result_path = Path(directory) / "result_stics.csv"
            pd.DataFrame([raw_values]).to_csv(raw_path, index=False)

            transformed = write_canonical_summary_csv(
                raw_path,
                result_path,
                self.configuration,
                "minimal",
            )
            stored = pd.read_csv(result_path)

        self.assertEqual(list(stored.columns), list(transformed.columns))
        self.assertNotIn("masec(n)", stored.columns)
        self.assertEqual(stored.loc[0, "Biom_ma"], 5000.0)
        self.assertEqual(stored.loc[0, "Yield"], 2.0)


if __name__ == "__main__":
    unittest.main()
