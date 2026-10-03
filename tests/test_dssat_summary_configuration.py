import unittest

import pandas as pd

from modfilegen.output_configuration import OutputConfiguration
from modfilegen.Converter.DssatConverter.summary_output import transform_summary_dataframe


class DssatSummaryConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.configuration = OutputConfiguration.from_files()

    def test_summary_out_is_mapped_and_converted_dynamically(self):
        raw = pd.DataFrame([{
            "Model": "Dssat", "Idsim": "sim-1", "PDAT": 2000120,
            "EDAT": 2000125, "ADAT": 2001200, "MDAT": 2001250,
            "HDAT": 2001255, "CWAM": 6500, "HWAM": 2400, "FHWAM": 3100,
            "H#AM": 500, "GNAM": 32, "LAIX": 4.2, "NLCM": 1.5,
            "NIAM": 18, "CNAM": 75, "ESCP": 90, "EPCP": 210,
            "LWAH": 800, "NFXM": 0, "N2OEC": 0.4,
        }])
        result = transform_summary_dataframe(raw, self.configuration, "legacy")
        self.assertEqual(
            list(result.columns),
            list(self.configuration.summary_columns("legacy")),
        )
        self.assertNotIn("PDAT", result.columns)
        self.assertNotIn("CWAM", result.columns)
        self.assertNotIn("FHWAM", result.columns)
        self.assertEqual(result.loc[0, "Planting"], 120)
        self.assertEqual(result.loc[0, "Emergence"], 125)
        self.assertEqual(result.loc[0, "Ant"], 565)
        self.assertAlmostEqual(result.loc[0, "Biom_ma"], 6500)
        self.assertAlmostEqual(result.loc[0, "Yield"], 2.4)
        self.assertAlmostEqual(result.loc[0, "FreshYield"], 3.1)
        self.assertAlmostEqual(result.loc[0, "GrainN_ma"], 32)
        self.assertTrue(pd.isna(result.loc[0, "LeafBiomass"]))
        self.assertAlmostEqual(result.loc[0, "CumN2OEmissions"], 0.4)
        self.assertTrue(pd.isna(result.loc[0, "RootBiomass"]))
        self.assertTrue(pd.isna(result.loc[0, "PotentialIrrigationRequirement"]))

    def test_fresh_yield_zero_is_preserved(self):
        required = {
            "Model": "Dssat", "Idsim": "sim-0", "PDAT": 2000120,
            "EDAT": 2000125, "ADAT": 2000200, "MDAT": 2000250,
            "CWAM": 6500, "HWAM": 2400, "H#AM": 500, "LAIX": 4.2,
            "NLCM": 1.5, "NIAM": 18, "CNAM": 75, "ESCP": 90,
            "EPCP": 210, "FHWAM": 0, "NFXM": 0,
        }
        result = transform_summary_dataframe(
            pd.DataFrame([required]), self.configuration, "legacy"
        )
        self.assertEqual(result.loc[0, "FreshYield"], 0)
        self.assertEqual(result.loc[0, "CumNFixed"], 0)

    def test_negative_dssat_sentinels_become_missing(self):
        raw = pd.DataFrame([{
            "Model": "Dssat", "Idsim": "sim-2", "PDAT": 2000120,
            "EDAT": -99, "ADAT": -99, "MDAT": -99, "CWAM": -99,
            "HWAM": -99, "H#AM": -99, "LAIX": -99, "NLCM": -99,
            "NIAM": -99, "CNAM": -99, "ESCP": -99, "EPCP": -99,
        }])
        result = transform_summary_dataframe(raw, self.configuration, "legacy")
        self.assertTrue(pd.isna(result.loc[0, "Emergence"]))
        self.assertTrue(pd.isna(result.loc[0, "Yield"]))


if __name__ == "__main__":
    unittest.main()
