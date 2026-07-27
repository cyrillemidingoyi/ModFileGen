import sqlite3
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from modfilegen.Converter.SticsV11Converter.sticsconverter import (
    build_balanced_simulation_chunks,
    fetch_data_from_sqlite,
    get_simulation_weights,
    partition_simulations,
)
from modfilegen.Converter.SticsV11Converter.sticssuccessiveconverter import (
    adapt_usm_calendar,
    build_season_row,
    fetch_rotation_seasons,
)


DATA_DIR = Path(__file__).parent
IDSIM = "5.925_6.025_2000_ROT_MAIZE_PEANUT_3Y_2"


def load_example():
    connection = sqlite3.connect(DATA_DIR / "MasterInput.db")
    connection.row_factory = sqlite3.Row
    simulation = dict(
        connection.execute("SELECT * FROM SimUnitList WHERE idsim = ?", (IDSIM,)).fetchone()
    )
    return connection, simulation


class TestSticsSuccessiveConverter(unittest.TestCase):
    def test_all_simulations_are_balanced_by_number_of_seasons(self):
        database = DATA_DIR / "MasterInput.db"
        simulations = fetch_data_from_sqlite(database)
        weights = get_simulation_weights(database, simulations)

        chunks = build_balanced_simulation_chunks(simulations, weights, 2)
        loads = [sum(weights[str(row["idsim"])] for row in chunk) for chunk in chunks]

        self.assertEqual(len(simulations), 4)
        self.assertEqual(loads, [3, 3])
        self.assertEqual(
            [[weights[str(row["idsim"])] for row in chunk] for chunk in chunks],
            [[3], [1, 1, 1]],
        )

    def test_climate_files_follow_season_start_and_end_years(self):
        template = """:datedebut
0
:datefin
0
:fclim1
unset
:fclim2
unset
:nbans
0
:culturean
0
"""
        cases = [
            (
                {"StartYear": 2000, "StartDay": 120, "EndYear": 2000,
                 "EndDay": 350, "idPoint": "5.925_6.025"},
                "cli5.925_6.025j.2000",
                "cli5.925_6.025j.2000",
                "1",
            ),
            (
                {"StartYear": 2000, "StartDay": 351, "EndYear": 2001,
                 "EndDay": 350, "idPoint": "5.925_6.025"},
                "cli5.925_6.025j.2000",
                "cli5.925_6.025j.2001",
                "2",
            ),
        ]
        for row, expected_fclim1, expected_fclim2, expected_nbans in cases:
            with self.subTest(row=row), tempfile.TemporaryDirectory() as directory:
                usm = Path(directory) / "new_travail.usm"
                usm.write_text(template)
                adapt_usm_calendar(directory, row)
                values = usm.read_text().splitlines()
                parameters = {
                    values[index][1:]: values[index + 1]
                    for index in range(0, len(values), 2)
                }
                self.assertEqual(parameters["fclim1"], expected_fclim1)
                self.assertEqual(parameters["fclim2"], expected_fclim2)
                self.assertEqual(parameters["nbans"], expected_nbans)

    def test_single_entry_point_partitions_standard_and_successive_managements(self):
        database = DATA_DIR / "MasterInput.db"
        simulations = fetch_data_from_sqlite(database)

        standard, successive = partition_simulations(database, simulations)

        self.assertEqual(len(standard), 3)
        self.assertTrue(all(row["idMangt"] == "Mgt1M0_135" for row in standard))
        self.assertEqual([row["idsim"] for row in successive], [IDSIM])

    def test_three_year_rotation_is_expanded_from_one_simulation(self):
        connection, simulation = load_example()
        try:
            seasons = fetch_rotation_seasons(connection, simulation)
        finally:
            connection.close()

        self.assertEqual([season["SeasonOrder"] for season in seasons], [1, 2, 3])
        self.assertEqual(
            [season["Plants"][0]["Idcultivar"] for season in seasons],
            ["testcult", "testcult2", "testcult"],
        )
        self.assertEqual(
            [season["Plants"][0]["SowingDate"].year for season in seasons],
            [2000, 2001, 2002],
        )

    def test_next_season_starts_the_day_after_previous_harvest(self):
        connection, simulation = load_example()
        try:
            seasons = fetch_rotation_seasons(connection, simulation)
        finally:
            connection.close()

        self.assertEqual(
            seasons[1]["StartDate"], seasons[0]["EndDate"] + timedelta(days=1)
        )
        self.assertEqual(
            seasons[2]["StartDate"], seasons[1]["EndDate"] + timedelta(days=1)
        )

        rows = [build_season_row(simulation, season) for season in seasons]
        self.assertEqual(
            [(row["StartYear"], row["EndYear"]) for row in rows],
            [(2000, 2000), (2000, 2001), (2001, 2002)],
        )


if __name__ == "__main__":
    unittest.main()
