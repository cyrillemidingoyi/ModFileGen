import shutil
import sqlite3
from pathlib import Path

import pytest

from modfilegen.Converter.CelsiusV32Converter.core import convert_database


SOURCE = Path(__file__).parent / "stics_successive" / "MasterInput.db"


TARGET_SCHEMA = """
CREATE TABLE Dweather (
  idDclim TEXT, idjourclim TEXT, annee INTEGER, mois INTEGER, jour INTEGER,
  jda INTEGER, tmin REAL, tmax REAL, tmoy REAL, rg REAL, etp REAL, plu REAL
);
CREATE TABLE ListPAnnexes (
  CodePosteAnnexe TEXT, altitude INTEGER, latitudeDD REAL, longitude REAL,
  idDclim TEXT, CO2c INTEGER, ConcNplu REAL, Codcc TEXT
);
CREATE TABLE ParamIni (
  IdIni TEXT, Stockinit REAL, Qpaillisinit REAL, iniSolhautON INTEGER, Ninit REAL
);
CREATE TABLE Tech_Commun (
  IdTech_Com TEXT, NbCult INTEGER, NomSC TEXT, SerreTunnelON INTEGER,
  imulch INTEGER, CodParamMulch INTEGER, QpaillisApport REAL, AltiCult INTEGER,
  IrrigON INTEGER, fertiminON INTEGER, fertiorgON INTEGER, tApportMON REAL,
  tApportMinN REAL, CsurN_AO REAL, KresY REAL, DriveRuiObs INTEGER
);
CREATE TABLE Tech_perCrop (
  idTechPerCrop TEXT, idTech_Com TEXT, NumCrop INTEGER, IdCultivar TEXT,
  TypInstal INTEGER, RepiquageON INTEGER, isem INTEGER, ilev INTEGER,
  DensSem REAL, irepiqu REAL, DensRepiqu REAL, DbutoirNouvSemis INTEGER,
  SemisAutoDebut INTEGER, SeuilCumPrecip REAL
);
CREATE TABLE SimUnitList (
  idsim TEXT, idTech_Com TEXT, idweather TEXT, idsoil TEXT, idIni TEXT,
  idGenParam TEXT, idCodModel INTEGER, StartYear INTEGER, StartDay INTEGER,
  EndYear INTEGER, EndDay INTEGER, codCC TEXT, ChampTri REAL, Situation TEXT,
  codesuite INTEGER
);
CREATE TABLE Soil (
  idsoil TEXT, NbCouches INTEGER, Zmes INTEGER, ZObstacleRac REAL, StockN REAL,
  TypeRui INTEGER, Clay REAL, pHeau REAL
);
CREATE TABLE Soil_layers (
  idsoil TEXT, NumCouche INTEGER, epc REAL, hcc REAL, hmin REAL, da REAL
);
CREATE TABLE Irrigation_List (
  IdTech_Com TEXT, IdIrrig TEXT, DateIrrig TEXT, YearIrrig INTEGER,
  JourYrIrrig INTEGER, Irrigation REAL
);
CREATE TABLE FertiMin_List (
  IdTech_Com TEXT, IdFertiMin TEXT, DateFertiMin TEXT, YearFertiMin INTEGER,
  JourYrFertiMin TEXT, Nmineral REAL, Pmineral REAL, Kmineral REAL
);
CREATE TABLE FertiOrga_List (
  IdTech_Com TEXT, IdFertiOrg TEXT, DateFertiOrg TEXT, YearFertiOrg INTEGER,
  JourYrFertiOrg TEXT, Norganique REAL, Porganique REAL, Korganique REAL,
  QorgaTot REAL, TypeMOrga TEXT, CsurNRes REAL
);
CREATE TABLE General_Parameters (idGenParam TEXT);
CREATE TABLE OptionsModel (
  idCodModel INTEGER, ActiveWstress INTEGER, ActiveNstress INTEGER,
  ActivePstress INTEGER, ActiveKstress INTEGER
);
INSERT INTO General_Parameters VALUES ('1');
"""


def make_target(path):
    with sqlite3.connect(path) as connection:
        connection.executescript(TARGET_SCHEMA)


def rows(path, query, params=()):
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        return [dict(row) for row in connection.execute(query, params)]


def test_successive_expands_seasons_and_preserves_associated_crops(tmp_path):
    target = tmp_path / "celsius-v32.db"
    make_target(target)

    convert_database(SOURCE, target, mode="successive")

    simulations = rows(
        target, "SELECT * FROM SimUnitList ORDER BY ChampTri"
    )
    assert len(simulations) == 6
    rotation = [
        row for row in simulations
        if row["Situation"].endswith("ROT_MAIZE_PEANUT_3Y_2")
    ]
    assert [row["codesuite"] for row in rotation] == [0, 1, 1]
    assert [row["ChampTri"] for row in rotation] == [2.0, 3.0, 4.0]
    assert rotation[1]["StartYear"] == 2000
    assert rotation[1]["StartDay"] == 351
    assert rotation[1]["EndYear"] == 2001

    first_standard = simulations[0]
    crops = rows(
        target,
        "SELECT NumCrop, IdCultivar FROM Tech_perCrop "
        "WHERE idTech_Com=? ORDER BY NumCrop",
        (first_standard["idTech_Com"],),
    )
    assert crops == [
        {"NumCrop": 1, "IdCultivar": "testcult"},
        {"NumCrop": 2, "IdCultivar": "testcult2"},
    ]
    tech = rows(
        target,
        "SELECT NbCult FROM Tech_Commun WHERE IdTech_Com=?",
        (first_standard["idTech_Com"],),
    )
    assert tech[0]["NbCult"] == 2


def test_successive_converts_weather_soils_options_and_operations(tmp_path):
    target = tmp_path / "celsius-v32.db"
    make_target(target)

    convert_database(SOURCE, target, mode="successive")

    assert rows(target, "SELECT COUNT(*) AS n FROM Dweather")[0]["n"] == 1096
    assert rows(target, "SELECT COUNT(*) AS n FROM Soil_layers")[0]["n"] == 2
    assert rows(target, "SELECT NbCouches FROM Soil")[0]["NbCouches"] == 2
    assert rows(target, "SELECT COUNT(*) AS n FROM Irrigation_List")[0]["n"] == 3
    assert rows(target, "SELECT COUNT(*) AS n FROM OptionsModel")[0]["n"] == 1


def test_standard_rejects_a_multi_season_management(tmp_path):
    target = tmp_path / "celsius-v32.db"
    make_target(target)

    with pytest.raises(ValueError, match="use celsius_mode='successive'"):
        convert_database(SOURCE, target, mode="standard")


def test_standard_keeps_one_simulation_and_two_associated_crops(tmp_path):
    master = tmp_path / "MasterInput.db"
    shutil.copy2(SOURCE, master)
    with sqlite3.connect(master) as connection:
        connection.execute(
            "DELETE FROM SimUnitList WHERE idMangt='ROT_MAIZE_PEANUT_3Y'"
        )
        connection.commit()
    target = tmp_path / "celsius-v32.db"
    make_target(target)

    convert_database(master, target, mode="standard")

    simulations = rows(target, "SELECT * FROM SimUnitList ORDER BY ChampTri")
    assert len(simulations) == 3
    assert {row["codesuite"] for row in simulations} == {0}
    assert rows(target, "SELECT DISTINCT NbCult FROM Tech_Commun") == [
        {"NbCult": 2}
    ]


CELSIUS_FIXTURES = Path(__file__).parent / "celsius"


@pytest.mark.parametrize(
    ("dailyoutput", "expected"),
    [(None, 0), (0, 0), (1, 1), ("1", 1)],
)
def test_run_enables_daily_output_only_when_dailyoutput_is_one(
    tmp_path, monkeypatch, dailyoutput, expected
):
    from modfilegen import GlobalVariables
    from modfilegen.Converter.CelsiusV32Converter.runner import run

    master = tmp_path / "MasterInput.db"
    shutil.copy2(CELSIUS_FIXTURES / "MasterInput.db", master)
    output = tmp_path / "celsius_v32.db"
    with sqlite3.connect(master) as connection:
        idsim = connection.execute(
            "SELECT idsim FROM SimUnitList ORDER BY idsim LIMIT 1"
        ).fetchone()[0]

    settings = {
        "dbMasterInput": str(master),
        "dbCelsiusV32Template": str(CELSIUS_FIXTURES / "celsius_model_input.db"),
        "celsiusV32Output": str(output),
        "celsiusIdsim": idsim,
        "runCelsiusV32": 0,
        "dt": 0,
    }
    for name, value in settings.items():
        monkeypatch.setitem(GlobalVariables, name, value)
    if dailyoutput is None:
        monkeypatch.delitem(GlobalVariables, "dailyoutput", raising=False)
    else:
        monkeypatch.setitem(GlobalVariables, "dailyoutput", dailyoutput)

    run("standard")

    assert rows(output, "SELECT DISTINCT EcritDResus FROM OptionsModel") == [
        {"EcritDResus": expected}
    ]


FAKE_ENGINE = """#!{python}
import sqlite3, sys
db = sys.argv[1]
with sqlite3.connect(db) as c:
    c.execute("CREATE TABLE IF NOT EXISTS OutputSynt (Idsim TEXT, LAI REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS OutputD_1 (IdSimJ TEXT, idSim TEXT, Jul INTEGER)")
    c.execute("DELETE FROM OutputSynt")
    c.execute("DELETE FROM OutputD_1")
    rows = c.execute(
        "SELECT idsim, codesuite, idTech_Com FROM SimUnitList ORDER BY ChampTri"
    ).fetchall()
    if rows and int(rows[0][1] or 0) != 0:
        sys.exit(3)  # a chain of successive simulations was split
    techs = {{row[0] for row in c.execute("SELECT IdTech_Com FROM Tech_Commun")}}
    if techs != {{row[2] for row in rows}}:
        sys.exit(4)  # management rows of other workers were not removed
    for idsim, _, _ in rows:
        c.execute("INSERT INTO OutputSynt (Idsim, LAI) VALUES (?, ?)", (idsim, len(idsim)))
        for day in (1, 2):
            c.execute("INSERT INTO OutputD_1 (IdSimJ, idSim, Jul) VALUES (?, ?, ?)", (f"{{idsim}} {{day}}", idsim, day))
"""


def make_fake_engine(tmp_path, body=FAKE_ENGINE):
    import stat
    import sys

    engine = tmp_path / "fake_celsius"
    engine.write_text(body.format(python=sys.executable), encoding="utf-8")
    engine.chmod(engine.stat().st_mode | stat.S_IEXEC)
    return str(engine)


def make_standard_database(tmp_path, count=7):
    database = tmp_path / "standard.db"
    shutil.copy2(CELSIUS_FIXTURES / "celsius_model_input.db", database)
    with sqlite3.connect(CELSIUS_FIXTURES / "MasterInput.db") as connection:
        ids = [
            row[0]
            for row in connection.execute(
                "SELECT idsim FROM SimUnitList ORDER BY idsim LIMIT ?", (count,)
            )
        ]
    convert_database(CELSIUS_FIXTURES / "MasterInput.db", database, simulation_ids=ids)
    return database


def outputs(path):
    return (
        rows(path, "SELECT Idsim, LAI FROM OutputSynt ORDER BY rowid"),
        rows(path, "SELECT IdSimJ, idSim, Jul FROM OutputD_1 ORDER BY rowid"),
    )


def test_partition_keeps_chains_whole_and_balances_parts():
    from modfilegen.Converter.CelsiusV32Converter.runner import _partition_chains

    chains = [[1, 2, 3], [4], [5, 6], [7], [8, 9, 10]]
    parts = _partition_chains(chains, 3)
    assert [row for part in parts for row in part] == list(range(1, 11))
    assert len(parts) == 3
    for chain in chains:
        assert sum(set(chain) <= set(part) for part in parts) == 1
    assert _partition_chains(chains, 20) == [[1, 2, 3], [4], [5, 6], [7], [8, 9, 10]]
    assert _partition_chains(chains, 1) == [list(range(1, 11))]


def test_conversion_indexes_per_simulation_lookups(tmp_path):
    database = make_standard_database(tmp_path, count=1)
    indexes = {
        row["tbl_name"]
        for row in rows(
            database,
            "SELECT tbl_name FROM sqlite_master "
            "WHERE type='index' AND name LIKE 'ix_modfilegen_%'",
        )
    }
    assert {"Dweather", "Tech_perCrop", "Soil_layers", "StadePheno"} <= indexes


def test_parallel_run_matches_a_single_process(tmp_path):
    from modfilegen.Converter.CelsiusV32Converter.runner import run_model

    engine = make_fake_engine(tmp_path)
    single = make_standard_database(tmp_path)
    parallel = tmp_path / "parallel.db"
    shutil.copy2(single, parallel)

    run_model(single, engine, workers=1)
    run_model(parallel, engine, workers=3)

    assert outputs(parallel) == outputs(single)
    assert len(outputs(parallel)[0]) == 7
    assert not list(tmp_path.glob("parallel_workers_*"))


def test_parallel_run_never_splits_successive_chains(tmp_path):
    from modfilegen.Converter.CelsiusV32Converter.runner import run_model

    target = tmp_path / "successive.db"
    make_target(target)
    convert_database(SOURCE, target, mode="successive")
    engine = make_fake_engine(tmp_path)

    run_model(target, engine, workers=4)

    expected = [row["idsim"] for row in rows(target, "SELECT idsim FROM SimUnitList ORDER BY ChampTri")]
    assert [row["Idsim"] for row in outputs(target)[0]] == expected


def test_parallel_run_failure_keeps_worker_files(tmp_path):
    from modfilegen.Converter.CelsiusV32Converter.runner import run_model

    engine = make_fake_engine(
        tmp_path, "#!{python}\nimport sys\nprint('engine exploded')\nsys.exit(2)\n"
    )
    database = make_standard_database(tmp_path, count=4)

    with pytest.raises(RuntimeError, match="engine exploded"):
        run_model(database, engine, workers=2)
    work_dirs = list(tmp_path.glob("standard_workers_*"))
    assert len(work_dirs) == 1
    assert len(list(work_dirs[0].glob("part_*.log"))) == 2
