"""STICS v11 converter for rotations and long-term experiments.

One SimUnitList row describes the whole experiment.  Its idMangt points to
multiple CropManagement rows, ordered first by SeasonOrder and then by
PlantOrder.  PlantOrder therefore remains local to a season and can represent
mixed crops without being confused with the temporal sequence.
"""

from datetime import date, timedelta
from pathlib import Path
from time import time
import os
import shutil
import sqlite3
import subprocess
import traceback
import uuid

import pandas as pd

from modfilegen import GlobalVariables
from . import sticsclimatconverter
from . import sticsficiniconverter
from . import sticsficplt1converter
from . import sticsfictec1converter
from . import sticsnewtravailconverter
from . import sticsparamsolconverter
from . import sticsstationconverter
from . import sticstempoparconverter
from .sticsconverter import (
    common_prof,
    common_rap,
    common_tempopar,
    common_tempoparv6,
    common_var,
    collect_daily_outputs,
    collect_profile_outputs,
    create_df_summary,
    export as prepare_sqlite_indexes,
    fetch_data_from_sqlite,
    write_file,
)


STATE_FILES = ("recup.tmp",)
REQUIRED_CROP_COLUMNS = {
    "idMangt",
    "SeasonOrder",
    "PlantOrder",
    "SowingYearOffset",
    "sowingdate",
    "DHarvest",
}
REPORT_HEADER = (
    "P_usm;wlieu;ansemis;P_iwater;ancours;ifin;nbdays;P_ichsl;group;"
    "P_codeplante;stade;nomversion;masec(n);mafruit;chargefruit;iplts;"
    "ilevs;iflos;imats;irecs;laimax;QNplante;Qles;QNapp;ces;cep"
)


def julian_date(year, day):
    return date(int(year), 1, 1) + timedelta(days=int(day) - 1)


def year_day(value):
    return value.year, value.timetuple().tm_yday


def simulation_start_date(simulation):
    return julian_date(simulation["StartYear"], simulation["StartDay"])


def simulation_end_date(simulation):
    return julian_date(simulation["EndYear"], simulation["EndDay"])


def _table_columns(connection, table):
    return {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}


def fetch_rotation_seasons(connection, simulation):
    """Return validated seasons for one long-running SimUnitList row."""
    missing = REQUIRED_CROP_COLUMNS - _table_columns(connection, "CropManagement")
    if missing:
        raise ValueError(
            "CropManagement is missing successive-simulation columns: "
            + ", ".join(sorted(missing))
        )

    dataframe = pd.read_sql_query(
        """
        SELECT cm.*, lc.SpeciesName
        FROM CropManagement AS cm
        LEFT JOIN ListCultivars AS lc ON lc.IdCultivar = cm.Idcultivar
        WHERE cm.idMangt = ?
        ORDER BY cm.SeasonOrder, cm.PlantOrder
        """,
        connection,
        params=(str(simulation["idMangt"]),),
    )
    if dataframe.empty:
        raise ValueError(f"No CropManagement rows for idMangt={simulation['idMangt']}")

    for column in ("SeasonOrder", "PlantOrder", "SowingYearOffset", "sowingdate", "DHarvest"):
        dataframe[column] = pd.to_numeric(dataframe[column], errors="raise").astype(int)

    orders = sorted(dataframe["SeasonOrder"].unique().tolist())
    if orders != list(range(1, len(orders) + 1)):
        raise ValueError(f"SeasonOrder must be contiguous from 1; found {orders}")

    experiment_start = simulation_start_date(simulation)
    experiment_end = simulation_end_date(simulation)
    seasons = []
    previous_end = None

    for season_order, plants_df in dataframe.groupby("SeasonOrder", sort=True):
        plants_df = plants_df.sort_values("PlantOrder")
        plant_orders = plants_df["PlantOrder"].tolist()
        if plant_orders != list(range(1, len(plant_orders) + 1)):
            raise ValueError(
                f"PlantOrder for season {season_order} must be contiguous from 1; "
                f"found {plant_orders}"
            )

        year_offsets = plants_df["SowingYearOffset"].unique().tolist()
        if len(year_offsets) != 1:
            raise ValueError(
                f"All plants in season {season_order} must share SowingYearOffset"
            )

        sowing_year = int(simulation["StartYear"]) + int(year_offsets[0])
        plants = plants_df.to_dict(orient="records")
        for plant in plants:
            plant["SowingDate"] = julian_date(sowing_year, plant["sowingdate"])
            plant["HarvestDate"] = plant["SowingDate"] + timedelta(days=plant["DHarvest"])

        season_start = experiment_start if previous_end is None else previous_end + timedelta(days=1)
        season_end = max(plant["HarvestDate"] for plant in plants)
        first_sowing = min(plant["SowingDate"] for plant in plants)

        if first_sowing < season_start:
            raise ValueError(
                f"Season {season_order} is sown on {first_sowing}, before its period "
                f"starts on {season_start}"
            )
        if season_end > experiment_end:
            raise ValueError(
                f"Season {season_order} ends on {season_end}, after SimUnitList ends "
                f"on {experiment_end}"
            )

        seasons.append(
            {
                "SeasonOrder": int(season_order),
                "StartDate": season_start,
                "EndDate": season_end,
                "SowingYearOffset": int(year_offsets[0]),
                "IsMixedCrop": len(plants) > 1,
                "Plants": plants,
            }
        )
        previous_end = season_end

    return seasons


def build_season_row(simulation, season):
    row = dict(simulation)
    row["SeasonOrder"] = season["SeasonOrder"]
    row["is_mixed_crop"] = int(season["IsMixedCrop"])
    row["StartYear"], row["StartDay"] = year_day(season["StartDate"])
    row["EndYear"], row["EndDay"] = year_day(season["EndDate"])
    return row


def set_usm_parameter(usm_file, parameter, value):
    path = Path(usm_file)
    lines = path.read_text().splitlines()
    marker = f":{parameter}"
    for index, line in enumerate(lines):
        if line.strip().lower() == marker.lower():
            if index + 1 >= len(lines):
                raise ValueError(f"Missing value after {marker} in {path}")
            lines[index + 1] = str(value)
            path.write_text("\n".join(lines) + "\n")
            return
    raise ValueError(f"{marker} not found in {path}")


def is_leap_year(year):
    year = int(year)
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def stics_datefin(start_year, end_year, end_day):
    total = int(end_day)
    for year in range(int(start_year), int(end_year)):
        total += 366 if is_leap_year(year) else 365
    return total


def adapt_usm_calendar(usmdir, row):
    usm_file = Path(usmdir) / "new_travail.usm"
    number_of_years = int(row["EndYear"]) - int(row["StartYear"]) + 1
    set_usm_parameter(usm_file, "datedebut", int(row["StartDay"]))
    set_usm_parameter(
        usm_file,
        "datefin",
        stics_datefin(row["StartYear"], row["EndYear"], row["EndDay"]),
    )
    set_usm_parameter(usm_file, "fclim1", f"cli{row['idPoint']}j.{row['StartYear']}")
    second_climate_year = (
        int(row["StartYear"])
        if int(row["StartYear"]) == int(row["EndYear"])
        else int(row["StartYear"]) + 1
    )
    set_usm_parameter(usm_file, "fclim2", f"cli{row['idPoint']}j.{second_climate_year}")
    set_usm_parameter(usm_file, "nbans", number_of_years)
    set_usm_parameter(usm_file, "culturean", number_of_years)


def normalize_successive_recup(usmdir):
    recup_path = Path(usmdir) / "recup.tmp"
    if not recup_path.exists():
        raise FileNotFoundError(f"Missing recup.tmp in {usmdir}")
    lines = recup_path.read_text().splitlines()
    if not lines or not lines[0].strip():
        raise ValueError(f"Empty recup.tmp in {usmdir}")
    first_line = lines[0].split()
    if len(first_line) == 4:
        usm_lines = (Path(usmdir) / "new_travail.usm").read_text().splitlines()
        marker_index = next(
            i for i, value in enumerate(usm_lines) if value.strip().lower() == ":nbplantes"
        )
        first_line.append(str(int(float(usm_lines[marker_index + 1]))))
        lines[0] = " ".join(first_line)
        recup_path.write_text("\n".join(lines) + "\n")


def copy_successive_state(previous_usmdir, current_usmdir):
    normalize_successive_recup(previous_usmdir)
    missing = [name for name in STATE_FILES if not (Path(previous_usmdir) / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing successive state files: {missing}")
    for name in STATE_FILES:
        shutil.copy2(Path(previous_usmdir) / name, Path(current_usmdir) / name)


def load_static_stics_files(package):
    parameters = Path(package) / "data" / "stics_params"
    if not parameters.exists():
        return common_rap(), common_var(), common_prof()
    return tuple((parameters / name).read_text() for name in ("rap.mod", "var.mod", "prof.mod"))


def create_context(mi, md, directory_path, temp_dir, pltfolder, package):
    rap, var, prof = load_static_stics_files(package)
    return {
        "directory_path": directory_path,
        "temp_dir": temp_dir,
        "pltfolder": pltfolder,
        "rap": rap,
        "var": var,
        "prof": prof,
        "tempopar": common_tempopar(md),
        "tempoparv6": common_tempoparv6(md),
        "master": sqlite3.connect(mi),
        "dictionary": sqlite3.connect(md),
    }


def generate_season_inputs(simulation, season, context):
    row = build_season_row(simulation, season)
    season_key = f"{row['idsim']}__season_{season['SeasonOrder']:03d}"
    usmdir = Path(context["temp_dir"]) / season_key
    usmdir.mkdir(parents=True, exist_ok=True)
    sim_path = os.path.join(
        context["directory_path"], str(row["idsim"]), str(row["idPoint"]), str(row["StartYear"])
    )
    season_order = season["SeasonOrder"]

    write_file(str(usmdir), "tempoparv6.sti", context["tempoparv6"])
    sticstempoparconverter.SticsTempoparConverter().export(
        sim_path, context["master"], context["tempopar"], str(usmdir)
    )
    sticsparamsolconverter.SticsParamSolConverter().export(
        sim_path, context["dictionary"], context["master"], str(usmdir)
    )
    sticsstationconverter.SticsStationConverter().export(
        sim_path, context["dictionary"], context["master"], context["rap"],
        context["var"], context["prof"], str(usmdir), season_order=season_order,
    )
    sticsnewtravailconverter.SticsNewTravailConverter().export(
        sim_path, context["dictionary"], context["master"], str(usmdir),
        season_order=season_order,
    )
    sticsficiniconverter.SticsFicIniConverter().export(
        sim_path, context["dictionary"], context["master"], str(usmdir),
        season_order=season_order,
    )
    sticsclimatconverter.SticsClimatConverter().export(
        sim_path,
        context["dictionary"],
        context["master"],
        str(usmdir),
        start_year=row["StartYear"],
        end_year=row["EndYear"],
    )
    sticsfictec1converter.SticsFictec1Converter().export(
        sim_path, context["dictionary"], context["master"], str(usmdir),
        season_order=season_order,
        date_offset=(
            date(season["Plants"][0]["SowingDate"].year, 1, 1)
            - date(row["StartYear"], 1, 1)
        ).days,
    )
    sticsficplt1converter.SticsFicplt1Converter().export(
        sim_path, context["master"], context["pltfolder"], str(usmdir),
        season_order=season_order,
    )
    adapt_usm_calendar(str(usmdir), row)
    return row, str(usmdir), season_key


def run_stics(usmdir, output_dir, dailyoutput=0):
    # Reusing a seasonal directory must not append results from a previous run.
    for report_name in ("mod_rapport.sti", "mod_rapportA.sti", "mod_rapportP.sti"):
        report = Path(usmdir) / report_name
        if report.exists():
            report.unlink()
    if int(dailyoutput) == 1:
        for daily_file in Path(usmdir).glob("mod_s*.sti"):
            daily_file.unlink()
        for profile_file in Path(usmdir).glob("mod_profil*.sti"):
            profile_file.unlink()
    script = Path(__file__).with_name("sticsrun.sh")
    return subprocess.run(
        ["bash", str(script), usmdir, output_dir, "0", str(int(dailyoutput))],
        capture_output=True,
        check=True,
        text=True,
        timeout=180,
    )


def collect_reports(simulation, season, season_key, directory_path, dt):
    if season["IsMixedCrop"]:
        reports = [("A", f"mod_rapportA_{season_key}.sti"), ("P", f"mod_rapportP_{season_key}.sti")]
    else:
        reports = [("", f"mod_rapport_{season_key}.sti")]

    dataframes = []
    for plant_role, filename in reports:
        report = Path(directory_path) / filename
        if not report.exists():
            print(f"Warning: {report} does not exist", flush=True)
            continue
        lines = report.read_text().splitlines()
        if lines and "ansemis" not in lines[0]:
            report.write_text(REPORT_HEADER + "\n" + "\n".join(lines) + "\n")
        dataframe = create_df_summary(str(report), dt, str(simulation["idsim"]), plant_role)
        dataframe.insert(3, "SeasonOrder", season["SeasonOrder"])
        dataframes.append(dataframe)
        report.unlink()
    return dataframes


def process_simulation(
    simulation, mi, md, directory_path, temp_dir, pltfolder, package, dt,
    dailyoutput=0,
):
    context = create_context(mi, md, directory_path, temp_dir, pltfolder, package)
    usm_dirs = []
    dataframes = []
    daily_dataframes = []
    profile_dataframes = []
    previous_usmdir = None
    try:
        seasons = fetch_rotation_seasons(context["master"], simulation)
        for season_index, season in enumerate(seasons):
            print(
                f"Successive iteration {season_index}/{len(seasons) - 1}: ",
                flush=True,
            )
            _, usmdir, season_key = generate_season_inputs(simulation, season, context)
            usm_dirs.append(usmdir)
            usm_file = Path(usmdir) / "new_travail.usm"
            if previous_usmdir is None:
                set_usm_parameter(usm_file, "codesuite", 0)
            else:
                copy_successive_state(previous_usmdir, usmdir)
                set_usm_parameter(usm_file, "codesuite", 1)

            run_stics(usmdir, directory_path, dailyoutput)
            for state_file in STATE_FILES:
                if not (Path(usmdir) / state_file).exists():
                    raise FileNotFoundError(
                        f"STICS did not create {state_file} for season {season['SeasonOrder']}"
                    )
            normalize_successive_recup(usmdir)
            dataframes.extend(
                collect_reports(simulation, season, season_key, directory_path, dt)
            )
            if int(dailyoutput) == 1:
                daily_dataframes.extend(
                    collect_daily_outputs(
                        directory_path,
                        season_key,
                        simulation["idsim"],
                        season["SeasonOrder"],
                        season["IsMixedCrop"],
                    )
                )
                profile_dataframes.extend(
                    collect_profile_outputs(
                        directory_path,
                        season_key,
                        simulation["idsim"],
                        season["SeasonOrder"],
                        season["IsMixedCrop"],
                    )
                )
            previous_usmdir = usmdir
    finally:
        context["master"].close()
        context["dictionary"].close()
        if dt == 1:
            for usmdir in usm_dirs:
                shutil.rmtree(usmdir, ignore_errors=True)

    summary = pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()
    daily = (
        pd.concat(daily_dataframes, ignore_index=True)
        if daily_dataframes else pd.DataFrame()
    )
    profile = (
        pd.concat(profile_dataframes, ignore_index=True)
        if profile_dataframes else pd.DataFrame()
    )
    return summary, daily, profile


def main(simulations=None):
    from joblib import Parallel, delayed

    mi = GlobalVariables.get("dbMasterInput")
    md = GlobalVariables.get("dbModelsDictionary")
    directory_path = GlobalVariables.get("directorypath", os.getcwd())
    pltfolder = GlobalVariables.get("pltfolder")
    temp_dir = GlobalVariables.get("tempDir") or os.path.join(directory_path, "temp")
    package = GlobalVariables.get("package") or str(Path(__file__).resolve().parents[3])
    nthreads = max(1, int(GlobalVariables.get("nthreads", 1)))
    dt = int(GlobalVariables.get("dt", 0))
    dailyoutput = int(GlobalVariables.get("dailyoutput", 0))

    if not mi or not md:
        raise ValueError("dbMasterInput and dbModelsDictionary must be configured")
    if not pltfolder:
        raise ValueError("pltfolder must be configured")
    Path(directory_path).mkdir(parents=True, exist_ok=True)
    Path(temp_dir).mkdir(parents=True, exist_ok=True)

    started = time()
    prepare_sqlite_indexes(mi, md)
    if simulations is None:
        simulations = fetch_data_from_sqlite(mi)
    if not simulations:
        print("No simulation to process.", flush=True)
        return None

    results = Parallel(n_jobs=nthreads, backend="loky")(
        delayed(process_simulation)(
            simulation, mi, md, directory_path, temp_dir, pltfolder, package, dt,
            dailyoutput,
        )
        for simulation in simulations
    )
    frames = [
        result[0] for result in results
        if result is not None and result[0] is not None and not result[0].empty
    ]
    if not frames:
        print("No STICS reports produced.", flush=True)
        return None

    result_path = Path(directory_path) / f"{uuid.uuid4()}_stics_successive.csv"
    pd.concat(frames, ignore_index=True).to_csv(result_path, index=False)
    print(f"Results saved to {result_path}", flush=True)
    print(f"STICS successive total time: {time() - started:.2f}s", flush=True)
    return str(result_path)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise
