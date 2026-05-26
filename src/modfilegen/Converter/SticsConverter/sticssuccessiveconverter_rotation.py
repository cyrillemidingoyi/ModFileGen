"""STICS successive converter for crop rotations.

Compared to sticssuccessiveconverter.py:
- SimUnitList has a SeasonOrder column (one row per crop per year, not one per year).
- CropManagement has a SeasonOrder column — each (idMangt, SeasonOrder) pair defines
  one crop in the rotation sequence.
- Bare-soil periods are explicit CropManagement rows (Idcultivar='bare_soil') rather
  than automatically injected intercrop simulations.
- Automatic intercrop bare-soil is still inserted for gaps between the last season of
  year N and the first season of year N+1 when no explicit row covers that period.
- Group ordering is by (StartYear, SeasonOrder) so seasons within the same year run
  before moving to the next year.

NOTE: The underlying converters (sticsfictec1converter, sticsficplt1converter) must
join CropManagement with the SeasonOrder filter:
    CropManagement.idMangt = SimUnitList.idMangt
    AND CropManagement.SeasonOrder = SimUnitList.SeasonOrder
Until that change is made those converters may return data from the wrong season.
"""

from pathlib import Path
from time import time
from datetime import date, timedelta
import os
import shutil
import sqlite3
import subprocess
import traceback
import uuid

import pandas as pd
from joblib import Parallel, delayed, parallel_backend

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
    create_df_summary,
    export as prepare_sqlite_indexes,
    fetch_data_from_sqlite,
    write_file,
)
from .sticssuccessiveconverter import (
    STATE_FILES,
    adapt_usm_calendar,
    close_context,
    copy_successive_state,
    create_bare_soil_usm,
    create_context,
    date_to_period,
    is_leap_year,
    julian_date,
    load_static_stics_files,
    normalize_successive_recup,
    row_end_date,
    row_start_date,
    run_intercrop,
    run_stics,
    set_bare_soil_dates,
    set_bare_soil_plant,
    set_usm_parameter,
    simulation_path,
    stics_datefin,
    year_day,
)


SUCCESSION_COLUMNS = ["idPoint", "idMangt", "idOption"]


# ---------------------------------------------------------------------------
# Grouping — sort within each group by (StartYear, SeasonOrder)
# ---------------------------------------------------------------------------

def build_successive_groups(rows):
    dataframe = pd.DataFrame(rows)
    required_columns = {"idsim", "StartYear", "SeasonOrder", *SUCCESSION_COLUMNS}
    missing = sorted(required_columns.difference(dataframe.columns))
    if missing:
        raise ValueError(f"Missing columns in SimUnitList: {missing}")

    dataframe["SeasonOrder"] = dataframe["SeasonOrder"].fillna(1).astype(int)

    groups = []
    for _, group in dataframe.groupby(SUCCESSION_COLUMNS, dropna=False, sort=False):
        ordered = group.sort_values(["StartYear", "SeasonOrder", "idsim"])
        groups.append(ordered.to_dict(orient="records"))
    return groups


# ---------------------------------------------------------------------------
# Bare-soil detection — explicit seasons in CropManagement
# ---------------------------------------------------------------------------

def fetch_season_cultivar(conn, id_mangt, season_order):
    cur = conn.cursor()
    cur.execute(
        "SELECT Idcultivar FROM CropManagement WHERE idMangt=? AND SeasonOrder=?",
        (str(id_mangt), int(season_order)),
    )
    result = cur.fetchone()
    return str(result[0]).strip().lower() if result else None


def is_bare_soil_row(row, conn):
    cultivar = fetch_season_cultivar(
        conn, row["idMangt"], row.get("SeasonOrder", 1)
    )
    return cultivar == "bare_soil"


# ---------------------------------------------------------------------------
# USM generation — SeasonOrder-aware caching and bare-soil handling
# ---------------------------------------------------------------------------

def generate_usm_inputs(row, context, caches):
    usmdir = os.path.join(context["temp_dir"], str(row["idsim"]))
    sim_path = simulation_path(context["directory_path"], row)
    season_order = int(row.get("SeasonOrder", 1))
    Path(usmdir).mkdir(parents=True, exist_ok=True)

    write_file(usmdir, "tempoparv6.sti", context["tempoparv6"])

    tempopar_id = row["idOption"]
    if tempopar_id not in caches["tempopar"]:
        converter = sticstempoparconverter.SticsTempoparConverter()
        caches["tempopar"][tempopar_id] = converter.export(
            sim_path,
            context["master_input_connection"],
            context["tempopar"],
            usmdir,
        )
    else:
        write_file(usmdir, "tempopar.sti", caches["tempopar"][tempopar_id])

    soil_id = row["idsoil"]
    if soil_id not in caches["soil"]:
        paramsol_converter = sticsparamsolconverter.SticsParamSolConverter()
        param_sol = paramsol_converter.export(
            sim_path,
            context["model_dictionary_connection"],
            context["master_input_connection"],
            usmdir,
        )
        station_converter = sticsstationconverter.SticsStationConverter()
        station = station_converter.export(
            sim_path,
            context["model_dictionary_connection"],
            context["master_input_connection"],
            context["rap"],
            context["var"],
            context["prof"],
            usmdir,
        )
        caches["soil"][soil_id] = [param_sol, station]
    else:
        write_file(usmdir, "param.sol", caches["soil"][soil_id][0])
        write_file(usmdir, "station.txt", caches["soil"][soil_id][1][0])
        write_file(usmdir, "snow_variables.txt", caches["soil"][soil_id][1][1])
        write_file(usmdir, "prof.mod", context["prof"])
        write_file(usmdir, "rap.mod", context["rap"])
        write_file(usmdir, "var.mod", context["var"])

    new_travail_converter = sticsnewtravailconverter.SticsNewTravailConverter()
    new_travail_converter.export(
        sim_path,
        context["model_dictionary_connection"],
        context["master_input_connection"],
        usmdir,
        season_order=season_order,
    )
    adapt_usm_calendar(usmdir, row)

    ini_id = ".".join([str(row["idsoil"]), str(row["idIni"])])
    if ini_id not in caches["ini"]:
        ini_converter = sticsficiniconverter.SticsFicIniConverter()
        caches["ini"][ini_id] = ini_converter.export(
            sim_path,
            context["model_dictionary_connection"],
            context["master_input_connection"],
            usmdir,
        )
    else:
        write_file(usmdir, "ficini.txt", caches["ini"][ini_id])

    climate_id = ".".join([str(row["idPoint"]), str(row["StartYear"])])
    if climate_id not in caches["weather"]:
        climate_converter = sticsclimatconverter.SticsClimatConverter()
        caches["weather"][climate_id] = climate_converter.export(
            sim_path,
            context["model_dictionary_connection"],
            context["master_input_connection"],
            usmdir,
        )
    else:
        write_file(usmdir, "climat.txt", caches["weather"][climate_id])

    # Cache key includes SeasonOrder: season 1 and season 2 of the same idMangt
    # have different technical files and plant files.
    tec_id = ".".join([str(row["idMangt"]), str(season_order), str(row["idsoil"])])
    if tec_id not in caches["tec"]:
        tec_converter = sticsfictec1converter.SticsFictec1Converter()
        caches["tec"][tec_id] = tec_converter.export(
            sim_path,
            context["model_dictionary_connection"],
            context["master_input_connection"],
            usmdir,
            season_order=season_order,
        )
    else:
        write_file(usmdir, "fictec1.txt", caches["tec"][tec_id])

    plant_id = ".".join([str(row["idMangt"]), str(season_order)])
    if plant_id not in caches["plant"]:
        plant_converter = sticsficplt1converter.SticsFicplt1Converter()
        caches["plant"][plant_id] = plant_converter.export(
            sim_path,
            context["master_input_connection"],
            context["pltfolder"],
            usmdir,
            season_order=season_order,
        )
    else:
        write_file(usmdir, "ficplt1.txt", caches["plant"][plant_id])

    # Apply bare-soil override when this season is explicitly modelled as bare soil.
    if is_bare_soil_row(row, context["master_input_connection"]):
        set_bare_soil_plant(usmdir)
        set_bare_soil_dates(usmdir, row["StartDay"])

    return usmdir


# ---------------------------------------------------------------------------
# Crop execution — same logic as the original, uses local generate_usm_inputs
# ---------------------------------------------------------------------------

def run_crop(row, previous_usmdir, context, caches, directory_path):
    idsim = str(row["idsim"])
    usmdir = generate_usm_inputs(row, context, caches)
    usm_file = os.path.join(usmdir, "new_travail.usm")

    if previous_usmdir is None:
        set_usm_parameter(usm_file, "codesuite", 0)
    else:
        copy_successive_state(previous_usmdir, usmdir)
        set_usm_parameter(usm_file, "codesuite", 1)

    try:
        run_stics(usmdir, directory_path)
    finally:
        if previous_usmdir is not None:
            set_usm_parameter(usm_file, "codesuite", 0)

    configured_end = row_end_date(row)

    for name in STATE_FILES:
        state_file = os.path.join(usmdir, name)
        if not os.path.exists(state_file):
            raise FileNotFoundError(f"STICS did not create {name} for {idsim}")

    normalize_successive_recup(usmdir)
    return usmdir, configured_end


# ---------------------------------------------------------------------------
# Group processing
# ---------------------------------------------------------------------------

def process_successive_group(group, mi, md, directory_path, temp_dir, pltfolder, rap, var, prof, tempopar, tempoparv6, dt):
    group_key = tuple(str(group[0][column]) for column in SUCCESSION_COLUMNS)
    print(f"Processing rotation group {group_key} with {len(group)} simulation(s)", flush=True)

    context = create_context(
        mi, md, directory_path, temp_dir, pltfolder, rap, var, prof, tempopar, tempoparv6,
    )
    # plant cache added for rotation (season-specific plant files)
    caches = {"weather": {}, "soil": {}, "tempopar": {}, "tec": {}, "ini": {}, "plant": {}}
    dataframes = []
    usm_dirs = []
    previous_usmdir = None

    try:
        for index, row in enumerate(group):
            idsim = str(row["idsim"])
            usmdir, crop_end_date = run_crop(
                row,
                previous_usmdir,
                context,
                caches,
                directory_path,
            )
            usm_dirs.append(usmdir)

            report = os.path.join(directory_path, f"mod_rapport_{idsim}.sti")
            if not os.path.exists(report):
                print(f"Warning: {report} does not exist", flush=True)
            else:
                dataframes.append(create_df_summary(report))
                if dt == 1:
                    os.remove(report)

            previous_usmdir = usmdir

            if index < len(group) - 1:
                next_row = group[index + 1]
                next_start_date = row_start_date(next_row)
                intercrop_start = crop_end_date + timedelta(days=1)
                intercrop_end = next_start_date - timedelta(days=1)

                # Insert an automatic bare-soil intercrop only for gaps between
                # the last explicit season of year N and the first season of year N+1.
                # Gaps between seasons within the same year are already covered by
                # explicit CropManagement rows with Idcultivar='bare_soil'.
                current_season = int(row.get("SeasonOrder", 1))
                next_season = int(next_row.get("SeasonOrder", 1))
                is_year_boundary = next_season <= current_season  # season resets to 1

                if is_year_boundary and intercrop_start <= intercrop_end:
                    intercrop_row = date_to_period(
                        next_row,
                        start_date=intercrop_start,
                        end_date=intercrop_end,
                    )
                    intercrop_row["idsim"] = f"{idsim}__intercrop__{next_row['idsim']}"
                    intercrop_row["_intercrop_start"] = intercrop_start
                    intercrop_row["_intercrop_end"] = intercrop_end
                    intercrop_usmdir = run_intercrop(
                        intercrop_row,
                        usmdir,
                        context,
                        directory_path,
                    )
                    usm_dirs.append(intercrop_usmdir)
                    previous_usmdir = intercrop_usmdir

    except subprocess.CalledProcessError as error:
        print(f"STICS failed with return code {error.returncode}", flush=True)
        print("STDOUT:\n", error.stdout, flush=True)
        print("STDERR:\n", error.stderr, flush=True)
        print(f"Error during rotation group {group_key}", flush=True)
        traceback.print_exc()
        raise
    except Exception:
        print(f"Error during rotation group {group_key}", flush=True)
        traceback.print_exc()
        raise
    finally:
        close_context(context)
        if dt == 1:
            for usmdir in usm_dirs:
                shutil.rmtree(usmdir, ignore_errors=True)

    if not dataframes:
        return pd.DataFrame()
    return pd.concat(dataframes, ignore_index=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    mi = GlobalVariables.get("dbMasterInput")
    md = GlobalVariables.get("dbModelsDictionary")
    directory_path = GlobalVariables.get("directorypath", os.getcwd())
    pltfolder = GlobalVariables.get("pltfolder")
    nthreads = max(1, int(GlobalVariables.get("nthreads", 4)))
    dt = int(GlobalVariables.get("dt", 0))
    temp_dir = GlobalVariables.get("tempDir") or os.path.join(directory_path, "temp")
    package = GlobalVariables.get("package") or str(Path(__file__).resolve().parents[3])

    if not mi or not md:
        raise ValueError("dbMasterInput and dbModelsDictionary must be set in GlobalVariables")
    if not pltfolder:
        raise ValueError("pltfolder must be set in GlobalVariables")

    os.makedirs(directory_path, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)

    start = time()
    prepare_sqlite_indexes(mi, md)
    rows = fetch_data_from_sqlite(mi)
    groups = build_successive_groups(rows)
    rap, var, prof = load_static_stics_files(package)
    tempopar = common_tempopar(md)
    tempoparv6 = common_tempoparv6(md)

    print(f"Total simulations to process: {len(rows)}", flush=True)
    print(f"Rotation groups: {len(groups)}", flush=True)
    print(f"Parallel workers: {nthreads}", flush=True)

    result_name = f"{uuid.uuid4()}_stics_rotation"
    result_path = os.path.join(directory_path, f"{result_name}.csv")
    write_header = True
    groups_written = 0

    try:
        with parallel_backend("loky", n_jobs=nthreads):
            group_results = Parallel()(
                delayed(process_successive_group)(
                    group, mi, md, directory_path, temp_dir, pltfolder,
                    rap, var, prof, tempopar, tempoparv6, dt,
                )
                for group in groups
            )

        for dataframe in group_results:
            if not dataframe.empty:
                dataframe.to_csv(result_path, mode="a", header=write_header, index=False)
                write_header = False
                groups_written += 1
            del dataframe

        if groups_written == 0:
            print("No data to process.", flush=True)
            return None

        print(f"Results saved to {result_path}", flush=True)
        print(f"STICS rotation total time: {time() - start:.2f}s", flush=True)
        return result_path

    except Exception:
        print("Error during rotation STICS processing:", flush=True)
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
