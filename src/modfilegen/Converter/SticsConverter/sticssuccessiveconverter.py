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


SUCCESSION_COLUMNS = ["idPoint", "idMangt", "idOption"]
STATE_FILES = ("recup.tmp", "snow_variables.txt")


def set_usm_parameter(usm_file, parameter, value):
    path = Path(usm_file)
    lines = path.read_text().splitlines()
    marker = f":{parameter}"
    for index, line in enumerate(lines):
        if line.strip().lower() == marker.lower():
            if index + 1 >= len(lines):
                raise ValueError(f"Missing value line after {marker} in {path}")
            lines[index + 1] = str(value)
            path.write_text("\n".join(lines) + "\n")
            return
    raise ValueError(f"{marker} not found in {path}")


def get_usm_parameter(usm_file, parameter):
    path = Path(usm_file)
    lines = path.read_text().splitlines()
    marker = f":{parameter}"
    for index, line in enumerate(lines):
        if line.strip().lower() == marker.lower():
            if index + 1 >= len(lines):
                raise ValueError(f"Missing value line after {marker} in {path}")
            return lines[index + 1].strip()
    raise ValueError(f"{marker} not found in {path}")


def load_static_stics_files(package):
    stics_params = Path(package) / "data" / "stics_params"
    if not stics_params.exists():
        return common_rap(), common_var(), common_prof()

    return (
        (stics_params / "rap.mod").read_text(),
        (stics_params / "var.mod").read_text(),
        (stics_params / "prof.mod").read_text(),
    )


def simulation_path(directory_path, row):
    return os.path.join(
        directory_path,
        str(row["idsim"]),
        str(row["idPoint"]),
        str(row["StartYear"]),
    )


def build_successive_groups(rows):
    dataframe = pd.DataFrame(rows)
    required_columns = {"idsim", "StartYear", *SUCCESSION_COLUMNS}
    missing = sorted(required_columns.difference(dataframe.columns))
    if missing:
        raise ValueError(f"Missing columns in SimUnitList: {missing}")

    groups = []
    for _, group in dataframe.groupby(SUCCESSION_COLUMNS, dropna=False, sort=False):
        ordered = group.sort_values(["StartYear", "idsim"])
        groups.append(ordered.to_dict(orient="records"))
    return groups


def julian_date(year, day):
    return date(int(year), 1, 1) + timedelta(days=int(day) - 1)


def year_day(value):
    return value.year, value.timetuple().tm_yday


def date_to_period(row, start_date=None, end_date=None):
    current = dict(row)
    if start_date is not None:
        current["StartYear"], current["StartDay"] = year_day(start_date)
    if end_date is not None:
        current["EndYear"], current["EndDay"] = year_day(end_date)
    return current


def row_start_date(row):
    return julian_date(row["StartYear"], row["StartDay"])


def row_end_date(row):
    return julian_date(row["EndYear"], row["EndDay"])


def adapt_group_calendar(group):
    """Preview the effective calendar used by the successive converter."""
    adapted = []
    previous_crop_end = None
    for row in group:
        current = dict(row)
        if previous_crop_end is not None:
            current["_intercrop_start"] = previous_crop_end + timedelta(days=1)
            current["_crop_start"] = row_start_date(current)
        adapted.append(current)
        previous_crop_end = row_end_date(current)
    return adapted


def is_leap_year(year):
    year = int(year)
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def stics_datefin(start_year, end_year, end_day):
    if int(start_year) == int(end_year):
        return int(end_day)
    return int(end_day) + 365 + int(is_leap_year(start_year))


def adapt_usm_calendar(usmdir, row):
    usm_file = os.path.join(usmdir, "new_travail.usm")
    two_years = int(row["StartYear"]) != int(row["EndYear"])
    set_usm_parameter(usm_file, "datedebut", int(row["StartDay"]))
    set_usm_parameter(
        usm_file,
        "datefin",
        stics_datefin(row["StartYear"], row["EndYear"], row["EndDay"]),
    )
    set_usm_parameter(usm_file, "fclim1", f"cli{row['idPoint']}j.{int(row['StartYear'])}")
    set_usm_parameter(usm_file, "fclim2", f"cli{row['idPoint']}j.{int(row['StartYear']) + 1}")
    set_usm_parameter(usm_file, "nbans", 2 if two_years else 1)
    set_usm_parameter(usm_file, "culturean", 2 if two_years else 1)


def set_bare_soil_plant(usmdir):
    path = Path(usmdir) / "ficplt1.txt"
    lines = path.read_text().splitlines()
    for index, line in enumerate(lines):
        if line.strip().lower() == "codeplante":
            if index + 1 >= len(lines):
                raise ValueError(f"Missing codeplante value in {path}")
            lines[index + 1] = "snu"
            path.write_text("\n".join(lines) + "\n")
            return
    raise ValueError(f"codeplante not found in {path}")


def set_bare_soil_dates(usmdir, start_day):
    """Set planting date to simulation start for bare soil.
    
    STICS automatically sets iplt0 = P_iwater for 'snu' (bare soil).
    See Stics_Lectures.f90 line 160.
    """
    path = Path(usmdir) / "fictec1.txt"
    lines = path.read_text().splitlines()
    for index, line in enumerate(lines):
        if line.strip().lower() == "iplt0":
            if index + 1 >= len(lines):
                raise ValueError(f"Missing iplt0 value in {path}")
            lines[index + 1] = str(int(start_day))
            path.write_text("\n".join(lines) + "\n")
            return
    raise ValueError(f"iplt0 not found in {path}")


def normalize_successive_recup(usmdir):
    recup_path = Path(usmdir) / "recup.tmp"
    if not recup_path.exists():
        raise FileNotFoundError(f"Missing recup.tmp in {usmdir}")

    lines = recup_path.read_text().splitlines()
    if not lines or not lines[0].strip():
        raise ValueError(f"Empty recup.tmp in {usmdir}")

    first_line = lines[0].split()
    if len(first_line) == 4:
        nbplantes = get_usm_parameter(Path(usmdir) / "new_travail.usm", "nbplantes")
        first_line.append(str(int(float(nbplantes))))
        lines[0] = " ".join(first_line)
        recup_path.write_text("\n".join(lines) + "\n")
    elif len(first_line) < 4:
        raise ValueError(f"Invalid recup.tmp first line in {usmdir}: {lines[0]}")


def create_bare_soil_usm(row, start_date, end_date, source_usmdir, context):
    fallow_id = str(row["idsim"])
    usmdir = os.path.join(context["temp_dir"], fallow_id)
    Path(usmdir).mkdir(parents=True, exist_ok=True)

    for filename in (
        "tempoparv6.sti",
        "tempopar.sti",
        "param.sol",
        "station.txt",
        "snow_variables.txt",
        "prof.mod",
        "rap.mod",
        "var.mod",
        "ficini.txt",
        "fictec1.txt",
        "ficplt1.txt",
        "new_travail.usm",
    ):
        shutil.copy2(os.path.join(source_usmdir, filename), os.path.join(usmdir, filename))

    set_bare_soil_plant(usmdir)
    period = date_to_period(row, start_date, end_date)
    set_bare_soil_dates(usmdir, period["StartDay"])
    adapt_usm_calendar(usmdir, period)
    # An intercrop is one STICS run, even when it crosses a calendar year.
    # If nbans > 1, STICS forces a full one-year sequence from datedebut.
    set_usm_parameter(os.path.join(usmdir, "new_travail.usm"), "nbans", 
                      2 if int(period["StartYear"]) != int(period["EndYear"]) else 1)
    set_usm_parameter(
        os.path.join(usmdir, "new_travail.usm"),
        "culturean",
        2 if int(period["StartYear"]) != int(period["EndYear"]) else 1,
    )
    set_usm_parameter(os.path.join(usmdir, "new_travail.usm"), "codesuite", 1)
    set_usm_parameter(os.path.join(usmdir, "new_travail.usm"), "nom", "bare_soil")
    set_usm_parameter(os.path.join(usmdir, "new_travail.usm"), "flai1", "null")

    climate_converter = sticsclimatconverter.SticsClimatConverter()
    climate_converter.export(
        simulation_path(context["directory_path"], period),
        context["model_dictionary_connection"],
        context["master_input_connection"],
        usmdir,
    )
    return usmdir


def generate_usm_inputs(row, context, caches):
    usmdir = os.path.join(context["temp_dir"], str(row["idsim"]))
    sim_path = simulation_path(context["directory_path"], row)
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

    tec_id = ".".join([str(row["idMangt"]), str(row["idsoil"])])
    if tec_id not in caches["tec"]:
        tec_converter = sticsfictec1converter.SticsFictec1Converter()
        caches["tec"][tec_id] = tec_converter.export(
            sim_path,
            context["model_dictionary_connection"],
            context["master_input_connection"],
            usmdir,
        )
    else:
        write_file(usmdir, "fictec1.txt", caches["tec"][tec_id])

    plant_converter = sticsficplt1converter.SticsFicplt1Converter()
    plant_converter.export(
        sim_path,
        context["master_input_connection"],
        context["pltfolder"],
        usmdir,
    )
    return usmdir


def copy_successive_state(previous_usmdir, current_usmdir):
    normalize_successive_recup(previous_usmdir)
    missing = [
        name
        for name in STATE_FILES
        if not os.path.exists(os.path.join(previous_usmdir, name))
    ]
    if missing:
        raise FileNotFoundError(
            f"Missing successive state file(s) in {previous_usmdir}: {missing}"
        )

    for name in STATE_FILES:
        shutil.copy2(os.path.join(previous_usmdir, name), os.path.join(current_usmdir, name))


def run_stics(usmdir, output_dir):
    script = Path(__file__).with_name("sticsrun.sh")
    return subprocess.run(
        ["bash", str(script), usmdir, output_dir, "0"],
        capture_output=True,
        check=True,
        text=True,
        timeout=180,
    )


def create_context(mi, md, directory_path, temp_dir, pltfolder, rap, var, prof, tempopar, tempoparv6):
    return {
        "directory_path": directory_path,
        "temp_dir": temp_dir,
        "pltfolder": pltfolder,
        "rap": rap,
        "var": var,
        "prof": prof,
        "tempopar": tempopar,
        "tempoparv6": tempoparv6,
        "master_input_connection": sqlite3.connect(mi),
        "model_dictionary_connection": sqlite3.connect(md),
    }


def close_context(context):
    context["master_input_connection"].close()
    context["model_dictionary_connection"].close()


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


def run_intercrop(row, crop_usmdir, context, directory_path):
    usmdir = create_bare_soil_usm(
        row,
        row["_intercrop_start"],
        row["_intercrop_end"],
        crop_usmdir,
        context,
    )
    copy_successive_state(crop_usmdir, usmdir)
    run_stics(usmdir, directory_path)

    for name in STATE_FILES:
        state_file = os.path.join(usmdir, name)
        if not os.path.exists(state_file):
            raise FileNotFoundError(f"STICS did not create {name} for {row['idsim']}")
    normalize_successive_recup(usmdir)
    return usmdir


def process_successive_group(group, mi, md, directory_path, temp_dir, pltfolder, rap, var, prof, tempopar, tempoparv6, dt):
    group_key = tuple(str(group[0][column]) for column in SUCCESSION_COLUMNS)
    print(f"Processing successive group {group_key} with {len(group)} simulation(s)", flush=True)

    context = create_context(
        mi,
        md,
        directory_path,
        temp_dir,
        pltfolder,
        rap,
        var,
        prof,
        tempopar,
        tempoparv6,
    )
    caches = {"weather": {}, "soil": {}, "tempopar": {}, "tec": {}, "ini": {}}
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
                if intercrop_start <= intercrop_end:
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
        print(f"Error during successive group {group_key}", flush=True)
        traceback.print_exc()
        raise
    except Exception:
        print(f"Error during successive group {group_key}", flush=True)
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
    print(f"Successive groups: {len(groups)}", flush=True)
    print(f"Parallel workers: {nthreads}", flush=True)

    result_name = f"{uuid.uuid4()}_stics_successive"
    result_path = os.path.join(directory_path, f"{result_name}.csv")
    write_header = True
    groups_written = 0

    try:
        with parallel_backend("loky", n_jobs=nthreads):
            group_results = Parallel()(
                delayed(process_successive_group)(
                    group,
                    mi,
                    md,
                    directory_path,
                    temp_dir,
                    pltfolder,
                    rap,
                    var,
                    prof,
                    tempopar,
                    tempoparv6,
                    dt,
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
        print(f"STICS successive total time: {time() - start:.2f}s", flush=True)
        return result_path

    except Exception:
        print("Error during successive STICS processing:", flush=True)
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
