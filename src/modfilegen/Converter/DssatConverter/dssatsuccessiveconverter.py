
"""DSSAT successive simulation converter.

This converter uses DSSAT's native sequence mode (RNMODE = Q).  One
SimUnitList row describes the simulation unit and the CropManagement rows
linked by idMangt describe its ordered seasons.  The seasons are converted
into one SQX experiment file and a versioned DSSBatch file.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from time import time
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import traceback
import uuid

import pandas as pd
from joblib import Parallel, delayed, parallel_backend

from modfilegen import GlobalVariables
from . import dssatcultivarconverter, dssatsoilconverter, dssatxconverter
from . import dssatweatherconverter_v2 as dssatweatherconverter
from .dssatconverter import export as prepare_sqlite_indexes
from .dssatconverter import (
    DSSAT_DAILY_FILES,
    DSSAT_DAILY_OUTPUT_TABLE,
    align_sqlite_table,
    fetch_data_from_sqlite,
    transform,
)


SEQ_FILE_NAME = "ITSA1301.SQX"
SUMMARY_PREFIX = "Summary_"


def batch_filename_for_version(dssat_version):
    batch_filenames = {
        "v47": "DSSBatch.v47",
        "v48": "DSSBatch.v48",
    }
    try:
        return batch_filenames[dssat_version]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported DSSAT version {dssat_version!r}; expected v47 or v48"
        ) from exc


@dataclass
class RotationInput:
    row: dict
    index: int
    crop: str
    sections: dict[str, list[str]]
    management: dict | None = None


def julian_date(year, day):
    return date(int(year), 1, 1) + timedelta(days=int(day) - 1)


def year_day(value):
    return value.year, value.timetuple().tm_yday


def date_to_yydoy(value):
    return f"{value.year % 100:02d}{value.timetuple().tm_yday:03d}"


def group_start_yydoy(group):
    first_start = row_start_date(group[0])
    return date_to_yydoy(first_start)


def row_start_date(row):
    return julian_date(row["StartYear"], row["StartDay"])


def row_end_date(row):
    return julian_date(row["EndYear"], row["EndDay"])


def add_years(value, years):
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        return value.replace(month=2, day=28, year=value.year + years)


def dssat_sequence_years(group):
    """Return the minimal NYERS that covers the whole successive sequence.

    DSSAT Q stops after the interval that starts on the first sequence SDATE
    and spans NYERS calendar years.  We therefore choose the smallest NYERS
    whose end boundary still includes the last configured simulation end date.
    """
    first_start = row_start_date(group[0])
    last_end = max(row_end_date(row) for row in group)
    years = 1
    while add_years(first_start, years) - timedelta(days=1) <= last_end:
        years += 1
    return years


def dssat_sequence_end_date(group, nyers=None):
    """Return the inclusive end of DSSAT's NYERS interval."""
    first_start = row_start_date(group[0])
    if nyers is None:
        nyers = dssat_sequence_years(group)
    return add_years(first_start, nyers) - timedelta(days=1)


def sequence_weather_years(group):
    first_year = row_start_date(group[0]).year
    # DSSAT reads weather beyond the last completed Q-sequence cycle while
    # deciding whether another cycle starts.  For a non-January SDATE this
    # crosses into the calendar year after the NYERS boundary year.
    first_start = row_start_date(group[0])
    boundary = add_years(first_start, dssat_sequence_years(group))
    last_year = boundary.year + (boundary.timetuple().tm_yday > 1)
    return list(range(first_year, last_year + 1))


def build_successive_groups(rows):
    """Return one processing group per SimUnitList row.

    Successive seasons now live in CropManagement, not in repeated
    SimUnitList rows.  Keeping groups as one-item lists limits churn in the
    processing pipeline while making the ownership explicit.
    """
    required_columns = {
        "idsim", "StartYear", "StartDay", "EndYear", "EndDay", "idMangt",
        "idPoint", "idsoil", "idIni", "idOption",
    }
    dataframe = pd.DataFrame(rows)
    missing = sorted(required_columns.difference(dataframe.columns))
    if missing:
        raise ValueError(f"Missing columns in SimUnitList: {missing}")
    return [[row] for row in dataframe.to_dict(orient="records")]


def validate_successive_group(group):
    if len(group) != 1:
        raise ValueError("A DSSAT successive group must contain exactly one SimUnitList row")


def group_key(group):
    return str(group[0]["idsim"])


def safe_group_id(group):
    value = group_key(group)
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)[:180]


def simulation_path(directory_path, row):
    return os.path.join(
        directory_path,
        str(row["idsim"]),
        str(row["idPoint"]),
        str(row["StartYear"]),
        str(row["idMangt"]),
    )


def soil_simulation_path(directory_path, row):
    return os.path.join(
        directory_path,
        str(row["idsim"]),
        str(row["idsoil"]),
        str(row["idPoint"]),
        str(row["StartYear"]),
        str(row["idMangt"]),
    )


def x_simulation_path(directory_path, row):
    return os.path.join(directory_path, str(row["idsim"]), str(row["idMangt"]))


def generated_xfile_path(usmdir):
    candidates = sorted(Path(usmdir).glob("ITSA1301.*X"))
    if not candidates:
        raise FileNotFoundError(f"No generated DSSAT X file found in {usmdir}")
    return candidates[0]


def read_generated_xfile(usmdir):
    return generated_xfile_path(usmdir).read_text()


def render_sections(sections):
    return "\n".join(
        line for section_lines in sections.values() for line in section_lines
    ).rstrip() + "\n"


def parse_sections(content):
    sections: dict[str, list[str]] = {}
    current = None
    for line in content.splitlines():
        if line.startswith("*"):
            current = line.split()[0].upper()
            sections[current] = [line]
        elif current is not None:
            sections[current].append(line)
    return sections


def replace_first_int(line, value):
    if not line.strip() or line.lstrip().startswith(("@", "*", "!", "$")):
        return line
    match = re.match(r"^(\s*)([-+]?\d+)", line)
    if not match:
        return line
    # DSSAT sections use fixed-width level fields.  The leading whitespace is
    # part of that field, so a two-digit level must consume it instead of
    # shifting every following column one character to the right.
    width = len(match.group(1)) + len(match.group(2))
    return f"{int(value):>{width}d}{line[match.end():]}"


def replace_section_level(lines, value):
    return [replace_first_int(line, value) for line in lines]


def replace_second_token(line, value):
    """Replace a DSSAT data row's second fixed-width field."""
    if not line.strip() or line.lstrip().startswith(("@", "*", "!", "$")):
        return line
    match = re.match(r"^(\s*\S+\s+)(\S+)(.*)$", line)
    if not match:
        return line
    width = len(match.group(2))
    return f"{match.group(1)}{str(value):>{width}}{match.group(3)}"


def set_section_date(sections, section, value):
    if section in sections:
        sections[section] = [
            replace_second_token(line, value) for line in sections[section]
        ]


def data_lines(lines):
    return [line for line in lines if line.strip() and not line.lstrip().startswith(("@", "*", "!", "$"))]


def section_has_data(sections, section):
    return section in sections and bool(data_lines(sections[section]))


def policy_code_enabled(value):
    """Return whether a management policy identifier selects a policy.

    Policy identifiers are commonly textual foreign keys (for example
    ``MA_IA55``), while legacy databases may use the numeric sentinel 0.
    """
    if value is None:
        return False
    text = str(value).strip()
    return bool(text) and text not in {"0", "0.0"}


def query_one(connection, sql):
    dataframe = pd.read_sql_query(sql, connection)
    if dataframe.empty:
        raise ValueError(f"Query returned no rows: {sql}")
    return dataframe.iloc[0]


def default_value(model_dictionary_connection, table, champ):
    query = (
        "Select IFNULL([defaultValueOtherSource], [Default_Value_Datamill]) As dv "
        f"From Variables Where model = 'dssat' And [Table] = '{table}' And Champ = '{champ}';"
    )
    row = query_one(model_dictionary_connection, query)
    return row["dv"]


def management_flags(id_sim, master_input_connection, management=None):
    query = """
        Select SimUnitList.idsim, SoilTillPolicy.NumTillOperations,
               OrganicFertilizationPolicy.NumOrganicFerti,
               CropManagement.IrrigationPolicyCode, CropManagement.InoFertiPolicyCode
        From OrganicFertilizationPolicy
        Inner Join (SoilTillPolicy Inner Join (CropManagement Inner Join SimUnitList
        On CropManagement.idMangt = SimUnitList.idMangt)
        On SoilTillPolicy.SoilTillPolicyCode = CropManagement.SoilTillPolicyCode)
        On OrganicFertilizationPolicy.OFertiPolicyCode = CropManagement.OFertiPolicyCode
        Where IdSim = ?
    """
    params = [id_sim]
    if management is not None:
        query += " And CropManagement.SeasonOrder = ? And CropManagement.PlantOrder = ?"
        params.extend([management["SeasonOrder"], management["PlantOrder"]])
    dataframe = pd.read_sql_query(query, master_input_connection, params=params)
    if dataframe.empty:
        raise ValueError(
            f"No management policy flags found for simulation {id_sim}, "
            f"season {management['SeasonOrder'] if management else 'unknown'}"
        )
    return dataframe.iloc[0]


def successive_managements(row, master_input_connection):
    """Load the ordered management seasons belonging to one simulation unit."""
    columns = {
        item[1]
        for item in master_input_connection.execute("PRAGMA table_info(CropManagement)")
    }
    required = {"PlantOrder", "SeasonOrder", "SowingYearOffset"}
    missing = sorted(required.difference(columns))
    if missing:
        raise ValueError(
            "CropManagement is missing DSSAT successive columns: "
            + ", ".join(missing)
        )

    dataframe = pd.read_sql_query(
        """
        SELECT *
        FROM CropManagement
        WHERE idMangt = ?
        ORDER BY SeasonOrder, PlantOrder
        """,
        master_input_connection,
        params=(row["idMangt"],),
    )
    if dataframe.empty:
        raise ValueError(f"No CropManagement rows found for {row['idMangt']!r}")

    duplicated_seasons = dataframe.groupby("SeasonOrder").size()
    duplicated_seasons = duplicated_seasons[duplicated_seasons > 1]
    if not duplicated_seasons.empty:
        seasons = ", ".join(str(value) for value in duplicated_seasons.index)
        raise ValueError(
            "DSSAT successive currently supports one PlantOrder per SeasonOrder; "
            f"management {row['idMangt']!r} has multiple plants in season(s) {seasons}"
        )
    return dataframe.to_dict(orient="records")


def season_simunit_row(
    simunit_row, management, is_last_season=False, previous_season_end=None
):
    """Create the dated row used by the legacy one-management block writers."""
    row = dict(simunit_row)
    sowing_year = int(simunit_row["StartYear"]) + int(management["SowingYearOffset"])
    sowing_day = int(management["sowingdate"])
    if not 1 <= sowing_day <= 366:
        raise ValueError(
            f"Invalid sowingdate {sowing_day} for season {management['SeasonOrder']}; "
            "use SowingYearOffset for years after the first"
        )
    planting = julian_date(sowing_year, sowing_day)
    harvest = planting + timedelta(days=max(1, int(management["DHarvest"])))
    simulation_start = row_start_date(simunit_row)
    simulation_end = row_end_date(simunit_row)
    season_start = (
        previous_season_end + timedelta(days=1)
        if previous_season_end is not None
        else simulation_start
    )
    if is_last_season and harvest > simulation_end:
        harvest = simulation_end
    if (
        season_start < simulation_start
        or season_start > simulation_end
        or planting < season_start
        or planting > simulation_end
        or harvest > simulation_end
    ):
        raise ValueError(
            f"Season {management['SeasonOrder']} ({planting} to {harvest}) is outside "
            f"simulation {simunit_row['idsim']} ({simulation_start} to {simulation_end})"
        )
    row["StartYear"], row["StartDay"] = year_day(season_start)
    row["EndYear"], row["EndDay"] = year_day(harvest)
    return row


def successive_season_rows(simunit_row, managements):
    """Build continuous season periods for one simulation unit."""
    seasons = []
    previous_season_end = None
    for index, management in enumerate(managements):
        season = season_simunit_row(
            simunit_row,
            management,
            is_last_season=index == len(managements) - 1,
            previous_season_end=previous_season_end,
        )
        seasons.append(season)
        previous_season_end = row_end_date(season)
    return seasons


def successive_group_connection(source_connection):
    """Clone MasterInput once and retain a source copy of its managements."""
    connection = sqlite3.connect(":memory:")
    source_connection.backup(connection)
    connection.execute(
        "CREATE TEMP TABLE SuccessiveCropManagement AS SELECT * FROM CropManagement"
    )
    return connection


def configure_season_connection(connection, simunit_row, management):
    """Expose one rotation to legacy writers in a reusable in-memory database."""
    connection.execute("DELETE FROM CropManagement")
    connection.execute(
        """
        INSERT INTO CropManagement
        SELECT * FROM SuccessiveCropManagement
        WHERE idMangt = ? AND SeasonOrder = ? AND PlantOrder = ?
        """,
        (
            management["idMangt"],
            management["SeasonOrder"],
            management["PlantOrder"],
        ),
    )
    connection.execute(
        """
        UPDATE SimUnitList
        SET StartYear = ?, StartDay = ?, EndYear = ?, EndDay = ?
        WHERE idsim = ?
        """,
        (
            simunit_row["StartYear"], simunit_row["StartDay"],
            simunit_row["EndYear"], simunit_row["EndDay"], simunit_row["idsim"],
        ),
    )
    connection.commit()
    return connection


def season_connection(source_connection, simunit_row, management):
    """Backward-compatible one-season connection constructor."""
    connection = successive_group_connection(source_connection)
    return configure_season_connection(connection, simunit_row, management)


def treatment_line(rotation, model_dictionary_connection, master_input_connection, sections):
    row = rotation.row
    fmt = dssatxconverter.v_fmt_treat
    flags = management_flags(row["idsim"], master_input_connection, rotation.management)
    values = {
        "N": 1,
        "R": rotation.index,
        "O": float(default_value(model_dictionary_connection, "dssat_x_treatment", "ROTOPT")),
        "C": float(default_value(model_dictionary_connection, "dssat_x_treatment", "CRPNO")),
        "TNAME": str(row["idsim"])[:25],
        "CU": rotation.index,
        "FL": 1,
        "SA": 1 if section_has_data(sections, "*SOIL") else 0,
        "IC": 1 if rotation.index == 1 else 0,
        "MP": rotation.index,
        "MI": rotation.index
        if policy_code_enabled(flags["IrrigationPolicyCode"])
        and section_has_data(sections, "*IRRIGATION")
        else 0,
        "MF": rotation.index
        if policy_code_enabled(flags["InoFertiPolicyCode"])
        and section_has_data(sections, "*FERTILIZERS")
        else 0,
        "MR": rotation.index if int(flags["NumOrganicFerti"]) != 0 and section_has_data(sections, "*RESIDUES") else 0,
        "MC": 0,
        "MT": rotation.index if int(flags["NumTillOperations"]) != 0 and section_has_data(sections, "*TILLAGE") else 0,
        "ME": 0,
        "MH": rotation.index if section_has_data(sections, "*HARVEST") else 0,
        "SM": 1,
    }
    line = ""
    line += fmt["N"].format(values["N"])
    line += fmt["R"].format(values["R"])
    line += fmt["O"].format(values["O"])
    line += fmt["C"].format(values["C"]) + " "
    line += fmt["TNAME"].format(values["TNAME"])
    for key in ("CU", "FL", "SA", "IC", "MP", "MI", "MF", "MR", "MC", "MT", "ME", "MH", "SM"):
        line += fmt[key].format(values[key])
    return line


def split_section_body(lines):
    return lines[1:] if lines else []


def merge_single_level_section(title, rotations, section, level_for_rotation=True, first_only=False):
    selected = [rotation for rotation in rotations if section in rotation.sections]
    if not selected:
        return []
    if first_only:
        selected = selected[:1]

    output = [selected[0].sections[section][0]]
    seen_headers = set()
    for rotation in selected:
        body = split_section_body(rotation.sections[section])
        level = rotation.index if level_for_rotation else 1
        for line in replace_section_level(body, level):
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("@"):
                if stripped not in seen_headers:
                    output.append(line)
                    seen_headers.add(stripped)
                continue
            output.append(line)
    return output


def replace_fixed_width(line, start, end, value):
    return f"{line[:start]}{str(value):>{end - start}}{line[end:]}"


def replace_sim_control_id(line, rotation_index, nyers, sequence_start):
    line = replace_first_int(line, rotation_index)
    if re.match(r"^\s*\d+\s+GE\b", line):
        line = replace_fixed_width(line, 15, 20, int(nyers))
        line = replace_fixed_width(line, 33, 38, sequence_start)
    return line


def merge_simulation_controls(rotations, nyers, sequence_start):
    output = ["*SIMULATION CONTROLS"]
    for rotation in rotations:
        section = rotation.sections.get("*SIMULATION")
        if not section:
            raise ValueError(f"Missing *SIMULATION CONTROLS for {rotation.row['idsim']}")
        for line in split_section_body(section):
            if not line.strip():
                continue
            output.append(replace_sim_control_id(line, rotation.index, nyers, sequence_start))
        output.append("")
    return output


def build_sequence_file(rotations, model_dictionary_connection, master_input_connection, nyers, sequence_start):
    first_sections = rotations[0].sections
    lines = [f"*EXP.DETAILS: {safe_group_id([rotations[0].row])} DSSAT SEQUENCE", ""]

    if "*GENERAL" in first_sections:
        lines.extend(first_sections["*GENERAL"])
        lines.append("")

    lines.append("*TREATMENTS                        -------------FACTOR LEVELS------------")
    lines.append("@N R O C TNAME.................... CU FL SA IC MP MI MF MR MC MT ME MH SM")
    for rotation in rotations:
        lines.append(treatment_line(rotation, model_dictionary_connection, master_input_connection, rotation.sections))
    lines.append("")

    for section in ("*CULTIVARS",):
        lines.extend(merge_single_level_section(section, rotations, section, level_for_rotation=True))
        lines.append("")

    for section in ("*FIELDS", "*SOIL", "*INITIAL"):
        if section in first_sections:
            lines.extend(merge_single_level_section(section, rotations, section, level_for_rotation=False, first_only=True))
            lines.append("")

    for section in (
        "*PLANTING",
        "*IRRIGATION",
        "*FERTILIZERS",
        "*RESIDUES",
        "*TILLAGE",
        "*HARVEST",
    ):
        merged = merge_single_level_section(section, rotations, section, level_for_rotation=True)
        if merged:
            lines.extend(merged)
            lines.append("")

    lines.extend(merge_simulation_controls(rotations[:1], nyers, sequence_start))
    return "\n".join(lines).rstrip() + "\n"


def batch_line(file_name, rotation_index):
    return f"{file_name.ljust(92)}{1:7d}{1:7d}{rotation_index:7d}{1:7d}{0:7d}"


def build_batch_file(rotations):
    lines = [
        "",
        "$BATCH(EXPERIMENT)",
        "@FILEX                                                                                        TRTNO     RP     SQ     OP     CO",
    ]
    lines.extend(batch_line(SEQ_FILE_NAME, rotation.index) for rotation in rotations)
    return "\n".join(lines) + "\n"


def remove_rotation_workdirs(sequence_dir, rotations):
    """Remove per-rotation work directories after the SQX has been assembled."""
    for rotation in rotations:
        rotation_dir = Path(sequence_dir, f"_rotation_{rotation.index}")
        shutil.rmtree(rotation_dir, ignore_errors=True)


def generate_rotation_input(
    row,
    management,
    index,
    context,
    sequence_dir,
    is_last_season=False,
    previous_season_end=None,
    season_database=None,
):
    single_dir = os.path.join(sequence_dir, f"_rotation_{index}")
    Path(single_dir).mkdir(parents=True, exist_ok=True)
    season_row = season_simunit_row(
        row,
        management,
        is_last_season=is_last_season,
        previous_season_end=previous_season_end,
    )
    connection = configure_season_connection(
        season_database, season_row, management
    )
    crop = dssatcultivarconverter.DssatCultivarConverter().export(
        simulation_path(context["directory_path"], season_row),
        connection,
        context["pltfolder"],
        sequence_dir,
        context["dssat_version"],
    )

    dssatsoilconverter.DssatSoilConverter().export(
        soil_simulation_path(context["directory_path"], season_row),
        context["model_dictionary_connection"],
        connection,
        sequence_dir,
    )

    dssatxconverter.DssatXConverter().export(
        x_simulation_path(context["directory_path"], season_row),
        context["model_dictionary_connection"],
        connection,
        single_dir,
        crop,
        context["dailyoutput"],
        context["dssat_version"],
    )

    sections = parse_sections(read_generated_xfile(single_dir))
    set_section_date(
        sections, "*HARVEST", date_to_yydoy(row_end_date(season_row))
    )
    generated_xfile_path(single_dir).write_text(render_sections(sections))
    return RotationInput(
        row=season_row,
        index=index,
        crop=crop,
        sections=sections,
        management=management,
    )


def generate_successive_rotations(group, context, sequence_dir):
    row = group[0]
    managements = successive_managements(row, context["master_input_connection"])
    season_database = successive_group_connection(
        context["master_input_connection"]
    )
    try:
        rotations = []
        previous_season_end = None
        for index, management in enumerate(managements):
            rotation = generate_rotation_input(
                row,
                management,
                index + 1,
                context,
                sequence_dir,
                is_last_season=index == len(managements) - 1,
                previous_season_end=previous_season_end,
                season_database=season_database,
            )
            rotations.append(rotation)
            previous_season_end = row_end_date(rotation.row)
        return rotations
    finally:
        season_database.close()


def create_context(mi, md, directory_path, pltfolder, dt, dailyoutput, dssat_version="v47"):
    return {
        "directory_path": directory_path,
        "pltfolder": pltfolder,
        "dt": dt,
        "dailyoutput": dailyoutput,
        "dssat_version": dssat_version,
        "master_input_connection": sqlite3.connect(mi),
        "model_dictionary_connection": sqlite3.connect(md),
    }


def close_context(context):
    context["master_input_connection"].close()
    context["model_dictionary_connection"].close()


def raise_on_dssat_error(usmdir, args):
    error_file = Path(usmdir, "ERROR.OUT")
    if not error_file.exists():
        return
    content = error_file.read_text(errors="ignore")
    if re.search(r"Error key:|Unknown ERROR|Invalid format", content, re.IGNORECASE):
        sys.stderr.write(content)
        raise subprocess.CalledProcessError(99, args)


def run_dssat_q(usmdir, output_dir, summary_id, dssat_version="v47"):
    script = Path(__file__).with_name("dssatrun_successive.sh")
    args = ["bash", str(script), usmdir, output_dir, summary_id, dssat_version]
    result = subprocess.run(
        args,
        stdout=subprocess.DEVNULL,
        stderr=sys.stderr,
        check=True,
        text=True,
        timeout=600,
    )
    raise_on_dssat_error(usmdir, args)
    return result


def run_dssat_b(usmdir, output_dir, dt, dssat_version="v47"):
    script = Path(__file__).with_name("dssatrun.sh")
    args = ["bash", str(script), usmdir, output_dir, str(dt), "0", dssat_version]
    result = subprocess.run(
        args,
        stdout=subprocess.DEVNULL,
        stderr=sys.stderr,
        check=True,
        text=True,
        timeout=300,
    )
    raise_on_dssat_error(usmdir, args)
    return result


def export_grouped_weather(group, context, sequence_dir):
    row = group[0]
    years = sequence_weather_years(group)
    generated = dssatweatherconverter.DssatweatherConverter().export_years(
        simulation_path(context["directory_path"], row),
        context["model_dictionary_connection"],
        context["master_input_connection"],
        sequence_dir,
        years=years,
    )
    if len(generated) != len(years):
        raise RuntimeError(
            f"Expected {len(years)} DSSAT weather files for years {years}, "
            f"but generated {sorted(generated)}"
        )


def transform_sequence(summary_path, rotations):
    with open(summary_path, "r") as handle:
        lines = handle.readlines()
    if len(lines) < 5:
        return pd.DataFrame()

    variable_ids = str.split(lines[3][1:])[13:]
    records = []
    summary_lines = []
    for line in lines[4:]:
        parts = str.split(line)
        if len(parts) >= 13 + len(variable_ids):
            summary_lines.append((line, parts))

    rotation_by_sequence = {rotation.index: rotation for rotation in rotations}
    seen_rotation_indexes = set()
    repeated_single_rotation = len(rotations) == 1

    for index, (line, parts) in enumerate(summary_lines):
        if repeated_single_rotation:
            rotation_index = 1
            rotation = rotations[0]
        else:
            try:
                rotation_index = int(float(parts[2]))
            except (ValueError, IndexError):
                rotation_index = index + 1
            rotation = rotation_by_sequence.get(rotation_index)
            if rotation is None or rotation_index in seen_rotation_indexes:
                continue
            seen_rotation_indexes.add(rotation_index)
        row = rotation.row
        values = list(map(float, parts[13:13 + len(variable_ids)]))
        record = {variable_ids[i]: values[i] for i in range(len(variable_ids))}
        record["Model"] = "Dssat"
        record["Idsim"] = row["idsim"]
        record["Texte"] = ""
        record["SeasonOrder"] = (
            index + 1
            if repeated_single_rotation
            else int(rotation.management["SeasonOrder"])
        )
        planting_year = int(record.get("PDAT", 0)) // 1000
        record["ys"] = planting_year or int(row["StartYear"])
        if repeated_single_rotation and records:
            record["y0"] = int(records[-1]["ys"])
        else:
            record["y0"] = int(
                rotation.row["StartYear"]
                if rotation.index == 1
                else rotations[rotation.index - 2].row["StartYear"]
            )
        coords = re.findall(r"([-]?\d+[.]?\d+)[_]", str(row["idsim"]))
        if len(coords) >= 3:
            record["lat"] = float(coords[0])
            record["lon"] = float(coords[1])
            record["time"] = int(float(coords[2]))
        else:
            record["lat"] = None
            record["lon"] = None
            record["time"] = int(row["StartYear"])
        records.append(record)

    dataframe = pd.DataFrame(records)
    if dataframe.empty:
        return dataframe
    dataframe = dataframe.rename(columns={
        "PDAT": "Planting",
        "EDAT": "Emergence",
        "ADAT": "Ant",
        "MDAT": "Mat",
        "CWAM": "Biom_ma",
        "HWAM": "Yield",
        "H#AM": "GNumber",
        "LAIX": "MaxLai",
        "NLCM": "Nleac",
        "NIAM": "SoilN",
        "CNAM": "CroN_ma",
        "ESCP": "CumE",
        "EPCP": "Transp",
    })
    first = ["Model", "Idsim", "Texte"]
    rest = [column for column in dataframe.columns if column not in first]
    return dataframe[first + rest]


def read_sequence_daily(sequence_dir, idsim):
    output_files = {
        source: os.path.join(sequence_dir, filename)
        for source, filename in DSSAT_DAILY_FILES.items()
        if os.path.exists(os.path.join(sequence_dir, filename))
    }
    if not output_files:
        return pd.DataFrame()

    daily = None
    for source, file_path in output_files.items():
        module_data = read_sequence_daily_file(file_path, source)
        daily = (
            module_data
            if daily is None
            else daily.merge(module_data, on=["YEAR", "DOY"], how="outer")
        )
    daily = daily.sort_values(["YEAR", "DOY"]).reset_index(drop=True)
    daily.insert(0, "Idsim", str(idsim))
    daily.insert(0, "Model", "Dssat")
    return daily


def read_sequence_daily_file(file_path, source):
    """Read a sequence output whose columns may change between crops."""
    with open(file_path, "r") as output_stream:
        lines = output_stream.readlines()

    columns = None
    records = []
    for line in lines:
        stripped = line.strip()
        if line.lstrip().startswith("@YEAR"):
            columns = line.lstrip()[1:].split()
            continue
        if columns is None or not stripped or stripped.startswith(("*", "@", "!")):
            continue
        values = stripped.split()
        if len(values) != len(columns):
            continue
        records.append(dict(zip(columns, values)))

    if columns is None:
        raise ValueError(f"No @YEAR header found in {file_path}")
    data = pd.DataFrame.from_records(records)
    if data.empty:
        return pd.DataFrame(columns=["YEAR", "DOY"])
    for column in data.columns:
        data[column] = pd.to_numeric(data[column], errors="coerce")
    data = data.dropna(subset=["YEAR", "DOY"])
    data["YEAR"] = data["YEAR"].astype(int)
    data["DOY"] = data["DOY"].astype(int)
    data = data.replace(-99, float("nan"))
    return data.rename(
        columns={
            column: f"{source}_{column}"
            for column in data.columns
            if column not in ("YEAR", "DOY")
        }
    )


def summary_for_master_input(dataframe):
    """Normalize successive summary rows like the standard DSSAT converter."""
    summary_columns = [
        "Model", "Idsim", "Texte", "SeasonOrder", "Planting", "Emergence",
        "Ant", "Mat", "Biom_ma", "Yield", "GNumber", "MaxLai", "Nleac",
        "SoilN", "CroN_ma", "CumE", "Transp",
    ]
    result = dataframe.copy()
    for column in summary_columns:
        if column not in result.columns:
            result[column] = None
    if "ys" not in result.columns or "y0" not in result.columns:
        raise ValueError("Successive DSSAT summary is missing season date context")

    result = result.replace(-99, float("nan"))
    for column in ("Planting", "Emergence", "Ant", "Mat"):
        result[column] = cumulative_sequence_dates(
            result[column], result["ys"], result["y0"]
        )
    for column in ("Yield", "Biom_ma"):
        result[column] = result[column] / 1000
    value_columns = summary_columns[4:]
    result[value_columns] = result[value_columns].mask(result[value_columns] < 0)
    return result[summary_columns]


def cumulative_sequence_dates(values, season_years, sequence_start_years):
    """Convert DSSAT dates to cumulative DOY from each run's start year."""
    converted = []
    for value, season_year, start_year in zip(
        values, season_years, sequence_start_years
    ):
        if pd.isna(value):
            converted.append(float("nan"))
            continue
        numeric_value = int(float(value))
        if numeric_value < 0:
            converted.append(float("nan"))
            continue
        doy = numeric_value % 1000
        if doy <= 0:
            converted.append(float("nan"))
            continue
        elapsed = (date(int(season_year), 1, 1) - date(int(start_year), 1, 1)).days
        converted.append(elapsed + doy)
    return pd.Series(converted, index=values.index, dtype="float64")


def save_successive_outputs(mi, summaries, daily_frames, save_summary, save_daily):
    with sqlite3.connect(mi) as connection:
        if save_summary:
            summary = pd.concat(summaries, ignore_index=True)
            summary = summary_for_master_input(summary)
            connection.execute("DELETE FROM SummaryOutput WHERE Model = 'Dssat'")
            summary.to_sql("SummaryOutput", connection, if_exists="append", index=False)
            print(f"{len(summary)} rows inserted into SummaryOutput.", flush=True)

        if save_daily:
            daily = pd.concat(daily_frames, ignore_index=True)
            table_exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (DSSAT_DAILY_OUTPUT_TABLE,),
            ).fetchone()
            if table_exists:
                connection.execute(
                    f'DELETE FROM "{DSSAT_DAILY_OUTPUT_TABLE}" WHERE Model = ?',
                    ("Dssat",),
                )
                daily = align_sqlite_table(connection, DSSAT_DAILY_OUTPUT_TABLE, daily)
                daily.to_sql(
                    DSSAT_DAILY_OUTPUT_TABLE, connection, if_exists="append", index=False
                )
            else:
                daily.to_sql(
                    DSSAT_DAILY_OUTPUT_TABLE, connection, if_exists="replace", index=False
                )
            connection.execute(
                f'CREATE INDEX IF NOT EXISTS "idx_{DSSAT_DAILY_OUTPUT_TABLE}_idsim_date" '
                f'ON "{DSSAT_DAILY_OUTPUT_TABLE}" ("Idsim", "YEAR", "DOY")'
            )
            print(
                f"{len(daily)} rows inserted into {DSSAT_DAILY_OUTPUT_TABLE}.",
                flush=True,
            )


def process_single_row(row, context, directory_path, sequence_dir):
    crop = dssatcultivarconverter.DssatCultivarConverter().export(
        simulation_path(directory_path, row),
        context["master_input_connection"],
        context["pltfolder"],
        sequence_dir,
        context["dssat_version"],
    )
    dssatweatherconverter.DssatweatherConverter().export(
        simulation_path(directory_path, row),
        context["model_dictionary_connection"],
        context["master_input_connection"],
        sequence_dir,
    )
    dssatsoilconverter.DssatSoilConverter().export(
        soil_simulation_path(directory_path, row),
        context["model_dictionary_connection"],
        context["master_input_connection"],
        sequence_dir,
    )
    dssatxconverter.DssatXConverter().export(
        x_simulation_path(directory_path, row),
        context["model_dictionary_connection"],
        context["master_input_connection"],
        sequence_dir,
        crop,
        context["dailyoutput"],
        context["dssat_version"],
    )
    run_dssat_b(sequence_dir, directory_path, context["dt"], context["dssat_version"])
    summary = os.path.join(directory_path, f"Summary_{row['idsim']}.OUT")
    if not os.path.exists(summary):
        print(f"Summary file {summary} not found.", flush=True)
        return pd.DataFrame()
    dataframe = transform(summary, context["dt"])
    if context["dt"] == 1:
        os.remove(summary)
    return dataframe


def process_successive_group(
    group, mi, md, directory_path, temp_dir, pltfolder, dt, dailyoutput,
    dssat_version="v47"
):
    validate_successive_group(group)
    group_id = safe_group_id(group)
    sequence_dir = os.path.join(temp_dir, group_id)
    shutil.rmtree(sequence_dir, ignore_errors=True)
    Path(sequence_dir).mkdir(parents=True, exist_ok=True)
    print(f"Processing DSSAT successive group {group_key(group)} with {len(group)} simulation(s)", flush=True)

    context = create_context(mi, md, directory_path, pltfolder, dt, dailyoutput, dssat_version)
    try:
        managements = successive_managements(
            group[0], context["master_input_connection"]
        )
        export_grouped_weather(group, context, sequence_dir)
        nyers = dssat_sequence_years(group)
        rotations = generate_successive_rotations(group, context, sequence_dir)
        sequence_start = group_start_yydoy(group)
        Path(sequence_dir, SEQ_FILE_NAME).write_text(
            build_sequence_file(
                rotations,
                context["model_dictionary_connection"],
                context["master_input_connection"],
                nyers,
                sequence_start,
            )
        )
        Path(sequence_dir, batch_filename_for_version(dssat_version)).write_text(
            build_batch_file(rotations)
        )
        #remove_rotation_workdirs(sequence_dir, rotations)

        run_dssat_q(sequence_dir, directory_path, group_id, dssat_version)
        summary = os.path.join(directory_path, f"{SUMMARY_PREFIX}{group_id}.OUT")
        if not os.path.exists(summary):
            print(f"Summary file {summary} not found.", flush=True)
            return pd.DataFrame(), pd.DataFrame()
        dataframe = transform_sequence(summary, rotations)
        daily = (
            read_sequence_daily(sequence_dir, group[0]["idsim"])
            if dailyoutput == 1
            else pd.DataFrame()
        )
        if dt == 1:
            os.remove(summary)
        return dataframe, daily
    except subprocess.CalledProcessError as error:
        print(f"DSSAT failed with return code {error.returncode} for group {group_key(group)}", flush=True)
        traceback.print_exc()
        raise
    except Exception:
        print(f"Error during DSSAT successive group {group_key(group)}", flush=True)
        traceback.print_exc()
        raise
    finally:
        close_context(context)
        if dt == 1:
            shutil.rmtree(sequence_dir, ignore_errors=True)


def main():
    mi = GlobalVariables.get("dbMasterInput")
    md = GlobalVariables.get("dbModelsDictionary")
    directory_path = GlobalVariables.get("directorypath", os.getcwd())
    pltfolder = GlobalVariables.get("pltfolder")
    nthreads = max(1, int(GlobalVariables.get("nthreads", 4)))
    dt = int(GlobalVariables.get("dt", 0))
    dailyoutput = int(GlobalVariables.get("dailyoutput", 0))
    dssat_version = GlobalVariables.get("dssat_version", "v47")
    batch_filename_for_version(dssat_version)
    temp_dir = GlobalVariables.get("tempDir") or os.path.join(directory_path, "temp_dssat_successive")

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

    print(f"Total simulations to process: {len(rows)}", flush=True)
    print(f"DSSAT successive groups: {len(groups)}", flush=True)
    print(f"Parallel workers: {nthreads}", flush=True)

    result_path = os.path.join(directory_path, f"{uuid.uuid4()}_dssat_successive.csv")
    write_header = True
    groups_written = 0

    try:
        with parallel_backend("loky", n_jobs=nthreads):
            results = Parallel()(
                delayed(process_successive_group)(
                    group,
                    mi,
                    md,
                    directory_path,
                    temp_dir,
                    pltfolder,
                    dt,
                    dailyoutput,
                    dssat_version,
                )
                for group in groups
            )

        summary_frames = []
        daily_frames = []
        for dataframe, daily in results:
            if dataframe.empty:
                continue
            dataframe.to_csv(result_path, mode="a", header=write_header, index=False)
            summary_frames.append(dataframe)
            if not daily.empty:
                daily_frames.append(daily)
            write_header = False
            groups_written += 1

        if groups_written == 0:
            print("No data to process.", flush=True)
            return None

        save_summary = dt == 0 and bool(summary_frames)
        save_daily = dailyoutput == 1 and bool(daily_frames)
        if save_summary or save_daily:
            save_successive_outputs(
                mi, summary_frames, daily_frames, save_summary, save_daily
            )
        elif dailyoutput == 1:
            print("Warning: no DSSAT successive daily results were imported.", flush=True)

        print(f"Results saved to {result_path}", flush=True)
        print(f"DSSAT successive total time, {time() - start}", flush=True)
        return result_path
    except Exception:
        print("DSSAT successive export not completed successfully!", flush=True)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
