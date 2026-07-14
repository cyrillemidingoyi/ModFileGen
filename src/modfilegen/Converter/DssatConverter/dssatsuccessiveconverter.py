
"""DSSAT successive simulation converter.

This converter follows the same grouping idea as the STICS successive
converter, but uses DSSAT's native sequence mode (RNMODE = Q).  A group of
SimUnitList rows is converted into one SQX experiment file and a DSSBatch.v47
file with one batch row per rotation.
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
from . import dssatcultivarconverter, dssatweatherconverter, dssatsoilconverter, dssatxconverter
from .dssatconverter import export as prepare_sqlite_indexes
from .dssatconverter import fetch_data_from_sqlite, transform


SUCCESSION_COLUMNS = ["idPoint", "idMangt", "idOption"]
CONSISTENT_COLUMNS = ["idPoint", "idsoil", "idIni", "idOption"]
SEQ_FILE_NAME = "ITSA1301.SQX"
BATCH_FILE_NAME = "DSSBatch.v47"
SUMMARY_PREFIX = "Summary_"


@dataclass
class RotationInput:
    row: dict
    index: int
    crop: str
    sections: dict[str, list[str]]
    is_bare_soil: bool = False


def julian_date(year, day):
    return date(int(year), 1, 1) + timedelta(days=int(day) - 1)


def year_day(value):
    return value.year, value.timetuple().tm_yday


def date_to_yydoy(value):
    return f"{value.year % 100:02d}{value.timetuple().tm_yday:03d}"


def dssat_yydoy_to_date(value, anchor_year):
    value = str(value).strip()
    if not re.fullmatch(r"\d{5}", value):
        raise ValueError(f"Invalid DSSAT YYDDD date: {value}")
    yy = int(value[:2])
    doy = int(value[2:])
    century = (int(anchor_year) // 100) * 100
    year = century + yy
    while year < int(anchor_year) - 50:
        year += 100
    while year > int(anchor_year) + 50:
        year -= 100
    return julian_date(year, doy)


def first_section_date(sections, section):
    for line in data_lines(sections.get(section, [])):
        parts = line.split()
        if len(parts) > 1 and re.fullmatch(r"\d{5}", parts[1]):
            return parts[1]
    return None


def rotation_event_date(rotation, section, fallback):
    value = first_section_date(rotation.sections, section)
    if value is None:
        return fallback(rotation.row)
    return dssat_yydoy_to_date(value, rotation.row["StartYear"])


def rotation_planting_date(rotation):
    return rotation_event_date(rotation, "*PLANTING", row_start_date)


def rotation_harvest_date(rotation):
    return rotation_event_date(rotation, "*HARVEST", row_end_date)


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
    first_start = row_start_date(group[0])
    if nyers is None:
        nyers = dssat_sequence_years(group)
    return add_years(first_start, nyers) - timedelta(days=1)


def terminal_bare_soil_harvest_date(group, nyers=None):
    """Return terminal fallow harvest using DSSAT-style year plus DOY logic.

    The requested rule is to keep the terminal harvest DOY equal to
    ``StartDay - 1`` in the last sequence year.  For ``StartDay == 1``, this
    wraps to the last day of the previous year.
    """
    first_row = group[0]
    if nyers is None:
        nyers = dssat_sequence_years(group)
    terminal_year = int(first_row["StartYear"]) + int(nyers)
    terminal_doy = int(first_row["StartDay"]) - 1
    if terminal_doy >= 1:
        return julian_date(terminal_year, terminal_doy)
    return date(terminal_year - 1, 12, 31)


def sequence_weather_years(group):
    first_year = row_start_date(group[0]).year
    last_year = terminal_bare_soil_harvest_date(group).year
    return list(range(first_year, last_year + 1))


def build_successive_groups(rows):
    dataframe = pd.DataFrame(rows)
    required_columns = {"idsim", "StartYear", "StartDay", "EndYear", "EndDay", *SUCCESSION_COLUMNS}
    missing = sorted(required_columns.difference(dataframe.columns))
    if missing:
        raise ValueError(f"Missing columns in SimUnitList: {missing}")

    groups = []
    for _, group in dataframe.groupby(SUCCESSION_COLUMNS, dropna=False, sort=False):
        ordered = group.sort_values(["StartYear", "StartDay", "idsim"])
        groups.append(ordered.to_dict(orient="records"))
    return groups


def validate_successive_group(group):
    for column in CONSISTENT_COLUMNS:
        values = {str(row[column]) for row in group if column in row}
        if len(values) > 1:
            raise ValueError(
                f"DSSAT sequence group {group_key(group)} has inconsistent {column}: {sorted(values)}"
            )


def group_key(group):
    return "__".join(str(group[0][column]) for column in SUCCESSION_COLUMNS)


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


def read_generated_xfile(usmdir):
    candidates = sorted(Path(usmdir).glob("ITSA1301.*X"))
    if not candidates:
        raise FileNotFoundError(f"No generated DSSAT X file found in {usmdir}")
    return candidates[0].read_text()


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
    width = len(match.group(2))
    return f"{match.group(1)}{int(value):>{width}d}{line[match.end():]}"


def replace_section_level(lines, value):
    return [replace_first_int(line, value) for line in lines]


def replace_second_token(line, value):
    if not line.strip() or line.lstrip().startswith(("@", "*", "!", "$")):
        return line
    match = re.match(r"^(\s*\S+\s+)(\S+)(.*)$", line)
    if not match:
        return line
    width = max(len(match.group(2)), len(str(value)))
    return f"{match.group(1)}{str(value):>{width}}{match.group(3)}"


def set_section_date(sections, section, value):
    if section not in sections:
        return
    sections[section] = [replace_second_token(line, value) for line in sections[section]]


def set_harvest_to_simunit_end(row, sections):
    set_section_date(sections, "*HARVEST", date_to_yydoy(row_end_date(row)))


def data_lines(lines):
    return [line for line in lines if line.strip() and not line.lstrip().startswith(("@", "*", "!", "$"))]


def section_has_data(sections, section):
    return section in sections and bool(data_lines(sections[section]))


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


def management_flags(id_sim, master_input_connection):
    query = f"""
        Select SimUnitList.idsim, SoilTillPolicy.NumTillOperations,
               OrganicFertilizationPolicy.NumOrganicFerti,
               CropManagement.IrrigationPolicyCode, CropManagement.InoFertiPolicyCode
        From OrganicFertilizationPolicy
        Inner Join (SoilTillPolicy Inner Join (CropManagement Inner Join SimUnitList
        On CropManagement.idMangt = SimUnitList.idMangt)
        On SoilTillPolicy.SoilTillPolicyCode = CropManagement.SoilTillPolicyCode)
        On OrganicFertilizationPolicy.OFertiPolicyCode = CropManagement.OFertiPolicyCode
        Where IdSim = '{id_sim}'
    """
    return query_one(master_input_connection, query)


def treatment_line(rotation, model_dictionary_connection, master_input_connection, sections):
    row = rotation.row
    fmt = dssatxconverter.v_fmt_treat
    if rotation.is_bare_soil:
        values = {
            "N": 1,
            "R": rotation.index,
            "O": float(default_value(model_dictionary_connection, "dssat_x_treatment", "ROTOPT")),
            "C": 0,
            "TNAME": str(row["idsim"])[:25],
            "CU": rotation.index,
            "FL": 1,
            "SA": 0,
            "IC": 0,
            "MP": 0,
            "MI": 0,
            "MF": 0,
            "MR": 0,
            "MC": 0,
            "MT": 0,
            "ME": 0,
            "MH": rotation.index,
            "SM": 1,
        }
    else:
        flags = management_flags(row["idsim"], master_input_connection)
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
            "MI": rotation.index if int(flags["IrrigationPolicyCode"]) != 0 and section_has_data(sections, "*IRRIGATION") else 0,
            "MF": rotation.index if int(flags["InoFertiPolicyCode"]) != 0 and section_has_data(sections, "*FERTILIZERS") else 0,
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


def generate_rotation_input(row, index, context, sequence_dir):
    single_dir = os.path.join(sequence_dir, f"_rotation_{index}")
    Path(single_dir).mkdir(parents=True, exist_ok=True)

    crop = dssatcultivarconverter.DssatCultivarConverter().export(
        simulation_path(context["directory_path"], row),
        context["master_input_connection"],
        context["pltfolder"],
        sequence_dir,
    )

    dssatsoilconverter.DssatSoilConverter().export(
        soil_simulation_path(context["directory_path"], row),
        context["model_dictionary_connection"],
        context["master_input_connection"],
        sequence_dir,
    )

    dssatxconverter.DssatXConverter().export(
        x_simulation_path(context["directory_path"], row),
        context["model_dictionary_connection"],
        context["master_input_connection"],
        single_dir,
        crop,
        context["dt"],
    )

    sections = parse_sections(read_generated_xfile(single_dir))
    set_harvest_to_simunit_end(row, sections)
    return RotationInput(
        row=row,
        index=index,
        crop=crop,
        sections=sections,
    )


def create_bare_soil_rotation(previous_row, next_row, index, start_date, end_date):
    row = dict(next_row)
    row["idsim"] = f"{previous_row['idsim']}__intercrop__{next_row['idsim']}"
    row["StartYear"], row["StartDay"] = start_date.year, start_date.timetuple().tm_yday
    row["EndYear"], row["EndDay"] = end_date.year, end_date.timetuple().tm_yday
    harvest_date = date_to_yydoy(end_date)
    sections = {
        "*CULTIVARS": [
            "*CULTIVARS",
            "@C CR INGENO CNAME",
            f"{index:2d} FA IB0001 FALLOW",
        ],
        "*HARVEST": [
            "*HARVEST DETAILS",
            "@H HDATE  HSTG  HCOM HSIZE   HPC  HBPC HNAME",
            f"{index:2d} {harvest_date} GS000   -99   -99   100     0 bare_soil",
        ],
    }
    return RotationInput(row=row, index=index, crop="FA", sections=sections, is_bare_soil=True)


def append_terminal_bare_soil_rotation(rotations, terminal_harvest_date):
    if not rotations:
        return
    last_rotation = rotations[-1]
    terminal_start = rotation_harvest_date(last_rotation) + timedelta(days=1)
    if terminal_start > terminal_harvest_date:
        return
    rotations.append(
        create_bare_soil_rotation(
            last_rotation.row,
            last_rotation.row,
            len(rotations) + 1,
            terminal_start,
            terminal_harvest_date,
        )
    )


def generate_successive_rotations(group, context, sequence_dir, terminal_harvest_date):
    crop_rotations = [
        generate_rotation_input(row, index + 1, context, sequence_dir)
        for index, row in enumerate(group)
    ]

    rotations = []
    for group_index, crop_rotation in enumerate(crop_rotations):
        crop_rotation.index = len(rotations) + 1
        rotations.append(crop_rotation)
        if group_index >= len(crop_rotations) - 1:
            continue

        next_rotation = crop_rotations[group_index + 1]
        bare_start = rotation_harvest_date(crop_rotation) + timedelta(days=1)
        bare_end = rotation_planting_date(next_rotation) - timedelta(days=1)
        if bare_start <= bare_end:
            rotations.append(
                create_bare_soil_rotation(
                    crop_rotation.row,
                    next_rotation.row,
                    len(rotations) + 1,
                    bare_start,
                    bare_end,
                )
            )
    append_terminal_bare_soil_rotation(rotations, terminal_harvest_date)
    return rotations


def create_context(mi, md, directory_path, pltfolder, dt):
    return {
        "directory_path": directory_path,
        "pltfolder": pltfolder,
        "dt": dt,
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


def run_dssat_q(usmdir, output_dir, summary_id):
    script = Path(__file__).with_name("dssatrun_successive.sh")
    args = ["bash", str(script), usmdir, output_dir, summary_id]
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


def run_dssat_b(usmdir, output_dir, dt):
    script = Path(__file__).with_name("dssatrun.sh")
    args = ["bash", str(script), usmdir, output_dir, str(dt)]
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
    dssatweatherconverter.DssatweatherConverter().export(
        simulation_path(context["directory_path"], row),
        context["model_dictionary_connection"],
        context["master_input_connection"],
        sequence_dir,
        years=sequence_weather_years(group),
        single_file=True,
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

    for index, (line, parts) in enumerate(summary_lines):
        try:
            rotation_index = int(float(parts[2]))
        except (ValueError, IndexError):
            rotation_index = index + 1
        rotation = rotation_by_sequence.get(rotation_index)
        if rotation is None or rotation.is_bare_soil or rotation_index in seen_rotation_indexes:
            continue
        seen_rotation_indexes.add(rotation_index)
        row = rotation.row
        values = list(map(float, parts[13:13 + len(variable_ids)]))
        record = {variable_ids[i]: values[i] for i in range(len(variable_ids))}
        record["Model"] = "Dssat"
        record["Idsim"] = row["idsim"]
        record["Texte"] = ""
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


def process_single_row(row, context, directory_path, sequence_dir):
    crop = dssatcultivarconverter.DssatCultivarConverter().export(
        simulation_path(directory_path, row),
        context["master_input_connection"],
        context["pltfolder"],
        sequence_dir,
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
        context["dt"],
    )
    run_dssat_b(sequence_dir, directory_path, context["dt"])
    summary = os.path.join(directory_path, f"Summary_{row['idsim']}.OUT")
    if not os.path.exists(summary):
        print(f"Summary file {summary} not found.", flush=True)
        return pd.DataFrame()
    dataframe = transform(summary)
    if context["dt"] == 1:
        os.remove(summary)
    return dataframe


def process_successive_group(group, mi, md, directory_path, temp_dir, pltfolder, dt):
    validate_successive_group(group)
    group_id = safe_group_id(group)
    sequence_dir = os.path.join(temp_dir, group_id)
    shutil.rmtree(sequence_dir, ignore_errors=True)
    Path(sequence_dir).mkdir(parents=True, exist_ok=True)
    print(f"Processing DSSAT successive group {group_key(group)} with {len(group)} simulation(s)", flush=True)

    context = create_context(mi, md, directory_path, pltfolder, dt)
    try:
        if len(group) == 1:
            return process_single_row(group[0], context, directory_path, sequence_dir)

        export_grouped_weather(group, context, sequence_dir)
        nyers = dssat_sequence_years(group)
        terminal_harvest_date = terminal_bare_soil_harvest_date(group, nyers)
        rotations = generate_successive_rotations(group, context, sequence_dir, terminal_harvest_date)
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
        Path(sequence_dir, BATCH_FILE_NAME).write_text(build_batch_file(rotations))

        run_dssat_q(sequence_dir, directory_path, group_id)
        summary = os.path.join(directory_path, f"{SUMMARY_PREFIX}{group_id}.OUT")
        if not os.path.exists(summary):
            print(f"Summary file {summary} not found.", flush=True)
            return pd.DataFrame()
        dataframe = transform_sequence(summary, rotations)
        if dt == 1:
            os.remove(summary)
        return dataframe
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
                delayed(process_successive_group)(group, mi, md, directory_path, temp_dir, pltfolder, dt)
                for group in groups
            )

        for dataframe in results:
            if dataframe.empty:
                continue
            dataframe.to_csv(result_path, mode="a", header=write_header, index=False)
            write_header = False
            groups_written += 1

        if groups_written == 0:
            print("No data to process.", flush=True)
            return None

        print(f"Results saved to {result_path}", flush=True)
        print(f"DSSAT successive total time, {time() - start}", flush=True)
        return result_path
    except Exception:
        print("DSSAT successive export not completed successfully!", flush=True)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
