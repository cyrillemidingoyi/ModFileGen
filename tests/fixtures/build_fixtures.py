"""Export and rebuild the test fixture databases from versioned text sources.

Each fixture is stored as readable text so that every change shows up in a
git diff:

* ``sources/<fixture>/schema.sql`` holds the ``CREATE TABLE`` and
  ``CREATE INDEX`` statements;
* ``sources/<fixture>/<Table>.csv`` holds the rows of each table.

Three fixtures exist:

``masterinput``
    The MasterInput input tables. Output tables (``SummaryOutput``,
    ``*DailyOutput``, ``SticsProfile``) are left out: their columns depend on
    the output configuration and the converters create them when missing.

``modelsdictionary``
    Only the ``Variables`` table of ModelsDictionary, the one the converters
    read.

``celsius_v32_template``
    The CELSIUS V32 model database used as conversion template. Reference
    tables (cultivars, species, stages, residues...) are kept whole. The
    converter empties and refills the simulation tables, reading only the
    first row of some of them as default values: those keep their first
    row, the others and the output tables are stored empty.

CSV conventions: UTF-8, comma separator, header row, rows sorted by rowid.
SQL ``NULL`` is written as the literal ``NULL``; an empty field is an empty
string.

Usage::

    # Rebuild a database from the sources
    python tests/fixtures/build_fixtures.py build --out /tmp/MasterInput.db
    python tests/fixtures/build_fixtures.py build --fixture celsius_v32_template \\
        --out /tmp/celsius_model_input.db

    # Re-export the sources from a database (overwrites the CSV files)
    python tests/fixtures/build_fixtures.py export \\
        --from tests/stics_successive/MasterInput.db
    python tests/fixtures/build_fixtures.py export --fixture celsius_v32_template \\
        --from tests/dssatsuccessive/celsius_model_input.db
"""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

FIXTURES_DIR = Path(__file__).resolve().parent
SOURCES_DIR = FIXTURES_DIR / "sources"
MASTERINPUT_SOURCES = SOURCES_DIR / "masterinput"
CELSIUS_V32_TEMPLATE_SOURCES = SOURCES_DIR / "celsius_v32_template"
MODELSDICTIONARY_SOURCES = SOURCES_DIR / "modelsdictionary"
MODELSDICTIONARY_TABLES = {"variables"}

NULL_MARKER = "NULL"
OUTPUT_TABLES = {"summaryoutput", "sticsprofile"}
OUTPUT_TABLE_SUFFIXES = ("dailyoutput",)

# CELSIUS V32 template: tables whose first row the converter reads as default
# values before emptying them, and tables it empties and refills.
CELSIUS_V32_DEFAULT_ROW_TABLES = {
    "listpannexes", "paramini", "soil", "tech_commun", "tech_percrop",
    "general_parameters", "optionsmodel",
}
CELSIUS_V32_EMPTY_TABLES = {
    "dweather", "simunitlist", "soil_layers", "irrigation_list",
    "fertimin_list", "fertiorga_list", "ruissellementobs",
    "outputsynt", "outputd_1", "outputd_2",
}


def is_output_table(name):
    lowered = name.lower()
    return lowered in OUTPUT_TABLES or lowered.endswith(OUTPUT_TABLE_SUFFIXES)


def celsius_v32_template_row_limit(name):
    """Number of rows kept for a template table; ``None`` keeps them all."""
    lowered = name.lower()
    if lowered in CELSIUS_V32_EMPTY_TABLES:
        return 0
    if lowered in CELSIUS_V32_DEFAULT_ROW_TABLES:
        return 1
    return None


def quote_identifier(name):
    return '"' + name.replace('"', '""') + '"'


def _tables(connection, skip_table=None):
    rows = connection.execute(
        "SELECT name, sql FROM sqlite_master WHERE type = 'table' "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [
        (name, sql) for name, sql in rows
        if skip_table is None or not skip_table(name)
    ]


def _indexes(connection, tables):
    wanted = {name for name, _ in tables}
    rows = connection.execute(
        "SELECT tbl_name, name, sql FROM sqlite_master WHERE type = 'index' "
        "AND sql IS NOT NULL ORDER BY tbl_name, name"
    ).fetchall()
    return [sql for table, _, sql in rows if table in wanted]


def export_database(source_db, sources_dir, skip_table=None, row_limit=None):
    """Write ``schema.sql`` and one CSV per table of ``source_db``.

    ``skip_table(name)`` leaves a table out entirely. ``row_limit(name)``
    returns how many rows to keep (in rowid order), or ``None`` for all.
    """
    source_db = Path(source_db)
    sources_dir = Path(sources_dir)
    sources_dir.mkdir(parents=True, exist_ok=True)
    uri = f"file:{source_db}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    try:
        tables = _tables(connection, skip_table)
        schema = [sql.strip() + ";" for _, sql in tables]
        schema += [sql.strip() + ";" for sql in _indexes(connection, tables)]
        (sources_dir / "schema.sql").write_text(
            "\n\n".join(schema) + "\n", encoding="utf-8"
        )

        for stale in sources_dir.glob("*.csv"):
            stale.unlink()
        for name, _ in tables:
            limit = row_limit(name) if row_limit else None
            query = f"SELECT * FROM {quote_identifier(name)} ORDER BY rowid"
            if limit is not None:
                query += f" LIMIT {int(limit)}"
            cursor = connection.execute(query)
            header = [description[0] for description in cursor.description]
            with open(sources_dir / f"{name}.csv", "w", encoding="utf-8",
                      newline="") as handle:
                writer = csv.writer(handle, lineterminator="\n")
                writer.writerow(header)
                for row in cursor:
                    writer.writerow([_encode(name, value) for value in row])
        return [name for name, _ in tables]
    finally:
        connection.close()


def _encode(table, value):
    if value is None:
        return NULL_MARKER
    if isinstance(value, bytes):
        raise ValueError(f"BLOB values are not supported (table {table})")
    if isinstance(value, str) and value == NULL_MARKER:
        raise ValueError(
            f"Text value {NULL_MARKER!r} collides with the NULL marker (table {table})"
        )
    return repr(value) if isinstance(value, float) else value


def build_database(out_db, sources_dir):
    """Create ``out_db`` from ``schema.sql`` and the CSV files."""
    out_db = Path(out_db)
    sources_dir = Path(sources_dir)
    if out_db.exists():
        out_db.unlink()
    out_db.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(out_db)
    try:
        connection.executescript((sources_dir / "schema.sql").read_text(encoding="utf-8"))
        for name, _ in _tables(connection):
            csv_path = sources_dir / f"{name}.csv"
            if not csv_path.exists():
                continue
            with open(csv_path, encoding="utf-8", newline="") as handle:
                reader = csv.reader(handle)
                header = next(reader)
                columns = ", ".join(quote_identifier(column) for column in header)
                marks = ", ".join("?" for _ in header)
                connection.executemany(
                    f"INSERT INTO {quote_identifier(name)} ({columns}) VALUES ({marks})",
                    ([None if value == NULL_MARKER else value for value in row]
                     for row in reader),
                )
        connection.commit()
    finally:
        connection.close()
    return out_db


def export_masterinput(source_db, sources_dir=MASTERINPUT_SOURCES):
    return export_database(source_db, sources_dir, skip_table=is_output_table)


def build_masterinput(out_db, sources_dir=MASTERINPUT_SOURCES):
    return build_database(out_db, sources_dir)


def export_modelsdictionary(source_db, sources_dir=MODELSDICTIONARY_SOURCES):
    return export_database(
        source_db,
        sources_dir,
        skip_table=lambda name: name.lower() not in MODELSDICTIONARY_TABLES,
    )


def build_modelsdictionary(out_db, sources_dir=MODELSDICTIONARY_SOURCES):
    return build_database(out_db, sources_dir)


def export_celsius_v32_template(source_db, sources_dir=CELSIUS_V32_TEMPLATE_SOURCES):
    return export_database(
        source_db, sources_dir, row_limit=celsius_v32_template_row_limit
    )


def build_celsius_v32_template(out_db, sources_dir=CELSIUS_V32_TEMPLATE_SOURCES):
    return build_database(out_db, sources_dir)


FIXTURES = {
    "masterinput": (export_masterinput, build_masterinput, MASTERINPUT_SOURCES),
    "modelsdictionary": (
        export_modelsdictionary,
        build_modelsdictionary,
        MODELSDICTIONARY_SOURCES,
    ),
    "celsius_v32_template": (
        export_celsius_v32_template,
        build_celsius_v32_template,
        CELSIUS_V32_TEMPLATE_SOURCES,
    ),
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)

    build = commands.add_parser("build", help="Build a database from the sources")
    build.add_argument("--fixture", choices=sorted(FIXTURES), default="masterinput")
    build.add_argument("--out", type=Path, required=True, help="Database to create")
    build.add_argument("--sources", type=Path)

    export = commands.add_parser("export", help="Export the sources from a database")
    export.add_argument("--fixture", choices=sorted(FIXTURES), default="masterinput")
    export.add_argument("--from", dest="source", type=Path, required=True)
    export.add_argument("--sources", type=Path)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    export, build, default_sources = FIXTURES[args.fixture]
    sources = args.sources or default_sources
    if args.command == "build":
        print(build(args.out, sources))
    else:
        tables = export(args.source, sources)
        print(f"Exported {len(tables)} tables to {sources}")


if __name__ == "__main__":
    main(sys.argv[1:])
