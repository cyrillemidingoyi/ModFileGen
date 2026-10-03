"""Export and rebuild the MasterInput test fixture from versioned text sources.

The fixture is stored as readable text so that every change shows up in a
git diff:

* ``sources/masterinput/schema.sql`` holds the ``CREATE TABLE`` and
  ``CREATE INDEX`` statements of the input tables;
* ``sources/masterinput/<Table>.csv`` holds the rows of each input table.

Output tables (``SummaryOutput``, ``*DailyOutput``, ``SticsProfile``) are not
part of the fixture: their columns depend on the output configuration and the
converters create them when they are missing.

CSV conventions: UTF-8, comma separator, header row, rows sorted by rowid.
SQL ``NULL`` is written as the literal ``NULL``; an empty field is an empty
string.

Usage::

    # Rebuild a MasterInput database from the sources
    python tests/fixtures/build_fixtures.py build --out /tmp/MasterInput.db

    # Re-export the sources from a database (overwrites the CSV files)
    python tests/fixtures/build_fixtures.py export \
        --from tests/stics_successive/MasterInput.db
"""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

FIXTURES_DIR = Path(__file__).resolve().parent
MASTERINPUT_SOURCES = FIXTURES_DIR / "sources" / "masterinput"

NULL_MARKER = "NULL"
OUTPUT_TABLES = {"summaryoutput", "sticsprofile"}
OUTPUT_TABLE_SUFFIXES = ("dailyoutput",)


def is_output_table(name):
    lowered = name.lower()
    return lowered in OUTPUT_TABLES or lowered.endswith(OUTPUT_TABLE_SUFFIXES)


def quote_identifier(name):
    return '"' + name.replace('"', '""') + '"'


def _input_tables(connection):
    rows = connection.execute(
        "SELECT name, sql FROM sqlite_master WHERE type = 'table' "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [(name, sql) for name, sql in rows if not is_output_table(name)]


def _indexes(connection, tables):
    wanted = {name for name, _ in tables}
    rows = connection.execute(
        "SELECT tbl_name, name, sql FROM sqlite_master WHERE type = 'index' "
        "AND sql IS NOT NULL ORDER BY tbl_name, name"
    ).fetchall()
    return [sql for table, _, sql in rows if table in wanted]


def export_masterinput(source_db, sources_dir=MASTERINPUT_SOURCES):
    """Write ``schema.sql`` and one CSV per input table of ``source_db``."""
    source_db = Path(source_db)
    sources_dir = Path(sources_dir)
    sources_dir.mkdir(parents=True, exist_ok=True)
    uri = f"file:{source_db}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    try:
        tables = _input_tables(connection)
        schema = [sql.strip() + ";" for _, sql in tables]
        schema += [sql.strip() + ";" for sql in _indexes(connection, tables)]
        (sources_dir / "schema.sql").write_text(
            "\n\n".join(schema) + "\n", encoding="utf-8"
        )

        for stale in sources_dir.glob("*.csv"):
            stale.unlink()
        for name, _ in tables:
            cursor = connection.execute(
                f"SELECT * FROM {quote_identifier(name)} ORDER BY rowid"
            )
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


def build_masterinput(out_db, sources_dir=MASTERINPUT_SOURCES):
    """Create ``out_db`` from ``schema.sql`` and the CSV files."""
    out_db = Path(out_db)
    sources_dir = Path(sources_dir)
    if out_db.exists():
        out_db.unlink()
    out_db.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(out_db)
    try:
        connection.executescript((sources_dir / "schema.sql").read_text(encoding="utf-8"))
        tables = [name for name, _ in _input_tables(connection)]
        for name in tables:
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


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)

    build = commands.add_parser("build", help="Build MasterInput.db from the sources")
    build.add_argument("--out", type=Path, required=True, help="Database to create")
    build.add_argument("--sources", type=Path, default=MASTERINPUT_SOURCES)

    export = commands.add_parser("export", help="Export the sources from a database")
    export.add_argument("--from", dest="source", type=Path, required=True)
    export.add_argument("--sources", type=Path, default=MASTERINPUT_SOURCES)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.command == "build":
        print(build_masterinput(args.out, args.sources))
    else:
        tables = export_masterinput(args.source, args.sources)
        print(f"Exported {len(tables)} tables to {args.sources}")


if __name__ == "__main__":
    main(sys.argv[1:])
