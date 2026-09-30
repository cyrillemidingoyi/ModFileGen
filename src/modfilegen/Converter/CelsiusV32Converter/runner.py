"""Configuration and process runner for CELSIUS V32."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile

import pandas as pd

from modfilegen import GlobalVariables
from modfilegen.output_configuration import OutputConfiguration
from modfilegen.Converter.CelsiusConverter.summary_output import (
    transform_summary_dataframe,
)

from .core import convert_database


def _import_summary(master, celsius_database):
    output_configuration = OutputConfiguration.from_files(
        GlobalVariables.get("outputVariablesConfig"),
        GlobalVariables.get("outputSelectionsConfig"),
        GlobalVariables.get("profileVariablesConfig"),
    )
    output_selection = GlobalVariables.get("outputSelection", "legacy")
    with sqlite3.connect(celsius_database) as source, sqlite3.connect(master) as target:
        added_columns = output_configuration.ensure_summary_output_schema(
            target, output_selection
        )
        target.execute("DELETE FROM SummaryOutput WHERE lower(Model)='celsiusv32'")
        outputs = pd.read_sql_query(
            """
            SELECT o.*, s.Situation, s.codesuite
            FROM OutputSynt AS o
            LEFT JOIN SimUnitList AS s ON s.idsim=o.Idsim
            ORDER BY s.ChampTri
            """,
            source,
        )
        if outputs.empty:
            target.commit()
            return

        season_orders = []
        original_ids = []
        for _, row in outputs.iterrows():
            generated_id = str(row.get("Idsim", ""))
            season_order = 1
            if "__S" in generated_id:
                try:
                    season_order = int(generated_id.rsplit("__S", 1)[1][:3])
                except ValueError:
                    season_order = 1
            season_orders.append(season_order)
            original_ids.append(row.get("Situation") or generated_id)
        outputs["Idsim"] = original_ids
        outputs["SeasonOrder"] = season_orders
        outputs["PlantOrder"] = 1
        summary = transform_summary_dataframe(
            outputs,
            output_configuration,
            output_selection,
            model="celsiusv32",
        )
        summary.to_sql(
            output_configuration.summary_table,
            target,
            if_exists="append",
            index=False,
        )
        target.commit()
    if added_columns:
        print(
            "SummaryOutput columns added: " + ", ".join(added_columns),
            flush=True,
        )


def _set_daily_output(celsius_database, enabled):
    """Write daily OutputD_* rows only when requested; they dominate run time."""
    with sqlite3.connect(celsius_database) as connection:
        connection.execute(
            "UPDATE OptionsModel SET EcritDResus = ?", (1 if enabled else 0,)
        )


OUTPUT_TABLES = ("OutputSynt", "OutputD_1", "OutputD_2")

# Per-simulation input rows removed from each worker copy, keyed by the
# SimUnitList column that references them. Keeping them would make the engine
# reload rows of other workers (Tech_Commun is read in full per simulation).
WORKER_FILTERS = (
    ("Tech_Commun", "IdTech_Com", "idTech_Com"),
    ("Tech_perCrop", "idTech_Com", "idTech_Com"),
    ("Irrigation_List", "IdTech_Com", "idTech_Com"),
    ("FertiMin_List", "IdTech_Com", "idTech_Com"),
    ("FertiOrga_List", "IdTech_Com", "idTech_Com"),
    ("Dweather", "idDclim", "idweather"),
)


def _simulation_chains(celsius_database):
    """Return SimUnitList rowids grouped into chains, in engine order.

    The engine carries the final state of a simulation into the next one when
    ``codesuite`` is not 0, so a chain must stay in a single process.
    """
    chains = []
    with sqlite3.connect(celsius_database) as connection:
        for rowid, codesuite in connection.execute(
            "SELECT rowid, codesuite FROM SimUnitList ORDER BY ChampTri"
        ):
            if not chains or int(codesuite or 0) == 0:
                chains.append([])
            chains[-1].append(rowid)
    return chains


def _partition_chains(chains, workers):
    """Split chains into at most ``workers`` contiguous, balanced parts."""
    total = sum(len(chain) for chain in chains)
    workers = max(1, min(int(workers), len(chains)))
    parts = [[]]
    done = 0
    for index, chain in enumerate(chains):
        if parts[-1] and len(parts) < workers:
            target = total * len(parts) / workers
            # Cut where the part ends closest to its share, and give each
            # remaining chain its own part when processes would stay idle.
            closer_after_cut = done + len(chain) / 2 > target
            idle_otherwise = len(chains) - index <= workers - len(parts)
            if closer_after_cut or idle_otherwise:
                parts.append([])
        parts[-1].extend(chain)
        done += len(chain)
    return parts


def _table_columns(connection, schema, table):
    return {
        row[1].lower(): row[1]
        for row in connection.execute(f"PRAGMA [{schema}].table_info([{table}])")
    }


def _prepare_worker_database(celsius_database, worker_database, rowids):
    shutil.copy2(celsius_database, worker_database)
    with sqlite3.connect(worker_database) as connection:
        connection.execute("CREATE TEMP TABLE kept_simulations (id INTEGER PRIMARY KEY)")
        connection.executemany(
            "INSERT INTO kept_simulations VALUES (?)", [(rowid,) for rowid in rowids]
        )
        connection.execute(
            "DELETE FROM SimUnitList WHERE rowid NOT IN (SELECT id FROM kept_simulations)"
        )
        simulation_columns = _table_columns(connection, "main", "SimUnitList")
        for table, column, reference in WORKER_FILTERS:
            columns = _table_columns(connection, "main", table)
            if column.lower() not in columns or reference.lower() not in simulation_columns:
                continue
            connection.execute(
                f"DELETE FROM [{table}] WHERE [{columns[column.lower()]}] NOT IN "
                f"(SELECT [{simulation_columns[reference.lower()]}] FROM SimUnitList)"
            )
        for table in OUTPUT_TABLES:
            if _table_columns(connection, "main", table):
                connection.execute(f"DELETE FROM [{table}]")


def _merge_worker_outputs(celsius_database, worker_databases):
    """Append worker outputs to the main database, in worker order."""
    with sqlite3.connect(celsius_database) as connection:
        for table in OUTPUT_TABLES:
            if _table_columns(connection, "main", table):
                connection.execute(f"DELETE FROM [{table}]")
        connection.commit()  # ATTACH is not allowed inside a transaction.
        for worker_database in worker_databases:
            connection.execute("ATTACH DATABASE ? AS worker", (str(worker_database),))
            try:
                for table in OUTPUT_TABLES:
                    source_columns = _table_columns(connection, "worker", table)
                    if not source_columns:
                        continue
                    target_columns = _table_columns(connection, "main", table)
                    if not target_columns:
                        create_sql = connection.execute(
                            "SELECT sql FROM worker.sqlite_master "
                            "WHERE type='table' AND name=?",
                            (table,),
                        ).fetchone()[0]
                        connection.execute(create_sql)
                        target_columns = source_columns
                    names = ", ".join(
                        f"[{target_columns[name]}]"
                        for name in source_columns
                        if name in target_columns
                    )
                    selected = ", ".join(
                        f"[{source_columns[name]}]"
                        for name in source_columns
                        if name in target_columns
                    )
                    connection.execute(
                        f"INSERT INTO main.[{table}] ({names}) "
                        f"SELECT {selected} FROM worker.[{table}]"
                    )
                connection.commit()
            finally:
                connection.execute("DETACH DATABASE worker")


def _run_worker(executable, worker_database):
    log_path = worker_database.with_suffix(".log")
    with open(log_path, "w", encoding="utf-8") as log:
        completed = subprocess.run(
            [executable, str(worker_database)],
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
    return completed.returncode, log_path


def _log_tail(log_path, lines=20):
    try:
        content = Path(log_path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    return "\n".join(content.splitlines()[-lines:])


def run_model(celsius_database, executable="celsiusV32", workers=1):
    """Run CELSIUS V32 on a converted database, optionally in parallel.

    With several workers, simulations are split into contiguous parts that
    never cut a chain of successive simulations. Each worker runs the
    unmodified engine on its own reduced copy of the database, then the
    outputs are merged back in simulation order.
    """
    celsius_database = Path(celsius_database)
    chains = _simulation_chains(celsius_database)
    parts = _partition_chains(chains, workers) if chains else []
    if len(parts) <= 1:
        subprocess.run([executable, str(celsius_database)], check=True, text=True)
        return

    work_dir = Path(
        tempfile.mkdtemp(prefix=f"{celsius_database.stem}_workers_", dir=celsius_database.parent)
    )
    worker_databases = [work_dir / f"part_{index:03d}.db" for index in range(len(parts))]
    for worker_database, rowids in zip(worker_databases, parts):
        _prepare_worker_database(celsius_database, worker_database, rowids)

    print(
        f"CELSIUS V32: {sum(map(len, parts))} simulations on {len(parts)} processes",
        flush=True,
    )
    failures = []
    with ThreadPoolExecutor(max_workers=len(parts)) as executor:
        futures = [
            executor.submit(_run_worker, executable, worker_database)
            for worker_database in worker_databases
        ]
        for index, future in enumerate(futures, 1):
            returncode, log_path = future.result()
            if returncode:
                failures.append((returncode, log_path))
            print(
                f"CELSIUS V32: process {index}/{len(parts)} "
                f"{'failed' if returncode else 'done'}",
                flush=True,
            )
    if failures:
        details = "\n\n".join(
            f"{log_path} (exit {returncode}):\n{_log_tail(log_path)}"
            for returncode, log_path in failures
        )
        raise RuntimeError(
            f"CELSIUS V32 failed in {len(failures)} process(es); "
            f"worker files kept in {work_dir}\n{details}"
        )

    _merge_worker_outputs(celsius_database, worker_databases)
    shutil.rmtree(work_dir, ignore_errors=True)


def run(mode):
    master = GlobalVariables.get("dbMasterInput")
    template = (
        GlobalVariables.get("dbCelsiusV32Template")
        or GlobalVariables.get("dbCelsius")
    )
    if not master or not template:
        raise ValueError(
            "dbMasterInput and dbCelsiusV32Template (or dbCelsius) must be set"
        )

    output = GlobalVariables.get("celsiusV32Output")
    if not output:
        directory = Path(GlobalVariables.get("tempDir") or GlobalVariables.get("directorypath") or ".")
        output = directory / "celsius_v32_input.db"
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if Path(template).resolve() != output.resolve():
        shutil.copy2(template, output)

    simulation_ids = GlobalVariables.get("celsiusIdsim")
    if isinstance(simulation_ids, str):
        simulation_ids = [simulation_ids]
    convert_database(master, output, mode=mode, simulation_ids=simulation_ids)
    _set_daily_output(output, int(GlobalVariables.get("dailyoutput", 0)) == 1)
    if int(GlobalVariables.get("runCelsiusV32", 1)):
        executable = str(GlobalVariables.get("celsiusV32Executable", "celsiusV32"))
        workers = max(1, int(GlobalVariables.get("nthreads", 1) or 1))
        run_model(output, executable, workers)
        if int(GlobalVariables.get("dt", 1)) == 0:
            _import_summary(master, output)
    return output
