"""Check that the weather of each simulation unit covers its whole period.

A simulation unit runs from ``StartYear``/``StartDay`` to ``EndYear``/``EndDay``
(day of year). Its weather comes from ``RAclimateD`` rows of its ``idPoint``.
A simulation whose period is not covered by consecutive weather days is not
run: converters drop it from the batch they process, report it, and leave the
``SimUnitList`` row untouched.

The weather availability of every point is computed once, as the list of
periods of consecutive days found in ``RAclimateD``.
"""

from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

Period = Tuple[date, date]


@dataclass(frozen=True)
class SkippedSimulation:
    idsim: str
    idPoint: str
    start: Optional[date]
    end: Optional[date]
    reason: str


def _value(row: Mapping, name: str):
    for key, value in row.items():
        if str(key).lower() == name.lower():
            return value
    return None


def day_of_year_date(year, day) -> date:
    return date(int(year), 1, 1) + timedelta(days=int(day) - 1)


def simulation_period(row: Mapping) -> Period:
    """Return the inclusive (start, end) dates of one SimUnitList row."""
    start = day_of_year_date(_value(row, "StartYear"), _value(row, "StartDay"))
    end = day_of_year_date(_value(row, "EndYear"), _value(row, "EndDay"))
    return start, end


def _merge_days(days: Iterable[date]) -> List[Period]:
    periods: List[Period] = []
    for day in sorted(set(days)):
        if periods and day == periods[-1][1] + timedelta(days=1):
            periods[-1] = (periods[-1][0], day)
        else:
            periods.append((day, day))
    return periods


def weather_periods(
    connection: sqlite3.Connection, points: Optional[Iterable[str]] = None
) -> Dict[str, List[Period]]:
    """Return the periods of consecutive weather days of each ``idPoint``."""
    wanted = None if points is None else {str(point) for point in points}
    if wanted is not None and not wanted:
        return {}
    days: Dict[str, List[date]] = {}
    for point, year, doy in connection.execute(
        "SELECT idPoint, year, DOY FROM RAclimateD"
    ):
        point = str(point)
        if year is None or doy is None or (wanted is not None and point not in wanted):
            continue
        days.setdefault(point, []).append(day_of_year_date(year, doy))
    return {point: _merge_days(values) for point, values in days.items()}


def is_covered(period: Period, periods: Sequence[Period]) -> bool:
    start, end = period
    return any(first <= start and end <= last for first, last in periods)


def check_simulations(
    rows: Sequence[Mapping], connection: sqlite3.Connection
) -> Tuple[List[Mapping], List[SkippedSimulation]]:
    """Split simulation rows into those to run and those to skip."""
    availability = weather_periods(
        connection, (_value(row, "idPoint") for row in rows)
    )
    kept: List[Mapping] = []
    skipped: List[SkippedSimulation] = []
    for row in rows:
        idsim = str(_value(row, "idsim"))
        point = str(_value(row, "idPoint"))
        try:
            period = simulation_period(row)
        except (TypeError, ValueError):
            skipped.append(SkippedSimulation(
                idsim, point, None, None, "invalid StartYear/StartDay/EndYear/EndDay"
            ))
            continue
        if period[1] < period[0]:
            skipped.append(SkippedSimulation(
                idsim, point, period[0], period[1], "end before start"
            ))
            continue
        periods = availability.get(point, [])
        if is_covered(period, periods):
            kept.append(row)
            continue
        if periods:
            available = ", ".join(f"{first} to {last}" for first, last in periods)
            reason = f"weather available only from {available}"
        else:
            reason = "no weather for this idPoint"
        skipped.append(SkippedSimulation(idsim, point, period[0], period[1], reason))
    return kept, skipped


def write_skipped_report(
    skipped: Sequence[SkippedSimulation], directory, model: str
) -> Optional[Path]:
    """Write the skipped simulations as CSV in ``directory``; return the path."""
    if not skipped or not directory:
        return None
    path = Path(directory) / f"skipped_simulations_{model.lower()}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["idsim", "idPoint", "start", "end", "reason"])
        for item in skipped:
            writer.writerow([item.idsim, item.idPoint, item.start, item.end, item.reason])
    return path


def keep_simulations_with_weather(
    rows: Sequence[Mapping], master_input, model: str, report_directory=None
) -> List[Mapping]:
    """Return the rows whose period is covered by weather; report the others.

    ``master_input`` is a path or an open SQLite connection. Skipped rows are
    printed and, when ``report_directory`` is given, written to
    ``skipped_simulations_<model>.csv``. The database is never modified.
    """
    if isinstance(master_input, sqlite3.Connection):
        kept, skipped = check_simulations(rows, master_input)
    else:
        connection = sqlite3.connect(master_input)
        try:
            kept, skipped = check_simulations(rows, connection)
        finally:
            connection.close()
    if skipped:
        print(
            f"⚠ {len(skipped)} of {len(rows)} {model} simulation(s) skipped: "
            "weather does not cover StartYear/StartDay to EndYear/EndDay.",
            flush=True,
        )
        for item in skipped[:10]:
            print(f"  - {item.idsim}: {item.reason}", flush=True)
        if len(skipped) > 10:
            print(f"  ... and {len(skipped) - 10} more", flush=True)
        report = write_skipped_report(skipped, report_directory, model)
        if report is not None:
            print(f"  Skipped simulations listed in {report}", flush=True)
    return kept


def keep_rows_in_simulation_period(
    dataframe, simulation: Mapping, year_column: str = "YEAR", doy_column: str = "DOY"
):
    """Drop daily output rows outside the simulation period.

    Models such as DSSAT simulate whole years and may write days after
    ``EndYear``/``EndDay``, possibly on rollover weather. Those days are not
    part of the simulation and must not reach the outputs.
    """
    import pandas as pd

    if dataframe is None or dataframe.empty:
        return dataframe
    start, end = simulation_period(simulation)
    years = pd.to_numeric(dataframe[year_column], errors="coerce")
    days = pd.to_numeric(dataframe[doy_column], errors="coerce")
    valid = years.notna() & days.notna()
    dates = pd.Series(pd.NaT, index=dataframe.index)
    dates[valid] = pd.to_datetime(
        years[valid].astype(int).astype(str)
        + days[valid].astype(int).astype(str).str.zfill(3),
        format="%Y%j",
    )
    inside = dates.notna() & (dates >= pd.Timestamp(start)) & (dates <= pd.Timestamp(end))
    return dataframe.loc[inside].reset_index(drop=True)
