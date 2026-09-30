"""Build a CELSIUS V32 SQLite input database from MasterInput.

The V32 model is intentionally kept independent from the legacy Datamill
converter.  MasterInput remains the canonical representation: one SimUnitList
row describes an experiment, CropManagement.SeasonOrder describes its temporal
seasons, and PlantOrder describes crops associated within one season.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
import sqlite3


REQUIRED_TARGET_TABLES = {
    "Dweather", "ListPAnnexes", "ParamIni", "Tech_Commun",
    "Tech_perCrop", "SimUnitList", "Soil", "Soil_layers",
    "Irrigation_List", "FertiMin_List", "FertiOrga_List",
    "General_Parameters", "OptionsModel",
}


def _rows(connection: sqlite3.Connection, query: str, params=()):
    return [dict(row) for row in connection.execute(query, params)]


def _tables(connection: sqlite3.Connection):
    return {
        row[0].lower(): row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }


def _columns(connection: sqlite3.Connection, table: str):
    return [row[1] for row in connection.execute(f"PRAGMA table_info([{table}])")]


def _value(row: dict, *names, default=None):
    lowered = {str(key).lower(): value for key, value in row.items()}
    for name in names:
        if name.lower() in lowered and lowered[name.lower()] is not None:
            return lowered[name.lower()]
    return default


def _as_int(value, default=0):
    if value in (None, ""):
        return default
    return int(float(value))


def _as_float(value, default=0.0):
    if value in (None, ""):
        return default
    return float(value)


def _julian(year, day):
    return date(_as_int(year), 1, 1) + timedelta(days=_as_int(day) - 1)


def _iso(value: date):
    return datetime(value.year, value.month, value.day).isoformat()


def _insert(connection, table: str, row: dict):
    target = {name.lower(): name for name in _columns(connection, table)}
    values = {
        target[name.lower()]: value
        for name, value in row.items()
        if name.lower() in target
    }
    if not values:
        raise ValueError(f"No values match the CELSIUS V32 table {table}")
    columns = list(values)
    placeholders = ", ".join("?" for _ in columns)
    connection.execute(
        f"INSERT INTO [{table}] ({', '.join(f'[{name}]' for name in columns)}) "
        f"VALUES ({placeholders})",
        [values[name] for name in columns],
    )


def _first_row(connection, table: str):
    cursor = connection.execute(f"SELECT * FROM [{table}] LIMIT 1")
    row = cursor.fetchone()
    return dict(row) if row is not None else {}


def _validate_target(connection):
    available = set(_tables(connection).values())
    missing = sorted(REQUIRED_TARGET_TABLES - available)
    if missing:
        raise ValueError(
            "The target is not a CELSIUS V32 database; missing tables: "
            + ", ".join(missing)
        )


def _managements(source):
    crop_columns = {name.lower() for name in _columns(source, "CropManagement")}
    season_expression = (
        "cm.SeasonOrder" if "seasonorder" in crop_columns else "1"
    )
    plant_expression = (
        "cm.PlantOrder" if "plantorder" in crop_columns else "1"
    )
    offset_expression = (
        "cm.SowingYearOffset" if "sowingyearoffset" in crop_columns
        else "cm.SeasonYearOffset" if "seasonyearoffset" in crop_columns
        else "0"
    )
    rows = _rows(
        source,
        f"""
        SELECT cm.*, lc.IdcultivarCelsius, lc.IdCultivar AS MasterCultivar,
               lc.CodePSpecies,
               {season_expression} AS ResolvedSeasonOrder,
               {plant_expression} AS ResolvedPlantOrder,
               {offset_expression} AS ResolvedSowingYearOffset
        FROM CropManagement AS cm
        LEFT JOIN ListCultivars AS lc
          ON lower(lc.IdCultivar) = lower(cm.IdCultivar)
        ORDER BY lower(cm.idMangt), ResolvedSeasonOrder, ResolvedPlantOrder
        """,
    )
    grouped = defaultdict(lambda: defaultdict(list))
    for row in rows:
        management = str(_value(row, "idMangt"))
        season = _as_int(_value(row, "ResolvedSeasonOrder"), 1)
        plant = _as_int(_value(row, "ResolvedPlantOrder"), 1)
        row["SeasonOrder"] = season
        row["PlantOrder"] = plant
        row["SowingYearOffset"] = _as_int(
            _value(row, "ResolvedSowingYearOffset"), 0
        )
        grouped[management.lower()][season].append(row)
    for seasons in grouped.values():
        for plants in seasons.values():
            plants.sort(key=lambda item: item["PlantOrder"])
    return grouped


def _validate_plants(plants, management, season):
    orders = [row["PlantOrder"] for row in plants]
    if orders != list(range(1, len(orders) + 1)):
        raise ValueError(
            f"PlantOrder must be contiguous from 1 for {management!r}, "
            f"season {season}; found {orders}"
        )
    if len(plants) > 2:
        raise ValueError(
            f"CELSIUS V32 supports at most two associated crops; "
            f"{management!r}, season {season} has {len(plants)}"
        )
    for field in (
        "OFertiPolicyCode", "InoFertiPolicyCode", "IrrigationPolicyCode",
        "SoilTillPolicyCode",
    ):
        values = {str(_value(row, field, default="")) for row in plants}
        if len(values) > 1:
            raise ValueError(
                f"Associated crops must share {field} for {management!r}, "
                f"season {season}; found {sorted(values)}"
            )


def _season_definitions(simulation, seasons, mode):
    orders = sorted(seasons)
    if orders != list(range(1, len(orders) + 1)):
        raise ValueError(
            f"SeasonOrder for idMangt={_value(simulation, 'idMangt')!r} must "
            f"be contiguous from 1; found {orders}"
        )
    if mode == "standard" and len(orders) != 1:
        raise ValueError(
            f"Simulation {_value(simulation, 'idsim')!r} has {len(orders)} "
            "management seasons; use celsius_mode='successive'"
        )

    experiment_start = _julian(
        _value(simulation, "StartYear"), _value(simulation, "StartDay")
    )
    experiment_end = _julian(
        _value(simulation, "EndYear"), _value(simulation, "EndDay")
    )
    previous_end = None
    definitions = []
    for output_order, season_order in enumerate(orders, 1):
        plants = seasons[season_order]
        _validate_plants(
            plants, str(_value(simulation, "idMangt")), season_order
        )
        offsets = {
            _as_int(_value(plant, "SowingYearOffset", "SeasonYearOffset"), 0)
            for plant in plants
        }
        if len(offsets) != 1:
            raise ValueError(
                f"Associated crops must share SowingYearOffset in season {season_order}"
            )
        sowing_year = _as_int(_value(simulation, "StartYear")) + offsets.pop()
        sowing_dates = [
            _julian(sowing_year, _value(plant, "sowingdate")) for plant in plants
        ]
        harvest_dates = [
            sowing + timedelta(days=_as_int(_value(plant, "DHarvest"), 0))
            for sowing, plant in zip(sowing_dates, plants)
        ]
        season_start = experiment_start if previous_end is None else previous_end + timedelta(days=1)
        season_end = min(max(harvest_dates), experiment_end)
        if min(sowing_dates) < season_start:
            raise ValueError(
                f"Season {season_order} is sown before its CELSIUS period starts"
            )
        if season_start > season_end:
            raise ValueError(f"Empty CELSIUS period for season {season_order}")
        definitions.append(
            {
                "output_order": output_order,
                "season_order": season_order,
                "plants": plants,
                "sowing_dates": sowing_dates,
                "start": season_start,
                "end": season_end,
            }
        )
        previous_end = season_end
    return definitions


def _copy_weather(source, target, point_ids):
    target.execute("DELETE FROM Dweather")
    if not point_ids:
        return
    placeholders = ", ".join("?" for _ in point_ids)
    for row in _rows(
        source,
        f"""
        SELECT idPoint, year, DOY, Nmonth, NdayM, srad, tmax, tmin,
               tmoy, rain, Etppm
        FROM RAclimateD
        WHERE idPoint IN ({placeholders})
        ORDER BY idPoint, year, DOY
        """,
        tuple(point_ids),
    ):
        point = str(_value(row, "idPoint"))
        year = _as_int(_value(row, "year"))
        doy = _as_int(_value(row, "DOY"))
        _insert(
            target,
            "Dweather",
            {
                "idDclim": point,
                "idjourclim": f"{point}.{year}.{doy}",
                "annee": year,
                "jda": doy,
                "mois": _as_int(_value(row, "Nmonth")),
                "jour": _as_int(_value(row, "NdayM")),
                "rg": _as_float(_value(row, "srad")),
                "tmax": _as_float(_value(row, "tmax")),
                "tmin": _as_float(_value(row, "tmin")),
                "tmoy": _as_float(_value(row, "tmoy")),
                "plu": _as_float(_value(row, "rain")),
                "etp": _as_float(_value(row, "Etppm")),
            },
        )


def _copy_points(source, target, point_ids):
    defaults = _first_row(target, "ListPAnnexes")
    target.execute("DELETE FROM ListPAnnexes")
    if not point_ids:
        return
    placeholders = ", ".join("?" for _ in point_ids)
    for row in _rows(
        source,
        f"SELECT * FROM Coordinates WHERE idPoint IN ({placeholders})",
        tuple(point_ids),
    ):
        point = str(_value(row, "idPoint"))
        values = dict(defaults)
        values.update(
            {
                "idDclim": point,
                "CodePosteAnnexe": point,
                "latitudeDD": _as_float(_value(row, "latitudeDD")),
                "longitude": _as_float(_value(row, "longitudeDD")),
                "altitude": _as_int(_value(row, "altitude"), -99),
                "CO2c": _as_int(_value(defaults, "CO2c"), 350),
                "ConcNplu": _as_float(_value(defaults, "ConcNplu"), 0.0),
                "Codcc": "0",
            }
        )
        _insert(target, "ListPAnnexes", values)


def _copy_initial_conditions(source, target, initial_ids):
    defaults = _first_row(target, "ParamIni")
    target.execute("DELETE FROM ParamIni")
    if not initial_ids:
        return
    placeholders = ", ".join("?" for _ in initial_ids)
    for row in _rows(
        source,
        f"SELECT * FROM InitialConditions WHERE idIni IN ({placeholders})",
        tuple(initial_ids),
    ):
        values = dict(defaults)
        values.update(
            {
                "IdIni": str(_value(row, "idIni")),
                "Stockinit": _as_float(_value(row, "WStockinit")),
                "Ninit": _as_float(_value(row, "Ninit")),
                "Qpaillisinit": _as_float(_value(defaults, "Qpaillisinit")),
                "iniSolhautON": _as_int(_value(defaults, "iniSolhautON")),
            }
        )
        _insert(target, "ParamIni", values)


def _copy_soils(source, target, soil_ids):
    defaults = _first_row(target, "Soil")
    target.execute("DELETE FROM Soil")
    target.execute("DELETE FROM Soil_layers")
    if not soil_ids:
        return
    placeholders = ", ".join("?" for _ in soil_ids)
    soils = _rows(
        source,
        f"SELECT * FROM Soil WHERE IdSoil IN ({placeholders})",
        tuple(soil_ids),
    )
    layers_by_soil = defaultdict(list)
    if "soillayers" in _tables(source):
        for layer in _rows(
            source,
            f"SELECT * FROM SoilLayers WHERE idsoil IN ({placeholders}) "
            "ORDER BY idsoil, NumLayer",
            tuple(soil_ids),
        ):
            layers_by_soil[str(_value(layer, "idsoil")).lower()].append(layer)

    for soil in soils:
        soil_id = str(_value(soil, "IdSoil"))
        layers = layers_by_soil.get(soil_id.lower(), [])
        values = dict(defaults)
        values.update(
            {
                "idsoil": soil_id,
                "NbCouches": len(layers) or 1,
                "Zmes": _as_int(_value(soil, "SoilTotalDepth")),
                "ZObstacleRac": _as_float(_value(soil, "SoilRDepth")),
                "StockN": _as_float(_value(soil, "OrganicNStock")),
                "TypeRui": _as_int(_value(soil, "RunoffType"), 1),
                "Clay": _as_float(_value(soil, "clay")),
                "pHeau": _as_float(_value(soil, "pH")),
            }
        )
        _insert(target, "Soil", values)

        if not layers:
            layers = [soil]
        for index, layer in enumerate(layers, 1):
            bulk_density = _as_float(_value(layer, "bd", default=_value(soil, "bd")), 1.0)
            coarse = _as_float(_value(layer, "cf", default=_value(soil, "cf")), 0.0)
            depth = _as_float(
                _value(layer, "DepthCm", default=_value(soil, "SoilTotalDepth"))
            )
            upper = _as_float(_value(layer, "Lup"), 0.0)
            thickness = depth - upper if _value(layer, "Ldown") is not None else depth
            factor = (1.0 - coarse / 100.0) / bulk_density if bulk_density else 0.0
            _insert(
                target,
                "Soil_layers",
                {
                    "idsoil": soil_id,
                    "NumCouche": _as_int(_value(layer, "NumLayer"), index),
                    "epc": thickness,
                    "hcc": factor * _as_float(_value(layer, "Wfc", default=_value(soil, "Wfc"))),
                    "hmin": factor * _as_float(_value(layer, "Wwp", default=_value(soil, "Wwp"))),
                    "da": bulk_density,
                },
            )


def _operations(source, table, policy_column, policy):
    if table.lower() not in _tables(source) or policy in (None, "", "0", 0):
        return []
    return _rows(
        source,
        f"SELECT * FROM [{table}] WHERE lower([{policy_column}])=lower(?)",
        (str(policy),),
    )


def _resolve_cultivar(target, requested):
    """Resolve MasterInput's CELSIUS code to the V32 cultivar primary key."""
    requested = str(requested)
    if "cultivars" not in _tables(target):
        return requested
    columns = {name.lower() for name in _columns(target, "Cultivars")}
    if "idcultivar" not in columns:
        return requested
    row = target.execute(
        "SELECT IdCultivar FROM Cultivars WHERE lower(IdCultivar)=lower(?) LIMIT 1",
        (requested,),
    ).fetchone()
    if row:
        return str(row[0])
    if "codcultivar" in columns:
        matches = target.execute(
            "SELECT IdCultivar FROM Cultivars "
            "WHERE lower(CodCultivar)=lower(?)",
            (requested,),
        ).fetchall()
        if len(matches) == 1:
            return str(matches[0][0])
        if len(matches) > 1:
            raise ValueError(
                f"Ambiguous CELSIUS V32 cultivar code {requested!r}"
            )
    raise ValueError(
        f"CELSIUS V32 cultivar {requested!r} is absent from the template"
    )


def _build_technical_tables(source, target, generated):
    tech_defaults = _first_row(target, "Tech_Commun")
    crop_defaults = _first_row(target, "Tech_perCrop")
    for table in (
        "Tech_Commun", "Tech_perCrop", "Irrigation_List",
        "FertiMin_List", "FertiOrga_List",
    ):
        target.execute(f"DELETE FROM [{table}]")

    for item in generated:
        plants = item["plants"]
        first = plants[0]
        tech_id = item["tech_id"]
        mineral = _operations(
            source, "InorganicFOperations", "InorgFertiPolicyCode",
            _value(first, "InoFertiPolicyCode"),
        )
        organic = _operations(
            source, "OrganicFOperations", "OFertiPolicyCode",
            _value(first, "OFertiPolicyCode"),
        )
        irrigation = _operations(
            source, "IrrigationFOperations", "IrrigationPolicyCode",
            _value(first, "IrrigationPolicyCode"),
        )
        residues_on = [
            operation for operation in organic
            if str(_value(operation, "In_OnManure", default="")).lower() == "on"
        ]
        mulch_operation = residues_on[0] if residues_on else None
        residue_code = 1
        if mulch_operation is not None and "listresidues" in _tables(source):
            found = source.execute(
                "SELECT IdResidueCelsius FROM ListResidues "
                "WHERE lower(TypeResidues)=lower(?) LIMIT 1",
                (str(_value(mulch_operation, "TypeResidues")),),
            ).fetchone()
            if found and found[0] not in (None, ""):
                residue_code = found[0]

        tech = dict(tech_defaults)
        tech.update(
            {
                "IdTech_Com": tech_id,
                "NbCult": len(plants),
                "NomSC": tech_id,
                "IrrigON": int(bool(irrigation)),
                "fertiminON": int(bool(mineral)),
                "fertiorgON": int(bool(organic)),
                "tApportMinN": sum(_as_float(_value(row, "N")) for row in mineral),
                "tApportMON": sum(
                    _as_float(_value(row, "NFerti"))
                    * _as_float(_value(row, "Qmanure"))
                    for row in organic
                ),
                "imulch": 0 if mulch_operation is None else (
                    item["extended_sowing_days"][0]
                    + _as_int(_value(mulch_operation, "Dferti"))
                ),
                "CodParamMulch": residue_code,
                "QpaillisApport": 0.0 if mulch_operation is None else _as_float(
                    _value(mulch_operation, "Qmanure")
                ),
            }
        )
        _insert(target, "Tech_Commun", tech)

        for plant, sowing_day in zip(plants, item["extended_sowing_days"]):
            number = plant["PlantOrder"]
            cultivar = _value(plant, "IdcultivarCelsius") or _value(
                plant, "MasterCultivar", "IdCultivar"
            )
            cultivar = _resolve_cultivar(target, cultivar)
            crop = dict(crop_defaults)
            crop.update(
                {
                    "idTechPerCrop": f"{tech_id}.P{number}",
                    "idTech_Com": tech_id,
                    "NumCrop": number,
                    "IdCultivar": str(cultivar),
                    "TypInstal": 2,
                    "RepiquageON": 0,
                    "isem": sowing_day,
                    "ilev": sowing_day + 5,
                    "DensSem": _as_float(_value(plant, "sdens")),
                    "irepiqu": sowing_day,
                    "DensRepiqu": _as_float(_value(plant, "sdens")),
                    "DbutoirNouvSemis": sowing_day + 100,
                    "SemisAutoDebut": 0,
                }
            )
            _insert(target, "Tech_perCrop", crop)

        sowing = item["sowing_dates"][0]
        for index, operation in enumerate(irrigation, 1):
            operation_date = sowing + timedelta(days=_as_int(_value(operation, "DIrrigation")))
            _insert(target, "Irrigation_List", {
                "IdTech_Com": tech_id,
                "IdIrrig": str(_value(operation, "idIrrigation", default=index)),
                "DateIrrig": _iso(operation_date),
                "YearIrrig": operation_date.year,
                "JourYrIrrig": operation_date.timetuple().tm_yday,
                "Irrigation": _as_float(_value(operation, "IrrigationAmount")),
            })
        for index, operation in enumerate(mineral, 1):
            operation_date = sowing + timedelta(days=_as_int(_value(operation, "Dferti")))
            _insert(target, "FertiMin_List", {
                "IdTech_Com": tech_id,
                "IdFertiMin": str(_value(operation, "idFertInorg", default=index)),
                "DateFertiMin": _iso(operation_date),
                "YearFertiMin": operation_date.year,
                "JourYrFertiMin": operation_date.timetuple().tm_yday,
                "Nmineral": _as_float(_value(operation, "N")),
                "Pmineral": _as_float(_value(operation, "P")),
                "Kmineral": _as_float(_value(operation, "K")),
            })
        for index, operation in enumerate(organic, 1):
            operation_date = sowing + timedelta(days=_as_int(_value(operation, "Dferti")))
            quantity = _as_float(_value(operation, "Qmanure"))
            _insert(target, "FertiOrga_List", {
                "IdTech_Com": tech_id,
                "IdFertiOrg": str(_value(operation, "idFertOrga", default=index)),
                "DateFertiOrg": _iso(operation_date),
                "YearFertiOrg": operation_date.year,
                "JourYrFertiOrg": operation_date.timetuple().tm_yday,
                "Norganique": _as_float(_value(operation, "NFerti")) * quantity,
                "Porganique": _as_float(_value(operation, "PFerti")) * quantity,
                "Korganique": 0.0,
                "QorgaTot": quantity,
                "TypeMOrga": str(_value(operation, "TypeResidues", default="")),
                "CsurNRes": _as_float(_value(operation, "CNferti")),
            })


def _build_simulations(source, target, simulations, managements, mode):
    target.execute("DELETE FROM SimUnitList")
    generated = []
    champ_tri = 0
    general_default = _first_row(target, "General_Parameters")
    general_id = str(_value(general_default, "idGenParam", default="1"))
    for simulation in simulations:
        management = str(_value(simulation, "idMangt"))
        seasons = managements.get(management.lower())
        if not seasons:
            raise ValueError(f"No CropManagement rows for idMangt={management!r}")
        definitions = _season_definitions(simulation, seasons, mode)
        for definition in definitions:
            champ_tri += 1
            sequence = definition["output_order"]
            base_id = str(_value(simulation, "idsim"))
            sim_id = base_id if mode == "standard" else f"{base_id}__S{sequence:03d}"
            tech_id = f"{sim_id}__TECH"
            start = definition["start"]
            end = definition["end"]
            extended_sowing = [
                (sowing - date(start.year, 1, 1)).days + 1
                for sowing in definition["sowing_dates"]
            ]
            item = dict(definition)
            item.update(
                {
                    "sim_id": sim_id,
                    "tech_id": tech_id,
                    "sowing_dates": definition["sowing_dates"],
                    "extended_sowing_days": extended_sowing,
                }
            )
            generated.append(item)
            _insert(target, "SimUnitList", {
                "idsim": sim_id,
                "idTech_Com": tech_id,
                "idweather": str(_value(simulation, "idPoint")),
                "idsoil": str(_value(simulation, "idsoil")),
                "idIni": str(_value(simulation, "idIni")),
                "idGenParam": general_id,
                "idCodModel": _as_int(_value(simulation, "idOption"), 1),
                "StartYear": start.year,
                "StartDay": start.timetuple().tm_yday,
                "EndYear": end.year,
                "EndDay": end.timetuple().tm_yday,
                "codCC": "0",
                "ChampTri": champ_tri,
                "Situation": base_id,
                "codesuite": 0 if sequence == 1 else 1,
            })
    return generated


def _copy_options(source, target, option_ids):
    if "simulationoptions" not in _tables(source):
        return
    defaults = _first_row(target, "OptionsModel")
    target.execute("DELETE FROM OptionsModel")
    placeholders = ", ".join("?" for _ in option_ids)
    for row in _rows(
        source,
        f"SELECT * FROM SimulationOptions WHERE IdOptions IN ({placeholders})",
        tuple(option_ids),
    ):
        values = dict(defaults)
        values.update({
            "idCodModel": _as_int(_value(row, "IdOptions")),
            "ActiveWstress": _as_int(_value(row, "StressW_YN")),
            "ActiveNstress": _as_int(_value(row, "StressN_YN")),
            "ActivePstress": _as_int(_value(row, "StressP_YN")),
            "ActiveKstress": _as_int(_value(row, "StressK_YN")),
        })
        _insert(target, "OptionsModel", values)


# Lookups issued by the CELSIUS V32 engine for every simulation. Without
# these indexes each simulation scans the whole table (notably Dweather).
LOOKUP_INDEXES = {
    "Dweather": ("idDclim", "annee", "jda"),
    "Tech_perCrop": ("idTech_Com", "NumCrop"),
    "Soil_layers": ("idsoil", "NumCouche"),
    "Irrigation_List": ("IdTech_Com",),
    "FertiMin_List": ("IdTech_Com",),
    "FertiOrga_List": ("IdTech_Com",),
    "StadePheno": ("CodCultivar", "NumStade"),
}


def create_lookup_indexes(connection):
    """Index the per-simulation lookups present in a CELSIUS V32 database."""
    tables = _tables(connection)
    for table, columns in LOOKUP_INDEXES.items():
        actual = tables.get(table.lower())
        if actual is None:
            continue
        available = {name.lower() for name in _columns(connection, actual)}
        if not all(column.lower() in available for column in columns):
            continue
        connection.execute(
            f"CREATE INDEX IF NOT EXISTS [ix_modfilegen_{table.lower()}] "
            f"ON [{actual}] ({', '.join(f'[{column}]' for column in columns)})"
        )


def convert_database(
    master_input, celsius_database, mode="standard", simulation_ids=None
):
    """Populate an existing CELSIUS V32 database from MasterInput.

    Static V32 parameter tables (species, cultivars, phenological stages,
    general parameters, mulch and residue coefficients) remain in the V32
    template.  Simulation-specific tables are rebuilt transactionally.
    """
    mode = str(mode).strip().lower()
    if mode not in {"standard", "successive"}:
        raise ValueError("CELSIUS V32 mode must be 'standard' or 'successive'")
    master_input = Path(master_input)
    celsius_database = Path(celsius_database)
    if not master_input.exists():
        raise FileNotFoundError(master_input)
    if not celsius_database.exists():
        raise FileNotFoundError(celsius_database)

    with sqlite3.connect(master_input) as source, sqlite3.connect(celsius_database) as target:
        source.row_factory = sqlite3.Row
        target.row_factory = sqlite3.Row
        _validate_target(target)
        if simulation_ids:
            simulation_ids = [str(value) for value in simulation_ids]
            placeholders = ", ".join("?" for _ in simulation_ids)
            simulations = _rows(
                source,
                f"SELECT * FROM SimUnitList WHERE idsim IN ({placeholders}) "
                "ORDER BY idsim",
                tuple(simulation_ids),
            )
            missing = sorted(
                set(simulation_ids)
                - {str(_value(row, "idsim")) for row in simulations}
            )
            if missing:
                raise ValueError(
                    "Unknown CELSIUS simulation ids: " + ", ".join(missing)
                )
        else:
            simulations = _rows(source, "SELECT * FROM SimUnitList ORDER BY idsim")
        if not simulations:
            raise ValueError("No simulations selected for CELSIUS V32")
        managements = _managements(source)
        point_ids = sorted({str(_value(row, "idPoint")) for row in simulations})
        soil_ids = sorted({str(_value(row, "idsoil")) for row in simulations})
        initial_ids = sorted({str(_value(row, "idIni")) for row in simulations})
        option_ids = sorted({_as_int(_value(row, "idOption"), 1) for row in simulations})

        with target:
            generated = _build_simulations(
                source, target, simulations, managements, mode
            )
            _build_technical_tables(source, target, generated)
            _copy_weather(source, target, point_ids)
            _copy_points(source, target, point_ids)
            _copy_initial_conditions(source, target, initial_ids)
            _copy_soils(source, target, soil_ids)
            _copy_options(source, target, option_ids)
            create_lookup_indexes(target)
    return celsius_database
