from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Iterable, Optional
import sqlite3

@dataclass(frozen=True)
class ParameterDefinition:
    name: str
    value_type: Optional[str]
    minimum: Optional[float]
    maximum: Optional[float]
    default_value: Any

class ParameterResolver:
    def __init__(
        self,
        model_dictionary_connection: sqlite3.Connection,
        master_input_connection: sqlite3.Connection,
    ):
        self._model_dictionary = model_dictionary_connection
        self._master_input = master_input_connection

        self._definitions = {}
        self._overrides = {}
        self._resolved = {}
        self._prefetched = set()
        
    @staticmethod
    def _normalize(value):
        if value is None:
            return None
        return str(value).strip().casefold()
    
    def _convert_value(self, raw_value, definition):
        if raw_value is None:
            return None

        value_type = self._normalize(definition.value_type)

        if value_type in {"integer", "int"}:
            return int(raw_value)

        if value_type in {
            "double",
            "float",
            "real",
            "numeric",
            "decimal",
        }:
            return float(raw_value)

        if value_type in {"boolean", "bool", "bit"}:
            return self._convert_boolean(raw_value)

        return str(raw_value)
    
    def _convert_boolean(self, raw_value):
        if isinstance(raw_value, bool):
            return raw_value

        normalized = self._normalize(raw_value)
        if normalized in {"1", "true", "yes"}:
            return True
        if normalized in {"0", "false", "no"}:
            return False

        raise ValueError(f"Cannot convert value '{raw_value}' to boolean.")

    @staticmethod
    def _placeholders(values):
        return ", ".join("?" for _ in values)

    def _load_definitions(self, model, target_tables):
        tables = tuple(sorted(target_tables))
        query = f"""
            SELECT [Table], Champ, Type, Minnval, Maxval,
                   COALESCE(defaultValueOtherSource, Default_Value_Datamill)
            FROM Variables
            WHERE lower(model) = ?
              AND lower([Table]) IN ({self._placeholders(tables)})
        """
        rows = self._model_dictionary.execute(
            query, (model, *tables)
        ).fetchall()

        for target_table, name, value_type, minimum, maximum, default in rows:
            table_key = self._normalize(target_table)
            name_key = self._normalize(name)
            definition = ParameterDefinition(
                name=name,
                value_type=value_type,
                minimum=minimum,
                maximum=maximum,
                default_value=None,
            )
            definition = ParameterDefinition(
                name=name,
                value_type=value_type,
                minimum=minimum,
                maximum=maximum,
                default_value=self._convert_value(default, definition),
            )
            self._definitions.setdefault((model, table_key), {})[name_key] = definition

    def _load_overrides(self, model, target_tables, soil_ids):
        tables = tuple(sorted(target_tables))
        soils = tuple(sorted(soil_ids))
        if not soils:
            return

        # Keep batches below SQLite's commonly configured parameter limit.
        for offset in range(0, len(soils), 500):
            batch = soils[offset:offset + 500]
            query = f"""
                SELECT IdSoil, TargetTable, Parameter, Value
                FROM SoilParameterOverrides
                WHERE lower(Model) = ?
                  AND lower(TargetTable) IN ({self._placeholders(tables)})
                  AND lower(IdSoil) IN ({self._placeholders(batch)})
            """
            rows = self._master_input.execute(
                query, (model, *tables, *batch)
            ).fetchall()

            for id_soil, target_table, parameter, raw_value in rows:
                table_key = self._normalize(target_table)
                soil_key = self._normalize(id_soil)
                parameter_key = self._normalize(parameter)
                definition = self._definitions[(model, table_key)][parameter_key]
                value = self._convert_value(raw_value, definition)
                self._overrides.setdefault(
                    (model, table_key, soil_key), {}
                )[parameter_key] = value

    def prefetch(self, model: str, target_tables: Iterable[str], soil_ids: Iterable[str]):
        model_key = self._normalize(model)
        table_keys = {
            self._normalize(target_table)
            for target_table in target_tables
        }
        soil_keys = {
            self._normalize(id_soil)
            for id_soil in soil_ids
            if id_soil is not None
        }

        if not table_keys:
            return

        self._load_definitions(model_key, table_keys)
        self._load_overrides(model_key, table_keys, soil_keys)

        for table_key in table_keys:
            definitions = self._definitions.get((model_key, table_key), {})
            defaults = {
                name: definition.default_value
                for name, definition in definitions.items()
            }
            for soil_key in soil_keys:
                resolved = defaults.copy()
                resolved.update(
                    self._overrides.get((model_key, table_key, soil_key), {})
                )
                self._resolved[(model_key, table_key, soil_key)] = MappingProxyType(
                    resolved
                )
                self._prefetched.add((model_key, table_key, soil_key))

    def resolve(self, model: str, target_table: str, id_soil: str):
        key = (
            self._normalize(model),
            self._normalize(target_table),
            self._normalize(id_soil),
        )
        try:
            return self._resolved[key]
        except KeyError as exc:
            raise RuntimeError(
                "Parameters were not prefetched for "
                f"model={model!r}, table={target_table!r}, idSoil={id_soil!r}"
            ) from exc

    def has_override(
        self, model: str, target_table: str, parameter: str, id_soil: str
    ) -> bool:
        key = (
            self._normalize(model),
            self._normalize(target_table),
            self._normalize(id_soil),
        )
        return self._normalize(parameter) in self._overrides.get(key, {})
