"""Configuration commune des sorties des modèles de culture.

Ce module ne lit aucun fichier de sortie propre à un modèle. Il charge les
catalogues, résout les variables sélectionnées, applique leurs conversions et
maintient le schéma partagé de ``SummaryOutput``.
"""

from __future__ import annotations

import calendar
import math
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple

import yaml


_DEFAULT_CONFIG_DIR = Path(__file__).resolve().parent / "config" / "outputs"
_SQL_TYPES = {"INTEGER", "REAL", "TEXT"}


class OutputConfigurationError(ValueError):
    """Configuration de sortie absente, incohérente ou non prise en charge."""


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise OutputConfigurationError(
                f"Cle YAML dupliquee {key!r} dans {loader.name}"
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            loader = _UniqueKeyLoader(stream)
            loader.name = str(path)
            try:
                data = loader.get_single_data()
            finally:
                loader.dispose()
    except FileNotFoundError as exc:
        raise OutputConfigurationError(f"Fichier de configuration absent: {path}") from exc
    except yaml.YAMLError as exc:
        raise OutputConfigurationError(f"YAML invalide dans {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise OutputConfigurationError(f"La racine YAML doit etre un objet: {path}")
    return data


def _normalise_model(model: str) -> str:
    value = str(model).strip().lower()
    if not value:
        raise OutputConfigurationError("Le nom du modele ne peut pas etre vide")
    return value


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(math.isnan(value))
    except (TypeError, ValueError):
        return False


def _dssat_extended_day_index(value: Any, context: Mapping[str, Any]) -> float:
    numeric = int(float(value))
    if numeric < 0:
        return float("nan")
    source_year, doy = divmod(numeric, 1000)
    if doy <= 0:
        return float("nan")
    reference_year = context.get("reference_year")
    if reference_year is None:
        raise OutputConfigurationError(
            "La transformation dssat_extended_day_index exige reference_year"
        )
    reference_year = int(reference_year)
    mode = str(context.get("mode", "successive")).lower()
    if mode == "standard":
        # Reproduit extract_corrected_doy utilisé par le convertisseur standard.
        correction = 0
        if source_year > reference_year:
            correction = 366 if calendar.isleap(source_year) else 365
        return float(doy + correction)
    return float((date(source_year, 1, 1) - date(reference_year, 1, 1)).days + doy)


@dataclass(frozen=True)
class OutputVariable:
    """Variable canonique et correspondance facultative pour un modèle."""

    key: str
    definition: Mapping[str, Any]
    model: Optional[str] = None
    mapping: Optional[Mapping[str, Any]] = None

    @property
    def column(self) -> str:
        return str(self.definition["column"])

    @property
    def sql_type(self) -> str:
        return str(self.definition["type"]).upper()

    @property
    def available(self) -> bool:
        return self.mapping is not None


class OutputConfiguration:
    """Catalogue validé des sorties partagées.

    Les chemins omis utilisent les trois fichiers embarqués dans le package.
    Les convertisseurs peuvent donc recevoir trois chemins utilisateur sans
    changer l'API commune.
    """

    def __init__(
        self,
        variables: Mapping[str, Any],
        selections: Mapping[str, Any],
        profiles: Mapping[str, Any],
    ) -> None:
        self.variables_document = dict(variables)
        self.selections_document = dict(selections)
        self.profiles_document = dict(profiles)
        self._validate()

    @classmethod
    def from_files(
        cls,
        variables_path: Optional[str] = None,
        selections_path: Optional[str] = None,
        profiles_path: Optional[str] = None,
    ) -> "OutputConfiguration":
        paths = (
            Path(variables_path) if variables_path else _DEFAULT_CONFIG_DIR / "output_variables.yml",
            Path(selections_path) if selections_path else _DEFAULT_CONFIG_DIR / "output_selections.yml",
            Path(profiles_path) if profiles_path else _DEFAULT_CONFIG_DIR / "profile_variables.yml",
        )
        return cls(*(_load_yaml(path) for path in paths))

    @property
    def variables(self) -> Mapping[str, Mapping[str, Any]]:
        return self.variables_document["variables"]

    @property
    def profile_variables(self) -> Mapping[str, Mapping[str, Any]]:
        return self.profiles_document["variables"]

    @property
    def identity_columns(self) -> Mapping[str, str]:
        return self.variables_document["summary_output"]["identity_columns"]

    @property
    def summary_table(self) -> str:
        return str(self.variables_document["summary_output"]["table"])

    def selection_names(self) -> Tuple[str, ...]:
        return tuple(self.selections_document["selections"])

    def selected_keys(self, selection: str = "legacy") -> Tuple[str, ...]:
        try:
            return tuple(self.selections_document["selections"][selection]["variables"])
        except KeyError as exc:
            raise OutputConfigurationError(
                f"Selection inconnue {selection!r}; choix: {', '.join(self.selection_names())}"
            ) from exc

    def selected_variables(
        self,
        selection: str = "legacy",
        model: Optional[str] = None,
        include_unavailable: bool = True,
    ) -> Tuple[OutputVariable, ...]:
        model_key = _normalise_model(model) if model is not None else None
        result = []
        for key in self.selected_keys(selection):
            definition = self.variables[key]
            mapping = None if model_key is None else definition.get("models", {}).get(model_key)
            variable = OutputVariable(key, definition, model_key, mapping)
            if include_unavailable or model_key is None or variable.available:
                result.append(variable)
        return tuple(result)

    def summary_columns(self, selection: str = "legacy") -> Mapping[str, str]:
        columns = dict(self.identity_columns)
        for variable in self.selected_variables(selection):
            columns[variable.column] = variable.sql_type
        return columns

    def model_mapping(self, variable: str, model: str) -> Optional[Mapping[str, Any]]:
        if variable not in self.variables:
            raise OutputConfigurationError(f"Variable inconnue: {variable}")
        return self.variables[variable].get("models", {}).get(_normalise_model(model))

    def source_fields(self, variable: str, model: str) -> Tuple[str, ...]:
        """Return the source field(s) used to build a canonical variable."""
        mapping = self.model_mapping(variable, model)
        if mapping is None:
            return ()
        field = str(mapping.get("field", "")).strip()
        fields = mapping.get("fields")
        if field and fields is not None:
            raise OutputConfigurationError(
                f"{variable}/{model}: field et fields sont mutuellement exclusifs"
            )
        if field:
            return (field,)
        if fields is None:
            return ()
        if not isinstance(fields, list) or not fields:
            raise OutputConfigurationError(
                f"{variable}/{model}: fields doit etre une liste non vide"
            )
        result = tuple(str(item).strip() for item in fields)
        if any(not item for item in result):
            raise OutputConfigurationError(
                f"{variable}/{model}: fields contient un nom vide"
            )
        return result

    def combine_values(self, variable: str, model: str, values: Tuple[Any, ...]) -> Any:
        """Combine one or more raw model values before unit conversion."""
        mapping = self.model_mapping(variable, model)
        if mapping is None:
            return None
        fields = self.source_fields(variable, model)
        if len(values) != len(fields):
            raise OutputConfigurationError(
                f"{variable}/{model}: {len(values)} valeurs pour {len(fields)} champs"
            )
        if any(_is_missing(value) for value in values):
            return float("nan")
        operation = mapping.get("operation", "identity")
        if operation == "identity" and len(values) == 1:
            return values[0]
        if operation == "sum":
            return sum(float(value) for value in values)
        raise OutputConfigurationError(
            f"Operation inconnue {operation!r} pour {variable}/{model}"
        )

    def is_missing_value(self, variable: str, model: str, value: Any) -> bool:
        """Return whether a raw value is missing, including mapping sentinels."""
        if _is_missing(value):
            return True
        mapping = self.model_mapping(variable, model)
        if mapping is None:
            return True
        for sentinel in mapping.get("missing_values", []):
            try:
                if float(value) == float(sentinel):
                    return True
            except (TypeError, ValueError):
                if value == sentinel:
                    return True
        return False

    def convert_value(
        self,
        variable: str,
        model: str,
        value: Any,
        context: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        mapping = self.model_mapping(variable, model)
        if mapping is None:
            return None
        if self.is_missing_value(variable, model, value):
            return float("nan")
        transform = mapping.get("transform")
        if transform in (None, "identity"):
            converted = value
        elif transform == "dssat_extended_day_index":
            converted = _dssat_extended_day_index(value, context or {})
        else:
            raise OutputConfigurationError(
                f"Transformation inconnue {transform!r} pour {variable}/{model}"
            )
        if _is_missing(converted):
            return converted
        scale = float(mapping.get("scale", 1.0))
        offset = float(mapping.get("offset", 0.0))
        if scale != 1.0 or offset != 0.0:
            converted = float(converted) * scale + offset
        sql_type = str(self.variables[variable]["type"]).upper()
        if sql_type == "INTEGER":
            return int(converted)
        if sql_type == "REAL":
            return float(converted)
        return converted

    def ensure_summary_output_schema(
        self,
        connection: sqlite3.Connection,
        selection: str = "legacy",
    ) -> Tuple[str, ...]:
        """Crée/étend SummaryOutput et retourne les colonnes ajoutées.

        Les comparaisons de noms sont insensibles à la casse afin de préserver
        les bases historiques qui utilisent tantôt ``IdSim``, tantôt ``Idsim``.
        """
        table = self.summary_table
        columns = self.summary_columns(selection)
        table_sql = table.replace('"', '""')
        existing = connection.execute(f'PRAGMA table_info("{table_sql}")').fetchall()
        if not existing:
            definitions = ", ".join(
                f'"{name.replace(chr(34), chr(34) * 2)}" {sql_type}'
                for name, sql_type in columns.items()
            )
            connection.execute(f'CREATE TABLE "{table_sql}" ({definitions})')
            return tuple(columns)
        names_lower = {str(row[1]).lower() for row in existing}
        added = []
        for name, sql_type in columns.items():
            if name.lower() in names_lower:
                continue
            escaped = name.replace('"', '""')
            connection.execute(
                f'ALTER TABLE "{table_sql}" ADD COLUMN "{escaped}" {sql_type}'
            )
            names_lower.add(name.lower())
            added.append(name)
        return tuple(added)

    def _validate(self) -> None:
        for label, document in (
            ("variables", self.variables_document),
            ("selections", self.selections_document),
            ("profiles", self.profiles_document),
        ):
            if document.get("version") != 1:
                raise OutputConfigurationError(f"Version {label} non prise en charge")
        summary = self.variables_document.get("summary_output")
        variables = self.variables_document.get("variables")
        selections = self.selections_document.get("selections")
        profiles = self.profiles_document.get("variables")
        if not isinstance(summary, dict) or not isinstance(summary.get("identity_columns"), dict):
            raise OutputConfigurationError("summary_output.identity_columns est obligatoire")
        if not isinstance(variables, dict) or not variables:
            raise OutputConfigurationError("Le catalogue de variables est vide")
        if not isinstance(selections, dict) or not selections:
            raise OutputConfigurationError("Le catalogue de selections est vide")
        if not isinstance(profiles, dict):
            raise OutputConfigurationError("Le catalogue de profils est invalide")
        seen_columns = {str(name).lower() for name in summary["identity_columns"]}
        for key, definition in variables.items():
            if not isinstance(definition, dict):
                raise OutputConfigurationError(f"Definition invalide pour {key}")
            for required in ("column", "type", "unit", "models"):
                if required not in definition:
                    raise OutputConfigurationError(f"{key}: champ {required} absent")
            sql_type = str(definition["type"]).upper()
            if sql_type not in _SQL_TYPES:
                raise OutputConfigurationError(f"{key}: type SQL non pris en charge {sql_type}")
            column = str(definition["column"])
            if column.lower() in seen_columns:
                raise OutputConfigurationError(f"Colonne SummaryOutput dupliquee: {column}")
            seen_columns.add(column.lower())
            if not isinstance(definition["models"], dict):
                raise OutputConfigurationError(f"{key}: models doit etre un objet")
            for model, mapping in definition["models"].items():
                if not isinstance(mapping, dict):
                    raise OutputConfigurationError(
                        f"{key}/{model}: la correspondance doit etre un objet"
                    )
                field = str(mapping.get("field", "")).strip()
                fields = mapping.get("fields")
                if field and fields is not None:
                    raise OutputConfigurationError(
                        f"{key}/{model}: field et fields sont mutuellement exclusifs"
                    )
                if fields is not None and (
                    not isinstance(fields, list)
                    or not fields
                    or any(not str(item).strip() for item in fields)
                ):
                    raise OutputConfigurationError(
                        f"{key}/{model}: fields doit etre une liste de noms non vides"
                    )
                operation = mapping.get("operation", "identity")
                if operation not in ("identity", "sum"):
                    raise OutputConfigurationError(
                        f"{key}/{model}: operation inconnue {operation!r}"
                    )
                if fields is not None and len(fields) > 1 and operation == "identity":
                    raise OutputConfigurationError(
                        f"{key}/{model}: plusieurs champs exigent une operation"
                    )
        for name, selection in selections.items():
            keys = selection.get("variables") if isinstance(selection, dict) else None
            if not isinstance(keys, list):
                raise OutputConfigurationError(f"Selection {name}: variables doit etre une liste")
            duplicates = [key for key in keys if keys.count(key) > 1]
            if duplicates:
                raise OutputConfigurationError(
                    f"Selection {name}: variable dupliquee {duplicates[0]}"
                )
            unknown = [key for key in keys if key not in variables]
            if unknown:
                raise OutputConfigurationError(
                    f"Selection {name}: variable inconnue {unknown[0]}"
                )
