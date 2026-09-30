"""Transform DSSAT synthesis outputs through the shared output catalogue."""

import math

import pandas as pd

from modfilegen.output_configuration import OutputConfigurationError


def _missing(value):
    if value is None:
        return True
    try:
        return bool(math.isnan(value)) or float(value) == -99.0
    except (TypeError, ValueError):
        return False


def transform_summary_dataframe(dataframe, output_configuration, selection="legacy"):
    """Map DSSAT ``Summary.OUT`` fields to configured shared columns.

    Mappings backed by other DSSAT output modules are intentionally returned as
    null until those modules have been parsed and aggregated.
    """
    source = dataframe.copy()
    source.columns = [str(column).strip() for column in source.columns]
    result = pd.DataFrame(index=source.index)
    identity_sources = {
        "Model": "Model",
        "IdSim": "Idsim",
        "Texte": "Texte",
        "SeasonOrder": "SeasonOrder",
        "PlantOrder": "PlantOrder",
        "lon": "lon",
        "lat": "lat",
        "time": "time",
    }
    for target, source_column in identity_sources.items():
        if source_column in source.columns:
            result[target] = source[source_column]
        elif target == "Model":
            result[target] = "Dssat"
        elif target in ("SeasonOrder", "PlantOrder"):
            result[target] = 1
        elif target == "Texte":
            result[target] = ""
        else:
            result[target] = None

    for variable in output_configuration.selected_variables(
        selection, "dssat", include_unavailable=True
    ):
        mapping = variable.mapping
        if (
            mapping is None
            or str(mapping.get("status", "")).lower() == "invalid"
            or mapping.get("source") != "Summary.OUT"
        ):
            result[variable.column] = None
            continue
        fields = output_configuration.source_fields(variable.key, "dssat")
        missing_fields = [field for field in fields if field not in source.columns]
        if not fields or missing_fields:
            if variable.definition.get("required", True):
                raise OutputConfigurationError(
                    "Champ DSSAT obligatoire absent de Summary.OUT: "
                    + ", ".join(missing_fields or ["<non configure>"])
                )
            result[variable.column] = None
            continue

        def convert(row, key=variable.key):
            values = tuple(row[field] for field in fields)
            if any(
                _missing(value)
                or output_configuration.is_missing_value(key, "dssat", value)
                for value in values
            ):
                return float("nan")
            combined = output_configuration.combine_values(key, "dssat", values)
            reference = row.get("PDAT")
            context = {"mode": "standard"}
            if not _missing(reference):
                context["reference_year"] = int(float(reference)) // 1000
            return output_configuration.convert_value(
                key, "dssat", combined, context=context
            )

        result[variable.column] = source.apply(convert, axis=1)

    numeric_columns = [
        variable.column
        for variable in output_configuration.selected_variables(selection)
        if variable.sql_type in ("INTEGER", "REAL")
    ]
    for column in numeric_columns:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")
            result[column] = result[column].mask(result[column] < 0)
    return result[list(output_configuration.summary_columns(selection))]
