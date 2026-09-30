"""Transform CELSIUS synthesis outputs through the shared output catalogue."""

import pandas as pd

from modfilegen.output_configuration import OutputConfigurationError


def transform_summary_dataframe(
    dataframe, output_configuration, selection="legacy", model="celsius"
):
    """Map raw OutputSynt fields to the selected shared columns."""
    model = str(model).strip().lower()
    if model not in {"celsius", "celsiusv32"}:
        raise OutputConfigurationError(f"Modele CELSIUS inconnu: {model}")
    source = dataframe.copy()
    source.columns = [str(column).strip() for column in source.columns]
    source_columns = {column.lower(): column for column in source.columns}
    result = pd.DataFrame(index=source.index)
    identity_sources = {
        "Model": "Model", "IdSim": "Idsim", "Texte": "Texte",
        "SeasonOrder": "SeasonOrder", "PlantOrder": "PlantOrder",
        "lon": "lon", "lat": "lat", "time": "time",
    }
    for target, requested_source in identity_sources.items():
        source_column = source_columns.get(requested_source.lower())
        if source_column is not None:
            result[target] = source[source_column]
        elif target == "Model":
            result[target] = "CelsiusV32" if model == "celsiusv32" else "Celsius"
        elif target in ("SeasonOrder", "PlantOrder"):
            result[target] = 1
        elif target == "Texte":
            result[target] = ""
        else:
            result[target] = None
    for variable in output_configuration.selected_variables(
        selection, model, include_unavailable=True
    ):
        mapping = variable.mapping
        if (
            mapping is None
            or str(mapping.get("status", "")).lower() == "invalid"
            or mapping.get("source") != "OutputSynt"
        ):
            result[variable.column] = None
            continue
        fields = output_configuration.source_fields(variable.key, model)
        resolved_fields = tuple(source_columns.get(field.lower()) for field in fields)
        missing_fields = [
            field for field, resolved in zip(fields, resolved_fields) if resolved is None
        ]
        if not fields or missing_fields:
            if variable.definition.get("required", True):
                raise OutputConfigurationError(
                    "Champ CELSIUS obligatoire absent de OutputSynt: "
                    + ", ".join(missing_fields or ["<non configure>"])
                )
            result[variable.column] = None
            continue
        def convert(row, key=variable.key, columns=resolved_fields):
            values = tuple(row[column] for column in columns)
            if any(
                output_configuration.is_missing_value(key, model, value)
                for value in values
            ):
                return float("nan")
            combined = output_configuration.combine_values(key, model, values)
            return output_configuration.convert_value(key, model, combined)
        result[variable.column] = source.apply(convert, axis=1)
    numeric_columns = [
        variable.column
        for variable in output_configuration.selected_variables(selection)
        if variable.sql_type in ("INTEGER", "REAL")
    ]
    for column in numeric_columns:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    return result[list(output_configuration.summary_columns(selection))]
