"""Construction des fichiers de sortie configurables de STICS v9."""

import pandas as pd

from modfilegen.output_configuration import (
    OutputConfiguration,
    OutputConfigurationError,
)


def stics_report_fields(output_configuration, selection="legacy"):
    """Return unique valid STICS report fields in selection order."""
    fields = []
    seen = set()
    for variable in output_configuration.selected_variables(
        selection, "stics", include_unavailable=False
    ):
        mapping = variable.mapping
        if mapping.get("source") != "mod_rapport.sti":
            continue
        if str(mapping.get("status", "")).lower() == "invalid":
            continue
        for field in output_configuration.source_fields(variable.key, "stics"):
            if field.lower() in seen:
                continue
            fields.append(field)
            seen.add(field.lower())
    return tuple(fields)


def build_rap_mod(output_configuration=None, selection="legacy", template=None):
    """Build ``rap.mod`` from a configured shared-output selection.

    The first five STICS control lines are preserved from an optional template;
    only the requested report-variable lines are rebuilt from the catalog.
    """
    if output_configuration is None:
        output_configuration = OutputConfiguration.from_files()
    if template is None:
        header = ["1", "1", "2", "1", "rec"]
    else:
        lines = [line.strip() for line in template.splitlines() if line.strip()]
        if len(lines) < 5:
            raise OutputConfigurationError(
                "Le modele rap.mod doit contenir au moins cinq lignes de controle"
            )
        header = lines[:5]
    return "\n".join([*header, *stics_report_fields(output_configuration, selection)]) + "\n"


def transform_summary_dataframe(dataframe, output_configuration, selection="legacy"):
    """Transform a raw STICS report frame into selected shared columns."""
    source = dataframe.copy()
    source.columns = [str(column).strip() for column in source.columns]

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
    result = pd.DataFrame(index=source.index)
    for target, source_column in identity_sources.items():
        if source_column in source.columns:
            result[target] = source[source_column]
        elif target == "Model":
            result[target] = "Stics"
        elif target in ("SeasonOrder", "PlantOrder"):
            result[target] = 1
        elif target == "Texte":
            result[target] = ""
        else:
            result[target] = None

    for variable in output_configuration.selected_variables(
        selection, "stics", include_unavailable=True
    ):
        mapping = variable.mapping
        if mapping is None or str(mapping.get("status", "")).lower() == "invalid":
            result[variable.column] = None
            continue
        fields = output_configuration.source_fields(variable.key, "stics")
        missing_fields = [field for field in fields if field not in source.columns]
        if not fields or missing_fields:
            if variable.definition.get("required", True):
                raise OutputConfigurationError(
                    "Champ STICS obligatoire absent de mod_rapport.sti: "
                    + ", ".join(missing_fields or ["<non configure>"])
                )
            result[variable.column] = None
            continue
        combined = source[list(fields)].apply(
            lambda row, key=variable.key: output_configuration.combine_values(
                key, "stics", tuple(row[field] for field in fields)
            ),
            axis=1,
        )
        result[variable.column] = combined.map(
            lambda value, key=variable.key: output_configuration.convert_value(
                key, "stics", value
            )
        )

    return result[list(output_configuration.summary_columns(selection))]
