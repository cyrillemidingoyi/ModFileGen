"""Construction des fichiers de sortie configurables de STICS v9."""

import pandas as pd

from modfilegen.output_configuration import (
    OutputConfiguration,
    OutputConfigurationError,
)


def stics_report_fields(output_configuration, selection="legacy", model="stics"):
    """Return unique valid STICS report fields in selection order."""
    fields = []
    seen = set()
    for variable in output_configuration.selected_variables(
        selection, model, include_unavailable=False
    ):
        mapping = variable.mapping
        if mapping.get("source") != "mod_rapport.sti":
            continue
        if str(mapping.get("status", "")).lower() == "invalid":
            continue
        for field in output_configuration.source_fields(variable.key, model):
            if field.lower() in seen:
                continue
            fields.append(field)
            seen.add(field.lower())
    return tuple(fields)


def build_rap_mod(output_configuration=None, selection="legacy", template=None, model="stics"):
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
    return "\n".join(
        [*header, *stics_report_fields(output_configuration, selection, model)]
    ) + "\n"


def transform_summary_dataframe(
    dataframe, output_configuration, selection="legacy", model="stics"
):
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
        selection, model, include_unavailable=True
    ):
        mapping = variable.mapping
        if mapping is None or str(mapping.get("status", "")).lower() == "invalid":
            result[variable.column] = None
            continue
        fields = output_configuration.source_fields(variable.key, model)
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
                key, model, tuple(row[field] for field in fields)
            ),
            axis=1,
        )
        result[variable.column] = combined.map(
            lambda value, key=variable.key: output_configuration.convert_value(
                key, model, value
            )
        )

    return result[list(output_configuration.summary_columns(selection))]


def write_canonical_summary_csv(
    raw_path,
    result_path,
    output_configuration,
    selection="legacy",
    existing_summary=None,
):
    """Transform raw rows and write one canonical STICS result CSV."""
    frames = []
    if existing_summary is not None and not existing_summary.empty:
        frames.append(
            existing_summary.reindex(
                columns=output_configuration.summary_columns(selection)
            )
        )
    if raw_path is not None:
        raw = pd.read_csv(raw_path)
        frames.append(
            transform_summary_dataframe(raw, output_configuration, selection)
        )
    if frames:
        result = pd.concat(frames, ignore_index=True)
    else:
        result = pd.DataFrame(
            columns=output_configuration.summary_columns(selection)
        )
    result.to_csv(result_path, index=False)
    return result
