from collections import defaultdict
import sqlite3
from typing import Iterable, Optional

# Soil albedo re-estimated from Soil.SoilTextureType (albedoStrategy "texture").
TEXTURE_ALBEDO = {
    "clay": 0.28,
    "clay loam": 0.20,
    "clayey loam": 0.20,
    "loam": 0.225,
    "loamy sand": 0.25,
}
DEFAULT_ALBEDO = 0.30  # texture missing or not in TEXTURE_ALBEDO
ALBEDO_STRATEGIES = ("measured", "texture")


def runoff_type_code(value):
    """Runoff type code as text: 2, 2.0 and '2' all give '2'.

    ``Soil.RunoffType`` and ``RunoffTypes.RunoffType`` are text codes; older
    databases may store them as numbers.
    """
    if value is None:
        return None
    text = str(value).strip()
    try:
        number = float(text)
    except ValueError:
        return text
    return str(int(number)) if number.is_integer() else text


def texture_albedo(texture):
    """Albedo of a soil texture class; DEFAULT_ALBEDO when unknown."""
    return TEXTURE_ALBEDO.get(str(texture).strip().lower(), DEFAULT_ALBEDO)


def soil_albedo(soil, albedo_strategy="measured"):
    """Soil albedo for ``albedo_strategy``: Soil.albedo or the texture estimate."""
    strategy = str(albedo_strategy).strip().casefold()
    if strategy not in ALBEDO_STRATEGIES:
        raise ValueError(
            f"albedoStrategy must be one of {ALBEDO_STRATEGIES}, not {albedo_strategy!r}"
        )
    if strategy == "texture":
        return texture_albedo(soil.get("SoilTextureType"))
    return soil.get("albedo")


class SoilDataRepository:
    """Preloaded access to Soil, RunoffTypes and SoilLayers records.

    ``albedo`` follows ``albedo_strategy``, by default the global option
    ``GlobalVariables["albedoStrategy"]``: ``"measured"`` keeps
    ``Soil.albedo``, ``"texture"`` re-estimates it from ``SoilTextureType``.
    Every converter reading soils through this repository uses the same value.
    """

    def __init__(
        self, master_input_connection: sqlite3.Connection,
        albedo_strategy: Optional[str] = None,
    ):
        if albedo_strategy is None:
            from modfilegen import GlobalVariables
            albedo_strategy = GlobalVariables.get("albedoStrategy", "measured")
        self._connection = master_input_connection
        self._albedo_strategy = albedo_strategy
        soil_albedo({}, albedo_strategy)  # reject an unknown strategy early
        self._soils = {}
        self._layers = defaultdict(list)

    @staticmethod
    def _normalize(value):
        return str(value).strip().casefold()

    @staticmethod
    def _placeholders(values):
        return ", ".join("?" for _ in values)

    @staticmethod
    def _as_dicts(cursor):
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def prefetch(self, soil_ids: Iterable[str]):
        soils = tuple(sorted({
            self._normalize(id_soil)
            for id_soil in soil_ids
            if id_soil is not None
        }))

        runoff_types = self._runoff_types()
        for offset in range(0, len(soils), 500):
            batch = soils[offset:offset + 500]
            placeholders = self._placeholders(batch)

            soil_cursor = self._connection.execute(
                f"""
                    SELECT Soil.*, Soil.IdSoil, Soil.SoilOption, Soil.OrganicC,
                           Soil.OrganicNStock AS OrganicNStock,
                           Soil.SoilRDepth, Soil.SoilTotalDepth,
                           Soil.SoilTextureType, Soil.Wwp, Soil.Wfc,
                           Soil.bd, Soil.albedo, Soil.Ph AS pH, Soil.cf,
                           Soil.Clay AS Clay, Soil.sand AS Sand
                    FROM Soil
                    WHERE lower(Soil.IdSoil) IN ({placeholders})
                """,
                batch,
            )
            for soil in self._as_dicts(soil_cursor):
                soil["albedo"] = soil_albedo(soil, self._albedo_strategy)
                soil["RunoffType"] = runoff_type_code(soil.get("RunoffType"))
                runoff = runoff_types.get(soil["RunoffType"], {})
                soil["RunoffCoefBSoil"] = runoff.get("runoffcoefbsoil")
                soil["CurveNumber"] = runoff.get("curvenumber")
                self._soils[self._normalize(soil["IdSoil"])] = soil

            layer_cursor = self._connection.execute(
                f"""
                    SELECT *
                    FROM SoilLayers
                    WHERE lower(IdSoil) IN ({placeholders})
                    ORDER BY lower(IdSoil), NumLayer
                """,
                batch,
            )
            for layer in self._as_dicts(layer_cursor):
                self._layers[self._normalize(layer["idsoil"])].append(layer)

    def _runoff_types(self):
        """RunoffTypes rows by text code, with lower-case column names."""
        cursor = self._connection.execute("SELECT * FROM RunoffTypes")
        return {
            runoff_type_code(row["runofftype"]): row
            for row in (
                {key.casefold(): value for key, value in record.items()}
                for record in self._as_dicts(cursor)
            )
        }

    def get_soil(self, id_soil: str):
        return self._soils[self._normalize(id_soil)]

    def get_layers(self, id_soil: str):
        return self._layers.get(self._normalize(id_soil), ())
