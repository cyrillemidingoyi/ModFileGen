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

        # DSSAT reads the runoff curve number; older databases may lack it.
        runoff_columns = {
            str(row[1]).casefold()
            for row in self._connection.execute("PRAGMA table_info(RunoffTypes)")
        }
        curve_number = (
            "RunoffTypes.CurveNumber" if "curvenumber" in runoff_columns else "NULL"
        )
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
                           RunoffTypes.RunoffCoefBSoil AS RunoffCoefBSoil,
                           {curve_number} AS CurveNumber,
                           Soil.Clay AS Clay, Soil.sand AS Sand
                    FROM Soil
                    LEFT JOIN RunoffTypes
                        ON RunoffTypes.RunoffType = Soil.RunoffType
                    WHERE lower(Soil.IdSoil) IN ({placeholders})
                """,
                batch,
            )
            for soil in self._as_dicts(soil_cursor):
                soil["albedo"] = soil_albedo(soil, self._albedo_strategy)
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

    def get_soil(self, id_soil: str):
        return self._soils[self._normalize(id_soil)]

    def get_layers(self, id_soil: str):
        return self._layers.get(self._normalize(id_soil), ())
