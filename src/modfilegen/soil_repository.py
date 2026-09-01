from collections import defaultdict
import sqlite3
from typing import Iterable


class SoilDataRepository:
    """Preloaded access to Soil, RunoffTypes and SoilLayers records."""

    def __init__(self, master_input_connection: sqlite3.Connection):
        self._connection = master_input_connection
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

        for offset in range(0, len(soils), 500):
            batch = soils[offset:offset + 500]
            placeholders = self._placeholders(batch)

            soil_cursor = self._connection.execute(
                f"""
                    SELECT Soil.IdSoil, Soil.SoilOption, Soil.OrganicC,
                           Soil.OrganicNStock AS OrganicNStock,
                           Soil.SoilRDepth, Soil.SoilTotalDepth,
                           Soil.SoilTextureType, Soil.Wwp, Soil.Wfc,
                           Soil.bd, Soil.albedo, Soil.Ph AS pH, Soil.cf,
                           RunoffTypes.RunoffCoefBSoil AS RunoffCoefBSoil,
                           Soil.Clay AS Clay, Soil.sand AS Sand
                    FROM Soil
                    INNER JOIN RunoffTypes
                        ON RunoffTypes.RunoffType = Soil.RunoffType
                    WHERE lower(Soil.IdSoil) IN ({placeholders})
                """,
                batch,
            )
            for soil in self._as_dicts(soil_cursor):
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
