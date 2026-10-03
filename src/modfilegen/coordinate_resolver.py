from dataclasses import dataclass
from typing import Iterable, Optional
import sqlite3


@dataclass(frozen=True)
class CoordinateValues:
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude: Optional[float] = None


class CoordinateResolver:
    """Prefetch and resolve authoritative point coordinates from MasterInput."""

    def __init__(self, master_input_connection: sqlite3.Connection):
        self._master_input = master_input_connection
        self._coordinates = {}
        self._prefetched = set()

    @staticmethod
    def _normalize(value):
        if value is None:
            return None
        return str(value).strip().casefold()

    @staticmethod
    def _placeholders(values):
        return ", ".join("?" for _ in values)

    def prefetch(self, point_ids: Iterable[str]):
        points = {
            self._normalize(point_id)
            for point_id in point_ids
            if point_id is not None
        }
        missing = tuple(sorted(points.difference(self._prefetched)))
        if not missing:
            return

        for offset in range(0, len(missing), 500):
            batch = missing[offset:offset + 500]
            rows = self._master_input.execute(
                f"""
                SELECT idPoint, latitudeDD, longitudeDD, altitude
                FROM Coordinates
                WHERE lower(idPoint) IN ({self._placeholders(batch)})
                """,
                batch,
            ).fetchall()
            for point_id, latitude, longitude, altitude in rows:
                self._coordinates[self._normalize(point_id)] = CoordinateValues(
                    latitude=latitude,
                    longitude=longitude,
                    altitude=altitude,
                )

        self._prefetched.update(missing)

    def resolve(self, point_id: str) -> CoordinateValues:
        key = self._normalize(point_id)
        if key not in self._prefetched:
            raise RuntimeError(
                f"Coordinates were not prefetched for idPoint={point_id!r}"
            )
        return self._coordinates.get(key, CoordinateValues())
