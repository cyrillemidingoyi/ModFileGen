from collections import defaultdict
import sqlite3
from typing import Iterable


class IrrigationRepository:
    """Prefetched access to dated irrigation operations by policy code."""

    def __init__(self, master_input_connection: sqlite3.Connection):
        self._connection = master_input_connection
        self._operations = defaultdict(list)

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

    def prefetch_managements(self, management_ids: Iterable[str]):
        management_ids = tuple(sorted({
            self._normalize(management_id)
            for management_id in management_ids
            if management_id is not None
        }))
        policy_codes = set()
        for offset in range(0, len(management_ids), 500):
            batch = management_ids[offset:offset + 500]
            cursor = self._connection.execute(
                f"""
                    SELECT DISTINCT IrrigationPolicyCode
                    FROM CropManagement
                    WHERE lower(idMangt) IN ({self._placeholders(batch)})
                """,
                batch,
            )
            policy_codes.update(
                self._normalize(row[0])
                for row in cursor.fetchall()
                if row[0] is not None and self._normalize(row[0]) != "0"
            )

        policy_codes = tuple(sorted(policy_codes))
        for offset in range(0, len(policy_codes), 500):
            batch = policy_codes[offset:offset + 500]
            cursor = self._connection.execute(
                f"""
                    SELECT IrrigationPolicyCode, IrrigationNumber,
                           DIrrigation, IrrigationAmount
                    FROM IrrigationFOperations
                    WHERE lower(IrrigationPolicyCode) IN (
                        {self._placeholders(batch)}
                    )
                    ORDER BY lower(IrrigationPolicyCode), IrrigationNumber
                """,
                batch,
            )
            for operation in self._as_dicts(cursor):
                policy = self._normalize(operation["IrrigationPolicyCode"])
                self._operations[policy].append(operation)

    def get_operations(self, policy_code):
        if policy_code is None or self._normalize(policy_code) == "0":
            return ()
        return self._operations.get(self._normalize(policy_code), ())
