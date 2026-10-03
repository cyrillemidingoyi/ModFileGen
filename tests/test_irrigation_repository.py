import sqlite3

from modfilegen.irrigation_repository import IrrigationRepository


def test_prefetches_operations_by_management_and_keeps_policy_zero_empty():
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        """
        CREATE TABLE CropManagement (
            idMangt TEXT,
            IrrigationPolicyCode TEXT
        );
        CREATE TABLE IrrigationFOperations (
            IrrigationPolicyCode TEXT,
            IrrigationNumber INTEGER,
            DIrrigation INTEGER,
            IrrigationAmount REAL
        );
        INSERT INTO CropManagement VALUES ('M1', 'IRR1'), ('M2', '0');
        INSERT INTO IrrigationFOperations VALUES
            ('IRR1', 2, 30, 40.0),
            ('IRR1', 1, -10, 20.0),
            ('UNUSED', 1, 0, 99.0);
        """
    )

    repository = IrrigationRepository(connection)
    repository.prefetch_managements(["m1", "M2"])

    operations = repository.get_operations("irr1")
    assert [operation["IrrigationNumber"] for operation in operations] == [1, 2]
    assert repository.get_operations("0") == ()
    assert repository.get_operations("UNUSED") == ()

