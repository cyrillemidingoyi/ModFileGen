import pandas as pd

from modfilegen.Converter.SticsV11Converter.sticsstationconverter import (
    SticsStationConverter,
)


def test_station_formatter_uses_point_override_and_preserves_other_defaults():
    class FakeResolver:
        def has_point_override(self, model, table, parameter, point_id):
            return parameter.casefold() == "aclim"

    converter = SticsStationConverter()
    converter._parameter_resolver = FakeResolver()
    converter._point_id = "point-a"
    converter._point_parameters = {"aclim": 18.0}
    defaults = pd.DataFrame([
        {"Champ": "aclim", "dv": 20.0},
        {"Champ": "concrr", "dv": 0.02},
    ])

    assert converter.FormatSticsData(defaults, "aclim", 6) == (
        "aclim\n18.000000\n"
    )
    assert converter.FormatSticsData(defaults, "concrr", 2) == (
        "concrr\n0.02\n"
    )
