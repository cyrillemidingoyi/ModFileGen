import pytest

from modfilegen.Converter.SticsV11Converter.soil_parameter_calculators import (
    calculate_q0,
)


@pytest.mark.parametrize(
    ("sand", "clay", "expected"),
    [
        (90, 5, 6.5),
        (20, 60, 7.4),
        (40, 30, 10.4),
    ],
)
def test_calculate_q0_uses_stics_texture_formula(sand, clay, expected):
    assert calculate_q0({"Sand": sand, "Clay": clay}) == pytest.approx(expected)
