import math

from modfilegen.Converter.DssatConverter.dssatsoilconverter import resolve_ssat


def test_resolve_ssat_prefers_soil_value():
    assert resolve_ssat(52, 30) == 0.52


def test_resolve_ssat_uses_wfc_fallback_for_missing_value():
    assert math.isclose(resolve_ssat(None, 30), 0.303)
    assert math.isclose(resolve_ssat(float("nan"), 30), 0.303)


def test_resolve_ssat_does_not_treat_zero_as_missing():
    assert resolve_ssat(0, 30) == 0
