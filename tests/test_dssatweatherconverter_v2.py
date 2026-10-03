import pytest

from modfilegen.Converter.DssatConverter.dssatweatherconverter_v2 import (
    format_dssat_weather_date,
)


def test_format_dssat_weather_date_uses_yyddd():
    assert format_dssat_weather_date(2000, 1) == "00001"
    assert format_dssat_weather_date(2000, 150) == "00150"
    assert format_dssat_weather_date(2001, 365) == "01365"
    assert format_dssat_weather_date(1999, 32) == "99032"


def test_format_dssat_weather_date_validates_leap_day():
    assert format_dssat_weather_date(2000, 366) == "00366"
    with pytest.raises(ValueError, match="Invalid day of year"):
        format_dssat_weather_date(2001, 366)
