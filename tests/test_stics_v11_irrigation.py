import pandas as pd

from modfilegen.Converter.SticsV11Converter.sticsfictec1converter import (
    SticsFictec1Converter,
    _format_irrigation_interventions,
    _format_irrigation_setting,
)


def test_formats_no_irrigation_for_policy_without_operations():
    assert _format_irrigation_interventions(120, ()) == "nbinterventions\n0\n"


def test_formats_signed_offsets_from_already_shifted_sowing_date():
    operations = [
        {"DIrrigation": -10, "IrrigationAmount": 20},
        {"DIrrigation": 30, "IrrigationAmount": 40.5},
    ]

    assert _format_irrigation_interventions(400, operations) == (
        "nbinterventions\n"
        "2\n"
        "julapI_or_sum_upvt amount\n"
        "390 20.0\n"
        "julapI_or_sum_upvt amount\n"
        "430 40.5\n"
    )


def test_forces_manual_julian_modes_when_operations_are_present():
    operations = [{"DIrrigation": 10, "IrrigationAmount": 20}]

    assert _format_irrigation_setting(
        "codecalirrig", operations, "codecalirrig\n1\n"
    ) == "codecalirrig\n2\n"
    assert _format_irrigation_setting(
        "codedateappH2O", operations, "codedateappH2O\n1\n"
    ) == "codedateappH2O\n2\n"


def test_preserves_dictionary_modes_without_manual_operations():
    assert _format_irrigation_setting(
        "codecalirrig", (), "codecalirrig\n1\n"
    ) == "codecalirrig\n1\n"


def test_all_dictionary_backed_fictec_parameters_can_be_overridden():
    class FakeResolver:
        def resolve_management(
            self, model, target_table, management_id,
            season_order, plant_order,
        ):
            assert target_table == "fictec2"
            return {"codefracappn": 2, "codrecolte": 7}

        def has_management_override(
            self, model, target_table, parameter, management_id,
            season_order, plant_order,
        ):
            return parameter.casefold() in {"codefracappn", "codrecolte"}

    defaults = pd.DataFrame([
        {"Champ": "codefracappN", "dv": 1},
        {"Champ": "codrecolte", "dv": 1},
        {"Champ": "codefauche", "dv": "2"},
    ])
    converter = SticsFictec1Converter()
    converter._activate_management_parameters(
        FakeResolver(),
        {"idMangt": "rotation", "SeasonOrder": 2, "PlantOrder": 2},
        "fictec2",
    )

    assert converter.format_item(defaults, "codefracappN") == (
        "codefracappN\n2\n"
    )
    assert converter.format_item(defaults, "codrecolte") == "codrecolte\n7\n"
    assert converter.format_item(defaults, "codefauche") == "codefauche\n2\n"
