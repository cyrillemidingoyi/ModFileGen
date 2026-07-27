from pathlib import Path

import pytest

from modfilegen.utils.stics_cultivar_file import SticsCultivarFile


DATA = Path(__file__).parent / "cultivars" / "sticsv9" / "maiplt1_c850.txt"


def test_read_and_access_parameters():
    plant = SticsCultivarFile.read(DATA)

    assert plant.cultivar_names == ("var_850",)
    assert plant.get_species_parameter("codeplante") == "mai"
    assert plant.get_cultivar_parameter("var_850", "pgrainmaxi") == ".34"
    assert plant.get_cultivar_parameter(
        "var_850", "extin", fallback_to_species=True
    ) == "0.70000"


def test_modify_clone_write_and_reload(tmp_path):
    plant = SticsCultivarFile.read(DATA)
    plant.set_species_parameter("alphaco2", "1.10")
    plant.set_cultivar_parameter(
        "var_850",
        {"pgrainmaxi": "0.40", "nbgrmax": 5500},
    )
    plant.create_cultivar(
        "nouveau_mais",
        default_cultivar="var_850",
        parameters={"pgrainmaxi": "0.55", "nbgrmax": 6000},
    )

    output = plant.write(tmp_path / "maiplt1.txt")
    reloaded = SticsCultivarFile.read(output)

    assert reloaded.get_species_parameter("alphaco2") == "1.10"
    assert reloaded.get_cultivar_parameter("var_850", "pgrainmaxi") == "0.40"
    assert reloaded.get_cultivar_parameter("var_850", "nbgrmax") == "5500"
    assert reloaded.get_cultivar_parameter("nouveau_mais", "pgrainmaxi") == "0.55"
    assert reloaded.get_cultivar_parameter("nouveau_mais", "nbgrmax") == "6000"
    assert reloaded.cultivar_names == ("var_850", "nouveau_mais")


def test_unknown_override_is_rejected():
    plant = SticsCultivarFile.read(DATA)

    with pytest.raises(KeyError, match="paramètre_inconnu"):
        plant.create_cultivar(
            "nouveau_mais",
            default_cultivar="var_850",
            parameters={"paramètre_inconnu": 1},
        )


def test_parameter_updates_are_atomic():
    plant = SticsCultivarFile.read(DATA)
    original = plant.get_cultivar_parameter("var_850", "pgrainmaxi")

    with pytest.raises(KeyError, match="paramètre_inconnu"):
        plant.set_cultivar_parameter(
            "var_850",
            {"pgrainmaxi": "0.99", "paramètre_inconnu": 1},
        )

    assert plant.get_cultivar_parameter("var_850", "pgrainmaxi") == original
