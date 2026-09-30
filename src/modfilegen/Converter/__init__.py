from importlib import import_module

from modfilegen import GlobalVariables
from time import perf_counter


def run_stics():
    version = str(
        GlobalVariables.get("stics_version", "")
    ).strip().lower()
    mode = str(GlobalVariables.get("stics_mode", "standard")).strip().lower()

    modules = {
        ("v9", "standard"): "modfilegen.Converter.SticsConverter.sticsconverter",
        ("9", "standard"): "modfilegen.Converter.SticsConverter.sticsconverter",
        ("v11", "standard"): "modfilegen.Converter.SticsV11Converter.sticsconverter",
        ("11", "standard"): "modfilegen.Converter.SticsV11Converter.sticsconverter",
        ("v11", "successive"): "modfilegen.Converter.SticsV11Converter.sticssuccessiveconverter",
        ("11", "successive"): "modfilegen.Converter.SticsV11Converter.sticssuccessiveconverter",
    }

    if version not in {"v9", "9", "v11", "11"}:
        raise ValueError(
            "GlobalVariables['stics_version'] must be 'v9' or 'v11'"
        )
    if mode not in {"standard", "successive"}:
        raise ValueError(
            "GlobalVariables['stics_mode'] must be 'standard' or 'successive'"
        )
    if (version, mode) not in modules:
        raise ValueError("STICS successive mode is supported only for version v11")

    runstics = import_module(modules[(version, mode)]).main

    start = perf_counter()
    try:
        return runstics()
    finally:
        print(f"STICS total time: {perf_counter()-start:.2f}s", flush=True)


def run_dssat():
    mode = str(GlobalVariables.get("dssat_mode", "standard")).strip().lower()
    modules = {
        "standard": "modfilegen.Converter.DssatConverter.dssatconverter",
        "successive": "modfilegen.Converter.DssatConverter.dssatsuccessiveconverter",
    }
    if mode not in modules:
        raise ValueError(
            "GlobalVariables['dssat_mode'] must be 'standard' or 'successive'"
        )
    return import_module(modules[mode]).main()


def run_apsim():
    return import_module(
        "modfilegen.Converter.ApsimConverter.apsimconverter"
    ).main()


def run_celsius():
    version = str(
        GlobalVariables.get("celsius_version", "v3")
    ).strip().lower()
    mode = str(
        GlobalVariables.get("celsius_mode", "standard")
    ).strip().lower()
    modules = {
        ("v3", "standard"): "modfilegen.Converter.CelsiusConverter.celsiusconverter",
        ("3", "standard"): "modfilegen.Converter.CelsiusConverter.celsiusconverter",
        ("v32", "standard"): "modfilegen.Converter.CelsiusV32Converter.celsiusv32converter",
        ("32", "standard"): "modfilegen.Converter.CelsiusV32Converter.celsiusv32converter",
        ("v32", "successive"): "modfilegen.Converter.CelsiusV32Converter.celsiusv32successiveconverter",
        ("32", "successive"): "modfilegen.Converter.CelsiusV32Converter.celsiusv32successiveconverter",
    }
    if version not in {"v3", "3", "v32", "32"}:
        raise ValueError(
            "GlobalVariables['celsius_version'] must be 'v3' or 'v32'"
        )
    if mode not in {"standard", "successive"}:
        raise ValueError(
            "GlobalVariables['celsius_mode'] must be 'standard' or 'successive'"
        )
    if (version, mode) not in modules:
        raise ValueError("CELSIUS successive mode is supported only for V32")
    return import_module(modules[(version, mode)]).main()

