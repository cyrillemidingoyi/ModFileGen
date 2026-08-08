from importlib import import_module

from modfilegen import GlobalVariables
from time import perf_counter


def run_stics():
    version = str(
        GlobalVariables.get("stics_version", "")
    ).strip().lower()

    modules = {
        "v9": "modfilegen.Converter.SticsConverter.sticsconverter",
        "9": "modfilegen.Converter.SticsConverter.sticsconverter",
        "v11": "modfilegen.Converter.SticsV11Converter.sticsconverter",
        "11": "modfilegen.Converter.SticsV11Converter.sticsconverter",
    }

    if version not in modules:
        raise ValueError(
            "GlobalVariables['stics_version'] must be 'v9' or 'v11'"
        )

    runstics = import_module(modules[version]).main

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
    return import_module(
        "modfilegen.Converter.CelsiusConverter.celsiusconverter"
    ).main()

