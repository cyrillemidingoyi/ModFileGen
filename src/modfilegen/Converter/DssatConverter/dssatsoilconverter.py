from modfilegen.converter import Converter
import pandas as pd
import traceback


def resolve_ssat(soil_ssat, soil_wfc, layer_ssat=None):
    """Return DSSAT SSAT with layer, soil, then Wfc fallback priority."""
    if pd.notna(layer_ssat):
        return float(layer_ssat) / 100
    if pd.notna(soil_ssat):
        return float(soil_ssat) / 100
    return float(soil_wfc) * 1.01 / 100


# ModelsDictionary tables (model 'dssat') holding the soil defaults. The
# table tells the nature of a parameter: every dssat_soil_data parameter may
# take one value per layer, dssat_soil_site parameters only NumLayer = 0.
SITE_TABLE = "dssat_soil_site"
LAYER_TABLE = "dssat_soil_data"
PARAMETER_TABLES = (SITE_TABLE, LAYER_TABLE)
_MISSING_CODE = format(-99, "6.0f")
# DSSAT SLDR (fraction/day) for each FAO drainage class (Soil.DrainFAO).
FAO_DRAINAGE_SLDR = {1: 0.01, 2: 0.05, 3: 0.25, 4: 0.40, 5: 0.60, 6: 0.75, 7: 0.85}
# source: https://repositorio.upf.br/server/api/core/bitstreams/63c040a1-2aef-434f-b6b8-144abadbd120/content

def _number(value):
    """SQLite NULL is written as nan, as when soils were read with pandas."""
    return float("nan") if pd.isna(value) else value


def drainage_rate(site, soil, has_override):
    """SLDR: explicit override, else the FAO drainage class, else the default."""
    drain_fao = soil.get("drainfao")
    if has_override or pd.isna(drain_fao):
        return float(site["sldr"])
    try:
        return FAO_DRAINAGE_SLDR[int(drain_fao)]
    except (KeyError, ValueError):
        raise ValueError(f"Soil.DrainFAO must be a class from 1 to 7, not {drain_fao!r}")


def _code(value, name):
    """Format an optional chemical code; -99 means not measured."""
    if float(value) == -99:
        return _MISSING_CODE
    return v_fmt[name].format(float(value))


class DssatSoilConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(
        self, parameter_resolver, soil_repository, usmdir, id_soil, id_mangt,
        coordinates,
    ):
        """Write ``XX.SOL`` for one soil and return its content.

        ``parameter_resolver`` must be prefetched for model ``dssat``,
        ``PARAMETER_TABLES`` and ``id_soil``; ``soil_repository`` for
        ``id_soil``. ``coordinates`` (latitude, longitude) are those of the
        simulation point; ``id_mangt`` gives the site code. Measured soil and
        layer properties are written unless an explicit override applies to
        the layer; model parameters come from ModelsDictionary defaults and
        ``SoilParameterOverrides`` (``NumLayer = 0`` for the whole profile,
        1..n for one layer).
        """
        soil = {key.casefold(): value for key, value in soil_repository.get_soil(id_soil).items()}
        layers = [
            {key.casefold(): value for key, value in layer.items()}
            for layer in soil_repository.get_layers(id_soil)
        ]
        simple = soil["soiloption"] == "simple"
        if not simple and not layers:
            raise ValueError(f"Detailed soil {id_soil!r} has no SoilLayers rows")
        if pd.isna(soil.get("curvenumber")):
            raise ValueError(f"Soil {id_soil!r} has no RunoffTypes.CurveNumber")
        # A 'simple' soil is one layer, written as two DSSAT lines.
        layer_count = 1 if simple else len(layers)
        site = parameter_resolver.resolve("dssat", SITE_TABLE, id_soil)
        # No site parameter is a layer parameter: reject NumLayer > 0 there.
        parameter_resolver.resolve_layers("dssat", SITE_TABLE, id_soil, layer_count, ())
        layer_values = parameter_resolver.resolve_layers(
            "dssat", LAYER_TABLE, id_soil, layer_count)

        def measured(name, value, number):
            """Measured value, unless an explicit override applies to the layer."""
            if parameter_resolver.has_override("dssat", LAYER_TABLE, name, id_soil, layer=number):
                return float(layer_values[number - 1][name])
            return value

        def layer_line(depth, number, values, wwp, wfc, ssat, ssks, bd, carbon,
                       clay, silt, cf, nitrogen, ph):
            return "".join([
                v_fmt["SLB"].format(depth),
                v_fmt["SLMH"].format(values["slmh"]),
                v_fmt["SLLL"].format(float(measured("slll", _number(wwp) / 100, number))),
                v_fmt["SDUL"].format(float(measured("sdul", _number(wfc) / 100, number))),
                v_fmt["SSAT"].format(float(measured("ssat", ssat, number))),
                v_fmt["SRGF"].format(float(values["srgf"])),
                v_fmt["SSKS"].format(float(
                    values["ssks"] if pd.isna(ssks) else measured("ssks", ssks, number))),
                v_fmt["SBDM"].format(_number(measured("sbdm", bd, number))),
                v_fmt["SLOC"].format(_number(measured("sloc", carbon, number))),
                v_fmt["SLCL"].format(_number(measured("slcl", clay, number))),
                v_fmt["SLSI"].format(_number(measured("slsi", silt, number))),
                v_fmt["SLCF"].format(_number(measured("slcf", cf, number))),
                v_fmt["SLNI"].format(_number(measured("slni", nitrogen, number))),
                v_fmt["SLHW"].format(_number(measured("slhw", ph, number))),
                _code(values["slhb"], "SLHB"),
                _code(values["scec"], "SCEC"),
                _code(values["sadc"], "SADC"),
                "\n",
            ])

        site_code = f"XX{str(id_mangt)[:4]}0101"
        lines = [
            "*SOILS: DSSAT Soil Input File\n",
            f"*{site_code}\n",
            "@SITE        COUNTRY          LAT     LONG SCS FAMILY\n",
            v_fmt["SITE"].format(site_code)
            + v_fmt["COUNTRY"].format(site["country"])
            + v_fmt["LAT"].format(_number(coordinates.latitude))
            + v_fmt["LONG"].format(_number(coordinates.longitude))
            + v_fmt["SCS FAMILY"].format(site["scs family"]) + "\n",
            "@ SCOM  SALB  SLU1  SLDR  SLRO  SLNF  SLPF  SMHB  SMPX  SMKE\n",
            v_fmt["SCOM"].format(site["scom"])
            + v_fmt["SALB"].format(_number(soil["albedo"]))
            + v_fmt["SLU1"].format(float(site["slu1"]))
            + v_fmt["SLDR"].format(drainage_rate(
                site, soil,
                parameter_resolver.has_override("dssat", SITE_TABLE, "sldr", id_soil)))
            + v_fmt["SLRO"].format(soil["curvenumber"])
            + v_fmt["SLNF"].format(float(site["slnf"]))
            + v_fmt["SLPF"].format(float(site["slpf"]))
            + v_fmt["SMHB"].format(site["smhb"])
            + v_fmt["SMPX"].format(site["smpx"])
            + v_fmt["SMKE"].format(site["smke"]) + "\n",
            "@  SLB  SLMH  SLLL  SDUL  SSAT  SRGF  SSKS  SBDM  SLOC  SLCL  SLSI  SLCF  SLNI  SLHW  SLHB  SCEC  SADC\n",
        ]
        if simple:
            ssat = resolve_ssat(soil.get("ssat"), soil["wfc"])
            for top in (True, False):
                lines.append(layer_line(
                    30 if top else soil["soiltotaldepth"], 1, layer_values[0],
                    soil["wwp"], soil["wfc"], ssat, soil.get("ssks"), soil["bd"],
                    soil["organicc"] if top else 0, soil["clay"], soil.get("silt"),
                    soil["cf"], soil["organicnstock"] if top else 0, soil["ph"],
                ))
        else:
            for number, layer in enumerate(layers, start=1):
                lines.append(layer_line(
                    layer["ldown"], number, layer_values[number - 1],
                    layer["wwp"], layer["wfc"],
                    resolve_ssat(soil.get("ssat"), _number(layer["wfc"]), layer.get("ssat")),
                    # Measured conductivity: layer, else soil.
                    layer.get("ssks") if pd.notna(layer.get("ssks")) else soil.get("ssks"),
                    layer["bd"], layer["organicc"],
                    layer.get("clay"), layer.get("silt"), layer.get("cf"),
                    layer.get("totaln"), layer.get("ph"),
                ))
        lines.append("@  SLB  SLPX  SLPT  SLPO CACO3  SLAL  SLFE  SLMN  SLBS  SLPA  SLPB  SLKE  SLMG  SLNA  SLSU  SLEC  SLCA\n")
        if simple:
            for top in (True, False):
                lines.append(
                    v_fmt["SLB"].format(30 if top else soil["soiltotaldepth"])
                    + v_fmt["SLPX"].format(_number(soil.get("extp")))
                    + v_fmt["SLPT"].format(_number(soil.get("totp")))
                    + _MISSING_CODE * 14 + "\n"
                )
        lines.append("\n")
        content = "".join(lines)
        try:
            self.write_file(usmdir, "XX.SOL", content)
        except Exception as e:
            traceback.print_exc()
            print(f"Error during writing file : {e}")
        return content


v_fmt = {
        "PEDON": "*{:<10}",
        "SOURCE": "  {:<11}",
        "TEXTURE": " {:<5}",
        "DEPTH": "{:6.0f}",
        "DESCRIPTION": " {:}",
        "SITE": " {:<12}",
        "COUNTRY": "{:<11}",
        "LAT": "{:9.3f}",
        "LONG": "{:9.3f}",
        "SCS FAMILY": " {:}",
        "SCOM": "{:>6}",
        "SALB": "{:6.2f}",
        "SLU1": "{:6.2f}",
        "SLDR": "{:6.2f}",
        "SLRO": "{:6.1f}",
        "SLNF": "{:6.2f}",
        "SLPF": "{:6.2f}",
        "SMHB": "{:>6}",
        "SMPX": "{:>6}",
        "SMKE": "{:>6}",
        # "SGRP": "{:>6}",
        "SLB": "{:6.0f}",
        "SLMH": "{:>6}",
        "SLLL": "{:6.3f}",
        "SDUL": "{:6.3f}",
        "SSAT": "{:6.3f}",
        "SRGF": "{:6.3f}",
        "SSKS": "{:6.3f}",
        "SBDM": "{:6.2f}",
        "SLOC": "{:6.3f}",
        "SLCL": "{:6.1f}",
        "SLSI": "{:6.1f}",
        "SLCF": "{:6.1f}",
        "SLNI": "{:6.2f}",
        "SLHW": "{:6.2f}",
        "SLHB": "{:6.2f}",
        "SCEC": "{:6.2f}",
        "SADC": "{:6f}",
        "SLPX": "{:6.2f}",
        "SLPT": "{:6.0f}",
        "SLPO": "{:6.0f}",
        "CACO3": "{:6.2f}",
        "SLAL": "{:6.2f}",
        "SLFE": "{:6f}",
        "SLMN": "{:6f}",
        "SLBS": "{:6f}",
        "SLPA": "{:6f}",
        "SLPB": "{:6f}",
        "SLKE": "{:6.2f}",
        "SLMG": "{:6.2f}",
        "SLNA": "{:6.2f}",
        "SLSU": "{:6f}",
        "SLEC": "{:6f}",
        "SLCA": "{:6.2f}",
        "ALFVG": "{:6.3f}",
        "MVG": "{:6.3f}",
        "NVG": "{:6.3f}",
        "WCRES": "{:6.3f}",
        "SCS Family": " {:}"
    }
