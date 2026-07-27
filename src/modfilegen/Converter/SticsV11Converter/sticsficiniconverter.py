from modfilegen.converter import Converter
import os
import pandas as pd
import traceback


STICS_LAYER_COUNT = 5


def _table_exists(connection, table_name):
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND lower(name) = lower(?)",
        (table_name,),
    ).fetchone()
    return row is not None


def _column_name(columns, expected):
    return next((column for column in columns if column.lower() == expected.lower()), None)


def _value(row, expected, default=None):
    column = _column_name(row.keys(), expected)
    return default if column is None else row[column]


def _require_value(row, expected, context):
    value = _value(row, expected)
    if value is None or pd.isna(value):
        raise ValueError(f"Missing {expected} for {context}")
    return float(value)


def _pad_with_zeros(values, context):
    if not values:
        raise ValueError(f"No layer available for {context}")
    if len(values) > STICS_LAYER_COUNT:
        raise ValueError(
            f"{context} has {len(values)} layers; STICS supports at most {STICS_LAYER_COUNT}"
        )
    return values + [0.0] * (STICS_LAYER_COUNT - len(values))


class SticsFicIniConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(
        self,
        directory_path,
        ModelDictionary_Connection,
        master_input_connection,
        usmdir,
        season_order=None,
        dt=1,
    ):
        file_name = "ficini.txt"
        file_lines = []
        id_sim = directory_path.split(os.path.sep)[-3]

        defaults_query = """
            SELECT Champ, Default_Value_Datamill, defaultValueOtherSource,
                   IFNULL(defaultValueOtherSource, Default_Value_Datamill) AS dv
            FROM Variables
            WHERE model = 'sticsv11' AND [Table] = 'ficini'
        """
        main_query = """
            SELECT Soil.IdSoil, Soil.SoilOption, Soil.Wwp, Soil.Wfc, Soil.bd,
                   InitialConditions.*
            FROM InitialConditions
            INNER JOIN SimUnitList
                ON InitialConditions.idIni = SimUnitList.idIni
            INNER JOIN Soil
                ON Lower(Soil.IdSoil) = Lower(SimUnitList.idsoil)
            WHERE SimUnitList.idSim = ?
        """
        defaults_frame = pd.read_sql_query(defaults_query, ModelDictionary_Connection)
        defaults = defaults_frame.set_index("Champ")["dv"].to_dict()
        data = pd.read_sql_query(main_query, master_input_connection, params=(id_sim,))

        option_column = _column_name(data.columns, "option")
        has_new_initial_schema = option_column is not None
        if int(dt) == 1 and has_new_initial_schema and not _table_exists(
            master_input_connection, "InitialConditionsLayers"
        ):
            raise ValueError(
                "InitialConditions.option exists, so the InitialConditionsLayers table is mandatory"
            )

        for row in data.to_dict(orient="records"):
            id_ini = str(_value(row, "idIni"))
            soil_option = str(_value(row, "SoilOption", "simple")).lower()
            initial_option = (
                str(_value(row, "option", "simple")).lower()
                if has_new_initial_schema
                else "legacy"
            )
            if soil_option not in {"simple", "detailed"}:
                raise ValueError(f"Unsupported SoilOption={soil_option!r} for {id_sim}")
            if initial_option not in {"legacy", "simple", "detailed"}:
                raise ValueError(
                    f"Unsupported InitialConditions.option={initial_option!r} for {id_ini}"
                )

            file_lines.append(":nbplantes:")
            plant_query = """
                SELECT Max(CropManagement.PlantOrder) AS MaxDePlantOrder
                FROM CropManagement
                INNER JOIN SimUnitList
                    ON CropManagement.idMangt = SimUnitList.idMangt
                WHERE SimUnitList.idsim = ?
            """
            plant_params = [id_sim]
            if season_order is not None:
                plant_query += " AND CropManagement.SeasonOrder = ?"
                plant_params.append(int(season_order))
            plant_data = pd.read_sql_query(
                plant_query, master_input_connection, params=tuple(plant_params)
            )
            nbplt = plant_data.iloc[0]["MaxDePlantOrder"] if not plant_data.empty else 1
            nbplt = 1 if pd.isna(nbplt) else nbplt
            file_lines.append(str(nbplt))

            file_lines.append(":plante:")
            file_lines.append(str(defaults["stade0"]))
            file_lines.append(f"{float(defaults['lai0']):.1f}")
            file_lines.append(f"{float(defaults['magrain0']):.1f}")
            file_lines.append(f"{float(defaults['zrac0']):.1f}")
            file_lines.append("code_acti_reserve")
            file_lines.append(str(defaults["code_acti_reserve"]))
            file_lines.append(f"{float(defaults['maperenne0']):.1f}")
            file_lines.append(f"{float(defaults['QNperenne0']):.1f}")
            file_lines.append(f"{float(defaults['masecnp0']):.1f}")
            file_lines.append(f"{float(defaults['QNplantenp0']):.1f}")
            file_lines.append(f"{float(defaults['masec0']):.1f}")
            file_lines.append(f"{float(defaults['QNplante0']):.1f}")
            file_lines.append(f"{float(defaults['restemp0']):.1f}")
            file_lines.append("densinitial")
            file_lines.append(f"{float(defaults['densinitial']):.1f} 0.0 0.0 0.0 0.0")
            file_lines.append(":plante:")
            if nbplt == 1:
                file_lines.extend([""] * 4)
                file_lines.extend(["code_acti_reserve", "2", "0", "0", "0", "0"])
                file_lines.extend([""] * 3)
                file_lines.extend([":densinitial:", "     "])
            else:
                file_lines.append(str(defaults["stade0_2"]))
                file_lines.append(f"{float(defaults['lai0_2']):.1f}")
                file_lines.append(f"{float(defaults['masec0_2']):.1f}")
                file_lines.append(f"{float(defaults['zrac0_2']):.1f}")
                file_lines.append("code_acti_reserve")
                file_lines.append(str(defaults["code_acti_reserve_2"]))
                file_lines.append(f"{float(defaults['maperenne0_2']):.1f}")
                file_lines.append(f"{float(defaults['QNperenne0_2']):.1f}")
                file_lines.append(f"{float(defaults['masecnp0_2']):.1f}")
                file_lines.append(f"{float(defaults['QNplantenp0_2']):.1f}")
                file_lines.append(f"{float(defaults['masec0_2']):.1f}")
                file_lines.append(f"{float(defaults['QNplante0_2']):.1f}")
                file_lines.append(f"{float(defaults['restemp0_2']):.1f}")
                file_lines.append(":densinitial:")
                file_lines.append(f"{float(defaults['densinitial_2']):.1f} 0.0 0.0 0.0 0.0")

            soil_data = pd.read_sql_query(
                "SELECT * FROM SoilLayers WHERE Lower(idsoil) = Lower(?) ORDER BY NumLayer",
                master_input_connection,
                params=(_value(row, "IdSoil"),),
            )
            soil_layers = soil_data.to_dict(orient="records")

            use_detailed_initial = soil_option == "detailed" and initial_option == "detailed"
            initial_layers = []
            if use_detailed_initial:
                try:
                    initial_data = pd.read_sql_query(
                        "SELECT * FROM InitialConditionsLayers WHERE idIni = ? ORDER BY NumLayer",
                        master_input_connection,
                        params=(id_ini,),
                    )
                except Exception as error:
                    raise ValueError(
                        "InitialConditions.option exists, so the "
                        "InitialConditionsLayers table is mandatory"
                    ) from error
                initial_layers = initial_data.to_dict(orient="records")

            if soil_option == "simple":
                soil_layers = [{
                    "NumLayer": 1,
                    "Wwp": _require_value(row, "Wwp", id_ini),
                    "Wfc": _require_value(row, "Wfc", id_ini),
                    "bd": _require_value(row, "bd", id_ini),
                }]
            elif not soil_layers:
                raise ValueError(f"SoilOption is detailed but no SoilLayers exist for {id_ini}")

            if use_detailed_initial:
                water_stocks = [
                    _require_value(layer, "WStockinit", f"{id_ini} layer {index}")
                    for index, layer in enumerate(initial_layers, start=1)
                ]
                no3_values = [
                    _require_value(layer, "Ninit", f"{id_ini} layer {index}")
                    for index, layer in enumerate(initial_layers, start=1)
                ]
                nh4_values = [
                    _require_value(layer, "NH4initf", f"{id_ini} layer {index}")
                    for index, layer in enumerate(initial_layers, start=1)
                ]
            else:
                water_stock = _require_value(row, "WStockinit", id_ini)
                ninit = _require_value(row, "Ninit", id_ini)
                nh4 = _value(row, "NH4initf")
                nh4 = float(defaults["NH4initf"]) if nh4 is None or pd.isna(nh4) else float(nh4)
                layer_count = len(soil_layers)
                water_stocks = [water_stock] * layer_count
                if soil_option == "simple":
                    no3_values = [ninit]
                    nh4_values = [nh4]
                else:
                    no3_values = [ninit / layer_count] * layer_count
                    nh4_values = [nh4 / layer_count] * layer_count

            hinit_values = []
            for index, (soil_layer, water_stock) in enumerate(
                zip(soil_layers, water_stocks), start=1
            ):
                wwp = _require_value(soil_layer, "Wwp", f"{id_ini} soil layer {index}")
                wfc = _require_value(soil_layer, "Wfc", f"{id_ini} soil layer {index}")
                bulk_density = _require_value(soil_layer, "bd", f"{id_ini} soil layer {index}")
                if bulk_density == 0:
                    raise ValueError(f"bd is zero for {id_ini}, soil layer {index}")
                hinit_values.append((wwp + water_stock * (wfc - wwp) / 100.0) / bulk_density)

            hinit_values = _pad_with_zeros(hinit_values, f"Hinitf for {id_ini}")
            no3_values = _pad_with_zeros(no3_values, f"NO3init for {id_ini}")
            nh4_values = _pad_with_zeros(nh4_values, f"NH4initf for {id_ini}")

            file_lines.extend([":Hinitf:", " ".join(f"{value:.4f}" for value in hinit_values)])
            file_lines.extend([":NO3init:", " ".join(f"{value:.1f}" for value in no3_values)])
            file_lines.extend([":NH4initf:", " ".join(f"{value:.1f}" for value in nh4_values)])

            file_lines.extend([
                ":snow:", "Sdepth0", "0", "Sdry0", "0", "Swet0", "0", "ps0", "0"
            ])

        file_lines.append("")
        content = "\n".join(file_lines)
        try:
            self.write_file(usmdir, file_name, content)
        except Exception as error:
            traceback.print_exc()
            print("Error during writing file : " + str(error))
        return content + "\n"
