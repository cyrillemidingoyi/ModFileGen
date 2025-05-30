from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback

class SticsClimatConverter(Converter):
    def __init__(self):
        super().__init__()


    def export(self, directory_path, usmdir, DT, climate_df):
        """
        Export un fichier climat.txt depuis des données déjà chargées en mémoire (climate_df).

        Args:
            directory_path (str): chemin du dossier (ex: "simu/SITE001/2021")
            DT (DataFrame): dictionnaire des valeurs par défaut
            climate_df (DataFrame): toutes les données climatiques déjà en RAM
            usmdir (str): dossier de sortie pour le fichier climat.txt
        """
        import os
        import traceback

        file_name = "climat.txt"
        fileContent = ""

        # Extraire Site et Year à partir du chemin
        ST = directory_path.split(os.sep)
        Site = ST[-2]
        Year = int(ST[-1])

        # Filtrer les données climatiques pour le site et l’année (année et année+1)
        """subset_df = climate_df[
            (climate_df["idPoint"] == Site) &
            (climate_df["year"].isin([Year, Year + 1]))
        ]"""
        
        subset_df = climate_df.loc[
            (climate_df["idPoint"] == Site) & 
            (climate_df["year"].isin({Year, Year + 1}))  # Using set for faster lookups
        ]

        if subset_df.empty:
            print(f"Aucune donnée trouvée pour le site {Site} et l’année {Year}")
            return ""
        
        # Pre-compute the DT lookups outside the loop
        vapeur_default = -999.9
        co2_default = -999.9

        try:
            vapeur_value = float(DT.loc[DT["Champ"] == "vapeurp", "dv"].values[0])
        except (IndexError, ValueError):
            vapeur_value = vapeur_default

        try:
            co2_value = float(DT.loc[DT["Champ"] == "co2", "dv"].values[0])
        except (IndexError, ValueError):
            co2_value = co2_default

        # Use string joining for better performance
        file_lines = []
        for _, row in subset_df.iterrows():
            line_parts = [
                row["idPoint"] + " ",
                str(row["year"]) + " ",
                str(row["Nmonth"]).rjust(3),
                str(row["NdayM"]).rjust(3),
                str(row["DOY"]).rjust(4),
                format(row["tmin"], ".1f").rjust(8),
                format(row["tmax"], ".1f").rjust(7),
                format(row["srad"] if pd.notna(row["srad"]) else -999.9, ".1f").rjust(7),
                format(row["Etppm"], ".1f").rjust(7),
                format(row["rain"], ".1f").rjust(7),
                format(row["wind"] if pd.notna(row["wind"]) else -999.9, ".1f").rjust(7),
                format(vapeur_value, ".1f").rjust(7),
                format(co2_value, ".1f").rjust(7),
                "\n"
            ]
            file_lines.append("".join(line_parts))

        fileContent = "".join(file_lines)

        # Convertir en liste de dicts
        """rows = subset_df.to_dict(orient='records')

        for row in rows:
            fileContent += row["idPoint"] + " "
            fileContent += str(row["year"]) + " "
            fileContent += str(row["Nmonth"]).rjust(3)
            fileContent += str(row["NdayM"]).rjust(3)
            fileContent += str(row["DOY"]).rjust(4)
            fileContent += format(row["tmin"], ".1f").rjust(8)
            fileContent += format(row["tmax"], ".1f").rjust(7)
            fileContent += format(row["srad"] if row["srad"] is not None else -999.9, ".1f").rjust(7)
            fileContent += format(row["Etppm"], ".1f").rjust(7)
            fileContent += format(row["rain"], ".1f").rjust(7)
            fileContent += format(row["wind"] if row["wind"] is not None else -999.9, ".1f").rjust(7)

            try:
                vapeur = float(DT[DT["Champ"] == "vapeurp"]["dv"].values[0])
            except:
                vapeur = -999.9
            fileContent += format(vapeur, ".1f").rjust(7)

            try:
                co2 = float(DT[DT["Champ"] == "co2"]["dv"].values[0])
            except:
                co2 = -999.9
            fileContent += format(co2, ".1f").rjust(7)

            fileContent += '\n'"""

        try:
            self.write_file(usmdir, file_name, fileContent)
        except Exception as e:
            print("Erreur lors de l’écriture du fichier : " + str(e))
            traceback.print_exc()

        return fileContent


    """def export(self, directory_path, ModelDictionary_Connection, master_input_connection, usmdir, DT):
        file_name = "climat.txt"
        fileContent = ""
        ST = directory_path.split(os.sep)        
        Site = ST[-2]
        Year = ST[-1]
        fetchAllQuery = "select * from RaClimateD where idPoint='" + Site + "' And (Year=" + Year + " or Year=" + str(int(Year) + 1) + ");"
        DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
        rows = DA.to_dict(orient='records')
        for row in rows:
            fileContent += row["idPoint"] + " "
            year = row["year"]
            fileContent += str(year) + " "
            mois = row["Nmonth"]
            fileContent += str(mois).rjust(3)
            jour = row["NdayM"]
            fileContent += str(jour).rjust(3)
            jjulien = row["DOY"]
            fileContent += str(jjulien).rjust(4)
            mintemp = row["tmin"]
            fileContent += format(mintemp, ".1f").rjust(8)
            maxtemp = row["tmax"]
            fileContent += format(maxtemp, ".1f").rjust(7)
            gradiation = row["srad"]
            if gradiation is not None: 
                fileContent += format(gradiation, ".1f").rjust(7)
            else:
                fileContent += format(-999.9, ".1f").rjust(7)
            ppet = row["Etppm"]
            fileContent += format(ppet, ".1f").rjust(7)
            precipitation = row["rain"]
            fileContent += format(precipitation, ".1f").rjust(7)
            vent = row["wind"]
            if vent is not None: 
                fileContent += format(vent, ".1f").rjust(7)
            else:
                fileContent += format(-999.9, ".1f").rjust(7)
            #surfpress = row["Surfpress"]
            rw = DT[DT["Champ"] == "vapeurp"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".1f").rjust(7)
            rw = DT[DT["Champ"] == "co2"]
            Dv = rw["dv"].values[0]
            fileContent += format(float(Dv), ".1f").rjust(7)
            fileContent += "\n"
        try:
            # Export file to specified directory    
            self.write_file(usmdir, file_name, fileContent)
        except Exception as e:
            print("Error during writing file : " + str(e))
            traceback.print_exc()
        
        return fileContent"""
