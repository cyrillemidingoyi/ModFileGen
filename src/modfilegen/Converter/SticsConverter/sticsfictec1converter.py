from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback

class SticsFictec1Converter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, ModelDictionary_Connection, master_input_connection, usmdir):
        file_name = "fictec1.txt"
        fileContent = ""
        ST = directory_path.split(os.sep)
        T = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model='stics') AND ([Table]='st_fictec'));"
        DT = pd.read_sql_query(T, ModelDictionary_Connection)
        fetchAllQuery = """SELECT SimUnitList.idsim, SimUnitList.idMangt, Soil.SoilTotalDepth, ListCultivars.idcultivarStics, CropManagement.sdens,
        CropManagement.sowingdate, CropManagement.SoilTillPolicyCode FROM Soil INNER JOIN (ListCultivars INNER JOIN (CropManagement INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt)
        ON ListCultivars.IdCultivar = CropManagement.Idcultivar) ON Lower(Soil.IdSoil) = Lower(SimUnitList.idsoil)  where idSim= '%s' ;"""%(ST[-3])
        DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
        rows = DA.to_dict(orient='records')
        rw = rows[0]
        fileContent += "nbinterventions\n"
        fetchallquery2 = """SELECT SimUnitList.idsim, CropManagement.sowingdate, OrganicFOperations.Dferti, OrganicFOperations.OFNumber, OrganicFOperations.CNferti, 
                OrganicFOperations.NFerti, OrganicFOperations.Qmanure, OrganicFOperations.TypeResidues, ListResidues.idresidueStics, CropManagement.SoilTillPolicyCode 
                FROM ListResidues INNER JOIN ((OrganicFertilizationPolicy INNER JOIN (CropManagement INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt) 
                ON OrganicFertilizationPolicy.OFertiPolicyCode = CropManagement.OFertiPolicyCode) INNER JOIN OrganicFOperations ON OrganicFertilizationPolicy.OFertiPolicyCode
                = OrganicFOperations.OFertiPolicyCode) ON ListResidues.TypeResidues = OrganicFOperations.TypeResidues where idSim='%s' Order by OFNumber ;"""%(ST[-3])
        DS2 = pd.read_sql_query(fetchallquery2, master_input_connection)   
        rows2 = DS2.to_dict(orient='records')
               
        if rows2[0]["idresidueStics"] is None:
            fileContent += "0\n"
        else:
            fileContent += str(len(rows2)) + "\n"
            if len(rows2) != 0:
                for i in range(len(rows2)):
                    fileContent += "opp1\n"
                    fileContent += str(int(rows2[i]["sowingdate"]) + int(rows2[i]["Dferti"])) + " "
                    fileContent += str(rows2[i]["idresidueStics"]) + " "
                    fileContent += str(rows2[i]["qmanure"]) + " "
                    fileContent += str(rows2[i]["CNferti"] * rows2[i]["NFerti"]) + " "
                    fileContent += str(rows2[i]["CNferti"]) + " "
                    fileContent += str(rows2[i]["NFerti"]) + " "
                    fileContent += self.FormatSticsRawData(DT, "supply of organic residus.eaures") + "\n"

        Sql = """SELECT SoilTillPolicy.SoilTillPolicyCode, SoilTillageOperations.STNumber, SoilTillPolicy.NumTillOperations, SoilTillageOperations.DepthResUp, SoilTillageOperations.DepthResLow, SoilTillageOperations.DSTill
            FROM SoilTillPolicy INNER JOIN SoilTillageOperations ON SoilTillPolicy.SoilTillPolicyCode = SoilTillageOperations.SoilTillPolicyCode
            where SoilTillPolicy.SoilTillPolicyCode= %s;"""%(rw["SoilTillPolicyCode"])
            
        Adp = pd.read_sql_query(Sql, master_input_connection)
        dataTill = Adp.to_dict(orient='records')
        fileContent += "nbinterventions\n"
        fileContent += format(dataTill[0]["NumTillOperations"], ".0f") + "\n"
        if dataTill[0]["NumTillOperations"] > 0:
            for i in range(len(dataTill)):
                fileContent += "opp1\n"
                fileContent += format(rw["sowingdate"] + dataTill[i]["DSTill"], ".0f") + " "
                fileContent += format(dataTill[i]["DepthResUp"], ".0f") + " "
                fileContent += format(dataTill[i]["DepthResLow"], ".0f") + "\n"

        fileContent += "iplt0\n"
        fileContent += format(rw["sowingdate"], ".0f") + "\n"
        fileContent += self.format_item( DT, "profsem")
        fileContent += "densitesem\n"
        fileContent += str(format(rw["sdens"], ".2f"))+ "\n"
        fileContent += "variete\n"
        fileContent += rw["idcultivarStics"] + "\n"
        fileContent += self.format_item(DT, "codetradtec")
        fileContent += self.format_item(DT, "interrang")
        fileContent += self.format_item(DT, "orientrang")
        fileContent += self.format_item(DT, "codedecisemis")
        fileContent += self.format_item(DT, "nbjmaxapressemis")
        fileContent += self.format_item(DT, "nbjseuiltempref")
        fileContent += self.format_item(DT, "codestade")
        fileContent += self.format_item(DT, "ilev")
        fileContent += self.format_item(DT, "iamf")
        fileContent += self.format_item(DT, "ilax")
        fileContent += self.format_item(DT, "isen")
        fileContent += self.format_item(DT, "ilan")
        fileContent += self.format_item(DT, "iflo")
        fileContent += self.format_item(DT, "idrp")
        fileContent += self.format_item(DT, "imat")
        fileContent += self.format_item(DT, "irec")
        fileContent += "irecbutoir\n"
        fileContent += format(rw["sowingdate"] + 250, ".0f") + "\n"
        fileContent += self.format_item(DT, "effirr")  
        

        fileContent += self.format_item(DT, "codecalirrig")
        fileContent += self.format_item(DT, "ratiol")
        fileContent += self.format_item(DT, "dosimx")
        fileContent += self.format_item(DT, "doseirrigmin")
        fileContent += self.format_item(DT, "codedateappH2O")
        
        fileContent += "nbinterventions\n"
        fileContent += "0\n"
        fileContent += self.format_item(DT, "codlocirrig")
        fileContent += self.format_item(DT, "locirrig")
        fileContent += "profmes\n"
        fileContent += format(rw["SoilTotalDepth"], ".0f") + "\n"      
        fileContent += self.format_item(DT, "engrais")
        fileContent += self.format_item(DT, "concirr")
        fileContent += self.format_item(DT, "codedateappN")
        fileContent += self.format_item(DT, "codefracappN")
        fileContent += self.format_item(DT, "fertilisation.Qtot_N",fieldIt=1)

        fetchallquery2 = """Select SimUnitList.idsim, InorganicFOperations.N, CropManagement.sowingdate, InorganicFOperations.Dferti, InorganicFertilizationPolicy.NumInorganicFerti
        FROM(InorganicFertilizationPolicy INNER JOIN InorganicFOperations On InorganicFertilizationPolicy.InorgFertiPolicyCode = InorganicFOperations.InorgFertiPolicyCode)
        INNER JOIN (CropManagement INNER JOIN SimUnitList On CropManagement.idMangt = SimUnitList.idMangt) On InorganicFertilizationPolicy.InorgFertiPolicyCode =
        CropManagement.InoFertiPolicyCode where idSim='%s';"""%(ST[-3])
        
        DS2 = pd.read_sql_query(fetchallquery2, master_input_connection)
        fileContent += "nbinterventions\n"
        fileContent += format(DS2.shape[0], ".0f") + "\n"
        if DS2.shape[0] > 0:
            for i in range(DS2.shape[0]):
                fileContent += "opp1\n"
                fileContent += str(int(DS2.iloc[i]["sowingdate"] + DS2.iloc[i]["Dferti"])) + " "
                fileContent += str(DS2.iloc[i]["N"]) + "\n"

        fileContent += self.format_item(DT, "codlocferti")
        fileContent += self.format_item(DT, "locferti")
        fileContent += self.format_item(DT, "ressuite")
        fileContent += self.format_item(DT, "codceuille")
        fileContent += self.format_item(DT, "nbceuille")
        fileContent += self.format_item(DT, "cadencerec")
        fileContent += self.format_item(DT, "codrecolte")
        fileContent += self.format_item(DT, "codeaumin")
        fileContent += self.format_item(DT, "h2ograinmin")
        fileContent += self.format_item(DT, "h2ograinmax")
        fileContent += self.format_item(DT, "sucrerec")
        fileContent += self.format_item(DT, "CNgrainrec")
        fileContent += self.format_item(DT, "huilerec")
        fileContent += self.format_item(DT, "coderecolteassoc")
        fileContent += self.format_item(DT, "codedecirecolte")
        fileContent += self.format_item(DT, "nbjmaxapresrecolte")
        fileContent += self.format_item(DT, "codefauche")
        fileContent += self.format_item(DT, "mscoupemini")
        fileContent += self.format_item(DT, "codemodfauche")
        fileContent += self.format_item(DT, "hautcoupedefaut")
        fileContent += self.format_item(DT, "stadecoupedf")
        
        fileContent += "nbinterventions\n"
        fileContent += "0\n"
        fileContent += "nbinterventions\n"
        fileContent += "0\n"

        fileContent += self.format_item(DT, "codepaillage")
        fileContent += self.format_item(DT, "couvermulchplastique")
        fileContent += self.format_item(DT, "albedomulchplastique")
        fileContent += self.format_item(DT, "codrognage")
        fileContent += self.format_item(DT, "largrogne")
        fileContent += self.format_item(DT, "hautrogne")
        fileContent += self.format_item(DT, "biorognem")
        fileContent += self.format_item(DT, "codcalrogne")
        fileContent += self.format_item(DT, "julrogne")
        fileContent += self.format_item(DT, "margerogne")
        fileContent += self.format_item(DT, "codeclaircie")
        fileContent += self.format_item(DT, "juleclair")
        fileContent += self.format_item(DT, "nbinfloecl")
        fileContent += self.format_item(DT, "codeffeuil")
        fileContent += self.format_item(DT, "codhauteff")
        fileContent += self.format_item(DT, "codcaleffeuil")
        fileContent += self.format_item(DT, "laidebeff")
        fileContent += self.format_item(DT, "effeuil")
        fileContent += self.format_item(DT, "juleffeuil")
        fileContent += self.format_item(DT, "laieffeuil")
        fileContent += self.format_item(DT, "codetaille")
        fileContent += self.format_item(DT, "jultaille")
        fileContent += self.format_item(DT, "codepalissage")
        fileContent += self.format_item(DT, "hautmaxtec")
        fileContent += self.format_item(DT, "largtec")
        fileContent += self.format_item(DT, "codabri")
        fileContent += self.format_item(DT, "transplastic")
        fileContent += self.format_item(DT, "surfouvre1")
        fileContent += self.format_item(DT, "julouvre2")
        fileContent += self.format_item(DT, "surfouvre2")
        fileContent += self.format_item(DT, "julouvre3")
        fileContent += self.format_item(DT, "surfouvre3")
        fileContent += self.format_item(DT, "codeDST")
        fileContent += self.format_item(DT, "dachisel")
        fileContent += self.format_item(DT, "dalabour")
        fileContent += self.format_item(DT, "rugochisel")
        fileContent += self.format_item(DT, "rugolabour")
        fileContent += self.format_item(DT, "codeDSTtass")
        fileContent += self.format_item(DT, "profhumsemoir")
        fileContent += self.format_item(DT, "dasemis")
        fileContent += self.format_item(DT, "profhumrecolteuse")
        fileContent += self.format_item(DT, "darecolte")
        fileContent += self.format_item(DT, "codeDSTnbcouche")

        try:
            # Export file to specified directory
            self.write_file(usmdir, file_name, fileContent)
        except Exception as e:
            print("Error during writing file : " + str(e))
            traceback.print_exc()

    def format_item(self, row, champ, precision = 5, fieldIt = 0):
        fieldName = champ
        fileContent = ""
        if (fieldIt != 0):
            x = fieldName.split(".")
            fieldName = ".".join(x[1:])
        rw = row[row["Champ"] == champ]
        data = rw["dv"].values[0]
        res = ""
        if isinstance(data, str) or isinstance(data, int):
            res = str(data)
        if isinstance(data, float):
            tmp = float(data)
            if precision > 0 and precision < 7:
                res = "{:.{}f}".format(tmp, precision)
            else:
                res = "{:0.3e}".format(tmp)
        if data is None:
            res = ""
        fileContent += fieldName + "\n"
        fileContent += res + "\n"
        return fileContent
        

    def FormatSticsRawData(self, data, champ, precision  = 1):
        rw2 = data[data["champ"]==champ]
        res = rw2["dv"].values[0]
        return res



