from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback

class SticsFictec1Converter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, ModelDictionary_Connection, master_input_connection, usmdir, season_order=None, date_offset=0):
        file_name = "fictec1.txt"
        file_name2 = "fictec2.txt"
        fileContent = ""
        ST = directory_path.split(os.sep)
             
        fetchAllQuery = """SELECT SimUnitList.idsim, SimUnitList.idMangt, Soil.SoilTotalDepth, ListCultivars.idcultivarStics, CropManagement.sdens,
        CropManagement.sowingdate, CropManagement.SoilTillPolicyCode FROM Soil INNER JOIN (ListCultivars INNER JOIN (CropManagement INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt)
        ON ListCultivars.IdCultivar = CropManagement.Idcultivar) ON Lower(Soil.IdSoil) = Lower(SimUnitList.idsoil)  where idSim= '%s'"""%(ST[-3])
        season_filter = "" if season_order is None else " AND CropManagement.SeasonOrder = %d" % int(season_order)
        fetchAllQuery += season_filter + " ORDER BY CropManagement.PlantOrder;"
        DA = pd.read_sql_query(fetchAllQuery, master_input_connection)
        if date_offset:
            DA["sowingdate"] = DA["sowingdate"] + int(date_offset)
        rows = DA.to_dict(orient='records')
        rw = rows[0]

        Sql = """SELECT SoilTillPolicy.SoilTillPolicyCode, SoilTillageOperations.STNumber, SoilTillPolicy.NumTillOperations, SoilTillageOperations.DepthResUp, SoilTillageOperations.DepthResLow, SoilTillageOperations.DSTill
            FROM SoilTillPolicy INNER JOIN SoilTillageOperations ON SoilTillPolicy.SoilTillPolicyCode = SoilTillageOperations.SoilTillPolicyCode
            where SoilTillPolicy.SoilTillPolicyCode= '%s';"""%(rw["SoilTillPolicyCode"])
        
        

        T = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model='sticsv11') AND ([Table]='fictec1'));"
        DT = pd.read_sql_query(T, ModelDictionary_Connection)

        fetchallquery2 = """SELECT SimUnitList.idsim, CropManagement.sowingdate, OrganicFOperations.Dferti, OrganicFOperations.OFNumber, OrganicFOperations.CNferti, 
                OrganicFOperations.NFerti, OrganicFOperations.Qmanure, OrganicFOperations.TypeResidues, ListResidues.idresidueStics, CropManagement.SoilTillPolicyCode 
                FROM ListResidues INNER JOIN ((OrganicFertilizationPolicy INNER JOIN (CropManagement INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt) 
                ON OrganicFertilizationPolicy.OFertiPolicyCode = CropManagement.OFertiPolicyCode) INNER JOIN OrganicFOperations ON OrganicFertilizationPolicy.OFertiPolicyCode
                = OrganicFOperations.OFertiPolicyCode) ON ListResidues.TypeResidues = OrganicFOperations.TypeResidues where idSim='%s' and CropManagement.PlantOrder=1"""%(ST[-3])
        fetchallquery2 += season_filter + " Order by OFNumber;"

        DS2 = pd.read_sql_query(fetchallquery2, master_input_connection)   
        if date_offset and not DS2.empty:
            DS2["sowingdate"] = DS2["sowingdate"] + int(date_offset)
        rows2 = DS2.to_dict(orient='records')
        Adp = pd.read_sql_query(Sql, master_input_connection)
        dataTill = Adp.to_dict(orient='records')
            
        fileContent += "nbinterventions\n"
        if not rows2 or rows2[0]["idresidueStics"] is None:
            fileContent += "0\n"
        else:
            fileContent += str(len(rows2)) + "\n"
            if len(rows2) != 0:
                for i in range(len(rows2)):
                    fileContent += "julres coderes qres Crespc CsurNres Nminres eaures" + "\n"
                    fileContent += str(int(rows2[i]["sowingdate"]) + int(rows2[i]["Dferti"])) + " "
                    fileContent += str(rows2[i]["idresidueStics"]) + " "
                    fileContent += str(int(rows2[i]["Qmanure"])/1000) + " "
                    fileContent += str(rows2[i]["CNferti"] * rows2[i]["NFerti"]) + " "
                    fileContent += str(rows2[i]["CNferti"]) + " "
                    fileContent += str(rows2[i]["NFerti"]) + " "
                    fileContent += self.FormatSticsRawData(DT, "supply of organic residus.eaures") + "\n"
        fileContent += self.format_item(DT, "code_auto_profres")
        fileContent += self.format_item(DT, "resk")
        fileContent += self.format_item(DT, "resz")
        fileContent += "nbinterventions\n"
        fileContent += format(dataTill[0]["NumTillOperations"], ".0f") + "\n"
        if dataTill[0]["NumTillOperations"] > 0:
            for i in range(len(dataTill)):
                fileContent += "jultrav profres proftrav \n"
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
        fileContent += self.format_item(DT, "code_strip")
        fileContent += self.format_item(DT, "nrow")
        fileContent += self.format_item(DT, "codedecisemis")
        fileContent += self.format_item(DT, "nbjmaxapressemis")
        fileContent += self.format_item(DT, "nbjseuiltempref")
        fileContent += self.format_item(DT, "nbj_pr_apres_semis") # add
        fileContent += self.format_item(DT, "eau_mini_decisemis") # add
        fileContent += self.format_item(DT, "humirac_decisemis") # add
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
        fileContent += self.format_item(DT, "effirr")  # change positioon
        fileContent += self.format_item(DT, "codecalirrig")
        fileContent += self.format_item(DT, "ratiol")
        fileContent += self.format_item(DT, "dosimx")
        fileContent += self.format_item(DT, "doseirrigmin")
        fileContent += self.format_item(DT, "codedate_irrigauto") # add
        fileContent += self.format_item(DT, "datedeb_irrigauto") # add
        fileContent += self.format_item(DT, "datefin_irrigauto") # add
        fileContent += self.format_item(DT, "stage_start_irrigauto") # add
        fileContent += self.format_item(DT, "stage_end_irrigauto") # add
        fileContent += self.format_item(DT, "codedateappH2O")
        
        fileContent += "nbinterventions\n"
        fileContent += "0\n"
        fileContent += self.format_item(DT, "codlocirrig")
        fileContent += self.format_item(DT, "locirrig")
        fileContent += "profmes\n"
        fileContent += format(rw["SoilTotalDepth"], ".0f") + "\n"      
        #fileContent += self.format_item(DT, "engrais") # remove
        fileContent += self.format_item(DT, "concirr")
        fileContent += self.format_item(DT, "codedateappN")
        fileContent += self.format_item(DT, "codefracappN")
        fileContent += self.format_item(DT, "Qtot_N")

        fetchallquery3 = """Select SimUnitList.idsim, InorganicFOperations.N, CropManagement.sowingdate, InorganicFOperations.Dferti, InorganicFertilizationPolicy.NumInorganicFerti
            FROM(InorganicFertilizationPolicy INNER JOIN InorganicFOperations On InorganicFertilizationPolicy.InorgFertiPolicyCode = InorganicFOperations.InorgFertiPolicyCode)
            INNER JOIN (CropManagement INNER JOIN SimUnitList On CropManagement.idMangt = SimUnitList.idMangt) On InorganicFertilizationPolicy.InorgFertiPolicyCode =
            CropManagement.InoFertiPolicyCode where idSim='%s' and CropManagement.PlantOrder = 1"""%(ST[-3])
        fetchallquery3 += season_filter + ";"

        DS2 = pd.read_sql_query(fetchallquery3, master_input_connection)            
        if date_offset and not DS2.empty:
            DS2["sowingdate"] = DS2["sowingdate"] + int(date_offset)
        fileContent += "nbinterventions\n"
        fileContent += format(DS2.shape[0], ".0f") + "\n"
        if DS2.shape[0] > 0:
            for i in range(DS2.shape[0]):
                fileContent += "julapN_or_sum_upvt absolute_value/% engrais \n"
                fileContent += str(int(DS2.iloc[i]["sowingdate"] + DS2.iloc[i]["Dferti"])) + " "
                fileContent += str(DS2.iloc[i]["N"]) + " "
                rw_engrais = DT[DT["Champ"] == "engrais"]
                data = rw_engrais["dv"].values[0]
                fileContent += str(data) + "\n"
        fileContent += self.format_item(DT, "codlocferti")
        fileContent += self.format_item(DT, "locferti")
        fileContent += "irecbutoir\n"   # to here
        fileContent += format(rw["sowingdate"] + 250, ".0f") + "\n"
        fileContent += self.format_item(DT, "ressuite")
        fileContent += "code_autoressuite\n"   # do not see in the documentation
        fileContent += "2\n"   # do not see in the documentation
        fileContent += "Stubblevegratio\n"   # do not see in the documentation
        fileContent += "0\n"   # do not see in the documentation            
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
        fileContent += "code_hautfauche_dyn\n"   # Need to be added in the database
        fileContent += "2\n"   # Need to be added in the database   
        fileContent += "codetempfauche\n"   # Need to be added in the database
        fileContent += "1\n"   # Need to be added in the database 
        fileContent += self.format_item(DT, "codemodfauche")
        fileContent += self.format_item(DT, "hautcoupedefaut")
        fileContent += self.format_item(DT, "stadecoupedf")
        #fileContent += self.format_item(DT, "mscoupemini")

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
        fileContent += "nbinterventions\n"    # added
        fileContent += "0\n"
        #fileContent += self.format_item(DT, "juleclair")
        #fileContent += self.format_item(DT, "nbinfloecl")
            
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
        fileContent += "codejourdes\n"
        fileContent += "2\n"
        fileContent += "juldes\n"
        fileContent += "999\n"
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
        if len(rows) == 1: return fileContent

        T = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model='sticsv11') AND ([Table]='fictec2'));"
        DT = pd.read_sql_query(T, ModelDictionary_Connection)            
        zz1 = fileContent
        
        fetchallquery2 = """SELECT SimUnitList.idsim, CropManagement.sowingdate, OrganicFOperations.Dferti, OrganicFOperations.OFNumber, OrganicFOperations.CNferti, 
        OrganicFOperations.NFerti, OrganicFOperations.Qmanure, OrganicFOperations.TypeResidues, ListResidues.idresidueStics, CropManagement.SoilTillPolicyCode 
        FROM ListResidues INNER JOIN ((OrganicFertilizationPolicy INNER JOIN (CropManagement INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt) 
        ON OrganicFertilizationPolicy.OFertiPolicyCode = CropManagement.OFertiPolicyCode) INNER JOIN OrganicFOperations ON OrganicFertilizationPolicy.OFertiPolicyCode
        = OrganicFOperations.OFertiPolicyCode) ON ListResidues.TypeResidues = OrganicFOperations.TypeResidues where idSim='%s' and CropManagement.PlantOrder=2"""%(ST[-3])
        fetchallquery2 += season_filter + " Order by OFNumber;"

        fileContent = ""
        rw = rows[1]
        Sql = """SELECT SoilTillPolicy.SoilTillPolicyCode, SoilTillageOperations.STNumber, SoilTillPolicy.NumTillOperations, SoilTillageOperations.DepthResUp, SoilTillageOperations.DepthResLow, SoilTillageOperations.DSTill
                FROM SoilTillPolicy INNER JOIN SoilTillageOperations ON SoilTillPolicy.SoilTillPolicyCode = SoilTillageOperations.SoilTillPolicyCode
                where SoilTillPolicy.SoilTillPolicyCode= '%s';"""%(rw["SoilTillPolicyCode"])
        DS2 = pd.read_sql_query(fetchallquery2, master_input_connection)   
        if date_offset and not DS2.empty:
            DS2["sowingdate"] = DS2["sowingdate"] + int(date_offset)
        rows2 = DS2.to_dict(orient='records')
        Adp = pd.read_sql_query(Sql, master_input_connection)
        dataTill = Adp.to_dict(orient='records')
                
        fileContent += "nbinterventions\n"
        if not rows2 or rows2[0]["idresidueStics"] is None:
            fileContent += "0\n"
        else:
            fileContent += str(len(rows2)) + "\n"
            if len(rows2) != 0:
                for i in range(len(rows2)):
                    fileContent += "julres coderes qres Crespc CsurNres Nminres eaures" + "\n"
                    fileContent += str(int(rows2[i]["sowingdate"]) + int(rows2[i]["Dferti"])) + " "
                    fileContent += str(rows2[i]["idresidueStics"]) + " "
                    fileContent += str(int(rows2[i]["Qmanure"])/1000) + " "
                    fileContent += str(rows2[i]["CNferti"] * rows2[i]["NFerti"]) + " "
                    fileContent += str(rows2[i]["CNferti"]) + " "
                    fileContent += str(rows2[i]["NFerti"]) + " "
                    fileContent += self.FormatSticsRawData(DT, "supply of organic residus.eaures") + "\n"
        fileContent += self.format_item(DT, "code_auto_profres")
        fileContent += self.format_item(DT, "resk")
        fileContent += self.format_item(DT, "resz")
        fileContent += "nbinterventions\n"
        fileContent += format(dataTill[0]["NumTillOperations"], ".0f") + "\n"
        if dataTill[0]["NumTillOperations"] > 0:
            for i in range(len(dataTill)):
                fileContent += "jultrav profres proftrav \n"
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
        fileContent += self.format_item(DT, "code_strip")
        fileContent += self.format_item(DT, "nrow")
        fileContent += self.format_item(DT, "codedecisemis")
        fileContent += self.format_item(DT, "nbjmaxapressemis")
        fileContent += self.format_item(DT, "nbjseuiltempref")
        fileContent += self.format_item(DT, "nbj_pr_apres_semis") # add
        fileContent += self.format_item(DT, "eau_mini_decisemis") # add
        fileContent += self.format_item(DT, "humirac_decisemis") # add
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
        fileContent += self.format_item(DT, "effirr")  # change positioon
        fileContent += self.format_item(DT, "codecalirrig")
        fileContent += self.format_item(DT, "ratiol")
        fileContent += self.format_item(DT, "dosimx")
        fileContent += self.format_item(DT, "doseirrigmin")
        fileContent += self.format_item(DT, "codedate_irrigauto") # add
        fileContent += self.format_item(DT, "datedeb_irrigauto") # add
        fileContent += self.format_item(DT, "datefin_irrigauto") # add
        fileContent += self.format_item(DT, "stage_start_irrigauto") # add
        fileContent += self.format_item(DT, "stage_end_irrigauto") # add
        fileContent += self.format_item(DT, "codedateappH2O")
                
        fileContent += "nbinterventions\n"
        fileContent += "0\n"
        fileContent += self.format_item(DT, "codlocirrig")
        fileContent += self.format_item(DT, "locirrig")
        fileContent += "profmes\n"
        fileContent += format(rw["SoilTotalDepth"], ".0f") + "\n"      
        #fileContent += self.format_item(DT, "engrais") # remove
        fileContent += self.format_item(DT, "concirr")
        fileContent += self.format_item(DT, "codedateappN")
        fileContent += self.format_item(DT, "codefracappN")
        fileContent += self.format_item(DT, "Qtot_N")


        fetchallquery3 = """Select SimUnitList.idsim, InorganicFOperations.N, CropManagement.sowingdate, InorganicFOperations.Dferti, InorganicFertilizationPolicy.NumInorganicFerti
                FROM(InorganicFertilizationPolicy INNER JOIN InorganicFOperations On InorganicFertilizationPolicy.InorgFertiPolicyCode = InorganicFOperations.InorgFertiPolicyCode)
                INNER JOIN (CropManagement INNER JOIN SimUnitList On CropManagement.idMangt = SimUnitList.idMangt) On InorganicFertilizationPolicy.InorgFertiPolicyCode =
                CropManagement.InoFertiPolicyCode where idSim='%s' and CropManagement.PlantOrder = 2"""%(ST[-3])
        fetchallquery3 += season_filter + ";"

        DS2 = pd.read_sql_query(fetchallquery3, master_input_connection)            
        if date_offset and not DS2.empty:
            DS2["sowingdate"] = DS2["sowingdate"] + int(date_offset)
        fileContent += "nbinterventions\n"
        fileContent += format(DS2.shape[0], ".0f") + "\n"
        if DS2.shape[0] > 0:
            for i in range(DS2.shape[0]):
                fileContent += "julapN_or_sum_upvt absolute_value/% engrais \n"
                fileContent += str(int(DS2.iloc[i]["sowingdate"] + DS2.iloc[i]["Dferti"])) + " "
                fileContent += str(DS2.iloc[i]["N"]) + " "
                rw_engrais = DT[DT["Champ"] == "engrais"]
                data = rw_engrais["dv"].values[0]
                fileContent += str(data) + "\n"
        fileContent += self.format_item(DT, "codlocferti")
        fileContent += self.format_item(DT, "locferti")
        fileContent += "irecbutoir\n"   # to here
        fileContent += format(rw["sowingdate"] + 250, ".0f") + "\n"
        fileContent += self.format_item(DT, "ressuite")
        fileContent += "code_autoressuite\n"   # do not see in the documentation
        fileContent += "2\n"   # do not see in the documentation
        fileContent += "Stubblevegratio\n"   # do not see in the documentation
        fileContent += "0\n"   # do not see in the documentation            
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
        fileContent += "code_hautfauche_dyn\n"   # Need to be added in the database
        fileContent += "2\n"   # Need to be added in the database   
        fileContent += "codetempfauche\n"   # Need to be added in the database
        fileContent += "1\n"   # Need to be added in the database 
        fileContent += self.format_item(DT, "codemodfauche")
        fileContent += self.format_item(DT, "hautcoupedefaut")
        fileContent += self.format_item(DT, "stadecoupedf")
        #fileContent += self.format_item(DT, "mscoupemini"
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
        fileContent += "nbinterventions\n"    # added
        fileContent += "0\n"
        #fileContent += self.format_item(DT, "juleclair")
        #fileContent += self.format_item(DT, "nbinfloecl")
                
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
        fileContent += "codejourdes\n"
        fileContent += "2\n"
        fileContent += "juldes\n"
        fileContent += "999\n"
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
            self.write_file(usmdir, file_name2, fileContent)
        except Exception as e:
            print("Error during writing file : " + str(e))
            traceback.print_exc()  
                
        zz2 = fileContent              
            
        return [zz1, zz2]

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
        rw2 = data[data["Champ"]==champ]
        res = rw2["dv"].values[0]
        return res



