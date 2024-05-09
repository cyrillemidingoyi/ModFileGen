from modfilegen import GlobalVariables
from modfilegen.converter import Converter
from . import sticstempoparv6converter, sticsficiniconverter, sticsnewtravailconverter, sticsparamsolconverter
from . import sticstempoparconverter, sticsclimatconverter, sticsfictec1converter
from . import sticsstationconverter, sticsficplt1converter
import sys, subprocess, shutil


import os
import datetime
import sqlite3
from sqlite3 import Connection
from pathlib import Path
from multiprocessing import Pool
import pandas as pd
from time import time
import traceback


class SticsConverter(Converter):
    def __init__(self):
        super().__init__()
        self.MasterInput= GlobalVariables["dbMasterInput"]
        self.ModelDictionary = GlobalVariables["dbModelsDictionary"]
        self.DirectoryPath = ""
        self.pltfolder = ""
        self.tempoparfix = self.common_tempopar()
        self.tempoparfv6fix = self.common_tempoparv6()
        self.rap = self.common_rap()
        self.var = self.common_prof()
        self.prof = self.common_prof()


    def common_rap(self):
        fileContent = ""
        fileContent += "1\n"
        fileContent += "1\n"
        fileContent += "2\n"
        fileContent += "1\n"
        fileContent += "rec\n"
        fileContent += "masec(n)\n"
        fileContent += "mafruit\n"
        fileContent += "chargefruit\n"
        fileContent += "iplts\n"
        fileContent += "ilevs\n"
        fileContent += "iflos\n"
        fileContent += "imats\n"
        fileContent += "irecs\n"
        fileContent += "laimax\n"
        fileContent += "QNplante\n"
        fileContent += "Qles\n"
        fileContent += "QNapp\n" #'        fileContent += "soilN\n"
        fileContent += "ces\n"
        fileContent += "cep\n"
        return fileContent
    
    def common_prof(self):
        fileContent = ""
        fileContent += "2"
        fileContent += "tsol(iz)\n"
        fileContent += "10\n"
        fileContent += "01 01 2000\n"
        return fileContent

    def common_var(self):
        fileContent = ""
        fileContent += "lai(n)\n"
        fileContent += "masec(n)\n"
        fileContent += "mafruit\n"
        fileContent += "HR(1)\n"
        fileContent += "HR(2)\n"
        fileContent += "HR(3)\n"
        fileContent += "HR(4)\n"
        fileContent += "HR(5)\n"
        fileContent += "resmes\n"
        fileContent += "drain\n"
        fileContent += "esol\n"
        fileContent += "et\n"
        fileContent += "zrac\n"
        fileContent += "tcult\n"
        fileContent += "AZnit(1)\n"
        fileContent += "AZnit(2)\n"
        fileContent += "AZnit(3)\n"
        fileContent += "AZnit(4)\n"
        fileContent += "AZnit(5)\n"
        fileContent += "Qles\n"
        fileContent += "QNplante\n"
        fileContent += "azomes\n"
        fileContent += "inn\n"
        fileContent += "chargefruit\n"
        fileContent += "AZamm(1)\n"
        fileContent += "AZamm(2)\n"
        fileContent += "AZamm(3)\n"
        fileContent += "AZamm(4)\n"
        fileContent += "AZamm(5)\n"
        #'fileContent += "leaching_from_plt\n"
        fileContent += "CNgrain\n"
        fileContent += "concNO3les\n"
        fileContent += "drat\n"
        fileContent += "fapar\n"
        fileContent += "hauteur\n"
        fileContent += "Hmax\n"
        fileContent += "humidite\n"
        fileContent += "LRACH(1)\n"
        fileContent += "LRACH(2)\n"
        fileContent += "LRACH(3)\n"
        fileContent += "LRACH(4)\n"
        fileContent += "LRACH(5)\n"
        fileContent += "mafrais\n"
        fileContent += "pdsfruitfrais\n"
        fileContent += "Qdrain\n"
        fileContent += "rnet\n"
        fileContent += "QNapp\n"
        #'fileContent += "soilN\n"
        fileContent += "ces\n"
        fileContent += "cep\n"
        #'fileContent += "QNplante\n"
        #'fileContent += "Qles\n"
        #'fileContent += "soilN\n"
        return fileContent
        

    def common_tempoparv6(self):
        fileContent = ""
        ModelDictionary_Connection = sqlite3.connect(self.ModelDictionary)

        # Tempopar query
        T = "Select  Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model='stics') AND ([Table]='st_tempoparv6'));"
        DT = pd.read_sql_query(T,ModelDictionary_Connection)

        # Ajouter les résultats à file_content
        fileContent += self.format_stics_data_v6(DT, "codetempfauche")
        fileContent += self.format_stics_data_v6(DT, "coefracoupe1", 1, 1)
        fileContent += self.format_stics_data_v6(DT, "coefracoupe2", 1, 1)
        fileContent += self.format_stics_data_v6(DT, "codepluiepoquet")
        fileContent += self.format_stics_data_v6(DT, "nbjoursrrversirrig")
        fileContent += self.format_stics_data_v6(DT, "swfacmin", 1)
        fileContent += self.format_stics_data_v6(DT, "codetranspitalle")
        fileContent += self.format_stics_data_v6(DT, "codedyntalle1", -1, 1)
        fileContent += self.format_stics_data_v6(DT, "SurfApex1", -1, 1)
        fileContent += self.format_stics_data_v6(DT, "SeuilMorTalle1", 2, 1)
        fileContent += self.format_stics_data_v6(DT, "SigmaDisTalle1", 1, 1)
        fileContent += self.format_stics_data_v6(DT, "VitReconsPeupl1", -1, 1)
        fileContent += self.format_stics_data_v6(DT, "SeuilReconsPeupl1", 1, 1)
        fileContent += self.format_stics_data_v6(DT, "MaxTalle1", 1, 1)
        fileContent += self.format_stics_data_v6(DT, "SeuilLAIapex1", 1, 1)
        fileContent += self.format_stics_data_v6(DT, "tigefeuilcoupe1", 1, 1)
        fileContent += self.format_stics_data_v6(DT, "codedyntalle2", 1, 1)
        fileContent += self.format_stics_data_v6(DT, "SurfApex2", -1, 1)
        fileContent += self.format_stics_data_v6(DT, "SeuilMorTalle2", 1, 1)
        fileContent += self.format_stics_data_v6(DT, "SigmaDisTalle2", 1, 1)
        fileContent += self.format_stics_data_v6(DT, "VitReconsPeupl2", -1, 1)
        fileContent += self.format_stics_data_v6(DT, "SeuilReconsPeupl2", 1, 1)
        fileContent += self.format_stics_data_v6(DT, "MaxTalle2", 1, 1)
        fileContent += self.format_stics_data_v6(DT, "SeuilLAIapex2", 1, 1)
        fileContent += self.format_stics_data_v6(DT, "tigefeuilcoupe2", 1, 1)
        fileContent += self.format_stics_data_v6(DT, "resplmax1", 2, 1)
        fileContent += self.format_stics_data_v6(DT, "resplmax2", 2, 1)
        fileContent += self.format_stics_data_v6(DT, "codemontaison1", -1, 1)
        fileContent += self.format_stics_data_v6(DT, "codemontaison2", -1, 1)
        fileContent += self.format_stics_data_v6(DT, "nbj_pr_apres_semis")
        fileContent += self.format_stics_data_v6(DT, "eau_mini_decisemis")
        fileContent += self.format_stics_data_v6(DT, "humirac_decisemis", 2)
        fileContent += self.format_stics_data_v6(DT, "codecalferti")
        fileContent += self.format_stics_data_v6(DT, "ratiolN", 5)
        fileContent += self.format_stics_data_v6(DT, "dosimxN", 5)
        fileContent += self.format_stics_data_v6(DT, "codetesthumN")
        fileContent += self.format_stics_data_v6(DT, "codeNmindec")
        fileContent += self.format_stics_data_v6(DT, "rapNmindec", 5)
        fileContent += self.format_stics_data_v6(DT, "fNmindecmin", 5)
        fileContent += self.format_stics_data_v6(DT, "codetrosee")
        fileContent += self.format_stics_data_v6(DT, "codeSWDRH")
        fileContent += self.format_stics_data_v6(DT, "P_codedate_irrigauto")
        fileContent += self.format_stics_data_v6(DT, "datedeb_irrigauto")
        fileContent += self.format_stics_data_v6(DT, "datefin_irrigauto")
        fileContent += self.format_stics_data_v6(DT, "stage_start_irrigauto")
        fileContent += self.format_stics_data_v6(DT, "stage_end_irrigauto")
        fileContent += self.format_stics_data_v6(DT, "codemortalracine")
        fileContent += self.format_stics_data_v6(DT, "option_thinning")
        fileContent += self.format_stics_data_v6(DT, "option_engrais_multiple")
        fileContent += self.format_stics_data_v6(DT, "option_pature")
        fileContent += self.format_stics_data_v6(DT, "coderes_pature")
        fileContent += self.format_stics_data_v6(DT, "pertes_restit_ext")
        fileContent += self.format_stics_data_v6(DT, "Crespc_pature")
        fileContent += self.format_stics_data_v6(DT, "Nminres_pature")
        fileContent += self.format_stics_data_v6(DT, "eaures_pature")
        fileContent += self.format_stics_data_v6(DT, "coef_calcul_qres")
        fileContent += self.format_stics_data_v6(DT, "engrais_pature")
        fileContent += self.format_stics_data_v6(DT, "coef_calcul_doseN")
        fileContent += self.format_stics_data_v6(DT, "codemineralOM")
        fileContent += self.format_stics_data_v6(DT, "GMIN1")
        fileContent += self.format_stics_data_v6(DT, "GMIN2")
        fileContent += self.format_stics_data_v6(DT, "GMIN3")
        fileContent += self.format_stics_data_v6(DT, "GMIN4")
        fileContent += self.format_stics_data_v6(DT, "GMIN5")
        fileContent += self.format_stics_data_v6(DT, "GMIN6")
        fileContent += self.format_stics_data_v6(DT, "GMIN7")
        fileContent += "\n"
        ModelDictionary_Connection.close()
        return fileContent

    def format_stics_data_v6(self, row, champ, precision=5, field_it=0):
        res = ""
        type_data = ""
        data = None
        field_name = champ
        file_content = ""

        # For repeated fields, build field name
        if field_it != 0:
            field_name = field_name[:-1] + "(" + field_name[-1] + ")"
            # champ = champ + str(field_it)

        # Fetch data
        rw = row[row['Champ'] == champ]
        if len(rw) == 0:
            pass
        else:
            data = rw["dv"].values[0]
            res = ""

            # If type is string or int
            if isinstance(data, str) or isinstance(data, int):
                res = str(data)

            # If type is real
            if isinstance(data, float):
                tmp = float(data)
                if 0 < precision < 7:
                    res = "{:.{}f}".format(tmp, precision)
                else:
                    res = "{:0.3e}".format(tmp)
            # If cell is null
            if data is None:
                res = ""
            # Print data in file
            file_content += field_name + "\n"
            file_content += res + "\n"
        return file_content
    
    def common_tempopar(self):
        ModelDictionary_Connection = sqlite3.connect(self.ModelDictionary)
        T = "Select  Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model='stics') AND ([Table]='st_tempopar')) OR ((model='stics') AND ([Table]='st_tempopar_2')) OR ((model='stics') AND ([Table]='st_tempopar_3'));"
        DT = pd.read_sql_query(T,ModelDictionary_Connection)

        # Ajouter les résultats à file_content
        fileContent = ""
        fileContent += self.format_stics_data(DT, "codeminopt")
        fileContent += self.format_stics_data(DT, "iniprofil")
        fileContent += self.format_stics_data(DT, "codeprofmes")
        fileContent += self.format_stics_data(DT, "codeinitprec")
        fileContent += self.format_stics_data(DT, "codemsfinal")
        fileContent += self.format_stics_data(DT, "codeactimulch")
        fileContent += self.format_stics_data(DT, "codefrmur")
        fileContent += self.format_stics_data(DT, "codemicheur")
        fileContent += self.format_stics_data(DT, "codeoutscient")
        fileContent += self.format_stics_data(DT, "codeseprapport")
        fileContent += self.format_stics_data(DT, "separateurrapport")
        fileContent += self.format_stics_data(DT, "codesensibilite")
        fileContent += self.format_stics_data(DT, "codesnow")
        fileContent += self.format_stics_data(DT, "flagecriture")
        fileContent += self.format_stics_data(DT, "parsurrg")
        fileContent += self.format_stics_data(DT, "coefb")
        fileContent += self.format_stics_data(DT, "proprac")
        fileContent += self.format_stics_data(DT, "y0msrac")
        fileContent += self.format_stics_data(DT, "khaut")
        fileContent += self.format_stics_data(DT, "dacohes")
        fileContent += self.format_stics_data(DT, "daseuilbas")
        fileContent += self.format_stics_data(DT, "daseuilhaut")
        fileContent += self.format_stics_data(DT, "beta")
        fileContent += self.format_stics_data(DT, "lvopt")
        fileContent += self.format_stics_data(DT, "rayon")
        fileContent += self.format_stics_data(DT, "difN")
        fileContent += self.format_stics_data(DT, "concrr")
        fileContent += self.format_stics_data(DT, "plNmin")
        fileContent += self.format_stics_data(DT, "irrlev")
        fileContent += self.format_stics_data(DT, "QNpltminINN")
        fileContent += self.format_stics_data(DT, "codesymbiose")
        fileContent += self.format_stics_data(DT, "codefxn")
        fileContent += self.format_stics_data(DT, "FTEMh")
        fileContent += self.format_stics_data(DT, "FTEMha")
        fileContent += self.format_stics_data(DT, "TREFh")
        fileContent += self.format_stics_data(DT, "FTEMr")
        fileContent += self.format_stics_data(DT, "FTEMra")
        fileContent += self.format_stics_data(DT, "TREFr")
        fileContent += self.format_stics_data(DT, "FINERT")
        fileContent += self.format_stics_data(DT, "FMIN1")
        fileContent += self.format_stics_data(DT, "FMIN2")
        fileContent += self.format_stics_data(DT, "FMIN3")
        fileContent += self.format_stics_data(DT, "Wh")
        fileContent += self.format_stics_data(DT, "pHminvol")
        fileContent += self.format_stics_data(DT, "pHmaxvol")
        fileContent += self.format_stics_data(DT, "Vabs2")
        fileContent += self.format_stics_data(DT, "Xorgmax")
        fileContent += self.format_stics_data(DT, "hminm")
        fileContent += self.format_stics_data(DT, "hoptm")
        fileContent += self.format_stics_data(DT, "alphaph")
        fileContent += self.format_stics_data(DT, "dphvolmax")
        fileContent += self.format_stics_data(DT, "phvols")
        fileContent += self.format_stics_data(DT, "fhminsat")
        fileContent += self.format_stics_data(DT, "fredkN")
        fileContent += self.format_stics_data(DT, "fredlN")
        fileContent += self.format_stics_data(DT, "fNCbiomin")
        fileContent += self.format_stics_data(DT, "fredNsup")
        fileContent += self.format_stics_data(DT, "Primingmax")
        fileContent += self.format_stics_data(DT, "hminn")
        fileContent += self.format_stics_data(DT, "hoptn")
        fileContent += self.format_stics_data(DT, "pHminnit")
        fileContent += self.format_stics_data(DT, "pHmaxnit")
        fileContent += self.format_stics_data(DT, "nh4_min")
        fileContent += self.format_stics_data(DT, "pHminden")
        fileContent += self.format_stics_data(DT, "pHmaxden")
        fileContent += self.format_stics_data(DT, "wfpsc")
        fileContent += self.format_stics_data(DT, "tdenitopt_gauss")
        fileContent += self.format_stics_data(DT, "scale_tdenitopt")
        fileContent += self.format_stics_data(DT, "Kd")
        fileContent += self.format_stics_data(DT, "k_desat")
        fileContent += self.format_stics_data(DT, "code_vnit")
        fileContent += self.format_stics_data(DT, "fnx")
        fileContent += self.format_stics_data(DT, "vnitmax")
        fileContent += self.format_stics_data(DT, "Kamm")
        fileContent += self.format_stics_data(DT, "code_tnit")
        fileContent += self.format_stics_data(DT, "tnitmin")
        fileContent += self.format_stics_data(DT, "tnitopt")
        fileContent += self.format_stics_data(DT, "tnitopt2")
        fileContent += self.format_stics_data(DT, "tnitmax")
        fileContent += self.format_stics_data(DT, "tnitopt_gauss")
        fileContent += self.format_stics_data(DT, "scale_tnitopt")
        fileContent += self.format_stics_data(DT, "code_rationit")
        fileContent += self.format_stics_data(DT, "rationit", 6)
        fileContent += self.format_stics_data(DT, "code_hourly_wfps_nit")
        fileContent += self.format_stics_data(DT, "code_pdenit")
        fileContent += self.format_stics_data(DT, "cmin_pdenit")
        fileContent += self.format_stics_data(DT, "cmax_pdenit")
        fileContent += self.format_stics_data(DT, "min_pdenit")
        fileContent += self.format_stics_data(DT, "max_pdenit")
        fileContent += self.format_stics_data(DT, "code_ratiodenit")
        fileContent += self.format_stics_data(DT, "ratiodenit")
        fileContent += self.format_stics_data(DT, "code_hourly_wfps_denit")
        fileContent += self.format_stics_data(DT, "pminruis")
        fileContent += self.format_stics_data(DT, "diftherm")
        fileContent += self.format_stics_data(DT, "bformnappe")
        fileContent += self.format_stics_data(DT, "rdrain")
        fileContent += self.format_stics_data(DT, "psihumin")
        fileContent += self.format_stics_data(DT, "psihucc")
        fileContent += self.format_stics_data(DT, "prophumtasssem")
        fileContent += self.format_stics_data(DT, "prophumtassrec")
        fileContent += self.format_stics_data(DT, "codhnappe")
        fileContent += self.format_stics_data(DT, "distdrain", 2, 0)
        fileContent += self.format_stics_data(DT, "proflabour")
        fileContent += self.format_stics_data(DT, "proftravmin")
        fileContent += self.format_stics_data(DT, "codetycailloux")
        fileContent += self.format_stics_data(DT, "masvolcx", 5, 1)
        fileContent += self.format_stics_data(DT, "hcccx", 5, 1)
        fileContent += self.format_stics_data(DT, "masvolcx", 5, 2)
        fileContent += self.format_stics_data(DT, "hcccx", 5, 2)
        fileContent += self.format_stics_data(DT, "masvolcx", 5, 3)
        fileContent += self.format_stics_data(DT, "hcccx", 5, 3)
        fileContent += self.format_stics_data(DT, "masvolcx", 5, 4)
        fileContent += self.format_stics_data(DT, "hcccx", 5, 4)
        fileContent += self.format_stics_data(DT, "masvolcx", 5, 5)
        fileContent += self.format_stics_data(DT, "hcccx", 5, 5)
        fileContent += self.format_stics_data(DT, "masvolcx", 5, 6)
        fileContent += self.format_stics_data(DT, "hcccx", 5, 6)
        fileContent += self.format_stics_data(DT, "masvolcx", 5, 7)
        fileContent += self.format_stics_data(DT, "hcccx", 5, 7)
        fileContent += self.format_stics_data(DT, "masvolcx", 5, 8)
        fileContent += self.format_stics_data(DT, "hcccx", 5, 8)
        fileContent += self.format_stics_data(DT, "masvolcx", 5, 9)
        fileContent += self.format_stics_data(DT, "hcccx", 5, 9)
        fileContent += self.format_stics_data(DT, "masvolcx", 5, 10)
        fileContent += self.format_stics_data(DT, "hcccx", 5, 10)
        fileContent += self.format_stics_data(DT, "codetypeng")
        fileContent += self.format_stics_data(DT, "engamm", 5, 1)
        fileContent += self.format_stics_data(DT, "orgeng", 5, 1)
        fileContent += self.format_stics_data(DT, "deneng", 5, 1)
        fileContent += self.format_stics_data(DT, "voleng", 5, 1)
        fileContent += self.format_stics_data(DT, "engamm", 5, 2)
        fileContent += self.format_stics_data(DT, "orgeng", 5, 2)
        fileContent += self.format_stics_data(DT, "deneng", 5, 2)
        fileContent += self.format_stics_data(DT, "voleng", 5, 2)
        fileContent += self.format_stics_data(DT, "engamm", 5, 3)
        fileContent += self.format_stics_data(DT, "orgeng", 5, 3)
        fileContent += self.format_stics_data(DT, "deneng", 5, 3)
        fileContent += self.format_stics_data(DT, "voleng", 5, 3)
        fileContent += self.format_stics_data(DT, "engamm", 5, 4)
        fileContent += self.format_stics_data(DT, "orgeng", 5, 4)
        fileContent += self.format_stics_data(DT, "deneng", 5, 4)
        fileContent += self.format_stics_data(DT, "voleng", 5, 4)
        fileContent += self.format_stics_data(DT, "engamm", 5, 5)
        fileContent += self.format_stics_data(DT, "orgeng", 5, 5)
        fileContent += self.format_stics_data(DT, "deneng", 5, 5)
        fileContent += self.format_stics_data(DT, "voleng", 5, 5)
        fileContent += self.format_stics_data(DT, "engamm", 5, 6)
        fileContent += self.format_stics_data(DT, "orgeng", 5, 6)
        fileContent += self.format_stics_data(DT, "deneng", 5, 6)
        fileContent += self.format_stics_data(DT, "voleng", 5, 6)
        fileContent += self.format_stics_data(DT, "engamm", 5, 7)
        fileContent += self.format_stics_data(DT, "orgeng", 5, 7)
        fileContent += self.format_stics_data(DT, "deneng", 5, 7)
        fileContent += self.format_stics_data(DT, "voleng", 5, 7)
        fileContent += self.format_stics_data(DT, "engamm", 5, 8)
        fileContent += self.format_stics_data(DT, "orgeng", 5, 8)
        fileContent += self.format_stics_data(DT, "deneng", 5, 8)
        fileContent += self.format_stics_data(DT, "voleng", 5, 8)
        fileContent += self.format_stics_data(DT, "codetypres")

        fileContent += self.format_stics_data(DT, "CroCo", 5, 1)
        fileContent += self.format_stics_data(DT, "akres", 5, 1)
        fileContent += self.format_stics_data(DT, "bkres", 5, 1)
        fileContent += self.format_stics_data(DT, "awb", 5, 1)
        fileContent += self.format_stics_data(DT, "bwb", 5, 1)
        fileContent += self.format_stics_data(DT, "cwb", 5, 1)
        fileContent += self.format_stics_data(DT, "ahres", 5, 1)
        fileContent += self.format_stics_data(DT, "bhres", 5, 1)
        fileContent += self.format_stics_data(DT, "kbio", 5, 1)
        fileContent += self.format_stics_data(DT, "yres", 5, 1)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 1)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 1)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 1)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 1)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 1)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 1)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 1)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 2)
        fileContent += self.format_stics_data(DT, "akres", 5, 2)
        fileContent += self.format_stics_data(DT, "bkres", 5, 2)
        fileContent += self.format_stics_data(DT, "awb", 5, 2)
        fileContent += self.format_stics_data(DT, "bwb", 5, 2)
        fileContent += self.format_stics_data(DT, "cwb", 5, 2)
        fileContent += self.format_stics_data(DT, "ahres", 5, 2)
        fileContent += self.format_stics_data(DT, "bhres", 5, 2)
        fileContent += self.format_stics_data(DT, "kbio", 5, 2)
        fileContent += self.format_stics_data(DT, "yres", 5, 2)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 2)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 2)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 2)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 2)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 2)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 2)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 2)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 3)
        fileContent += self.format_stics_data(DT, "akres", 5, 3)
        fileContent += self.format_stics_data(DT, "bkres", 5, 3)
        fileContent += self.format_stics_data(DT, "awb", 5, 3)
        fileContent += self.format_stics_data(DT, "bwb", 5, 3)
        fileContent += self.format_stics_data(DT, "cwb", 5, 3)
        fileContent += self.format_stics_data(DT, "ahres", 5, 3)
        fileContent += self.format_stics_data(DT, "bhres", 5, 3)
        fileContent += self.format_stics_data(DT, "kbio", 5, 3)
        fileContent += self.format_stics_data(DT, "yres", 5, 3)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 3)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 3)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 3)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 3)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 3)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 3)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 3)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 4)
        fileContent += self.format_stics_data(DT, "akres", 5, 4)
        fileContent += self.format_stics_data(DT, "bkres", 5, 4)
        fileContent += self.format_stics_data(DT, "awb", 5, 4)
        fileContent += self.format_stics_data(DT, "bwb", 5, 4)
        fileContent += self.format_stics_data(DT, "cwb", 5, 4)
        fileContent += self.format_stics_data(DT, "ahres", 5, 4)
        fileContent += self.format_stics_data(DT, "bhres", 5, 4)
        fileContent += self.format_stics_data(DT, "kbio", 5, 4)
        fileContent += self.format_stics_data(DT, "yres", 5, 4)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 4)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 4)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 4)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 4)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 4)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 4)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 4)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 5)
        fileContent += self.format_stics_data(DT, "akres", 5, 5)
        fileContent += self.format_stics_data(DT, "bkres", 5, 5)
        fileContent += self.format_stics_data(DT, "awb", 5, 5)
        fileContent += self.format_stics_data(DT, "bwb", 5, 5)
        fileContent += self.format_stics_data(DT, "cwb", 5, 5)
        fileContent += self.format_stics_data(DT, "ahres", 5, 5)
        fileContent += self.format_stics_data(DT, "bhres", 5, 5)
        fileContent += self.format_stics_data(DT, "kbio", 5, 5)
        fileContent += self.format_stics_data(DT, "yres", 5, 5)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 5)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 5)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 5)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 5)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 5)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 5)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 5)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 6)
        fileContent += self.format_stics_data(DT, "akres", 5, 6)
        fileContent += self.format_stics_data(DT, "bkres", 5, 6)
        fileContent += self.format_stics_data(DT, "awb", 5, 6)
        fileContent += self.format_stics_data(DT, "bwb", 5, 6)
        fileContent += self.format_stics_data(DT, "cwb", 5, 6)
        fileContent += self.format_stics_data(DT, "ahres", 5, 6)
        fileContent += self.format_stics_data(DT, "bhres", 5, 6)
        fileContent += self.format_stics_data(DT, "kbio", 5, 6)
        fileContent += self.format_stics_data(DT, "yres", 5, 6)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 6)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 6)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 6)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 6)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 6)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 6)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 6)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 7)
        fileContent += self.format_stics_data(DT, "akres", 5, 7)
        fileContent += self.format_stics_data(DT, "bkres", 5, 7)
        fileContent += self.format_stics_data(DT, "awb", 5, 7)
        fileContent += self.format_stics_data(DT, "bwb", 5, 7)
        fileContent += self.format_stics_data(DT, "cwb", 5, 7)
        fileContent += self.format_stics_data(DT, "ahres", 5, 7)
        fileContent += self.format_stics_data(DT, "bhres", 5, 7)
        fileContent += self.format_stics_data(DT, "kbio", 5, 7)

        fileContent += self.format_stics_data(DT, "yres", 5, 7)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 7)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 7)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 7)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 7)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 7)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 7)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 7)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 8)
        fileContent += self.format_stics_data(DT, "akres", 5, 8)
        fileContent += self.format_stics_data(DT, "bkres", 5, 8)
        fileContent += self.format_stics_data(DT, "awb", 5, 8)
        fileContent += self.format_stics_data(DT, "bwb", 5, 8)
        fileContent += self.format_stics_data(DT, "cwb", 5, 8)
        fileContent += self.format_stics_data(DT, "ahres", 5, 8)
        fileContent += self.format_stics_data(DT, "bhres", 5, 8)
        fileContent += self.format_stics_data(DT, "kbio", 5, 8)
        fileContent += self.format_stics_data(DT, "yres", 5, 8)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 8)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 8)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 8)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 8)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 8)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 8)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 8)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 9)
        fileContent += self.format_stics_data(DT, "akres", 5, 9)
        fileContent += self.format_stics_data(DT, "bkres", 5, 9)
        fileContent += self.format_stics_data(DT, "awb", 5, 9)
        fileContent += self.format_stics_data(DT, "bwb", 5, 9)
        fileContent += self.format_stics_data(DT, "cwb", 5, 9)
        fileContent += self.format_stics_data(DT, "ahres", 5, 9)
        fileContent += self.format_stics_data(DT, "bhres", 5, 9)
        fileContent += self.format_stics_data(DT, "kbio", 5, 9)
        fileContent += self.format_stics_data(DT, "yres", 5, 9)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 9)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 9)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 9)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 9)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 9)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 9)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 9)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 10)
        fileContent += self.format_stics_data(DT, "akres", 5, 10)
        fileContent += self.format_stics_data(DT, "bkres", 5, 10)
        fileContent += self.format_stics_data(DT, "awb", 5, 10)
        fileContent += self.format_stics_data(DT, "bwb", 5, 10)
        fileContent += self.format_stics_data(DT, "cwb", 5, 10)
        fileContent += self.format_stics_data(DT, "ahres", 5, 10)
        fileContent += self.format_stics_data(DT, "bhres", 5, 10)
        fileContent += self.format_stics_data(DT, "kbio", 5, 10)
        fileContent += self.format_stics_data(DT, "yres", 5, 10)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 10)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 10)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 10)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 10)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 10)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 10)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 10)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 11)
        fileContent += self.format_stics_data(DT, "akres", 5, 11)
        fileContent += self.format_stics_data(DT, "bkres", 5, 11)
        fileContent += self.format_stics_data(DT, "awb", 5, 11)
        fileContent += self.format_stics_data(DT, "bwb", 5, 11)
        fileContent += self.format_stics_data(DT, "cwb", 5, 11)
        fileContent += self.format_stics_data(DT, "ahres", 5, 11)
        fileContent += self.format_stics_data(DT, "bhres", 5, 11)
        fileContent += self.format_stics_data(DT, "kbio", 5, 11)
        fileContent += self.format_stics_data(DT, "yres", 5, 11)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 11)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 11)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 11)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 11)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 11)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 11)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 11)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 12)
        fileContent += self.format_stics_data(DT, "akres", 5, 12)
        fileContent += self.format_stics_data(DT, "bkres", 5, 12)
        fileContent += self.format_stics_data(DT, "awb", 5, 12)
        fileContent += self.format_stics_data(DT, "bwb", 5, 12)
        fileContent += self.format_stics_data(DT, "cwb", 5, 12)
        fileContent += self.format_stics_data(DT, "ahres", 5, 12)
        fileContent += self.format_stics_data(DT, "bhres", 5, 12)
        fileContent += self.format_stics_data(DT, "kbio", 5, 12)
        fileContent += self.format_stics_data(DT, "yres", 5, 12)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 12)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 12)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 12)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 12)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 12)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 12)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 12)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 13)
        fileContent += self.format_stics_data(DT, "akres", 5, 13)
        fileContent += self.format_stics_data(DT, "bkres", 5, 13)
        fileContent += self.format_stics_data(DT, "awb", 5, 13)
        fileContent += self.format_stics_data(DT, "bwb", 5, 13)
        fileContent += self.format_stics_data(DT, "cwb", 5, 13)
        fileContent += self.format_stics_data(DT, "ahres", 5, 13)
        fileContent += self.format_stics_data(DT, "bhres", 5, 13)
        fileContent += self.format_stics_data(DT, "kbio", 5, 13)
        fileContent += self.format_stics_data(DT, "yres", 5, 13)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 13)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 13)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 13)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 13)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 13)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 13)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 13)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 14)
        fileContent += self.format_stics_data(DT, "akres", 5, 14)
        fileContent += self.format_stics_data(DT, "bkres", 5, 14)
        fileContent += self.format_stics_data(DT, "awb", 5, 14)
        fileContent += self.format_stics_data(DT, "bwb", 5, 14)
        fileContent += self.format_stics_data(DT, "cwb", 5, 14)
        fileContent += self.format_stics_data(DT, "ahres", 5, 14)
        fileContent += self.format_stics_data(DT, "bhres", 5, 14)
        fileContent += self.format_stics_data(DT, "kbio", 5, 14)
        fileContent += self.format_stics_data(DT, "yres", 5, 14)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 14)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 14)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 14)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 14)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 14)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 14)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 14)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 15)
        fileContent += self.format_stics_data(DT, "akres", 5, 15)
        fileContent += self.format_stics_data(DT, "bkres", 5, 15)
        fileContent += self.format_stics_data(DT, "awb", 5, 15)
        fileContent += self.format_stics_data(DT, "bwb", 5, 15)
        fileContent += self.format_stics_data(DT, "cwb", 5, 15)
        fileContent += self.format_stics_data(DT, "ahres", 5, 15)
        fileContent += self.format_stics_data(DT, "bhres", 5, 15)
        fileContent += self.format_stics_data(DT, "kbio", 5, 15)
        fileContent += self.format_stics_data(DT, "yres", 5, 15)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 15)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 15)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 15)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 15)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 15)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 15)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 15)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 16)
        fileContent += self.format_stics_data(DT, "akres", 5, 16)
        fileContent += self.format_stics_data(DT, "bkres", 5, 16)
        fileContent += self.format_stics_data(DT, "awb", 5, 16)
        fileContent += self.format_stics_data(DT, "bwb", 5, 16)
        fileContent += self.format_stics_data(DT, "cwb", 5, 16)
        fileContent += self.format_stics_data(DT, "ahres", 5, 16)
        fileContent += self.format_stics_data(DT, "bhres", 5, 16)
        fileContent += self.format_stics_data(DT, "kbio", 5, 16)
        fileContent += self.format_stics_data(DT, "yres", 5, 16)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 16)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 16)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 16)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 16)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 16)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 16)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 16)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 17)
        fileContent += self.format_stics_data(DT, "akres", 5, 17)
        fileContent += self.format_stics_data(DT, "bkres", 5, 17)
        fileContent += self.format_stics_data(DT, "awb", 5, 17)
        fileContent += self.format_stics_data(DT, "bwb", 5, 17)
        fileContent += self.format_stics_data(DT, "cwb", 5, 17)
        fileContent += self.format_stics_data(DT, "ahres", 5, 17)
        fileContent += self.format_stics_data(DT, "bhres", 5, 17)
        fileContent += self.format_stics_data(DT, "kbio", 5, 17)
        fileContent += self.format_stics_data(DT, "yres", 5, 17)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 17)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 17)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 17)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 17)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 17)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 17)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 17)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 18)
        fileContent += self.format_stics_data(DT, "akres", 5, 18)
        fileContent += self.format_stics_data(DT, "bkres", 5, 18)
        fileContent += self.format_stics_data(DT, "awb", 5, 18)
        fileContent += self.format_stics_data(DT, "bwb", 5, 18)
        fileContent += self.format_stics_data(DT, "cwb", 5, 18)
        fileContent += self.format_stics_data(DT, "ahres", 5, 18)
        fileContent += self.format_stics_data(DT, "bhres", 5, 18)
        fileContent += self.format_stics_data(DT, "kbio", 5, 18)
        fileContent += self.format_stics_data(DT, "yres", 5, 18)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 18)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 18)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 18)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 18)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 18)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 18)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 18)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 19)
        fileContent += self.format_stics_data(DT, "akres", 5, 19)
        fileContent += self.format_stics_data(DT, "bkres", 5, 19)
        fileContent += self.format_stics_data(DT, "awb", 5, 19)
        fileContent += self.format_stics_data(DT, "bwb", 5, 19)
        fileContent += self.format_stics_data(DT, "cwb", 5, 19)
        fileContent += self.format_stics_data(DT, "ahres", 5, 19)
        fileContent += self.format_stics_data(DT, "bhres", 5, 19)
        fileContent += self.format_stics_data(DT, "kbio", 5, 19)
        fileContent += self.format_stics_data(DT, "yres", 5, 19)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 19)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 19)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 19)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 19)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 19)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 19)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 19)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 20)
        fileContent += self.format_stics_data(DT, "akres", 5, 20)
        fileContent += self.format_stics_data(DT, "bkres", 5, 20)
        fileContent += self.format_stics_data(DT, "awb", 5, 20)
        fileContent += self.format_stics_data(DT, "bwb", 5, 20)
        fileContent += self.format_stics_data(DT, "cwb", 5, 20)
        fileContent += self.format_stics_data(DT, "ahres", 5, 20)
        fileContent += self.format_stics_data(DT, "bhres", 5, 20)
        fileContent += self.format_stics_data(DT, "kbio", 5, 20)
        fileContent += self.format_stics_data(DT, "yres", 5, 20)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 20)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 20)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 20)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 20)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 20)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 20)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 20)

        fileContent += self.format_stics_data(DT, "CroCo", 5, 21)
        fileContent += self.format_stics_data(DT, "akres", 5, 21)
        fileContent += self.format_stics_data(DT, "bkres", 5, 21)
        fileContent += self.format_stics_data(DT, "awb", 5, 21)
        fileContent += self.format_stics_data(DT, "bwb", 5, 21)
        fileContent += self.format_stics_data(DT, "cwb", 5, 21)
        fileContent += self.format_stics_data(DT, "ahres", 5, 21)
        fileContent += self.format_stics_data(DT, "bhres", 5, 21)
        fileContent += self.format_stics_data(DT, "kbio", 5, 21)
        fileContent += self.format_stics_data(DT, "yres", 5, 21)
        fileContent += self.format_stics_data(DT, "CNresmin", 5, 21)
        fileContent += self.format_stics_data(DT, "CNresmax", 5, 21)
        fileContent += self.format_stics_data(DT, "qmulchruis0", 5, 21)
        fileContent += self.format_stics_data(DT, "mouillabilmulch", 5, 21)
        fileContent += self.format_stics_data(DT, "kcouvmlch", 5, 21)
        fileContent += self.format_stics_data(DT, "albedomulchresidus", 5, 21)
        fileContent += self.format_stics_data(DT, "Qmulchdec", 5, 21)

        fileContent += "\n"
        ModelDictionary_Connection.close()
        return fileContent


    def format_stics_data(self, row, champ, precision=5, field_it=0):
        res = ""
        type_data = ""
        data = None
        file_content = ""
        field_name = champ

        # For repeated fields, build field name
        if field_it != 0:
            champ = champ + str(field_it) 

        # Fetch data
        rw = row[row['Champ'] == champ]
        data = rw["dv"].values[0]
        res = ""

        # If type is string or int
        if isinstance(data, str) or isinstance(data, int):
            res = str(data)

        # If type is real
        if isinstance(data, float):
            tmp = float(data)
            if 0 < precision < 7:
                res = "{:.{}f}".format(tmp, precision)
            else:
                res = "{:0.3e}".format(tmp)
        # If cell is null
        if data is None:
            res = ""
        # Print data in file
        file_content += field_name + "\n"
        file_content += res + "\n"
        return file_content
        
    def process_chunk(self, chunk):
        # Apply series of functions to each row in the chunk
        weathertable = set()
        soiltable = set()
        tempopar = set()
        tectable = set()
        initable = set()
        
        for i, row in enumerate(chunk):
            ModelDictionary_Connection = sqlite3.connect(self.ModelDictionary)
            MasterInput_Connection = sqlite3.connect(self.MasterInput)
            print(f"Iteration {i}")
            # Création du chemin du fichier
            simPath = os.path.join(self.DirectoryPath, str(row["idsim"]), str(row["idPoint"]), str(row["StartYear"]))
            usmdir = os.path.join(self.DirectoryPath, str(row["idsim"]))
            try:
                # Tempoparv6
                tempoparv6Converter = sticstempoparv6converter.SticsTempoparv6Converter()
                tempoparv6Converter.export(self.tempoparfv6fix, usmdir)
                # Tempopar
                tempoparid =  row["idOption"]
                if tempoparid not in tempopar:
                    tempoparConverter = sticstempoparconverter.SticsTempoparConverter()
                    tempoparConverter.export(simPath, MasterInput_Connection, self.tempoparfix, usmdir)
                    newchunk = [elt for elt in chunk[i+1:] if elt["idOption"] ==  tempoparid]
                    for r in newchunk:
                        udir = os.path.join(self.DirectoryPath, str(r["idsim"]))
                        if not os.path.exists(udir):
                            Path(udir).mkdir(parents=True, exist_ok=True)
                        shutil.copy(os.path.join(usmdir, "ficini.txt"), udir)
                    tempopar.add(tempoparid)
                else: 
                    pass

                # Station
                # Param.Sol and Station # station and soil have same idsoil = idPoint
                soilid =  row["idsoil"]
                if soilid not in soiltable:
                    paramsolconverter = sticsparamsolconverter.SticsParamSolConverter()
                    paramsolconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)           
                    stationconverter = sticsstationconverter.SticsStationConverter()
                    stationconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, self.rap, self.var, self.prof, usmdir)
                    newchunk = [elt for elt in chunk[i+1:] if elt["idsoil"] ==  soilid]
                    for r in newchunk:
                        udir = os.path.join(self.DirectoryPath, str(r["idsim"]))
                        if not os.path.exists(udir):
                            Path(udir).mkdir(parents=True, exist_ok=True)
                        shutil.copy(os.path.join(usmdir, "param.sol"), udir)
                        shutil.copy(os.path.join(usmdir, "station.txt"), udir)
                    soiltable.add(soilid)
                else: 
                    pass
                # New Travail
                newtravailconverter = sticsnewtravailconverter.SticsNewTravailConverter()
                newtravailconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
                # FicIni 
                iniid =  ".".join([str(row["idsoil"]), str(row["idIni"])])
                if iniid not in initable:
                    ficiniconverter = sticsficiniconverter.SticsFicIniConverter()
                    ficiniconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
                    newchunk = [elt for elt in chunk[i+1:] if ".".join([elt["idsoil"], str(elt["idIni"])]) ==  iniid]
                    for r in newchunk:
                        udir = os.path.join(self.DirectoryPath, str(r["idsim"]))
                        if not os.path.exists(udir):
                            Path(udir).mkdir(parents=True, exist_ok=True)
                        shutil.copy(os.path.join(usmdir, "ficini.txt"), udir)
                    initable.add(iniid)
                else: 
                    pass

                # climat
                climid =  ".".join([str(row["idPoint"]), str(row["StartYear"])])
                if climid not in weathertable:
                    climatconverter = sticsclimatconverter.SticsClimatConverter()
                    climatconverter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
                    newchunk = [elt for elt in chunk[i+1:] if ".".join([elt["idPoint"], str(elt["StartYear"])]) ==  climid]
                    for r in newchunk:
                        udir = os.path.join(self.DirectoryPath, str(r["idsim"]))
                        if not os.path.exists(udir):
                            Path(udir).mkdir(parents=True, exist_ok=True)
                        shutil.copy(os.path.join(usmdir, "climat.txt"), udir)
                    weathertable.add(climid)
                else: 
                    pass
             
                # fictec1
                tecid =  ".".join([str(row["idMangt"]), str(row["idsoil"])])
                if tecid not in tectable:
                    fictec1converter = sticsfictec1converter.SticsFictec1Converter()
                    fictec1converter.export(simPath, ModelDictionary_Connection, MasterInput_Connection, usmdir)
                    newchunk = [elt for elt in chunk[i+1:] if ".".join([elt["idMangt"], elt["idsoil"]]) == tecid]
                    for r in newchunk:
                        udir = os.path.join(self.DirectoryPath, str(r["idsim"]))
                        if not os.path.exists(udir):
                            Path(udir).mkdir(parents=True, exist_ok=True)
                        shutil.copy(os.path.join(usmdir, "fictec1.txt"), udir)
                    tectable.add(tecid)
                else: 
                    pass

                # ficplt1
                ficplt1converter = sticsficplt1converter.SticsFicplt1Converter()
                ficplt1converter.export(simPath, MasterInput_Connection, self.pltfolder, usmdir)

                # run stics
                bs = os.path.join(Path(__file__).parent, "sticsrun.sh")
                subprocess.run(["bash", bs, usmdir, self.DirectoryPath])
                # 
            except Exception as ex:
                print("Error during Export STICS  :", ex)
                traceback.print_exc()
                sys.exit(1)
                  
            MasterInput_Connection.close()
            ModelDictionary_Connection.close()
                    
    def export(self):
        MasterInput_Connection = sqlite3.connect(self.MasterInput)
        ModelDictionary_Connection = sqlite3.connect(self.ModelDictionary)
        try:
            print(f"dbMasterInput: {self.MasterInput}")
            print(f"dbModelsDictionary: {self.ModelDictionary}")
            # Set PRAGMA synchronous to OFF
            MasterInput_Connection.execute("PRAGMA synchronous = OFF")
            ModelDictionary_Connection.execute("PRAGMA synchronous = OFF")
            # Set PRAGMA journal_mode to WAL
            MasterInput_Connection.execute("PRAGMA journal_mode = WAL")
            ModelDictionary_Connection.execute("PRAGMA journal_mode = WAL")
            # Run full checkpoint
            MasterInput_Connection.execute("PRAGMA wal_checkpoint(FULL)")
        except Exception as ex:
            print(f"Connection Error: {ex}")
        if not os.path.exists(self.DirectoryPath):
            os.makedirs(self, self.DirectoryPath)

        try:
            cursor = MasterInput_Connection.cursor()
            cursor2 = ModelDictionary_Connection.cursor()
            
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_idsim ON SimUnitList (idsim);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint_year ON RaClimateD (idPoint, year);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_idPoint ON RaClimateD (idPoint);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_idCoord ON Coordinates (idPoint);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_idMangt ON CropManagement (idMangt);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_idsoil ON Soil (IdSoil);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_idsoill ON Soil (Lower(IdSoil));")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_idsoiltl ON SoilTypes (Lower(SoilTextureType));")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_idoption ON SimulationOptions (idOptions);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cultivars ON ListCultivars (idCultivar);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cultopt ON ListCultivars (CodePSpecies);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cultoptspec ON ListCultOption (CodePSpecies);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_orga ON OrganicFOperations (idFertOrga);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_orga_res ON OrganicFOperations (TypeResidues);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_res ON ListResidues (TypeResidues);")
            cursor2.execute("CREATE INDEX IF NOT EXISTS idx_model_table ON Variables (model, [Table]);")
            MasterInput_Connection.commit()
            ModelDictionary_Connection.commit()
            print("Indexes created successfully!")

        except sqlite3.Error as e:
            print(f"Error creating indexes: {e}")
        MasterInput_Connection.close()
        ModelDictionary_Connection.close()
    
        # convert this code from vb to python:
        def fetch_data_from_sqlite():
            conn = sqlite3.connect(self.MasterInput)
            df = pd.read_sql_query(f"SELECT * FROM SimUnitList", conn)
            rows = df.to_dict(orient='records')
            conn.close()
            return rows
        
        # Split data into chunks
        #def chunk_data(data, chunk_size):
            #return [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
        
        def chunk_data(data, chunk_size):    # values, num_sublists 
            sublist_size = max(len(data) // chunk_size, 3)
            return [data[i:i + sublist_size] for i in range(0, len(data), sublist_size)]

        data = fetch_data_from_sqlite()
        # Split data into chunks
        chunks = chunk_data(data, chunk_size=self.nthreads)
        # Create a Pool of worker processes
        
        try:
            start = time()
            with Pool(processes=self.nthreads) as pool:
                # Apply the processing function to each chunk in parallel
                processed_data_chunks = pool.map(self.process_chunk, chunks)  
            print(f"total time, {time()-start}")
        except Exception as ex:          
            print("Export completed successfully!")

    