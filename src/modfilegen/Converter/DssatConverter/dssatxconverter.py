from modfilegen.converter import Converter
from sqlite3 import Connection
import os
import pandas as pd
import traceback
import shutil

v_fmt_general = {
    "PEOPLE": "{:<}",
    "ADDRESS": "{:<}",
    "SITE": "{:<}",
    "PAREA": "{:7.0f}",
    "PRNO": "{:6.0f}",
    "PLEN": "{:6.0f}",
    "PLDR": "{:6.0f}",
    "PLSP": "{:6.0f}",
    "PLAY": "{:6.0f}",
    "HAREA": "{:6.0f}",
    "HRNO": "{:6.0f}",
    "HLEN": "{:6.0f}",
    "HARM": "  {:<13s}",
    "NOTES": "{:<}"
}
v_fmt_treat = {
    "N": "{:2.0f}",
    "R": "{:2.0f}",
    "O": "{:2.0f}",
    "C": "{:2.0f}",
    "TNAME": "{:<25}",
    "CU": "{:3.0f}",
    "FL": "{:3.0f}",
    "SA": "{:3.0f}",
    "IC": "{:3.0f}",
    "MP": "{:3.0f}",
    "MI": "{:3.0f}",
    "MF": "{:3.0f}",
    "MR": "{:3.0f}",
    "MC": "{:3.0f}",
    "MT": "{:3.0f}",
    "ME": "{:3.0f}",
    "MH": "{:3.0f}",
    "SM": "{:3.0f}"
}

v_fmt_cultivars = {
    "C": "{:2.0f}",
    "CR": "{:>3s}",
    "INGENO": "{:>7s}",
    "CNAME": "{:<}"
}

v_fmt_fields = {"L" : "{:2.0f}", 
                 "ID_FIELD" : " {:<8s}", 
                 "WSTA" : " {:<8s}", 
                 "FLSA" : "{:>6s}",
                 "FLOB" : "{:6.0f}", 
                 "FLDT" : "{:>6s}", 
                 "FLDD" : "{:6.0f}", 
                 "FLDS" : "{:6.0f}",
                 "FLNAME" : "{:<s}",
                 "FLST" : "{:>6s}",
                 "SLTX" : " {:<6s}",
                 "SLDP" : "{:4.0f}",
                 "ID_SOIL" : "  {:<10s} ",
                 "XCRD" : "{:16.6f}", 
                 "YCRD" : "{:16.6f}", 
                 "ELEV" : "{:10.1f}",
                 "AREA" : "{:18.0f}", 
                 "SLEN" : "{:6.0f}", 
                 "FLWR" : "{:6.1f}", 
                 "SLAS" : "{:6.0f}",
                 "FLHST" : "{:>6s}", 
                 "FHDUR" : "{:6.0f}"
               }

v_fmt_soil = {"A" : "{:2.0f}", "SADAT" : "{:>6}", "SMHB" : "{:>6}", "SMPX" : "{:>6}", "SMKE" : "{:>6}",
               "SANAME" : "  {:<}", "SABL" : "{:6.0f}", "SADM" : "{:6.1f}", "SAOC" : "{:6.2f}",
               "SANI" : "{:6.2f}", "SAPHW" : "{:6.1f}", "SAPHB" : "{:6.0f}", "SAPX" : "{:6.1f}",
               "SAKE" : "{:6.1f}", "SASC" : "{:6.3f}"}

v_fmt_init = {"C" : "{:2.0f}", "PCR" : "{:>6s}", "ICDAT" : "{:>6s}", "ICRT" : "{:6.0f}", "ICND" : "{:6.0f}",
               "ICRN" : "{:6.0f}", "ICRE" : "{:6.0f}", "ICWD" : "{:6.0f}", "ICRES" : "{:6.0f}",
               "ICREN" : "{:6.2f}", "ICREP" : "{:6.2f}", "ICRIP" : "{:6.0f}", "ICRID" : "{:6.0f}",
               "ICNAME" : " {:<s}", "ICBL" : "{:6.0f}", "SH2O" : "{:6.3f}", "SNH4" : "{:6.2f}",
               "SNO3" : "{:6.2f}"}

v_fmt_plant = {'P' : "{:2.0f}", 'PDATE' : "{:>6s}", 'EDATE' : "{:>6s}", 'PPOP' : "{:6.1f}",
               'PPOE' : "{:6.1f}", 'PLME' : "{:>6s}", 'PLDS' : "{:>6s}", 'PLRS' : "{:6.1f}", 'PLRD' : "{:6.0f}",
               'PLDP' : "{:6.1f}", 'PLWT' : "{:6.0f}", 'PAGE' : "{:6.0f}", 'PENV' : "{:6.0f}", 'PLPH' : "{:6.0f}",
               'SPRL' : "{:6.1f}", 'PLNAME' : "                        {:<s}"}

v_fmt_irrigation = {"I" : "{:2.0f}", "EFIR" : "{:6.2f}", "IDEP" : "{:6.0f}", "ITHR" : "{:6.0f}",
               "IEPT" : "{:6.0f}", "IOFF" : "{:>6s}", "IAME" : "{:>6s}", "IAMT" : "{:6.0f}", "IRNAME" : " {:<s}", "IDATE" : "{:>6s}",
               "IROP" : "{:>6s}", "IRVAL" : "{:6.1f}"}




v_fmt_chemical = {"C" : "{:2.0f}", "CDATE" : "{:>6s}", "CHCOD" : "{:>6s}", "CHAMT" : "{:6.0f}", "CHME" : "{:>6s}",
               "CHDEP" : "{:6.0f}", "CHT" : " {:>5s}", "CHNAME" : "  {:<s}"}


v_fmt_environment =  {"E" : "{:2.0f}", "ODATE" : "{:>6s}", "EDAY" : " {:<4s}","EDAY1" : "{:<2s}", "ERAD" : "{:<4s}","ERAD1" : "{:<2s}", "EMAX" : "{:<4s}",
               "EMAX1" : "{:<2s}", "EMIN" : "{:<4s}", "EMIN1" : "{:<2s}", "ERAIN" : "{:<4s}", "ERAIN1" : "{:<2s}", "ECO2" : "{:<4s}", "ECO21" : "{:<2s}", "EDEW" : "{:<4s}",
               "EDEW1" : "{:<2s}", "EWIND" : "{:<4s}", "EWIND1" : "{:<2s}", "ENVNAME" : "{:<s}"}

v_fmt_fertilizers = {"F" : "{:2.0f}", "FDATE" : "{:>6s}", "FMCD" : "{:>6s}", "FACD" : "{:>6s}", "FDEP" : "{:6.0f}",
               "FAMN" : "{:6.0f}", "FAMP" : "{:6.0f}", "FAMK" : "{:6.0f}", "FAMC" : "{:6.0f}",
               "FAMO" : "{:6.0f}", "FOCD" : "{:>6s}", "FERNAME" : " {:<s}"}

v_fmt_harvest = {"H" : "{:2.0f}", "HDATE" : "{:>6s}", "HSTG" : "{:>6s}", "HCOM" : "{:>6s}", "HSIZE" : "{:>6s}",
               "HPC" : "{:6.0f}", "HBPC" : "{:6.0f}", "HNAME" : " {:<s}"}


v_fmt_residues = {"R" : "{:2.0f}", "RDATE" : "{:>6s}", "RCOD" : "{:>6s}", "RAMT" : "{:6.0f}", "RESN" : "{:6.2f}",
               "RESP" : "{:6.0f}", "RESK" : "{:6.0f}", "RINP" : "{:6.0f}", "RDEP" : "{:6.0f}", "RMET" : "{:>6s}",
               "RENAME" : " {:<s}"}

v_fmt_simulation = {"N" : "{:2.0f}", "GENERAL" : " {:<11s}", "NYERS" : "{:6.0f}", "NREPS" : "{:6.0f}",
               "START" : "{:>6s}", "SDATE" : "{:>6s}", "RSEED" : "{:6.0f}", "SNAME" : " {:<25s}",
               "SMODEL" : " {:<8s}", "MODEL" : " {:<8s}",
               "OPTIONS" : " {:<11s}", "WATER" : "{:>6s}", "NITRO" : "{:>6s}",
               "SYMBI" : "{:>6s}", "PHOSP" : "{:>6s}", "POTAS" : "{:>6s}", "DISES" : "{:>6s}", "CHEM" : "{:>6s}",
               "TILL" : "{:>6s}", "CO2" : "{:>6s}", "METHODS" : " {:<11s}", "WTHER" : "{:>6s}",
               "INCON" : "{:>6s}", "LIGHT" : "{:>6s}", "EVAPO" : "{:>6s}", "INFIL" : "{:>6s}", "PHOTO" : "{:>6s}",
               "HYDRO" : "{:>6s}", "NSWIT" : "{:6.0f}", "MESOM" : "{:>6s}", "MESEV" : "{:>6s}",
               "MESOL" : "{:6.0f}", "MANAGEMENT" : " {:<11s}", "PLANT" : "{:>6s}", "IRRIG" : "{:>6s}",
               "FERTI" : "{:>6s}", "RESID" : "{:>6s}", "HARVS" : "{:>6s}", "OUTPUTS" : " {:<11s}",
               "FNAME" : "{:>6s}", "OVVEW" : "{:>6s}", "SUMRY" : "{:>6s}", "FROPT" : "{:6.0f}",
               "GROUT" : "{:>6s}", "CAOUT" : "{:>6s}", "WAOUT" : "{:>6s}", "NIOUT" : "{:>6s}", "MIOUT" : "{:>6s}",
               "DIOUT" : "{:>6s}", "VBOSE" : "{:>6s}", "CHOUT" : "{:>6s}", "OPOUT" : "{:>6s}",
               "FMOPT" : "{:>6s}", "LONG" : "{:>6s}", "PLANTING" : " {:<11s}",
               "PFRST" : "{:>6s}", "PLAST" : "{:>6s}", "PH2OL" : "{:6.0f}", "PH2OU" : "{:6.0f}",
               "PH2OD" : "{:6.0f}", "PSTMX" : "{:6.0f}", "PSTMN" : "{:6.0f}", "IRRIGATION" : " {:<11s}",
               "IMDEP" : "{:6.0f}", "ITHRL" : "{:6.0f}", "ITHRU" : "{:6.0f}", "IROFF" : "{:>6s}",
               "IMETH" : "{:>6s}", "IRAMT" : "{:6.0f}", "IREFF" : "{:6.2f}", "NITROGEN" : " {:<11s}",
               "NMDEP" : "{:6.0f}", "NMTHR" : "{:6.2f}", "NAMNT" : "{:6.0f}", "NCODE" : "{:>6s}",
               "NAOFF" : "{:>6s}", "RESIDUES" : " {:<11s}", "RIPCN" : "{:6.0f}", "RTIME" : "{:6.0f}",
               "RIDEP" : "{:6.0f}", "HARVEST" : " {:<11s}", "HFRST" : "{:>6s}", "HLAST" : "{:>6s}",
               "HPCNP" : "{:6.0f}", "HPCNR" : "{:6.0f}",
               "SIMDATES" : " {:<11s}",
               "ENDAT" : "{:>6s}", "SDUR" : "{:8.0f}", "FODAT" : "{:8s}", "FSTRYR" : "{:8.0f}",
               "FENDYR" : "{:8.0f}", "FWFILE" : " {:<15s}", "FONAME" : "  {:<s}"}

v_fmt_tillage = {"T" : "{:2.0f}", "TDATE" : "{:>6s}", "TIMPL" : "{:>6s}", "TDEP" : "{:6.0f}", "TNAME" : " {:<s}"}





def writeBlockTreatment(dssat_tableName, idSim, modelDictionary_Connection, master_input_connection):
    fileContent = ""
    
    fetchAllQuery  = """Select SimUnitList.idsim, SoilTillPolicy.NumTillOperations, OrganicFertilizationPolicy.NumOrganicFerti, CropManagement.IrrigationPolicyCode, CropManagement.InoFertiPolicyCode 
        From OrganicFertilizationPolicy INNER Join (SoilTillPolicy INNER Join (CropManagement INNER Join SimUnitList 
        On CropManagement.idMangt = SimUnitList.idMangt) ON SoilTillPolicy.SoilTillPolicyCode = CropManagement.SoilTillPolicyCode) 
        ON OrganicFertilizationPolicy.OFertiPolicyCode = CropManagement.OFertiPolicyCode Where IdSim='%s'""" % (idSim)
    
    dataTable = pd.read_sql_query(fetchAllQuery, master_input_connection)
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    siteColumnsHeader = ["@N", "R", "O", "C", "TNAME....................", "CU", "FL", "SA", "IC", "MP", "MI", "MF", "MR", "MC", "MT", "ME", "MH", "SM"]
    fileContent += "\n"
    fileContent += "*TREATMENTS                        -------------FACTOR LEVELS------------\n"
    fileContent += " ".join(siteColumnsHeader) + "\n"
    storeNumMaxSimu = 0
    rw = DT[DT["Champ"] == "TRTNO"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_treat["N"].format(float(Dv))
    rw = DT[DT["Champ"] == "ROTNO"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_treat["R"].format(float(Dv))
    rw = DT[DT["Champ"] == "ROTOPT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_treat["O"].format(float(Dv))
    rw = DT[DT["Champ"] == "CRPNO"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_treat["C"].format(float(Dv)) + " "
    rw = DT[DT["Champ"] == "TITLET"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_treat["TNAME"].format(Dv)
    rw = DT[DT["Champ"] == "LNCU"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_treat["CU"].format(float(Dv))
    rw = DT[DT["Champ"] == "LNFLD"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_treat["FL"].format(float(Dv))
    rw = DT[DT["Champ"] == "LNSA"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_treat["SA"].format(float(Dv))
    rw = DT[DT["Champ"] == "LNIC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_treat["IC"].format(float(Dv))
    rw = DT[DT["Champ"] == "LNPLT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_treat["MP"].format(float(Dv))
    #rw = DT[DT["Champ"] == "LNIR"]  ######################### It depends on the context
    #Dv = rw["dv"].values[0]
    #fileContent += v_fmt_treat["MI"].format(float(Dv))
    if int(dataTable["IrrigationPolicyCode"].values[0]) == 0:
        fileContent += v_fmt_treat["MI"].format(0)
    else:
        fileContent += v_fmt_treat["MI"].format(1)
    #rw = DT[DT["Champ"] == "LNFER"] ######################### It depends on the context
    #Dv = rw["dv"].values[0]
    #fileContent += v_fmt_treat["MF"].format(float(Dv))
    if int(dataTable["InoFertiPolicyCode"].values[0]) == 0:
        fileContent += v_fmt_treat["MF"].format(0)
    else:
        fileContent += v_fmt_treat["MF"].format(1)
    if int(dataTable["NumOrganicFerti"].values[0]) == 0:
        fileContent += v_fmt_treat["MR"].format(0)
    else:
        fileContent += v_fmt_treat["MR"].format(1)
    #rw = DT[DT["Champ"] == "LNCHE"]                #### No chemical application
    #Dv = rw["dv"].values[0]
    #fileContent += v_fmt_treat["MC"].format(float(Dv))
    fileContent += v_fmt_treat["MC"].format(0)
    if int(dataTable["NumTillOperations"].values[0]) == 0:
        fileContent += v_fmt_treat["MT"].format(0)
    else:
        fileContent += v_fmt_treat["MT"].format(1)
    #rw = DT[DT["Champ"] == "LNENV"]
    #Dv = rw["dv"].values[0]
    #fileContent += v_fmt_treat["ME"].format(float(Dv))  #### No environmental modification
    fileContent += v_fmt_treat["ME"].format(0)
    rw = DT[DT["Champ"] == "LNHAR"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_treat["MH"].format(float(Dv))
    rw = DT[DT["Champ"] == "LNSIM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_treat["SM"].format(float(Dv))
    fileContent += "\n"
    return fileContent


def writeBlockCultivar(dssat_tableName, idMangt, modelDictionary_Connection, master_input_connection):
    fileContent = ""
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    fetchAllQuery  = "SELECT CropManagement.idMangt, ListCultivars.CodCultivar, ListCultivars.IdcultivarDssat, ListCultOption.CG From ListCultOption INNER JOIN (ListCultivars INNER Join CropManagement On ListCultivars.IdCultivar = CropManagement.Idcultivar) On ListCultOption.CodePSpecies = ListCultivars.CodePSpecies Where Idmangt ='%s';"%(idMangt)
    dataTable = pd.read_sql_query(fetchAllQuery, master_input_connection)
    siteColumnsHeader = ["@C", "CR", "INGENO", "CNAME"]
    fileContent += "\n"
    fileContent += "*CULTIVARS\n"
    fileContent += " ".join(siteColumnsHeader) + "\n"
    rw = DT[DT["Champ"] == "LNCU"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_cultivars["C"].format(float(Dv))
    fileContent += v_fmt_cultivars["CR"].format(dataTable["CG"].values[0])
    fileContent += v_fmt_cultivars["INGENO"].format(dataTable["IdcultivarDssat"].values[0])
    fileContent += " "
    fileContent += v_fmt_cultivars["CNAME"].format(dataTable["CodCultivar"].values[0]) + "\n"
    return fileContent
    


def writeBlockField(dssat_tableName, dssat_tableId, idMangt, modelDictionary_Connection):
    fileContent = ""
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource], [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    siteColumnsHeader = ["@L", "ID_FIELD", "WSTA....", " FLSA", " FLOB", " FLDT", " FLDD", " FLDS", " FLST", "SLTX ", "SLDP ", "ID_SOIL   ", "FLNAME"]
    siteColumnsHeader2 = ["@L", "...........XCRD", "...........YCRD", ".....ELEV", ".............AREA", ".SLEN", ".FLWR", ".SLAS", "FLHST", "FHDUR"]
    fileContent += "\n"
    fileContent += "*FIELDS\n"
    fileContent += " ".join(siteColumnsHeader) + "\n"
    rw = DT[DT["Champ"] == "FL"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["L"].format(float(Dv))
    rw = DT[DT["Champ"] == "ID_FIELD"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["ID_FIELD"].format(Dv)
    fileContent += v_fmt_fields["WSTA"].format(idMangt[0:4])
    rw = DT[DT["Champ"] == "FLSA"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["FLSA"].format(Dv)
    rw = DT[DT["Champ"] == "FLOB"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["FLOB"].format(float(Dv))
    rw = DT[DT["Champ"] == "FLDT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["FLDT"].format(Dv)
    rw = DT[DT["Champ"] == "FLDD"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["FLDD"].format(float(Dv))
    rw = DT[DT["Champ"] == "FLDS"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["FLDS"].format(float(Dv))
    rw = DT[DT["Champ"] == "FLST"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["FLST"].format(Dv)
    rw = DT[DT["Champ"] == "SLTX"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["SLTX"].format(Dv)
    rw = DT[DT["Champ"] == "SLDP"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["SLDP"].format(float(Dv))
    fileContent += v_fmt_fields["ID_SOIL"].format("XX" + idMangt[0:4] + "0101")
    rw = DT[DT["Champ"] == "FLNAME"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["FLNAME"].format(Dv) + "\n"
    fileContent += " ".join(siteColumnsHeader2) + "\n"
    rw = DT[DT["Champ"] == "FL"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["L"].format(float(Dv))
    rw = DT[DT["Champ"] == "XCRD"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["XCRD"].format(float(Dv))
    rw = DT[DT["Champ"] == "YCRD"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["YCRD"].format(float(Dv))
    rw = DT[DT["Champ"] == "ELEV"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["ELEV"].format(float(Dv))
    rw = DT[DT["Champ"] == "AREA"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["AREA"].format(float(Dv))
    rw = DT[DT["Champ"] == "SLEN"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["SLEN"].format(float(Dv))
    rw = DT[DT["Champ"] == "FLWR"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["FLWR"].format(float(Dv))
    rw = DT[DT["Champ"] == "SLAS"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["SLAS"].format(float(Dv))
    rw = DT[DT["Champ"] == "FLHST"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["FLHST"].format(Dv)
    rw = DT[DT["Champ"] == "FHDUR"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_fields["FHDUR"].format(float(Dv)) + "\n"
    return fileContent
    
    
def writeBlockSoilAnalysis(dssat_tableName, dssat_tableId, modelDictionary_Connection):
    fileContent = ""
    dssat_queryRead  = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    siteColumnsHeader = ["@A", "SADAT", " SMHB", " SMPX", " SMKE", " SANAME"]
    fileContent += "\n"
    fileContent += "*SOIL ANALYSIS\n"
    fileContent += " ".join(siteColumnsHeader) + "\n"
    rw = DT[DT["Champ"] == "LNSA"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_soil["A"].format(float(Dv))
    rw = DT[DT["Champ"] == "SADAT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_soil["SADAT"].format(Dv.strip())
    rw = DT[DT["Champ"] == "SMHB"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_soil["SMHB"].format(Dv.strip())
    rw = DT[DT["Champ"] == "SMPX"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_soil["SMPX"].format(Dv.strip())
    rw = DT[DT["Champ"] == "SMKE"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_soil["SMKE"].format(Dv.strip())
    rw = DT[DT["Champ"] == "SANAME"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_soil["SANAME"].format(Dv.strip()) + "\n"
    dssat_tableName = "dssat_x_soil_analysis_data"
    fileContent += writeBlockSoilAnalysisData(dssat_tableName, modelDictionary_Connection)
    return fileContent
    
    

def writeBlockSoilAnalysisData(dssat_tableName, Connection):
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, Connection)
    siteColumnsHeader = "@A  SABL  SADM  SAOC  SANI SAPHW SAPHB  SAPX  SAKE  SASC"
    fileContent = ""
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNSA"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["A"].format(float(Dv))
    rw = DT[DT["Champ"] == "SABL"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SABL"].format(float(Dv))
    rw = DT[DT["Champ"] == "SADM"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SADM"].format(float(Dv))
    rw = DT[DT["Champ"] == "SAOC"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SAOC"].format(float(Dv))
    rw = DT[DT["Champ"] == "SANI"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SANI"].format(float(Dv))
    rw = DT[DT["Champ"] == "SAPHW"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SAPHW"].format(float(Dv))
    rw = DT[DT["Champ"] == "SAPHB"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SAPHB"].format(float(Dv))
    rw = DT[DT["Champ"] == "SAPX"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SAPX"].format(float(Dv))
    rw = DT[DT["Champ"] == "SAKE"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_soil["SAKE"].format(float(Dv))
    rw = DT[DT["Champ"] == "SASC"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f") + '\n'
    else: fileContent += v_fmt_soil["SASC"].format(float(Dv)) + "\n"
    return fileContent


def writeBlockInitialCondition(dssat_tableName, idSim, modelDictionary_Connection, master_input_connection):
    fileContent = ""
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    fetchAllQuery = "Select SimUnitList.idsim, SimUnitList.StartYear, SimUnitList.StartDay,CropManagement.SowingDate, ListCultOption.PRCROP FROM (ListCultOption INNER JOIN (ListCultivars INNER JOIN CropManagement ON ListCultivars.IdCultivar = CropManagement.Idcultivar) ON ListCultOption.CodePSpecies = ListCultivars.CodePSpecies) INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt Where IdSim ='%s';"%(idSim)
    dataTable = pd.read_sql_query(fetchAllQuery, master_input_connection)
    siteColumnsHeader = "@C   PCR ICDAT  ICRT  ICND  ICRN  ICRE  ICWD ICRES ICREN ICREP ICRIP ICRID ICNAME"
    fileContent += "\n"
    fileContent += "*INITIAL CONDITIONS\n"
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNIC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_init["C"].format(float(Dv))
    fileContent += v_fmt_init["PCR"].format(dataTable["PRCROP"].values[0])
    s = str(dataTable["StartYear"].values[0])[2:4] + format(dataTable["StartDay"].values[0], "03.0f")
    fileContent += v_fmt_init["ICDAT"].format(s)
    rw = DT[DT["Champ"] == "WRESR"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_init["ICRT"].format(float(Dv))
    rw = DT[DT["Champ"] == "WRESND"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_init["ICND"].format(float(Dv))
    rw = DT[DT["Champ"] == "EFINOC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_init["ICRN"].format(float(Dv))
    rw = DT[DT["Champ"] == "EFNFIX"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_init["ICRE"].format(float(Dv))
    rw = DT[DT["Champ"] == "ICWD"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_init["ICWD"].format(float(Dv))
    rw = DT[DT["Champ"] == "ICRES"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_init["ICRES"].format(float(Dv))
    rw = DT[DT["Champ"] == "ICREN"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_init["ICREN"].format(float(Dv))
    rw = DT[DT["Champ"] == "ICREP"]
    Dv = rw["dv"].values[0]
    if float(Dv) == -99: fileContent += format(-99, "6.0f")
    else: fileContent += v_fmt_init["ICREP"].format(float(Dv))
    rw = DT[DT["Champ"] == "ICRIP"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_init["ICRIP"].format(float(Dv))
    rw = DT[DT["Champ"] == "ICRID"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_init["ICRID"].format(float(Dv))
    rw = DT[DT["Champ"] == "ICNAME"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_init["ICNAME"].format(Dv) + "\n"
    dssat_tableName = "dssat_x_initial_condition_data"
    fileContent += writeBlockInitialConditionData(dssat_tableName, idSim, modelDictionary_Connection, master_input_connection)
    return fileContent

 

def writeBlockInitialConditionData(dssat_tableName, idsim, Connection, MI_Connection):
    dssat_queryRead  = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, Connection)
    siteColumnsHeader = "@C  ICBL  SH2O  SNH4  SNO3"
    fetchAllQuery  = """SELECT DISTINCT Soil.Wwp AS 'Soil.Wwp', Soil.Wfc AS 'Soil.Wfc', soil.bd as 'soil.bd', Soil.*,
                SoilLayers.Wwp AS 'SoilLayers.Wwp', SoilLayers.Wfc AS 'SoilLayers.Wfc', SoilLayers.*, 
                InitialConditions.* FROM InitialConditions INNER JOIN 
                ((Soil INNER JOIN SimUnitList ON Lower(Soil.IdSoil) = Lower(SimUnitList.idsoil)) LEFT JOIN SoilLayers ON Lower(Soil.IdSoil) = Lower(SoilLayers.idsoil))
                ON InitialConditions.idIni = SimUnitList.idIni Where IdSim ='%s' Order by NumLayer;"""%(idsim)
    dataTable = pd.read_sql_query(fetchAllQuery, MI_Connection)
    fileContent = ""
    fileContent += siteColumnsHeader + "\n"
    if dataTable["SoilOption"].values[0].lower() == "simple":
        for i in range(2):
            rw = DT[DT["Champ"] == "LNIC"]
            Dv = rw["dv"].values[0]
            fileContent += v_fmt_init["C"].format(float(Dv))
            if i == 0:
                fileContent += v_fmt_init["ICBL"].format(30.0)
            else:
                fileContent += v_fmt_init["ICBL"].format(dataTable["SoilTotalDepth"].values[0])
            fileContent += v_fmt_init["SH2O"].format((dataTable["Soil.Wwp"].values[0] / 100) + dataTable["WStockinit"].values[0] * (dataTable["Soil.Wfc"].values[0] - dataTable["Soil.Wwp"].values[0]) / 10000)
            rw = DT[DT["Champ"] == "INH4"]
            Dv = rw["dv"].values[0]
            fileContent += v_fmt_init["SNH4"].format(float(Dv))
            fileContent += v_fmt_init["SNO3"].format(10 * dataTable["Ninit"].values[0] / (dataTable["soil.bd"].values[0] * dataTable["SoilTotalDepth"].values[0]))
            fileContent += "\n"
    else:
        for i in range(dataTable.shape[0]):
            rw = DT[DT["Champ"] == "LNIC"]
            Dv = rw["dv"].values[0]
            fileContent += v_fmt_init["C"].format(float(Dv))
            fileContent += v_fmt_init["ICBL"].format(dataTable["Ldown"].values[i])
            fileContent += v_fmt_init["SH2O"].format((dataTable["SoilLayers.Wwp"].values[i] / 100 + dataTable["WStockinit"].values[i] * (dataTable["SoilLayers.Wfc"].values[i] - dataTable["SoilLayers.Wwp"].values[i]) / 10000))
            rw = DT[DT["Champ"] == "INH4"]
            Dv = rw["dv"].values[0]
            fileContent += v_fmt_init["SNH4"].format(float(Dv))
            fileContent += v_fmt_init["SNO3"].format(10 * dataTable["Ninit"].values[i] / (dataTable["soil.bd"].values[i] * dataTable["SoilLayers.Depth"].values[i]), ".2f").rjust(5)
            fileContent += "\n"
    return fileContent
    


def writeBlockPlantingDetail(dssat_tableName, idSim, modelDictionary_Connection, master_input_connection):
    fileContent = ""
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    fetchAllQuery = "SELECT SimUnitList.idsim,  SimUnitList.StartYear, CropManagement.sdens, CropManagement.sowingdate FROM CropManagement INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt Where IdSim ='%s';"%(idSim)
    dataTable = pd.read_sql_query(fetchAllQuery, master_input_connection)
    siteColumnsHeader = "@P PDATE EDATE  PPOP  PPOE  PLME  PLDS  PLRS  PLRD  PLDP  PLWT  PAGE  PENV  PLPH  SPRL                        PLNAME"
    fileContent += "\n"
    fileContent += "*PLANTING DETAILS\n"
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNPLT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_plant["P"].format(float(Dv))
    if dataTable["StartYear"].values[0] % 4 == 0:
        Bissext = 1
    else:
        Bissext = 0
        
    if dataTable["sowingdate"].values[0] > 365 + Bissext:
        fileContent += v_fmt_plant["PDATE"].format(str(dataTable["StartYear"].values[0] + 1)[2:4] + str(dataTable["sowingdate"].values[0] - 365 - Bissext).rjust(3, "0"))
    else:
        fileContent += v_fmt_plant["PDATE"].format(str(dataTable["StartYear"].values[0])[2:4] + str(dataTable["sowingdate"].values[0]).rjust(3, "0"))
    rw = DT[DT["Champ"] == "IEMRG"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_plant["EDATE"].format(Dv)
    fileContent += v_fmt_plant["PPOP"].format(dataTable["sdens"].values[0])
    fileContent += v_fmt_plant["PPOE"].format(dataTable["sdens"].values[0])
    rw = DT[DT["Champ"] == "PLME"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_plant["PLME"].format(Dv)
    rw = DT[DT["Champ"] == "PLDS"]  
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_plant["PLDS"].format(Dv)
    rw = DT[DT["Champ"] == "ROWSPC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_plant["PLRS"].format(float(Dv))
    rw = DT[DT["Champ"] == "AZIR"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_plant["PLRD"].format(float(Dv))
    rw = DT[DT["Champ"] == "SDEPHT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_plant["PLDP"].format(float(Dv))
    rw = DT[DT["Champ"] == "SDWTPL"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_plant["PLWT"].format(float(Dv))
    rw = DT[DT["Champ"] == "SDAGE"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_plant["PAGE"].format(float(Dv))
    rw = DT[DT["Champ"] == "ATEMP"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_plant["PENV"].format(float(Dv))
    rw = DT[DT["Champ"] == "PLPH"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_plant["PLPH"].format(float(Dv))
    rw = DT[DT["Champ"] == "SPRLAP"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_plant["SPRL"].format(float(Dv))
    rw = DT[DT["Champ"] == "PLNAME"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_plant["PLNAME"].format(Dv) + "\n"
    return fileContent


def writeBlockIrrigationWater(dssat_tableName, dssat_tableId, modelDictionary_Connection):
    fileContent = ""
    dssat_queryRead  = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    siteColumnsHeader = "@I  EFIR  IDEP  ITHR  IEPT  IOFF  IAME  IAMT IRNAME"
    fileContent += "\n"
    fileContent += "*IRRIGATION AND WATER MANAGEMENT\n"
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNIR"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_irrigation["I"].format(float(Dv))
    rw = DT[DT["Champ"] == "EFFIRX"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_irrigation["EFIR"].format(float(Dv))
    rw = DT[DT["Champ"] == "DSOILX"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_irrigation["IDEP"].format(float(Dv))
    rw = DT[DT["Champ"] == "THETCX"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_irrigation["ITHR"].format(float(Dv))
    rw = DT[DT["Champ"] == "IEPTX"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_irrigation["IEPT"].format(float(Dv))
    rw = DT[DT["Champ"] == "IOFFX"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_irrigation["IOFF"].format(Dv)
    rw = DT[DT["Champ"] == "IAMEX"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_irrigation["IAME"].format(Dv)
    rw = DT[DT["Champ"] == "AIRAMX"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_irrigation["IAMT"].format(float(Dv))
    rw = DT[DT["Champ"] == "IRNAME"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_irrigation["IRNAME"].format(Dv) + "\n"
    dssat_tableName = "dssat_x_irrigation_water_data"
    fileContent += writeBlockIrrigationWaterData(dssat_tableName, modelDictionary_Connection)
    return fileContent

def writeBlockIrrigationWaterData(dssat_tableName, modelDictionary_Connection):
    fileContent = "" 
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    siteColumnsHeader = "@I IDATE  IROP IRVAL" 
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNIR"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_irrigation["I"].format(float(Dv))
    rw = DT[DT["Champ"] == "IDLAPL"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_irrigation["IDATE"].format(Dv)
    rw = DT[DT["Champ"] == "IRRCOD"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_irrigation["IROP"].format(Dv)
    rw = DT[DT["Champ"] == "IIRV"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_irrigation["IRVAL"].format(float(Dv))
    fileContent += "\n"
    return fileContent
     

def writeBlockFertilizer(dssat_tableName, idSim, modelDictionary_Connection, master_input_connection, Dv_ferti):
    fileContent = ""
    dssat_queryRead  = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    fetchAllQuery  = """SELECT SimUnitList.idsim, SimUnitList.StartYear, CropManagement.Sowingdate, InorganicFOperations.IFNumber,
                InorganicFOperations.N, InorganicFOperations.P, InorganicFOperations.Dferti FROM (InorganicFertilizationPolicy INNER JOIN 
                (CropManagement INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt) ON InorganicFertilizationPolicy.InorgFertiPolicyCode 
                = CropManagement.InoFertiPolicyCode) INNER JOIN InorganicFOperations ON InorganicFertilizationPolicy.InorgFertiPolicyCode =
                InorganicFOperations.InorgFertiPolicyCode Where Idsim ='%s' Order by InorganicFOperations.IFNumber;"""%(idSim)
    dataTable = pd.read_sql_query(fetchAllQuery, master_input_connection)
    siteColumnsHeader = "@F FDATE  FMCD  FACD  FDEP  FAMN  FAMP  FAMK  FAMC  FAMO  FOCD FERNAME"
    fileContent += "\n"
    fileContent += "*FERTILIZERS (INORGANIC)\n"
    fileContent += siteColumnsHeader + "\n"
    for i in range(dataTable.shape[0]):
        rw = DT[DT["Champ"] == "LNFER"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_fertilizers["F"].format(float(Dv))
        ifert = int(dataTable["sowingdate"].values[i] + dataTable["Dferti"].values[i])
        if dataTable["Dferti"].values[i] == 0: ifert += 1
        if dataTable["StartYear"].values[i] % 4 == 0:
            Bissext = 1
        else:
            Bissext = 0
        if ifert > 365 + Bissext and Dv_ferti != "D":
            fileContent += v_fmt_fertilizers["FDATE"].format(str(dataTable["StartYear"].values[i] + 1)[2:4] + str(ifert - 365 - Bissext).rjust(3, "0"))
        elif ifert <= 365 + Bissext and Dv_ferti != "D":
            fileContent += v_fmt_fertilizers["FDATE"].format(str(dataTable["StartYear"].values[i])[2:4] + str(ifert).rjust(3, "0"))
        elif Dv_ferti == "D":
            fileContent += v_fmt_fertilizers["FDATE"].format(str(int(dataTable["Dferti"].values[i])))
        
        rw = DT[DT["Champ"] == "IFTYPE"]
        Dv = rw["dv"].values[0] 
        fileContent += v_fmt_fertilizers["FMCD"].format(Dv)
        rw = DT[DT["Champ"] == "FERCOD"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_fertilizers["FACD"].format(Dv)
        rw = DT[DT["Champ"] == "DFERT"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_fertilizers["FDEP"].format(float(Dv))
        fileContent += v_fmt_fertilizers["FAMN"].format(dataTable["N"].values[i])
        fileContent += v_fmt_fertilizers["FAMP"].format(dataTable["P"].values[i])
        rw = DT[DT["Champ"] == "AKFER"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_fertilizers["FAMK"].format(float(Dv))
        rw = DT[DT["Champ"] == "ACFER"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_fertilizers["FAMC"].format(float(Dv))
        rw = DT[DT["Champ"] == "AOFER"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_fertilizers["FAMO"].format(float(Dv))
        rw = DT[DT["Champ"] == "FOCOD"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_fertilizers["FOCD"].format(Dv.strip())

        rw = DT[DT["Champ"] == "FERNAM"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_fertilizers["FERNAME"].format(Dv) + "\n"
    return fileContent


def writeBlockResidues(dssat_tableName, idSim, dssat_tableId, modelDictionary_Connection, master_input_connection):
    fileContent = ""
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    fetchAllQuery = """SELECT SimUnitList.idsim, SimUnitList.StartYear, CropManagement.sowingdate, ListResidues.idresidueDssat, 
                OrganicFOperations.In_OnManure, OrganicFOperations.Qmanure, OrganicFOperations.Dferti, OrganicFOperations.NFerti, 
                OrganicFOperations.PFerti, SoilTillageOperations.STNumber, SoilTillageOperations.DepthResLow, OrganicFOperations.OFNumber 
                FROM ((SoilTillageOperations INNER JOIN CropManagement ON SoilTillageOperations.SoilTillPolicyCode = CropManagement.SoilTillPolicyCode)
                INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt) INNER JOIN (ListResidues INNER JOIN OrganicFOperations ON 
                ListResidues.TypeResidues = OrganicFOperations.TypeResidues) ON CropManagement.OFertiPolicyCode = OrganicFOperations.OFertiPolicyCode
                Where Idsim ='%s' Order by Ofnumber;"""%(idSim)
    dataTable = pd.read_sql_query(fetchAllQuery, master_input_connection)
    siteColumnsHeader = "@R RDATE  RCOD  RAMT  RESN  RESP  RESK  RINP  RDEP  RMET RENAME"
    fileContent += "\n"
    fileContent += "*RESIDUES AND ORGANIC FERTILIZER\n"
    fileContent += siteColumnsHeader + "\n"
    for i in range(dataTable.shape[0]):
        rw = DT[DT["Champ"] == "LNRES"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_residues["R"].format(float(Dv))
        ifert = int(dataTable["sowingdate"].values[i] + dataTable["Dferti"].values[i])
        if dataTable["Dferti"].values[i] == 0: ifert += 1
        if dataTable["StartYear"].values[i] % 4 == 0:
            Bissext = 1
        else:
            Bissext = 0
        if ifert > 365 + Bissext:
            fileContent += v_fmt_residues["RDATE"].format(str(dataTable["StartYear"].values[i] + 1)[2:4] + str(ifert - 365 - Bissext).rjust(3, "0"))
        else:
            fileContent += v_fmt_residues["RDATE"].format(str(dataTable["StartYear"].values[i])[2:4] + str(ifert).rjust(3, "0"))
        if dataTable["idresidueDssat"].values : fileContent += v_fmt_residues["RCOD"].format(dataTable["idresidueDssat"].values[i])
        else: fileContent += format(" ", "6s")
        fileContent += v_fmt_residues["RAMT"].format(dataTable["Qmanure"].values[i])
        fileContent += v_fmt_residues["RESN"].format(round(100 * dataTable["NFerti"].values[i], 2))
        fileContent += v_fmt_residues["RESP"].format(round(100 * dataTable["PFerti"].values[i], 2))
        rw = DT[DT["Champ"] == "RESK"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_residues["RESK"].format(float(Dv))
        rw = DT[DT["Champ"] == "RINP"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_residues["RINP"].format(float(Dv))
        if dataTable["In_OnManure"].values[i] == 0:
            fileContent += v_fmt_residues["RDEP"].format(0.0)
        else: fileContent += v_fmt_residues["RDEP"].format(dataTable["DepthResLow"].values[i])
        rw = DT[DT["Champ"] == "RMET"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_residues["RMET"].format(Dv.strip())
        rw = DT[DT["Champ"] == "RENAME"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_residues["RENAME"].format(Dv.strip()) + "\n"
    return fileContent


def writeBlockChemicalApplication(dssat_tableName, dssat_tableId, modelDictionary_Connection):
    fileContent = ""
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    siteColumnsHeader = "@C CDATE CHCOD CHAMT  CHME CHDEP   CHT..CHNAME"
    fileContent += "\n"
    fileContent += "*CHEMICAL APPLICATIONS\n"
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNCHE"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_chemical["C"].format(float(Dv))
    rw = DT[DT["Champ"] == "CDATE"]                                ###TODO: Check if the date should be extracted from the MasterInput database
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_chemical["CDATE"].format(format(int(Dv), "05"))
    rw = DT[DT["Champ"] == "CHCOD"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_chemical["CHCOD"].format(Dv.strip())
    rw = DT[DT["Champ"] == "CHAMT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_chemical["CHAMT"].format(float(Dv))
    rw = DT[DT["Champ"] == "CHMET"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_chemical["CHME"].format(Dv.strip())
    rw = DT[DT["Champ"] == "CHDEP"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_chemical["CHDEP"].format(float(Dv))
    rw = DT[DT["Champ"] == "CHT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_chemical["CHT"].format(Dv.strip())
    rw = DT[DT["Champ"] == "CHNAME"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_chemical["CHNAME"].format(Dv.strip()) + "\n"
    return fileContent
 

def writeBlockTillageRotation(dssat_tableName, idSim, modelDictionary_Connection, master_input_connection):
    fileContent = ""
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource], [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    fetchAllQuery = """SELECT SimUnitList.idsim, SimUnitList.StartYear, CropManagement.sowingdate, OrganicFOperations.Qmanure, OrganicFOperations.Dferti, OrganicFOperations.NFerti, OrganicFOperations.PFerti, SoilTillageOperations.STNumber, SoilTillageOperations.DStill, SoilTillageOperations.DepthResLow
                FROM SoilTillageOperations INNER JOIN ((OrganicFertilizationPolicy INNER JOIN (CropManagement INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt) 
                ON OrganicFertilizationPolicy.OFertiPolicyCode = CropManagement.OFertiPolicyCode) INNER JOIN OrganicFOperations ON OrganicFertilizationPolicy.OFertiPolicyCode = 
                OrganicFOperations.OFertiPolicyCode) ON SoilTillageOperations.SoilTillPolicyCode = CropManagement.SoilTillPolicyCode Where Idsim ='%s';"""%(idSim)
    fileContent += "\n"
    fileContent += "*TILLAGE AND ROTATIONS\n"
    fileContent += "@T TDATE TIMPL  TDEP TNAME\n"
    dataTable = pd.read_sql_query(fetchAllQuery, master_input_connection)
    for i in range(dataTable.shape[0]):
        rw = DT[DT["Champ"] == "LNTIL"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_tillage["T"].format(float(Dv))
        ifert = int(dataTable["sowingdate"].values[i] + dataTable["DSTill"].values[i])
        if dataTable["StartYear"].values[i] % 4 == 0:
            Bissext = 1
        else:
            Bissext = 0
        if ifert > 365 + Bissext:
            fileContent += v_fmt_tillage["TDATE"].format(str(dataTable["StartYear"].values[i] + 1)[2:4] + str(ifert - 365 - Bissext).rjust(3, "0"))
        else:
            fileContent += v_fmt_tillage["TDATE"].format(str(dataTable["StartYear"].values[i])[2:4] + str(ifert).rjust(3, "0"))
        rw = DT[DT["Champ"] == "TIMPL"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_tillage["TIMPL"].format(Dv.strip())
        rw = DT[DT["Champ"] == "TDEP"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_tillage["TDEP"].format(float(Dv))
        rw = DT[DT["Champ"] == "TNAME"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_tillage["TNAME"].format(Dv.strip()) + "\n"
    return fileContent


def writeBlockEnvironment(dssat_tableName, dssat_tableId, modelDictionary_Connection):
    fileContent = ""
    dssat_queryRead  = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    siteColumnsHeader = "@E ODATE EDAY  ERAD  EMAX  EMIN  ERAIN ECO2  EDEW  EWIND ENVNAME"
    fileContent += "\n"
    fileContent += "*ENVIRONMENT MODIFICATIONS\n"
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNENV"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["E"].format(float(Dv))
    rw = DT[DT["Champ"] == "WMDATE"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["ODATE"].format(format(int(Dv), "05"))
    rw = DT[DT["Champ"] == "DAYFAC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["EDAY"].format(Dv.strip())
    rw = DT[DT["Champ"] == "DAYADJ"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["EDAY1"].format(Dv.strip())
    rw = DT[DT["Champ"] == "RADFAC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["ERAD"].format(Dv.strip())
    rw = DT[DT["Champ"] == "RADADJ"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["ERAD1"].format(Dv.strip())
    rw = DT[DT["Champ"] == "TXFAC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["EMAX"].format(Dv.strip())
    rw = DT[DT["Champ"] == "TXADJ"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["EMAX1"].format(Dv.strip())
    rw = DT[DT["Champ"] == "TMFAC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["EMIN"].format(Dv.strip())
    rw = DT[DT["Champ"] == "TMADJ"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["EMIN1"].format(Dv.strip())
    rw = DT[DT["Champ"] == "PRCFAC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["ERAIN"].format(Dv.strip())
    rw = DT[DT["Champ"] == "PRCADJ"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["ERAIN1"].format(Dv.strip())
    rw = DT[DT["Champ"] == "CO2FAC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["ECO2"].format(Dv.strip())
    rw = DT[DT["Champ"] == "CO2ADJ"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["ECO21"].format(Dv.strip())
    rw = DT[DT["Champ"] == "DPTFAC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["EDEW"].format(Dv.strip())
    rw = DT[DT["Champ"] == "DPTADJ"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["EDEW1"].format(Dv.strip())
    rw = DT[DT["Champ"] == "WNDFAC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["EWIND"].format(Dv.strip())
    rw = DT[DT["Champ"] == "WNDADJ"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["EWIND1"].format(Dv.strip())
    rw = DT[DT["Champ"] == "ENVNAME"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_environment["ENVNAME"].format(Dv.strip()) + "\n"

    return fileContent


def writeBlockHarvest(dssat_tableName, idSim, dssat_tableId, modelDictionary_Connection, master_input_connection, Dv_hari):
    fileContent = ""
    dssat_queryRead  = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s' or [Table] = 'dssat_x_simulation_management'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    siteColumnsHeader = "@H HDATE  HSTG  HCOM HSIZE   HPC  HBPC HNAME"
    fileContent += "\n"
    fileContent += "*HARVEST DETAILS\n"
    fileContent += siteColumnsHeader + "\n"
    
    fetchAllQuery  = """SELECT SimUnitList.idsim,  SimUnitList.StartYear, SimUnitList.EndYear,SimUnitList.EndDay, CropManagement.Sowingdate, 
                CropManagement.DHarvest FROM CropManagement JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt WHERE SimUnitList.idsim ='%s';"""%(idSim)

    #fetchAllQuery  = "SELECT SimUnitList.idsim, SimUnitList.EndYear,SimUnitList.EndDay FROM SimUnitList  Where Idsim ='%s';"%(idSim)
    dataTable = pd.read_sql_query(fetchAllQuery, master_input_connection)
    rw = DT[DT["Champ"] == "LNHAR"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_harvest["H"].format(float(Dv))
    rw = DT[DT["Champ"] == "IHARI"]
    Dv = rw["dv"].values[0]
    
    iharv = int(dataTable["sowingdate"].values[0] + dataTable["DHarvest"].values[0])
    if dataTable["DHarvest"].values[0] == 0: iharv+= 1
    if dataTable["StartYear"].values[0] % 4 == 0:
        Bissext = 1
    else:
        Bissext = 0
    if iharv > 365 + Bissext and Dv_hari != "D":
        fileContent += v_fmt_harvest["HDATE"].format(str(dataTable["StartYear"].values[0] + 1)[2:4] + str(iharv - 365 - Bissext).rjust(3, "0"))
    elif iharv <= 365 + Bissext and Dv_hari != "D":
        fileContent += v_fmt_harvest["HDATE"].format(str(dataTable["StartYear"].values[0])[2:4] + str(iharv).rjust(3, "0"))
    elif Dv_hari == "D":
        fileContent += v_fmt_harvest["HDATE"].format(str(int(dataTable["DHarvest"].values[0])))


    '''if Dv_hari == "D":
        fileContent += v_fmt_harvest["HDATE"].format(format(int( str(dataTable["EndDay"].values[0]))))
    else: fileContent += v_fmt_harvest["HDATE"].format(str(dataTable["EndYear"].values[0])[2:4] + str(dataTable["EndDay"].values[0]).rjust(3, "0"))'''
    rw = DT[DT["Champ"] == "HTSG"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_harvest["HSTG"].format(Dv.strip())
    rw = DT[DT["Champ"] == "HCOM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_harvest["HCOM"].format(Dv.strip())
    rw = DT[DT["Champ"] == "HSIZ"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_harvest["HSIZE"].format(Dv.strip())
    rw = DT[DT["Champ"] == "HPC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_harvest["HPC"].format(int(Dv))
    rw = DT[DT["Champ"] == "HBPC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_harvest["HBPC"].format(int(Dv))
    rw = DT[DT["Champ"] == "HNAME"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_harvest["HNAME"].format(Dv.strip()) + "\n"
    return fileContent



def writeBlockEndFile( idSim, modelDictionary_Connection, master_input_connection):
    fileContent = ""
    storeKeyDataN = 0
    storeNumMaxSimu = 1
    dssat_queryRead  = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = 'dssat_x_simulation_management' ))";
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    rw = DT[DT["Champ"] == "IPLTI"]
    Dv_planting = rw["dv"].values[0]
    rw = DT[DT["Champ"] == "IIRRI"]
    Dv_irri = rw["dv"].values[0]
    rw = DT[DT["Champ"] == "IFERI"]
    Dv_ferti = rw["dv"].values[0]
    rw = DT[DT["Champ"] == "IHARI"]
    Dv_hari = rw["dv"].values[0]
    rw = DT[DT["Champ"] == "IRESI"]
    Dv_resi = rw["dv"].values[0]

    while storeNumMaxSimu > storeKeyDataN:
        storeKeyDataN = storeKeyDataN + 1
        dssat_tableId1 = "dssat_x_exp_id"
        dssat_tableName1 = "dssat_x_simulation_general"
        fileContent += writeBlockGeneral(dssat_tableName1, dssat_tableId1, idSim, modelDictionary_Connection, master_input_connection)
        dssat_tableName1 = "dssat_x_simulation_option"
        fileContent += writeBlockOption(dssat_tableName1, dssat_tableId1, idSim, modelDictionary_Connection, master_input_connection)
        dssat_tableName1 = "dssat_x_simulation_method"
        fileContent += writeBlockMethod(dssat_tableName1, dssat_tableId1, idSim, modelDictionary_Connection)
        dssat_tableName1 = "dssat_x_simulation_management"
        fileContent += writeBlockManagement(dssat_tableName1, dssat_tableId1, idSim, modelDictionary_Connection)
        dssat_tableName1 = "dssat_x_simulation_outputs"
        fileContent += writeBlockoutputs(dssat_tableName1, dssat_tableId1, idSim, modelDictionary_Connection)
        fileContent += "\n"
        #z = 0
        if Dv_planting=="A" or Dv_irri=="A" or Dv_ferti=="A" or Dv_hari=="A" or Dv_resi=="A":
            fileContent += "@  AUTOMATIC MANAGEMENT"
            fileContent += "\n"
            dssat_tableName1 = "dssat_x_automatic_planting"
            fileContent += writeBlockAutomaticPlanting(dssat_tableName1, dssat_tableId1, idSim, modelDictionary_Connection, master_input_connection)
            dssat_tableName1 = "dssat_x_automatic_irrigation"
            fileContent += writeBlockAutomaticIrrigation(dssat_tableName1, dssat_tableId1, idSim, modelDictionary_Connection)
            dssat_tableName1 = "dssat_x_automatic_nitrogen"
            fileContent += writeBlockAutomaticNitrogen(dssat_tableName1, dssat_tableId1, idSim, modelDictionary_Connection)
            dssat_tableName1 = "dssat_x_automatic_residues"
            fileContent += writeBlockAutomaticResidue(dssat_tableName1, dssat_tableId1, idSim, modelDictionary_Connection)
            dssat_tableName1 = "dssat_x_automatic_harvest"
            fileContent += writeBlockAutomaticHarvest(dssat_tableName1, dssat_tableId1, idSim, modelDictionary_Connection, master_input_connection)
            fileContent += "\n"
    return fileContent

def writeBlockGeneral(dssat_tableName, dssat_tableId, idSim, modelDictionary_Connection, master_input_connection):
    fileContent = ""
    siteColumnsHeader = "@N GENERAL     NYERS NREPS START SDATE RSEED SNAME.................... SMODEL"
    fileContent += siteColumnsHeader + "\n"
    fetchAllQuery  = "SELECT SimUnitList.idsim, SimUnitList.StartYear,SimUnitList.StartDay, CropManagement.Sowingdate FROM CropManagement INNER JOIN SimUnitList ON CropManagement.idMangt = SimUnitList.idMangt WHERE Idsim ='%s';"%(idSim)
    dataTable = pd.read_sql_query(fetchAllQuery, master_input_connection)
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    rw = DT[DT["Champ"] == "LNSIM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["N"].format(float(Dv))
    rw = DT[DT["Champ"] == "TITCOM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["GENERAL"].format(Dv.strip())
    rw = DT[DT["Champ"] == "NYRS"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["NYERS"].format(int(Dv.strip()))
    rw = DT[DT["Champ"] == "NREPSQ"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["NREPS"].format(int(Dv.strip()))
    rw = DT[DT["Champ"] == "ISIMI"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["START"].format(Dv.strip())
    
    if dataTable["StartYear"].values[0] % 4 == 0:
        Bissext = 1
    else:
        Bissext = 0
    if dataTable["StartDay"].values[0] > 365 + Bissext:
        fileContent += v_fmt_simulation["SDATE"].format(str(dataTable["StartYear"].values[0] + 1)[2:4] + str(dataTable["StartDay"].values[0] - 365 - Bissext).rjust(3, "0"))
    else:
        fileContent += v_fmt_simulation["SDATE"].format(str(dataTable["StartYear"].values[0])[2:4] + str(dataTable["StartDay"].values[0]).rjust(3, "0"))
    rw = DT[DT["Champ"] == "RSEED"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["RSEED"].format(int(Dv.strip()))
    rw = DT[DT["Champ"] == "TITSIM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["SNAME"].format(Dv.strip())
    rw = DT[DT["Champ"] == "CROP_MODE"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["SMODEL"].format(Dv.strip()) + "\n"
    
    return fileContent

def writeBlockOption(dssat_tableName, dssat_tableId, idSim, modelDictionary_Connection, master_input_connection):
    fileContent = ""
    siteColumnsHeader = "@N OPTIONS     WATER NITRO SYMBI PHOSP POTAS DISES  CHEM  TILL   CO2"
    fetchAllQuery  = """SELECT SimUnitList.idsim, SimulationOptions.StressW_YN, SimulationOptions.StressN_YN, SimulationOptions.StressP_YN, SimulationOptions.StressK_YN
        FROM SimUnitList INNER JOIN SimulationOptions ON SimUnitList.IdOption = SimulationOptions.IdOptions Where idsim ='%s';"""%(idSim)
    dataTable = pd.read_sql_query(fetchAllQuery, master_input_connection)
    rows = dataTable.to_dict('records')
    row = rows[0]
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNSIM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["N"].format(float(Dv))
    rw = DT[DT["Champ"] == "TITOPT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["OPTIONS"].format(Dv.strip())
    if row["StressW_YN"]: fileContent += v_fmt_simulation["WATER"].format("Y")
    else: fileContent += v_fmt_simulation["WATER"].format("N")
    if row["StressN_YN"]: fileContent += v_fmt_simulation["NITRO"].format("Y")
    else: fileContent += v_fmt_simulation["NITRO"].format("N")
    rw = DT[DT["Champ"] == "ISWSYM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["SYMBI"].format(Dv.strip())
    if row["StressP_YN"]: fileContent += v_fmt_simulation["PHOSP"].format("Y")
    else: fileContent += v_fmt_simulation["PHOSP"].format("N")
    if row["StressK_YN"]: fileContent += v_fmt_simulation["POTAS"].format("Y")
    else: fileContent += v_fmt_simulation["POTAS"].format("N")
    rw = DT[DT["Champ"] == "ISWDIS"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["DISES"].format(Dv.strip())
    rw = DT[DT["Champ"] == "ISCHEM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["CHEM"].format(Dv.strip())
    rw = DT[DT["Champ"] == "ISTILL"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["TILL"].format(Dv.strip())
    rw = DT[DT["Champ"] == "ISCO2"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["CO2"].format(Dv.strip()) + "\n"
    return fileContent



def writeBlockMethod(dssat_tableName, dssat_tableId, idSim, modelDictionary_Connection):
    fileContent = ""
    siteColumnsHeader = "@N METHODS     WTHER INCON LIGHT EVAPO INFIL PHOTO HYDRO NSWIT MESOM MESEV MESOL"
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNSIM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["N"].format(float(Dv))
    rw = DT[DT["Champ"] == "TITMET"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["METHODS"].format(Dv.strip())
    rw = DT[DT["Champ"] == "MEWTH"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["WTHER"].format(Dv.strip())
    rw = DT[DT["Champ"] == "MESIC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["INCON"].format(Dv.strip())
    rw = DT[DT["Champ"] == "MELI"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["LIGHT"].format(Dv.strip())
    rw = DT[DT["Champ"] == "MEEVP"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["EVAPO"].format(Dv.strip())
    rw = DT[DT["Champ"] == "MEINF"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["INFIL"].format(Dv.strip())
    rw = DT[DT["Champ"] == "MEPHO"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["PHOTO"].format(Dv.strip())
    rw = DT[DT["Champ"] == "HYDRO"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["HYDRO"].format(Dv.strip())
    rw = DT[DT["Champ"] == "NSWIT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["NSWIT"].format(int(Dv))
    rw = DT[DT["Champ"] == "MESOM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["MESOM"].format(Dv.strip())
    rw = DT[DT["Champ"] == "MESEV"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["MESEV"].format(Dv.strip())
    rw = DT[DT["Champ"] == "MESOL"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["MESOL"].format(int(Dv)) + "\n"
    return fileContent



def writeBlockManagement(dssat_tableName, dssat_tableId, idSim, modelDictionary_Connection):
    fileContent = ""
    siteColumnsHeader = "@N MANAGEMENT  PLANT IRRIG FERTI RESID HARVS"
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNSIM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["N"].format(float(Dv))
    rw = DT[DT["Champ"] == "TITMAT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["MANAGEMENT"].format(Dv.strip())
    rw = DT[DT["Champ"] == "IPLTI"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["PLANT"].format(Dv.strip())
    rw = DT[DT["Champ"] == "IIRRI"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["IRRIG"].format(Dv.strip())  #TODO: Check if this management option should not be provided from the MasterInput database
    rw = DT[DT["Champ"] == "IFERI"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["FERTI"].format(Dv.strip())  #TODO: Check if this management option should not be provided from the MasterInput database
    rw = DT[DT["Champ"] == "IRESI"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["RESID"].format(Dv.strip())  
    rw = DT[DT["Champ"] == "IHARI"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["HARVS"].format(Dv.strip()) + "\n"
    return fileContent
    


def writeBlockoutputs(dssat_tableName, dssat_tableId, idSim, modelDictionary_Connection):
    fileContent = ""
    siteColumnsHeader = "@N OUTPUTS     FNAME OVVEW SUMRY FROPT GROUT CAOUT WAOUT NIOUT MIOUT DIOUT VBOSE CHOUT OPOUT"
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNSIM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["N"].format(float(Dv))
    rw = DT[DT["Champ"] == "TITOUT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["OUTPUTS"].format(Dv.strip())
    rw = DT[DT["Champ"] == "IOX"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["FNAME"].format(Dv.strip())
    rw = DT[DT["Champ"] == "IDETO"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["OVVEW"].format(Dv.strip())
    rw = DT[DT["Champ"] == "IDETS"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["SUMRY"].format(Dv.strip())
    rw = DT[DT["Champ"] == "FROP"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["FROPT"].format(int(Dv))
    rw = DT[DT["Champ"] == "IDETG"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["GROUT"].format("N")#format(Dv.strip())
    rw = DT[DT["Champ"] == "IDETC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["CAOUT"].format("N")#format(Dv.strip())
    rw = DT[DT["Champ"] == "IDETG"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["WAOUT"].format("N")#format(Dv.strip())
    rw = DT[DT["Champ"] == "IDETN"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["NIOUT"].format("N")#format(Dv.strip())
    rw = DT[DT["Champ"] == "IDETP"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["MIOUT"].format("N")#format(Dv.strip())
    rw = DT[DT["Champ"] == "IDETD"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["DIOUT"].format("N")#format(Dv.strip())
    rw = DT[DT["Champ"] == "IDETG"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["VBOSE"].format("N")#format(Dv.strip())
    rw = DT[DT["Champ"] == "IDETC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["CHOUT"].format("N")#format(Dv.strip())
    rw = DT[DT["Champ"] == "IDETG"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["OPOUT"].format(Dv.strip()) + "\n"
    return fileContent


def writeBlockAutomaticPlanting(dssat_tableName, dssat_tableId, idSim, modelDictionary_Connection, master_input_connection):
    fileContent = ""
    siteColumnsHeader = "@N PLANTING    PFRST PLAST PH2OL PH2OU PH2OD PSTMX PSTMN"
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)

    fetchAllQuery = "SELECT SimUnitList.idsim, SimUnitList.StartYear, SimUnitList.EndYear FROM SimUnitList  Where Idsim ='%s';"%(idSim)
    dataTable = pd.read_sql_query(fetchAllQuery, master_input_connection)
    
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNSIM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["N"].format(float(Dv))
    rw = DT[DT["Champ"] == "TITPLA"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["PLANTING"].format(Dv.strip())
    rw = DT[DT["Champ"] == "PWDINF"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["PFRST"].format(str(dataTable["StartYear"].values[0])[2:4] + format(int(Dv), "03"))
    #fileContent += v_fmt_simulation["PFRST"].format(format(int(Dv), "05"))
    rw = DT[DT["Champ"] == "PWDINL"]
    Dv = rw["dv"].values[0]
    if int(Dv) > 365: 
        fileContent += v_fmt_simulation["PLAST"].format(str(int(dataTable["StartYear"].values[0])+1)[2:4] + format(int(Dv) - 365, "03"))
    else:
        fileContent += v_fmt_simulation["PLAST"].format(str(dataTable["StartYear"].values[0])[2:4] + format(int(Dv), "03"))
    #fileContent += v_fmt_simulation["PLAST"].format(format(int(Dv), "05"))
    rw = DT[DT["Champ"] == "SWPLTL"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["PH2OL"].format(int(Dv))
    rw = DT[DT["Champ"] == "SWPLTH"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["PH2OU"].format(int(Dv))
    rw = DT[DT["Champ"] == "SWPLTD"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["PH2OD"].format(int(Dv))
    rw = DT[DT["Champ"] == "PTX"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["PSTMX"].format(int(Dv))
    rw = DT[DT["Champ"] == "PTTN"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["PSTMN"].format(int(Dv)) + "\n"
    return fileContent



def writeBlockAutomaticIrrigation(dssat_tableName, dssat_tableId, idSim, modelDictionary_Connection):
    fileContent = ""
    siteColumnsHeader = "@N IRRIGATION  IMDEP ITHRL ITHRU IROFF IMETH IRAMT IREFF"
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNSIM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["N"].format(float(Dv))
    rw = DT[DT["Champ"] == "TITIRR"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["IRRIGATION"].format(Dv.strip())
    rw = DT[DT["Champ"] == "DSOIL"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["IMDEP"].format(int(Dv))
    rw = DT[DT["Champ"] == "THETAC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["ITHRL"].format(int(Dv))
    rw = DT[DT["Champ"] == "IEPT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["ITHRU"].format(int(Dv))
    rw = DT[DT["Champ"] == "IOFF"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["IROFF"].format(Dv.strip())
    rw = DT[DT["Champ"] == "IAME"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["IMETH"].format(Dv.strip())
    rw = DT[DT["Champ"] == "AIRAMT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["IRAMT"].format(int(Dv))
    rw = DT[DT["Champ"] == "EFFIRR"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["IREFF"].format(int(Dv)) + "\n"
    return fileContent


def writeBlockAutomaticNitrogen(dssat_tableName, dssat_tableId, idSim, modelDictionary_Connection):
    fileContent = ""
    siteColumnsHeader = "@N NITROGEN    NMDEP NMTHR NAMNT NCODE NAOFF"
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNSIM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["N"].format(float(Dv))
    rw = DT[DT["Champ"] == "TITNIT"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["NITROGEN"].format(Dv.strip())
    rw = DT[DT["Champ"] == "DSOILN"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["NMDEP"].format(int(Dv))
    rw = DT[DT["Champ"] == "SOILNC"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["NMTHR"].format(int(Dv))
    rw = DT[DT["Champ"] == "SOILNX"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["NAMNT"].format(int(Dv))
    rw = DT[DT["Champ"] == "NCODE"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["NCODE"].format(Dv.strip())
    rw = DT[DT["Champ"] == "NEND"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["NAOFF"].format(Dv.strip()) + "\n"
    return fileContent
    

def writeBlockAutomaticResidue(dssat_tableName, dssat_tableId, idSim, modelDictionary_Connection):
    fileContent = ""
    siteColumnsHeader = "@N RESIDUES    RIPCN RTIME RIDEP"
    dssat_queryRead = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    fileContent += siteColumnsHeader + "\n"
    rw = DT[DT["Champ"] == "LNSIM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["N"].format(float(Dv))
    rw = DT[DT["Champ"] == "TITRES"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["RESIDUES"].format(Dv.strip())
    rw = DT[DT["Champ"] == "RIP"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["RIPCN"].format(int(Dv))
    rw = DT[DT["Champ"] == "NRESDL"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["RTIME"].format(int(Dv))
    rw = DT[DT["Champ"] == "DRESMG"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["RIDEP"].format(int(Dv)) + "\n"
    return fileContent


def writeBlockAutomaticHarvest(dssat_tableName, dssat_tableId, idSim, modelDictionary_Connection, master_input_connection):
    fileContent = ""
    siteColumnsHeader = "@N HARVEST     HFRST HLAST HPCNP HPCNR"
    dssat_queryRead = "Select Champ, Default_Value_Datamill, Variables.defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '%s'));"%(dssat_tableName)
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    fileContent += siteColumnsHeader + "\n"
    fetchAllQuery = "SELECT SimUnitList.idsim, SimUnitList.EndYear,SimUnitList.EndDay FROM SimUnitList  Where Idsim ='%s';"%(idSim)
    dataTable = pd.read_sql_query(fetchAllQuery, master_input_connection)
    rw = DT[DT["Champ"] == "LNSIM"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["N"].format(float(Dv))
    rw = DT[DT["Champ"] == "TITHAR"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["HARVEST"].format(Dv.strip())
    rw = DT[DT["Champ"] == "HDLAY"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["HFRST"].format(Dv.strip())
    fileContent += v_fmt_simulation["HLAST"].format(str(dataTable["EndYear"].values[0])[2:4] + str(dataTable["EndDay"].values[0]).rjust(3, "0"))
    rw = DT[DT["Champ"] == "HPP"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["HPCNP"].format(int(Dv))
    rw = DT[DT["Champ"] == "HRP"]
    Dv = rw["dv"].values[0]
    fileContent += v_fmt_simulation["HPCNR"].format(int(Dv)) + "\n"
    return fileContent



def writeBlockTreatment2(dssat_tableName, fileName, idSim, modelDictionary_Connection):
    fileContent = ""
    dssat_queryRead = f"Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],  [Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] = '{dssat_tableName}'));"
    DT = pd.read_sql_query(dssat_queryRead, modelDictionary_Connection)
    fileContent += "\n"
    fileContent += "$BATCH(EXPERIMENT)\n"
    siteColumnsHeader = '@FILEX                                                                                        TRTNO     RP     SQ     OP     CO'
    fileContent += siteColumnsHeader + "\n"
    fileContent += fileName.ljust(92)
    rw = DT[DT["Champ"] == "TRTNO"]
    Dv = rw["dv"].values[0]
    fileContent += str(Dv).rjust(7)
    rw = DT[DT["Champ"] == "ROTNO"]
    Dv = rw["dv"].values[0]
    fileContent += str(Dv).rjust(7)
    fileContent += str(1).rjust(7)
    rw = DT[DT["Champ"] == "ROTOPT"]
    Dv = rw["dv"].values[0]
    fileContent += str(Dv).rjust(7)
    rw = DT[DT["Champ"] == "CRPNO"]
    Dv = rw["dv"].values[0]
    fileContent += str(Dv).rjust(7)
    fileContent += "\n"
    return fileContent
    



class DssatXConverter(Converter):
    def __init__(self):
        super().__init__()

    def export(self, directory_path, modelDictionary_Connection, master_input_connection,usmdir, crop):
        ST = directory_path.split(os.sep)
        idSim = ST[-2]
        idMangt = ST[-1]
        T = "Select Champ, Default_Value_Datamill, defaultValueOtherSource, IFNULL([defaultValueOtherSource],[Default_Value_Datamill]) As dv From Variables Where ((model = 'dssat') And ([Table] like 'dssat_x_%'));"
        DT = pd.read_sql_query(T, modelDictionary_Connection)
        
        fetchAllQuery  = """Select SimUnitList.idsim, SoilTillPolicy.NumTillOperations, OrganicFertilizationPolicy.NumOrganicFerti, 
        CropManagement.IrrigationPolicyCode, CropManagement.InoFertiPolicyCode 
        From OrganicFertilizationPolicy INNER Join (SoilTillPolicy INNER Join (CropManagement INNER Join SimUnitList 
        On CropManagement.idMangt = SimUnitList.idMangt) ON SoilTillPolicy.SoilTillPolicyCode = CropManagement.SoilTillPolicyCode) 
        ON OrganicFertilizationPolicy.OFertiPolicyCode = CropManagement.OFertiPolicyCode Where IdSim='%s'""" % (idSim)
        dataTable = pd.read_sql_query(fetchAllQuery, master_input_connection)

        rw = DT[DT["Champ"] == "IPLTI"]
        Dv_planting = rw["dv"].values[0]
        rw = DT[DT["Champ"] == "IIRRI"]
        Dv_irri = rw["dv"].values[0]
        rw = DT[DT["Champ"] == "IFERI"]
        Dv_ferti = rw["dv"].values[0]
        rw = DT[DT["Champ"] == "IHARI"]
        Dv_hari = rw["dv"].values[0]
        rw = DT[DT["Champ"] == "IRESI"]
        Dv_resi = rw["dv"].values[0]
        
        rows = dataTable.to_dict('records')
        #rw = DT[DT["Champ"] == "filename"]
        #Dv = rw["dv"].values[0]
        fileName = f"ITSA1301.{crop}X"
        #rw = DT[DT["Champ"] == "header"]
        #Dv = rw["dv"].values[0]
        header = f"*EXP.DETAILS: {idSim} "
        fileContent = header + "\n\n"
        fileContent += "*GENERAL\n"
        fileContent += "@PEOPLE\n"
        rw = DT[DT["Champ"] == "PEOPLE"]
        Dv = rw["dv"].values[0]
        fileContent += Dv + "\n"
        fileContent += "@ADDRESS\n"
        rw = DT[DT["Champ"] == "ADDRESS"]
        Dv = rw["dv"].values[0]
        fileContent += Dv + "\n"
        fileContent += "@SITE\n"
        rw = DT[DT["Champ"] == "SITE"]
        Dv = rw["dv"].values[0]
        fileContent += Dv + "\n"
        site_columns_header = "@ PAREA  PRNO  PLEN  PLDR  PLSP  PLAY HAREA  HRNO  HLEN  HARM........."
        fileContent += site_columns_header + "\n"
        rw = DT[DT["Champ"] == "PAREA"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_general["PAREA"].format(float(Dv))
        rw = DT[DT["Champ"] == "PRNO"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_general["PRNO"].format(float(Dv))
        rw = DT[DT["Champ"] == "PLEN"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_general["PLEN"].format(float(Dv))
        rw = DT[DT["Champ"] == "PLDR"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_general["PLDR"].format(float(Dv))
        rw = DT[DT["Champ"] == "PLSP"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_general["PLSP"].format(float(Dv))
        rw = DT[DT["Champ"] == "PLAY"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_general["PLAY"].format(float(Dv))
        rw = DT[DT["Champ"] == "HAREA"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_general["HAREA"].format(float(Dv))
        rw = DT[DT["Champ"] == "HRNO"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_general["HRNO"].format(float(Dv))
        rw = DT[DT["Champ"] == "HLEN"]
        Dv = rw["dv"].values[0]
        fileContent += v_fmt_general["HLEN"].format(float(Dv))
        rw = DT[DT["Champ"] == "HARM"]
        Dv = rw["dv"].values[0] 
        fileContent += v_fmt_general["HARM"].format(Dv) + "\n"
        fileContent += "@NOTES\n"
        rw = DT[DT["Champ"] == "NOTES"]
        Dv = rw["dv"].values[0]
        fileContent += Dv + "\n\n"
        
        # TREATMENTS
        dssat_tableName = "dssat_x_treatment"
        dssat_tableId = "dssat_x_exp_id"
        fileContent += writeBlockTreatment(dssat_tableName, idSim, modelDictionary_Connection, master_input_connection)
        
        # CULTIVARS
        dssat_tableName = "dssat_x_cultivar"
        dssat_tableId = "dssat_x_exp_id"
        fileContent += writeBlockCultivar(dssat_tableName, idMangt, modelDictionary_Connection, master_input_connection)
        
        # FIELDS
        dssat_tableName = "dssat_x_field"
        dssat_tableId = "dssat_x_exp_id"
        fileContent += writeBlockField(dssat_tableName, dssat_tableId, idMangt, modelDictionary_Connection)
        
        # SOIL ANALYSIS
        dssat_tableName = "dssat_x_soil_analysis"
        dssat_tableId = "dssat_x_exp_id"
        fileContent += writeBlockSoilAnalysis(dssat_tableName, dssat_tableId, modelDictionary_Connection)   
  
        # Initial conditions
        dssat_tableName = "dssat_x_initial_condition"
        dssat_tableId = "dssat_x_exp_id"
        fileContent += writeBlockInitialCondition(dssat_tableName, idSim, modelDictionary_Connection, master_input_connection)      
        
        # Planting details
        dssat_tableName = "dssat_x_planting_detail"
        dssat_tableId = "dssat_x_exp_id"
        fileContent += writeBlockPlantingDetail(dssat_tableName, idSim, modelDictionary_Connection, master_input_connection)
        
        # Irrigation and water management
        dssat_tableName = "dssat_x_irrigation_water"
        dssat_tableId = "dssat_x_exp_id"
        if int(rows[0]["IrrigationPolicyCode"]) == 0:
            fileContent += "\n"
        else: fileContent += writeBlockIrrigationWater(dssat_tableName, dssat_tableId, modelDictionary_Connection)
        
        # Fertilizer
        dssat_tableName = "dssat_x_fertilizer"
        if int(rows[0]["InoFertiPolicyCode"]) == 0:
            fileContent += "\n"
        else: fileContent += writeBlockFertilizer(dssat_tableName, idSim, modelDictionary_Connection, master_input_connection, Dv_ferti)
        
        # Residues and organic fertilizer
        dssat_tableName = "dssat_x_residues"
        dssat_tableId = "dssat_x_exp_id"
        fileContent += writeBlockResidues(dssat_tableName, idSim, dssat_tableId, modelDictionary_Connection, master_input_connection)
        
        # CHEMICAL APPLICATIONS    No Chemical Application in the database
        dssat_tableName = "dssat_x_chemical_application"
        dssat_tableId = "dssat_x_exp_id"
        #fileContent += writeBlockChemicalApplication(dssat_tableName, dssat_tableId, modelDictionary_Connection)
        
        # TILLAGE AND ROTATIONS
        dssat_tableName = "dssat_x_tillage"
        dssat_tableId = "dssat_x_exp_id"
        fileContent += writeBlockTillageRotation(dssat_tableName, idSim, modelDictionary_Connection, master_input_connection)
        
        # ENVIRONMENT MODIFICATIONS            # No environment modification in the database
        dssat_tableName = "dssat_x_environment"
        dssat_tableId = "dssat_x_exp_id"
        #fileContent += writeBlockEnvironment(dssat_tableName, dssat_tableId, modelDictionary_Connection)
        
        # HARVEST DETAILS
        dssat_tableName = "dssat_x_harvest"
        dssat_tableId = "dssat_x_exp_id"
        fileContent += writeBlockHarvest(dssat_tableName, idSim, dssat_tableId, modelDictionary_Connection, master_input_connection, Dv_hari)
        
        # SIMULATION CONTROLS
        fileContent += "\n"
        fileContent += "*SIMULATION CONTROLS\n"
        fileContent += writeBlockEndFile(idSim, modelDictionary_Connection, master_input_connection)
        
        try:
            # Export file to specified directory
            self.write_file(usmdir, fileName, fileContent)
            fileContent = ""
        except Exception as e:
            print("Error during writing file")
            print(e)
            
        #  Fichier DSSBatch.v47
        dssat_tableName = "dssat_x_treatment"
        dssat_tableId = "dssat_x_exp_id"
        fileContent = writeBlockTreatment2(dssat_tableName, fileName, idSim, modelDictionary_Connection)
        self.write_file(usmdir, "DSSBatch.v47", fileContent)
        fileContent = ""







