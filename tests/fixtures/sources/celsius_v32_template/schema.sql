CREATE TABLE [CO2Yearly] ([yearCO2] INTEGER, [CO2] REAL);

CREATE TABLE [Cultivars] ([codeplante] TEXT, [CodePSpecies] TEXT, [NumCultivar] INTEGER, [IdCultivar] TEXT NOT NULL, [CodCultivar] TEXT, [NbStadesPheno] INTEGER, [SensPhot] REAL, [STrsChoc] REAL, [MOPP] REAL, [dlaimax] REAL, [adens] REAL, [bdens] REAL, [laicomp] REAL, [Vitircarb] REAL, [IRmax] REAL, [P1grainMax] REAL, [Ngrmax] INTEGER, [IFertMax] REAL, [Cgrain] INTEGER, [Cgrainv0] INTEGER, [RatioGrGou] REAL, [DurCycMax] INTEGER, [concNplante] REAL, [SensiSen] REAL);

CREATE TABLE [Dweather] ([idDclim] TEXT, [idjourclim] TEXT NOT NULL, [annee] INTEGER, [mois] INTEGER, [jour] INTEGER, [jda] INTEGER, [tmin] REAL, [tmax] REAL, [tmoy] REAL, [rg] REAL, [etp] REAL, [plu] REAL);

CREATE TABLE [FertiMin_List] ([IdTech_Com] TEXT, [IdFertiMin] TEXT, [DateFertiMin] TEXT, [YearFertiMin] INTEGER, [JourYrFertiMin] TEXT, [Nmineral] REAL, [Pmineral] REAL, [Kmineral] REAL);

CREATE TABLE [FertiOrga_List] ([IdTech_Com] TEXT, [IdFertiOrg] TEXT, [DateFertiOrg] TEXT, [YearFertiOrg] INTEGER, [JourYrFertiOrg] TEXT, [Norganique] REAL, [Porganique] REAL, [Korganique] REAL, [QorgaTot] REAL, [TypeMOrga] TEXT, [CsurNRes] REAL);

CREATE TABLE [General_Parameters] ([idGenParam] TEXT, [vlaimax] REAL, [codgenclim] INTEGER, [ParSurRg] REAL, [cas] TEXT);

CREATE TABLE [Irrigation_List] ([IdTech_Com] TEXT NOT NULL, [IdIrrig] TEXT, [DateIrrig] TEXT NOT NULL, [YearIrrig] INTEGER, [JourYrIrrig] INTEGER, [Irrigation] REAL);

CREATE TABLE [ListPAnnexes] ([CodePosteAnnexe] TEXT, [altitude] INTEGER, [latitudeDD] REAL, [longitude] REAL, [codestationP] TEXT, [idDclim] TEXT NOT NULL, [nomposte] TEXT, [CO2c] INTEGER, [ConcNplu] REAL, [commentaire] TEXT, [Selection] INTEGER, [Codcc] TEXT);

CREATE TABLE [ListResidus] ([CodeRes] TEXT, [descrResidu] TEXT, [Akres] REAL, [Bkres] REAL, [Ahres] REAL, [Bhres] REAL, [AWB] REAL, [BWB] REAL, [Yres] REAL, [Kbio] REAL, [Fbio] REAL, [Nrec] REAL);

CREATE TABLE [Mulch] ([idMulch] INTEGER, [nomtype] TEXT, [gamma_mulch] REAL, [CapaciteWMulch] REAL, [alpha_pail] REAL, [Beta_pail] REAL, [b_ruis] REAL);

CREATE TABLE [OptionsModel] ([idCodModel] INTEGER NOT NULL, [ActiveWstress] INTEGER, [ActiveNstress] INTEGER, [SimLevee] INTEGER, [EcritDResus] INTEGER, [CorrigAlti] INTEGER, [CyberST] INTEGER, [ActivePstress] INTEGER, [ActiveKstress] INTEGER, [TypeNPKstress] INTEGER, [ActivSarclage] INTEGER, [CodeDevelop] TEXT, [CCYNo] INTEGER);

CREATE TABLE [OutputD_1] ([IdSimJ] TEXT, [idSim] TEXT, [idweather] TEXT, [YearS] SINGLE, [idTech_Com] TEXT, [IdCultivar] TEXT, [DAP] INTEGER, [Jul] INTEGER, [Tmax] SINGLE, [Tmin] SINGLE, [Dvst] SINGLE, [Currestge] INTEGER, [Cropsta] INTEGER, [SomT] SINGLE, [LAI] SINGLE, [Biom] SINGLE, [grain] SINGLE, [Zrac] REAL, [Stsurf] SINGLE, [Esol] SINGLE, [Stnonrac] SINGLE, [Strac] REAL, [Transpi] SINGLE, [Drprofmax] SINGLE, [StockProf] REAL, [StockTot] REAL, [Plu] REAL, [Stockmes] REAL, [Emulch] REAL, [Ruis] REAL, [Qpaillis] REAL, [P1grain] REAL, [RG] REAL, [TpotMC] REAL, [DrainageON] INTEGER, [ConcNsol] REAL, [Dr] REAL, [perteNDrain] REAL, [Nuptake] REAL, [SigmaNuptake] REAL, [perteGaz] REAL, [AppMinNj] REAL, [AppOrgNj] REAL, [Navail] REAL, [NUPTtarget] REAL, [NRF] REAL, [WSfactH] REAL, [WSfact] REAL, [TurfacH] REAL, [Turfac] REAL, [Stger] REAL, [Competition] INTEGER, [CompFac] INTEGER, [raint] REAL, [SOC030] REAL, [SON030] REAL, [CO2hum] REAL, [CO2res] REAL, [NminMOSd] REAL, [NminResd] REAL);

CREATE TABLE [OutputD_2] ([IdSimJ] TEXT, [idSim] TEXT, [idweather] TEXT, [YearS] SINGLE, [idTech_Com] TEXT, [IdCultivar] TEXT, [DAP] INTEGER, [Jul] INTEGER, [Tmax] SINGLE, [Tmin] SINGLE, [Dvst] SINGLE, [Currestge] INTEGER, [Cropsta] INTEGER, [SomT] SINGLE, [LAI] SINGLE, [Biom] SINGLE, [grain] SINGLE, [Zrac] REAL, [Stsurf] SINGLE, [Esol] SINGLE, [Stnonrac] SINGLE, [Strac] REAL, [Transpi] SINGLE, [Drprofmax] SINGLE, [StockProf] REAL, [StockTot] REAL, [Plu] REAL, [Stockmes] REAL, [Emulch] REAL, [Ruis] REAL, [Qpaillis] REAL, [P1grain] REAL, [Etp] REAL, [TpotMC] REAL, [DrainageON] INTEGER, [ConcNsol] REAL, [Dr] REAL, [perteNDrain] REAL, [Nuptake] REAL, [SigmaNuptake] REAL, [perteGaz] REAL, [AppMinNj] REAL, [AppOrgNj] REAL, [Navail] REAL, [NUPTtarget] REAL, [NRF] REAL, [WSfactH] REAL, [WSfact] REAL, [TurfacH] REAL, [Turfac] REAL, [Stger] REAL, [Competition] INTEGER, [CompFac] INTEGER, [raint] REAL, [SOC030] REAL, [SON030] REAL, [CO2hum] REAL, [CO2res] REAL, [NminMOSd] REAL, [NminResd] REAL);

CREATE TABLE [OutputSynt] ([Idsim] TEXT NOT NULL, [JulPheno1_1] INTEGER, [JulPheno1_2] INTEGER, [JulPheno1_3] INTEGER, [JulPheno1_4] INTEGER, [JulPheno1_5] INTEGER, [JulPheno1_6] INTEGER, [death_day] INTEGER, [Biom(nrec)] SINGLE, [Grain(nrec)] SINGLE, [LAI] SINGLE, [SigmaSimEsol] REAL, [SigmaSimDr] REAL, [SigmaSimDrprofmax] REAL, [StockSol_init] REAL, [StockSol_final] REAL, [SigmaSimEmulch] REAL, [SigmaSimRuis] REAL, [SigmaTranspiMC] REAL, [SigmaSimPluM] REAL, [Ngrain] INTEGER, [P1grain] REAL, [Vitmoy] REAL, [iplt] INTEGER, [Nbsemis] INTEGER, [JulPheno2_1] INTEGER, [JulPheno2_2] INTEGER, [JulPheno2_3] INTEGER, [JulPheno2_4] INTEGER, [JulPheno2_5] INTEGER, [JulPheno2_6] INTEGER, [death_day_2] INTEGER, [Biom_nrec_2] SINGLE, [Grain_nrec_2] SINGLE, [LAI_2] SINGLE, [Ngrain_2] INTEGER, [P1grain_2] REAL, [Vitmoy_2] REAL, [iplt_2] INTEGER, [Nbsemis_2] INTEGER, [BilanNnonOK] INTEGER, [stockNsol] REAL, [RuisEtrange] INTEGER, [SigmaCultEsol] REAL, [txminN] REAL, [FminY1] REAL, [StockN1] REAL, [StockC1] REAL, [Cmintot] REAL, [CminRes] REAL, [NminMOSY] REAL, [NminRes] REAL, [Nreliquat] REAL, [Nplant1] REAL, [Nplant2] REAL, [grainN] REAL, [rootsN] REAL, [rootsC] REAL, [BioRoots] REAL, [SigmaCultDrprofmax] REAL, [CumPerteNdrainCult] REAL, [NminMOCult] REAL, [StsurfSemis] REAL, [StockC] REAL, [NprecipTot] REAL, [CumPerteNdrain] REAL, [Ninitial] REAL);

CREATE TABLE [ParamIni] ([IdIni] TEXT NOT NULL, [Stockinit] REAL, [Qpaillisinit] REAL, [iniSolhautON] INTEGER, [Ninit] REAL);

CREATE TABLE [PlantSpecies] ([nomplante1] TEXT, [nomplante2] TEXT, [codeplante] TEXT, [CodePspecies] TEXT NOT NULL, [tdmin] INTEGER, [tdmax] INTEGER, [tcmin] REAL, [tcmax] REAL, [tcopt] REAL, [lairecmax] REAL, [ebmax] REAL, [extin] REAL, [kmax] REAL, [DeltaRacMax] REAL, [Zracmax] REAL, [tcold] REAL, [Ndiecold] INTEGER, [Nbjgrain] INTEGER, [CTlevee] REAL, [Tger] REAL, [LegumON] INTEGER, [NJFletri] INTEGER, [Zgraine] INTEGER, [Nsymb] REAL, [NCvEmax] REAL, [NCvEmin] REAL, [alphaN] REAL, [PCvEmax] REAL, [PCvEmin] REAL, [alphaP] REAL, [KCvEmax] REAL, [KCvEmin] REAL, [alphaK] REAL, [Nsymbjour] REAL, [SeuilTurg] REAL, [SeuilWS] REAL, [alphaCo2] REAL, [commentaire] TEXT, [RootABGRatio] REAL, [RootCN] REAL, [NRootABGRatio] REAL, [NGrainABGRatio] REAL, [Pfactor] REAL);

CREATE TABLE [RuissellementObs] ([idTechCom] TEXT, [YearRuiObs] INTEGER, [JourRuiObs] REAL, [RuiObs] REAL);

CREATE TABLE [SimUnitList] ([idsim] TEXT NOT NULL, [idTech_Com] TEXT, [idweather] TEXT, [idsoil] TEXT, [idIni] TEXT, [idGenParam] TEXT, [idCodModel] INTEGER, [StartYear] INTEGER, [StartDay] INTEGER, [EndYear] INTEGER, [EndDay] INTEGER, [codCC] TEXT, [ChampTri] REAL, [Situation] TEXT, [codesuite] INTEGER);

CREATE TABLE [Soil] ([idsoil] TEXT NOT NULL, [zone] TEXT, [typchamp] TEXT, [typsol] TEXT, [NbCouches] INTEGER, [Zsurf] REAL, [Zmes] INTEGER, [SeuilEvap] REAL, [ZObstacleRac] REAL, [StockN] REAL, [CsurNhum] REAL, [NomSol] TEXT, [TypeRui] INTEGER, [SolSel] INTEGER, [txminN] REAL, [Fmin] REAL, [pHeau] REAL, [Clay] REAL, [PRedFact] REAL);

CREATE TABLE [Soil_layers] ([idsoil] TEXT, [NumCouche] INTEGER, [epc] REAL, [hcc] REAL, [hmin] REAL, [da] REAL);

CREATE TABLE [StadePheno] ([CodCultivar] TEXT NOT NULL, [NumStade] INTEGER NOT NULL, [CodStade] TEXT, [NomStade] TEXT, [CTstade] INTEGER, [TDV] REAL, [CodOrgForm] INTEGER, [commentaire] TEXT);

CREATE TABLE [Tech_Commun] ([IdTech_Com] TEXT NOT NULL, [NbCult] INTEGER, [NomSC] TEXT, [SerreTunnelON] INTEGER, [imulch] INTEGER, [CodParamMulch] INTEGER, [QpaillisApport] REAL, [AltiCult] INTEGER, [IrrigON] INTEGER, [fertiminON] INTEGER, [fertiorgON] INTEGER, [tApportMON] REAL, [tApportMinN] REAL, [CsurN_AO] REAL, [KresY] REAL, [DriveRuiObs] INTEGER);

CREATE TABLE [Tech_perCrop] ([idTechPerCrop] TEXT NOT NULL, [idTech_Com] TEXT, [NumCrop] INTEGER, [IdCultivar] TEXT, [TypInstal] INTEGER, [RepiquageON] INTEGER, [isem] INTEGER, [ilev] INTEGER, [DensSem] REAL, [irepiqu] REAL, [DensRepiqu] REAL, [DbutoirNouvSemis] INTEGER, [SemisAutoDebut] INTEGER, [SeuilCumPrecip] REAL);

CREATE TABLE [TypeSurfSol] ([TypeRui] INTEGER, [TexteRui] TEXT, [Ap1] REAL, [seuil_ruis] REAL, [Ap2] REAL, [Ap3] REAL, [Ap4] REAL, [comment] TEXT, [effetLAI] INTEGER);
