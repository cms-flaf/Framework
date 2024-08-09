import ROOT
import os
import sys
import yaml
import numpy as np
import matplotlib; import matplotlib.pyplot as plt
import matplotlib.colors as colors
import mplhep as hep
plt.style.use(hep.style.ROOT)

if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    ROOT.gInterpreter.Declare(f'#include "include/KinFitNamespace.h"')
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    ROOT.gROOT.ProcessLine('#include "include/AnalysisTools.h"')

import Common.Utilities as Utilities
from Analysis.HistHelper import *
from Analysis.hh_bbtautau import *
def defineP4(df, name):
    df = df.Define(f"{name}_p4", f"ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>({name}_pt,{name}_eta,{name}_phi,{name}_mass)")
    return df

inFiles = Utilities.ListToVector(["/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-1000/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-1250/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-1500/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-1750/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-2000/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-250/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-2500/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-260/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-270/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-280/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-300/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-3000/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-320/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-350/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-400/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-450/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-500/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-550/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-600/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-650/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-700/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-750/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-800/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-850/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-900/nanoHTT_0.root" ,"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-1000/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-1250/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-1500/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-1750/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-2000/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-250/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-2500/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-260/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-270/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-280/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-300/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-3000/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-320/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-350/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-400/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-450/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-500/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-550/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-600/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-650/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-700/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-750/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-800/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-850/nanoHTT_0.root","/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_onlyTauTau_HTT/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-900/nanoHTT_0.root"])

df_initial = ROOT.RDataFrame("Events", inFiles)

global_cfg_file = '/afs/cern.ch/work/v/vdamante/FLAF/config/HH_bbtautau/global.yaml'
global_cfg_dict = {}
with open(global_cfg_file, 'r') as f:
    global_cfg_dict = yaml.safe_load(f)

dfWrapped = PrepareDfForHistograms(DataFrameBuilderForHistograms(df_initial,global_cfg_dict))

for cat in ['boosted','inclusive', 'res2b', 'res1b']:
    df_cat = dfWrapped.df.Filter(f"OS_Iso && {cat}")
    if cat == 'boosted':
        df_cat = df_cat.Define("FatJet_atLeast1BHadron","SelectedFatJet_nBHadrons>0").Filter("SelectedFatJet_p4[FatJet_atLeast1BHadron].size()>0")
    else:
        df_cat = df_cat.Filter("b1_hadronFlavour==5 && b2_hadronFlavour==5 ")
    np_dict_cat = df_cat.AsNumpy(["bb_m_vis","tautau_m_vis"])

    np_array_mass_bb_cat = np_dict_cat['bb_m_vis']
    np_array_mass_tt_cat = np_dict_cat['tautau_m_vis']

    max_bb_mass =np.quantile(np_array_mass_bb_cat, 1-0.005)
    min_bb_mass =np.quantile(np_array_mass_bb_cat, 0.005)

    max_bb_mass_int = math.ceil(max_bb_mass / 10) * 10
    min_bb_mass_int = math.floor(min_bb_mass / 10) * 10

    df_cat_bb = df_cat.Filter(f"bb_m_vis > {min_bb_mass_int} && bb_m_vis < {max_bb_mass_int}")

    np_dict_cat_bb = df_cat_bb.AsNumpy(["tautau_m_vis"])
    np_array_mass_tt_cat_bb = np_dict_cat_bb["tautau_m_vis"]
    max_tt_mass =np.quantile(np_array_mass_tt_cat_bb, 1-0.005)
    min_tt_mass =np.quantile(np_array_mass_tt_cat_bb, 0.005)

    max_tt_mass_int = math.ceil(max_tt_mass / 10) * 10
    min_tt_mass_int = math.floor(min_tt_mass / 10) * 10

    print(f"quantile max (99.5%) for bb mass {cat} =  {max_bb_mass}, {max_bb_mass_int}")
    print(f"quantile min (99.5%) for bb mass {cat} =  {min_bb_mass}, {min_bb_mass_int}")
    print(f"quantile max (99.5%) for tt mass {cat} =  {max_tt_mass}, {max_tt_mass_int}")
    print(f"quantile min (99.5%) for tt mass {cat} =  {min_tt_mass}, {min_tt_mass_int}")


