import ROOT
import os
import sys
import yaml
import numpy as np



if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])



if __name__ == "__main__":
    import argparse
    import yaml
    import Common.Utilities as Utilities
    from Analysis.HistHelper import *
    from Analysis.hh_bbtautau import *
    import GetIntervals
    import GetIntervalsSimultaneously
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', required=False, type=str, default='2018')
    parser.add_argument('--cat', required=False, type=str, default='res2b')
    args = parser.parse_args()

    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    ROOT.gInterpreter.Declare(f'#include "include/KinFitNamespace.h"')
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    ROOT.gROOT.ProcessLine('#include "include/AnalysisTools.h"')

    inFiles = Utilities.ListToVector([
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1000/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1250/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1500/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1750/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-2000/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-250/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-2500/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-260/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-270/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-280/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-300/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-3000/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-320/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-350/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-400/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-450/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-500/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-550/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-600/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-650/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-700/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-750/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-800/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-850/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-900/nanoHTT_0.root" , f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1000/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1250/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1500/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1750/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-2000/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-250/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-2500/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-260/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-270/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-280/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-300/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-3000/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-320/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-350/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-400/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-450/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-500/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-550/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-600/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-650/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-700/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-750/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-800/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-850/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-900/nanoHTT_0.root"
    ])


    print("********************************************************************************")
    print(f"************************************* {args.year} *************************************")
    print("********************************************************************************")
    df_initial = ROOT.RDataFrame("Events", inFiles)

    global_cfg_file = '/afs/cern.ch/work/v/vdamante/FLAF/config/HH_bbtautau/global.yaml'
    global_cfg_dict = {}
    with open(global_cfg_file, 'r') as f:
        global_cfg_dict = yaml.safe_load(f)

    dfWrapped = PrepareDfForHistograms(DataFrameBuilderForHistograms(df_initial,global_cfg_dict, f"Run2_{args.year}"))
    particleNet_mass = 'particleNet_mass' if 'SelectedFatJet_particleNet_mass_boosted' in dfWrapped.df.GetColumnNames() else 'particleNetLegacy_mass'

    dfWrapped.df = dfWrapped.df.Define("bb_m_vis_pnet", f"""
                   if (!boosted || !boosted_cat3 || !boosted_inclusive){{
                       return static_cast<float>((b1_p4+b2_p4).M());
                       }}
                    return static_cast<float>(SelectedFatJet_{particleNet_mass}_boosted);""")
    dfWrapped.df = dfWrapped.df.Define("bb_m_vis_boosted_softdrop", f"""
                   if (!boosted || !boosted_cat3 || !boosted_inclusive){{
                       return static_cast<float>((b1_p4+b2_p4).M());
                       }}
                    return static_cast<float>(SelectedFatJet_msoftdrop_boosted);""")
    dfWrapped.df = dfWrapped.df.Define("bb_m_vis_boosted_fjmass", f"""
                   if (!boosted || !boosted_cat3 || !boosted_inclusive){{
                       return static_cast<float>((b1_p4+b2_p4).M());
                       }}
                    return static_cast<float>(SelectedFatJet_mass_boosted);""")

    if args.cat == 'res2b':
        GetIntervalsSimultaneously.GetMassCut(dfWrapped,global_cfg_dict,['eTau','muTau','tauTau'], [ "res2b_cat3", "res2b_cat2",],0.99)
    if args.cat == 'res1b':
        GetIntervalsSimultaneously.GetMassCut(dfWrapped,global_cfg_dict,['eTau','muTau','tauTau'], ["res1b_cat3", "res1b_cat2"],0.99)
    if args.cat == 'inclusive':
        GetIntervalsSimultaneously.GetMassCut(dfWrapped,global_cfg_dict,['eTau','muTau','tauTau'], [ "inclusive", "baseline"],0.99)
    if args.cat == 'boosted':
        GetIntervalsSimultaneously.GetMassCut(dfWrapped,global_cfg_dict,['eTau','muTau','tauTau'], [ "boosted"],0.99)
        #GetIntervalsSimultaneously.GetMassCut(dfWrapped,global_cfg_dict,['eTau','muTau','tauTau'], [ "boosted_cat3", "boosted"],0.99)
