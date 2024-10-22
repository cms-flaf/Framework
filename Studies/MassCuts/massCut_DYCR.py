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
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', required=False, type=str, default='2018')
    args = parser.parse_args()

    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    ROOT.gInterpreter.Declare(f'#include "include/KinFitNamespace.h"')
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    ROOT.gROOT.ProcessLine('#include "include/AnalysisTools.h"')


    print("********************************************************************************")
    print(f"************************************* {args.year} *************************************")
    print("********************************************************************************")
    df_initial = ROOT.RDataFrame("Events", f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/DYJetsToLL_M-50-amcatnloFXFX/*.root")

    global_cfg_file = '/afs/cern.ch/work/v/vdamante/FLAF/config/HH_bbtautau/global.yaml'
    global_cfg_dict = {}
    with open(global_cfg_file, 'r') as f:
        global_cfg_dict = yaml.safe_load(f)
    dfWrapped = PrepareDfForHistograms(DataFrameBuilderForHistograms(df_initial,global_cfg_dict, f"Run2_{args.year}"))
    GetIntervals.GetMassCut(dfWrapped,global_cfg_dict,['eE', 'muMu'], global_cfg_dict['categories'], 0.68)
