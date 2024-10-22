

import copy
import datetime
import os
import sys
import ROOT
import shutil
import zlib
import time
import numpy as np

from array import array
import matplotlib.pyplot as plt

ggR_samples = ["GluGluToRadionToHHTo2B2Tau_M-1500", "GluGluToRadionToHHTo2B2Tau_M-550", "GluGluToRadionToHHTo2B2Tau_M-250", "GluGluToRadionToHHTo2B2Tau_M-1750", "GluGluToRadionToHHTo2B2Tau_M-600", "GluGluToRadionToHHTo2B2Tau_M-260", "GluGluToRadionToHHTo2B2Tau_M-2000", "GluGluToRadionToHHTo2B2Tau_M-650", "GluGluToRadionToHHTo2B2Tau_M-2500", "GluGluToRadionToHHTo2B2Tau_M-270", "GluGluToRadionToHHTo2B2Tau_M-700", "GluGluToRadionToHHTo2B2Tau_M-3000", "GluGluToRadionToHHTo2B2Tau_M-750", "GluGluToRadionToHHTo2B2Tau_M-280", "GluGluToRadionToHHTo2B2Tau_M-800", "GluGluToRadionToHHTo2B2Tau_M-300", "GluGluToRadionToHHTo2B2Tau_M-850", "GluGluToRadionToHHTo2B2Tau_M-320", "GluGluToRadionToHHTo2B2Tau_M-900", "GluGluToRadionToHHTo2B2Tau_M-350", "GluGluToRadionToHHTo2B2Tau_M-1000", "GluGluToRadionToHHTo2B2Tau_M-1250", "GluGluToRadionToHHTo2B2Tau_M-400", "GluGluToRadionToHHTo2B2Tau_M-450", "GluGluToRadionToHHTo2B2Tau_M-500"]
ggBG_samples = ["GluGluToBulkGravitonToHHTo2B2Tau_M-800", "GluGluToBulkGravitonToHHTo2B2Tau_M-320", "GluGluToBulkGravitonToHHTo2B2Tau_M-850", "GluGluToBulkGravitonToHHTo2B2Tau_M-900", "GluGluToBulkGravitonToHHTo2B2Tau_M-350", "GluGluToBulkGravitonToHHTo2B2Tau_M-1000", "GluGluToBulkGravitonToHHTo2B2Tau_M-250", "GluGluToBulkGravitonToHHTo2B2Tau_M-260", "GluGluToBulkGravitonToHHTo2B2Tau_M-1250", "GluGluToBulkGravitonToHHTo2B2Tau_M-1500", "GluGluToBulkGravitonToHHTo2B2Tau_M-270", "GluGluToBulkGravitonToHHTo2B2Tau_M-1750", "GluGluToBulkGravitonToHHTo2B2Tau_M-280", "GluGluToBulkGravitonToHHTo2B2Tau_M-2000", "GluGluToBulkGravitonToHHTo2B2Tau_M-300", "GluGluToBulkGravitonToHHTo2B2Tau_M-2500", "GluGluToBulkGravitonToHHTo2B2Tau_M-3000", "GluGluToBulkGravitonToHHTo2B2Tau_M-400", "GluGluToBulkGravitonToHHTo2B2Tau_M-450", "GluGluToBulkGravitonToHHTo2B2Tau_M-500", "GluGluToBulkGravitonToHHTo2B2Tau_M-550", "GluGluToBulkGravitonToHHTo2B2Tau_M-600", "GluGluToBulkGravitonToHHTo2B2Tau_M-650", "GluGluToBulkGravitonToHHTo2B2Tau_M-700", "GluGluToBulkGravitonToHHTo2B2Tau_M-750", ]

if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])


ROOT.EnableThreadSafety()

import Common.Utilities as Utilities
import Common.BaselineSelection as Baseline
from Common.Setup import Setup
import AnaProd.HH_bbtautau.baseline as AnaBaseline

ROOT.gStyle.SetOptStat(0)

if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--period', required=False, type=str, default='Run2_2018')
    parser.add_argument('--sample', required=False, type=str, default='GluGluToRadion')
    parser.add_argument('--deepTauVersion', required=False, type=str, default='v2p1')
    parser.add_argument('--particleFile', type=str,
                        default=f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt")



    args = parser.parse_args()

    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "include/GenTools.h"')
    ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{args.particleFile}\");")
    setup = Setup.getGlobal(os.environ['ANALYSIS_PATH'], args.period)

    #### useful stuff ####

    inDir = f"/eos/cms/store/group/phys_higgs/HLepRare/HTT_skim_v1/{args.period}/"

    ########## dataFrame creation ##########
    sample_list = ggR_samples + ggBG_samples # if args.sample == 'GluGluToRadion' else ggBG_samples
    inFiles = []
    for sample_name in sample_list:
        mass_string = sample_name.split('-')[-1]
        mass_int = int(mass_string)
        #masses.append(mass_int)
        inFile = os.path.join(inDir,f"{sample_name}", "nanoHTT_0.root")
        if not os.path.exists(inFile) :
            print(f"{inFile} does not exist")
            continue
        inFiles.append(inFile)
    #print(inFiles)
    inFiles_vec= Utilities.ListToVector(inFiles)

    Baseline.Initialize()
    df_initial = ROOT.RDataFrame('Events', inFiles_vec)
    #df_initial = df_initial.Define("Muon_B0", f"""
    #    Muon_pt > 15 && abs(Muon_eta) < 2.4 && abs(Muon_dz) < 0.2 && abs(Muon_dxy) < 0.045 && Muon_tightId
    #    """) # && ( ((Muon_tightId || Muon_mediumId) && Muon_pfRelIso04_all < 0.5) || (Muon_highPtId && Muon_tkRelIso < 0.5) )
    #print(df_initial.Count().GetValue())
    df = Baseline.CreateRecoP4(df_initial, 'nano', setup.global_params["nano_version"])
    df = Baseline.SelectRecoP4(df, 'nano', setup.global_params["nano_version"])
    df = Baseline.DefineGenObjects(df,isData=False, isHH=True)
    df = df.Define("genchannelId","static_cast<int>(genHttCandidate->channel())")

    for pfreliso_th in [0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.2, 0.21, 0.22, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29, 0.3, 0.31, 0.32, 0.33, 0.34, 0.35]:
        print(f"using pfreliso th {pfreliso_th}")
        df = AnaBaseline.RecoHttCandidateSelection_ForEfficiency(df, setup.global_params, pfreliso_th)
        df = df.Define("recochannelId","static_cast<int>(HttCandidate.channel())")
        df_den = df.Filter("recochannelId==22 || recochannelId==23")
        print(f"events in reco channels containing a muon are: {df_den.Count().GetValue()}")
        df_num = df_den.Filter("genchannelId==recochannelId").Count().GetValue()
        print(f"events in reco channels containing a muon matching to gen channel are: {df_num.Count().GetValue()}")
        print(f"ratio is {df_num.Count().GetValue()/df_den.Count().GetValue()}")

    #df.Display({"Muon_genMatch", "Tau_genMatch"}).Print()
