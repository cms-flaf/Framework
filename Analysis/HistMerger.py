import ROOT
import sys
import os
import math
import shutil
from RunKit.sh_tools import sh_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *

if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputDir', required=True, type=str)
    parser.add_argument('--outDir', required=False, type=str)
    #parser.add_argument('--test', required=False, type=bool, default=False)
    #parser.add_argument('--nFiles', required=False, type=int, default=-1)
    #parser.add_argument('--deepTauVersion', required=False, type=str, default='v2p1')
    parser.add_argument('--compute_unc_variations', type=bool, default=False)
    args = parser.parse_args()

    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    inputVariables = os.listdir(args.inputDir)
    all_inputFiles = {}
    all_histograms = {}

    sample_cfg_dict = {}
    sample_cfg = "config/samples_Run2_2018.yaml"
    with open(sample_cfg, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)
    for inputVar in inputVariables:
        #print(inputVar)
        all_histograms[inputVar] = {}
        inputDir_tot = os.path.join(args.inputDir, inputVar)
        #all_inputFiles[inputVar] = [os.path.join(inputDir_tot, f) for f in os.listdir(inputDir_tot)]
        all_inputFiles[inputVar] = os.listdir(inputDir_tot)
        k = 0
        for inFile in all_inputFiles[inputVar]:
            sample_name = inFile.split('.')[0]
            if sample_name != 'GluGluToBulkGravitonToHHTo2B2Tau_M-1250' : continue
            print(sample_name)
            if "tmp" in sample_name:
                continue
            sample_type = sample_cfg_dict[sample_name]['sampleType']
            if 'mass' in sample_cfg_dict[sample_name].keys():
                mass = sample_cfg_dict[sample_name]['mass']
                sample_type+=f'_M-{mass}'
            #print(f"sampleType is {sample_type}")
            if sample_type not in all_histograms[inputVar].keys():
                all_histograms[inputVar][sample_type] = {}
            if sample_type == 'QCD' : continue
            all_inFile = os.path.join(inputDir_tot, inFile)
            inFile_root = ROOT.TFile.Open(all_inFile, "READ")
            if inFile_root.IsZombie():
                print(f"{inFile} is Zombie")
                continue
            #print([k.GetName() for k in inFile_root.GetListOfKeys()])
            for channel in channels:
                if channel not in all_histograms[inputVar][sample_type].keys():
                     all_histograms[inputVar][sample_type][channel] = {}
                dir_0 = inFile_root.Get(channel)
                for qcdRegion in QCDregions:
                    if qcdRegion not in all_histograms[inputVar][sample_type][channel].keys():
                        all_histograms[inputVar][sample_type][channel][qcdRegion] = {}
                    dir_1 = dir_0.Get(qcdRegion)
                    #print([k.GetName() for k in dir_1.GetListOfKeys()])
                    for cat in categories:
                        if cat not in all_histograms[inputVar][sample_type][channel][qcdRegion].keys():
                            all_histograms[inputVar][sample_type][channel][qcdRegion][cat] = {}
                        dir_2 = dir_1.Get(cat)
                        #print([k.GetName() for k in dir_2.GetListOfKeys()])
                        for key in dir_2.GetListOfKeys():
                            key_name = key.GetName()
                            #print(key_name)
                            obj = key.ReadObj()
                            if obj.IsA().InheritsFrom(ROOT.TH1.Class()):
                                obj.SetDirectory(0)
                                if key_name not in all_histograms[inputVar][sample_type][channel][qcdRegion][cat].keys():
                                    all_histograms[inputVar][sample_type][channel][qcdRegion][cat][key_name] = []
                                all_histograms[inputVar][sample_type][channel][qcdRegion][cat][key_name].append(obj)
            inFile_root.Close()


    # 1 merge histograms per sample:
    # let's try to fix first the var
    inVar = 'bbtautau_mass'
    all_histograms_inVar = all_histograms[inVar]
    for sample_type in all_histograms_inVar.keys():
        for channel in all_histograms_inVar[sample_type].keys():
            for QCDRegion in all_histograms_inVar[sample_type][channel].keys():
                for category in all_histograms_inVar[sample_type][channel][QCDRegion]:
                    print(sample_type, channel, QCDRegion, category, all_histograms_inVar[sample_type][channel][QCDRegion][category])
                    all_final_hists = []
                    for key_name,histlist in all_histograms_inVar[sample_type][channel][QCDRegion][category].items():
                        final_hist =  histlist[0]
                        if len(histlist) > 1:
                            final_hist = ROOT.TH1D()
                            #print(f'hist has {hist.GetEntries()} entries')
                            final_hist.Add(hist for hist in histlist)
                            #print(f'final hist has {final_hist.GetEntries()} entries')
                        #all_final_hists.append((final_hist))
                        all_histograms_inVar[sample_type][channel][QCDRegion][category][key_name] = final_hist
                    #print(sample_type, channel, QCDRegion, category, all_histograms_inVar[sample_type][channel][QCDRegion][category])
    #print(all_histograms['bbtautau_mass'].keys())

