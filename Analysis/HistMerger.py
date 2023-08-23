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
        for channel in channels :
            all_histograms[inputVar][channel] = {}
            for qcdRegion in QCDregions:
                all_histograms[inputVar][channel][qcdRegion] = {}
                for cat in categories:
                    all_histograms[inputVar][channel][qcdRegion][cat] = {}
        inputDir_tot = os.path.join(args.inputDir, inputVar)
        #all_inputFiles[inputVar] = [os.path.join(inputDir_tot, f) for f in os.listdir(inputDir_tot)]
        all_inputFiles[inputVar] = os.listdir(inputDir_tot)
        for inFile in all_inputFiles[inputVar]:
            #print(inFile)
            sample_name = inFile.split('.')[0]
            sample_type = sample_cfg_dict[sample_name]['sampleType']
            if 'mass' in sample_cfg_dict[sample_name].keys():
                mass = sample_cfg_dict[sample_name]['mass']
                sample_type+=f'_M-{mass}'
            #print(f"sampleType is {sample_type}")
            all_inFile = os.path.join(inputDir_tot, inFile)
            inFile_root = ROOT.TFile.Open(all_inFile, "READ")
            if inFile_root.IsZombie():
                print(f"{inFile} is Zombie")
                continue
            #print([k.GetName() for k in inFile_root.GetListOfKeys()])
            for channel in channels:
                dir_0 = inFile_root.Get(channel)
                for qcdRegion in QCDregions:
                    dir_1 = dir_0.Get(qcdRegion)
                    #print([k.GetName() for k in dir_1.GetListOfKeys()])
                    for cat in categories:
                        dir_2 = dir_1.Get(cat)
                        #print([k.GetName() for k in dir_2.GetListOfKeys()])
                        for key in dir_2.GetListOfKeys():
                            key_name = key.GetName()
                            obj = key.ReadObj()
                            if obj.IsA().InheritsFrom(ROOT.TH1.Class()):
                                if(sample_type not in all_histograms[inputVar][channel][qcdRegion][cat].keys()):
                                    all_histograms[inputVar][channel][qcdRegion][cat][sample_type] = []
                                all_histograms[inputVar][channel][qcdRegion][cat][sample_type].append(key_name)
            inFile_root.Close()


    # 1 merge histograms per sample:

    print(all_histograms)
