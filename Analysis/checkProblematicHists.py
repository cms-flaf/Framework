import ROOT
import sys
import os
import math
import shutil
from RunKit.sh_tools import sh_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

from Analysis.HistMerger import *

if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--inDir', required=False, type=str, default='')
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--sampleConfig', required=True, type=str)
    args = parser.parse_args()
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")

    hist_cfg_dict = {}
    unc_cfg_dict = {}
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    vars_to_plot = ['bbtautau_mass']
    sample_cfg_dict = {}
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)
    central_histName = ['DY']
    #inFiles = os.listdir(args.inDir)

    categories = list(sample_cfg_dict['GLOBAL']['categories'])
    QCDregions = list(sample_cfg_dict['GLOBAL']['QCDRegions'])
    channels = list(sample_cfg_dict['GLOBAL']['channelSelection'])
    inFiles = ['/eos/home-v/vdamante/HH_bbtautau_resonant_Run2/histograms/Run2_2018/v6_deepTau2p1/DYJetsToLL_0J/tau1_pt.root']
    for file_name in inFiles:
        all_integrals = {}
        inFileName = file_name
        #inFileName = os.path.join(args.inDir, file_name)
        print(f"scanning {inFileName}")
        inFileRoot = ROOT.TFile.Open(inFileName, "READ")
        if inFileRoot.IsZombie():
            raise RuntimeError(f"{inFile} is Zombie")
        for channel in channels:
            dir_0 = inFileRoot.Get(channel)
            for qcdRegion in QCDregions:
                dir_1 = dir_0.Get(qcdRegion)
                for cat in categories:
                    dir_2 = dir_1.Get(cat)
                    for key in dir_2.GetListOfKeys():
                        obj = key.ReadObj()
                        if not obj.IsA().InheritsFrom(ROOT.TH1.Class()): continue
                        obj.SetDirectory(0)
                        key_name = key.GetName()
                        obj_integral = obj.Integral(0, obj.GetNbinsX()+1)
                        #print(key_namex)
                        #sample,uncNameType,scale = histNamesDict[key_name]
                        #print(key_split)
                        key_total = ((channel, qcdRegion, cat), key_name)
                        #print(key_total)
                        #if key_total not in all_integrals.keys():
                        #    all_integrals[key_total] = []
                        all_integrals[key_total]=obj_integral
        inFileRoot.Close()
        #print(all_integrals)

        for key in all_integrals.keys():
            (channel, qcdRegion, cat), key_name = key
            key_central = ((channel, qcdRegion, cat), 'DY')
            if all_integrals[key_central] == 0:
                print(f"for {key_central} the integral is 0")
                break
            difference = abs(all_integrals[key] - all_integrals[key_central])*100/all_integrals[key_central]
            if difference>15:
                print(f"for key {key} \nthe difference is {difference}, \n the central value is {all_integrals[key_central]} \n the shifted value is {all_integrals[key]} ")