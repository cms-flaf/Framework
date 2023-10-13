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
from Analysis.hh_bbtautau import *


def CreateNamesDict(histNamesDict, sample_types, uncName, scales, sample_cfg_dict):
    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    for sample_key in sample_types.keys():
        if sample_key in sample_cfg_dict.keys():
            sample_type = sample_cfg_dict[sample_key]['sampleType']
        histNamesDict[sample_key] = (sample_key, 'Central','Central')
        if sample_key == 'data': continue
        if uncName == 'Central':continue
        for scale in scales:
            histName = f"{sample_key}_{uncName}{scale}"
            histKey = (sample_key, uncName, scale)
            histNamesDict[histName] = histKey



def fillHistDict(inFileRoot, all_histograms, unc_source,channels, QCDregions, categories, sample_type, histNamesDict,signals):
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
                    key_name_split = key_name.split('_')
                    if key_name_split[0] in signals:
                        key_name = f"{sample_type}"
                        if len(key_name_split)>1:
                            key_name+="_"
                            key_name += '_'.join(ks for ks in key_name_split[1:])
                    if key_name not in histNamesDict.keys(): continue
                    sample,uncNameType,scale = histNamesDict[key_name]
                    if sample=='data' and uncNameType!='Central':continue
                    if sample!='data' and uncNameType!=unc_source:continue
                    key_total = ((channel, qcdRegion, cat), (uncNameType, scale))
                    if key_total not in all_histograms.keys():
                        all_histograms[key_total] = []
                    all_histograms[key_total].append(obj)

def MergeHistogramsPerType(all_histograms):
    for key_name,histlist in all_histograms.items():
        final_hist =  histlist[0]
        objsToMerge = ROOT.TList()
        for hist in histlist[1:]:
            objsToMerge.Add(hist)
        final_hist.Merge(objsToMerge)
        all_histograms[key_name] = final_hist


if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--histDir', required=True, type=str)
    parser.add_argument('--hists', required=True, type=str)
    parser.add_argument('--sampleConfig', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--uncSource', required=False, type=str,default='Central')

    args = parser.parse_args()
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)

    all_samples_list,all_samples_types = GetSamplesStuff(sample_cfg_dict,args.histDir)
    histNamesDict = {}
    uncNameTypes = GetUncNameTypes(unc_cfg_dict)
    if args.uncSource != 'Central' and args.uncSource not in uncNameTypes:
        print("unknown unc source {args.uncSource}")
    CreateNamesDict(histNamesDict, all_samples_types, args.uncSource, scales, sample_cfg_dict)
    all_vars = args.hists.split(',')
    categories = list(sample_cfg_dict['GLOBAL']['categories'])
    QCDregions = list(sample_cfg_dict['GLOBAL']['QCDRegions'])
    channels = list(sample_cfg_dict['GLOBAL']['channelSelection'])
    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    all_files = {}
    for var in all_vars:
        fileName =  f"{var}.root"
        if var not in all_files.keys():
            all_files[var] = {}
        for sample_type in all_samples_types.keys():
            samples = all_samples_types[sample_type]
            histDirs = [os.path.join(args.histDir, sample) for sample in samples]
            all_files[var][sample_type] = [os.path.join(hist_dir,fileName) for hist_dir in histDirs]
    # 1. get Histograms
    all_histograms ={}
    for var in all_files.keys():
        if var not in all_histograms.keys():
            all_histograms[var] = {}
        for sample_type in all_files[var].keys():
            if sample_type not in all_histograms[var].keys():
                all_histograms[var][sample_type] = {}
            for inFileName in all_files[var][sample_type]:
                if not os.path.exists(inFileName): continue
                inFileRoot = ROOT.TFile.Open(inFileName, "READ")
                if inFileRoot.IsZombie():
                    raise RuntimeError(f"{inFile} is Zombie")
                fillHistDict(inFileRoot, all_histograms[var][sample_type],args.uncSource, channels, QCDregions, categories, sample_type, histNamesDict, signals)
                inFileRoot.Close()
            MergeHistogramsPerType(all_histograms[var][sample_type])
        AddQCDInHistDict(all_histograms[var], channels, categories, sample_type, args.uncSource, all_samples_list, scales)
    for var in all_histograms.keys():
        outDir = os.path.join(args.histDir,'all_histograms',var)
        if not os.path.exists(outDir):
            os.makedirs(outDir)
        outFileName = os.path.join(outDir, f'all_histograms_{var}_{args.uncSource}.root')
        if os.path.exists(outFileName):
            os.remove(outFileName)
        outFile = ROOT.TFile(outFileName, "RECREATE")
        for sample_type in all_histograms[var].keys():
            for key in all_histograms[var][sample_type]:
                (channel, qcdRegion, cat), (uncNameType, uncScale) = key

                if qcdRegion != 'OS_Iso': continue
                dirStruct = (channel, cat)
                dir_name = '/'.join(dirStruct)
                dir_ptr = mkdir(outFile,dir_name)
                hist = all_histograms[var][sample_type][key]

                hist_name =  sample_type
                if uncNameType!=args.uncSource: continue
                if uncNameType != 'Central':
                    if sample_type == 'data' : continue
                    if uncScale == 'Central': continue
                    hist_name+=f"_{uncNameType}{uncScale}"
                else:
                    if uncScale!='Central':continue
                hist.SetTitle(hist_name)
                hist.SetName(hist_name)
                dir_ptr.WriteTObject(hist, hist_name, "Overwrite")
        outFile.Close()
