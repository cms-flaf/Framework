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
#all_histograms_inputVar[sample_type][channel][QCDRegion][category][key_name]
# region A = OS_Iso
# region B = OS_ANTI ISO
# region C = SS_Iso
# region D = SS_AntiIso

def QCD_Estimation(histograms, all_samples_list, channel='tauTau', category='res2b', key_name = 'Central',data = 'data'):
    print(channel, category)
    hist_data = histograms[data][channel]
    hist_data_B = hist_data['OS_AntiIso'][category][key_name]
    #if channel != 'tauTau' and category != 'inclusive': return hist_data_B
    hist_data_C = hist_data['SS_Iso'][category][key_name]
    hist_data_D = hist_data['SS_AntiIso'][category][key_name]
    n_data_C = hist_data_C.Integral(0, hist_data_C.GetNbinsX()+1)
    n_data_D = hist_data_D.Integral(0, hist_data_D.GetNbinsX()+1)
    for sample in all_samples_list:
        if sample==data or "Radion" in sample or "Graviton" in sample or "node" in sample:
            #print(f"sample {sample} is not considered")
            continue
        #print(sample)
        # find kappa value
        hist_sample = histograms[sample][channel]
        hist_sample_B = hist_sample['OS_AntiIso'][category][key_name]
        hist_sample_C = hist_sample['SS_Iso'][category][key_name]
        hist_sample_D = hist_sample['SS_AntiIso'][category][key_name]
        n_sample_C = hist_sample_C.Integral(0, hist_sample_C.GetNbinsX()+1)
        n_data_C-=n_sample_C
        n_sample_D = hist_sample_D.Integral(0, hist_sample_D.GetNbinsX()+1)
        n_data_D-=n_sample_D
        hist_data_B.Add(hist_sample_B, -1)
    kappa = n_data_C/n_data_D
    if n_data_C <= 0 or n_data_D <= 0:
        print(f"n_data_C = {n_data_C}")
        print(f"n_data_D = {n_data_D}")
    if kappa<0:
        print(f"transfer factor <0")
        return ROOT.TH1D()
        #raise  RuntimeError(f"transfer factor <=0 ! {kappa}")
    hist_data_B.Scale(kappa)
    fix_negative_contributions,debug_info,negative_bins_info = FixNegativeContributions(hist_data_B)
    if not fix_negative_contributions:
        #return hist_data_B
        print(debug_info)
        print(negative_bins_info)
        print("Unable to estimate QCD")

        return ROOT.TH1D()
        #raise RuntimeError("Unable to estimate QCD")
    return hist_data_B

def AddQCDInHistDict(all_histograms, channels, categories, sample_type, uncNameTypes, all_samples_list):
    if 'QCD' not in all_histograms.keys():
            all_histograms[var]['QCD'] = {}
    for channel in channels:
        for cat in categories:
            for uncNameType in uncNameTypes:
                key =( (channel, 'OS_Iso', cat), uncNameType)
                all_histograms['QCD'][key] = QCD_Estimation(histograms, all_samples_list, channel, cat, uncNameType, 'data')

def fillHistDict(inFileRoot, all_histograms, channels, QCDregions, categories, sample_type, uncNameTypes):
    for channel in channels:
        dir_0 = inFile_root.Get(channel)
        for qcdRegion in QCDregions:
            dir_1 = dir_0.Get(qcdRegion)
            for cat in categories:
                dir_2 = dir_1.Get(cat)
                for key in dir_2.GetListOfKeys():
                    obj = key.ReadObj()
                    if not obj.IsA().InheritsFrom(ROOT.TH1.Class()): continue
                    obj.SetDirectory(0)
                    key_name = key.GetName()
                    key_split = key_name.split('-')[-1].split('_')
                    uncNameType = key_split[-1]
                    if uncNameType == sample_type:
                        uncNameType = 'Central'
                    key_total = ((channel, qcdRegion, cat), uncNameType)
                    if uncNameType not in uncNameTypes:
                        uncNameTypes.append(uncNameType)
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

def fillHistDict(inFileRoot, all_histograms, channels, QCDregions,categories ):
    for channel in channels:
        dir_0 = inFile_root.Get(channel)
        for qcdRegion in QCDregions:
            dir_1 = dir_0.Get(qcdRegion)
            for cat in categories:
                dir_2 = dir_1.Get(cat)
                for key in dir_2.GetListOfKeys():
                    obj = key.ReadObj()
                    if not obj.IsA().InheritsFrom(ROOT.TH1.Class()): continue
                    obj.SetDirectory(0)
                    key_name = key.GetName()
                    key_total = ((channel, qcdRegion, cat), (key_name))
                    if key_total not in all_histograms.keys():
                        all_histograms[key_total] = []
                    all_histograms[key_total].append(obj)

if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--histDir', required=True, type=str)
    parser.add_argument('--hists', required=True, type=str)
    parser.add_argument('--sampleConfig', required=True, type=str)

    args = parser.parse_args()
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)
    #print(sample_cfg_dict.keys())
    sample_to_not_consider = ['GLOBAL']
    all_samples_list = []
    all_samples_path = []
    all_samples_types = {'data':['data'],}
    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    for sample in sample_cfg_dict.keys():
        if not os.path.isdir(os.path.join(args.histDir, sample)): continue
        sample_type = sample_cfg_dict[sample]['sampleType']
        if sample_type in signals:
            sample_type = 'signal'
        if sample_type not in all_samples_types :
            all_samples_types[sample_type] = []
        all_samples_types[sample_type].append(sample)
        all_samples_path.append(os.path.join(args.histDir, sample))
        if sample_type !='signal':
            all_samples_list.append(sample)
    #print(all_samples)
    all_vars = args.hists.split(',')
    all_categories = list(sample_cfg_dict['GLOBAL']['categories'])
    all_QCDRegions = list(sample_cfg_dict['GLOBAL']['QCDRegions'])
    all_channels = list(sample_cfg_dict['GLOBAL']['channelSelection'])
    uncNameTypes = []
    all_files = {}
    for var in all_vars:
        fileName =  f"{var}.root"
        if var not in all_files.keys():
            all_files[var] = {}
        for sample_type in all_samples_types.keys():
            samples = all_samples_types[sample_type]
            histDirs = [os.path.join(args.histDir, sample) for sample in samples]
            all_files[var][sample_type] = [os.path.join(hist_dir,fileName) for hist_dir in histDirs]

    #print(all_files)
    # 1. get Histograms
    all_histograms ={}

    for var in all_files.keys():
        if var not in all_histograms.keys():
            all_histograms[var] = {}
        for sample_type in all_files[var].keys():
            if sample_type not in all_histograms[var].keys():
                all_histograms[var][sample_type] = {}
            for inFileName in all_files[var][sample_type]:
                inFileRoot = ROOT.TFile.Open(inFileName, "READ")
                if inFileRoot.IsZombie():
                    raise RuntimeError(f"{inFile} is Zombie")
                fillHistDict(inFileRoot, all_histograms[var][sample_type], channels, QCDregions,categories )
                inFileRoot.Close()
            MergeHistogramsPerType(all_histograms[var][sample_type])
        # Get QCD estimation:
        AddQCDInHistDict(all_histograms[var], channels, categories, sample_type, uncNameTypes, all_samples_list)


    for var in all_histograms.keys():
        outFileName = os.path.join(args.histDir, f'all_histograms_{var}.root')
        if os.path.exists(outFileName):
            os.remove(outFileName)
        outFile = ROOT.TFile(outFileName, "RECREATE")
        for sample_type in all_histograms[var].keys():
            for key in all_histograms[sample]:
                (channel, qcdRegion, cat), uncNameType = key
                new_histName = f'{sample_type}_{channel}_{QCDRegion}_{cat}'
                if uncNameType != 'Central':
                    new_histName+=f'_{uncNameType}'
                hist.SetTitle(new_histName)
                hist.SetName(new_histName)
                hist.Write()
        outFile.Close()