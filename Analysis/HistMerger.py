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

def CompareYields(histograms, all_samples_list, channel='tauTau', category='res2b', key_name = 'Central',data = 'data'):
    #print(channel, category)
    #print(histograms.keys())
    key_B_data = ((channel, 'OS_AntiIso', category), 'Central')
    key_B = ((channel, 'OS_AntiIso', category), key_name)
    key_C_data = ((channel, 'SS_Iso', category), 'Central')
    key_C = ((channel, 'SS_Iso', category), key_name)
    key_D_data = ((channel, 'SS_AntiIso', category), 'Central')
    key_D = ((channel, 'SS_AntiIso', category), key_name)
    hist_data = histograms[data]
    #print(hist_data.keys())
    hist_data_B = hist_data[key_B_data]
    #if channel != 'tauTau' and category != 'inclusive': return hist_data_B
    hist_data_C = hist_data[key_C_data]
    hist_data_D = hist_data[key_D_data]
    n_data_B = hist_data_B.Integral(0, hist_data_B.GetNbinsX()+1)
    n_data_C = hist_data_C.Integral(0, hist_data_C.GetNbinsX()+1)
    n_data_D = hist_data_D.Integral(0, hist_data_D.GetNbinsX()+1)
    print(f"Yield for data {key_B_data} is {n_data_B}")
    print(f"Yield for data {key_C_data} is {n_data_C}")
    print(f"Yield for data {key_D_data} is {n_data_D}")
    for sample in all_samples_list:
        #print(sample)
        # find kappa value
        hist_sample = histograms[sample]
        #print(histograms[sample].keys())
        hist_sample_B = hist_sample[key_B]
        hist_sample_C = hist_sample[key_C]
        hist_sample_D = hist_sample[key_D]
        n_sample_B = hist_sample_B.Integral(0, hist_sample_B.GetNbinsX()+1)
        n_sample_C = hist_sample_C.Integral(0, hist_sample_C.GetNbinsX()+1)
        n_sample_D = hist_sample_D.Integral(0, hist_sample_D.GetNbinsX()+1)

        print(f"Yield for {sample} {key_B} is {n_sample_B}")
        print(f"Yield for {sample} {key_C} is {n_sample_C}")
        print(f"Yield for {sample} {key_D} is {n_sample_D}")

def QCD_Estimation(histograms, all_samples_list, channel, category, uncName, scale):
    #print(channel, category)
    #print(histograms.keys())
    key_B_data = ((channel, 'OS_AntiIso', category), ('Central', 'Central'))
    key_B = ((channel, 'OS_AntiIso', category), (uncName, scale))
    key_C_data = ((channel, 'SS_Iso', category), ('Central', 'Central'))
    key_C = ((channel, 'SS_Iso', category), (uncName, scale))
    key_D_data = ((channel, 'SS_AntiIso', category), ('Central', 'Central'))
    key_D = ((channel, 'SS_AntiIso', category), (uncName, scale))
    hist_data = histograms['data']
    #print(hist_data.keys())
    hist_data_B = hist_data[key_B_data]
    #if channel != 'tauTau' and category != 'inclusive': return hist_data_B
    hist_data_C = hist_data[key_C_data]
    hist_data_D = hist_data[key_D_data]
    n_data_C = hist_data_C.Integral(0, hist_data_C.GetNbinsX()+1)
    n_data_D = hist_data_D.Integral(0, hist_data_D.GetNbinsX()+1)
    print(f"Yield for data {key_C_data} is {n_data_C}")
    print(f"Yield for data {key_D_data} is {n_data_D}")
    for sample in all_samples_list:
        if sample=='data' :
            print(f"sample {sample} is not considered")
            continue
        #print(sample)
        # find kappa value
        hist_sample = histograms[sample]
        #print(histograms[sample].keys())
        hist_sample_B = hist_sample[key_B]
        hist_sample_C = hist_sample[key_C]
        hist_sample_D = hist_sample[key_D]
        n_sample_C = hist_sample_C.Integral(0, hist_sample_C.GetNbinsX()+1)
        n_data_C-=n_sample_C
        n_sample_D = hist_sample_D.Integral(0, hist_sample_D.GetNbinsX()+1)
        n_data_D-=n_sample_D
        print(f"Yield for data {key_C_data} after removing {sample} with yield {n_sample_C} is {n_data_C}")
        print(f"Yield for data {key_D_data} after removing {sample} with yield {n_sample_D} is {n_data_D}")

        hist_data_B.Add(hist_sample_B, -1)
    if n_data_C <= 0 or n_data_D <= 0:
        print(f"n_data_C = {n_data_C}")
        print(f"n_data_D = {n_data_D}")
    kappa = n_data_C/n_data_D
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

def AddQCDInHistDict(all_histograms, channels, categories, sample_type, uncNameTypes, all_samples_list, scales):
    if 'QCD' not in all_histograms.keys():
            all_histograms['QCD'] = {}
    for channel in channels:
        print(f"adding QCD for channel {channel}")
        for cat in categories:
            print(f".. and category {cat}")
            key =( (channel, 'OS_Iso', cat), ('Central', 'Central'))
            #CompareYields(all_histograms, all_samples_list, channel, cat, uncNameType, 'data')
            print(f".. and uncNameType Central")
            all_histograms['QCD'][key] = QCD_Estimation(all_histograms, all_samples_list, channel, cat, 'Central', 'Central')
            for uncNameType in uncNameTypes:
                print(f".. and uncNameType {uncNameType}")
                for scale in scales:
                    print(f" .. and uncScale {scale}")
                    key =( (channel, 'OS_Iso', cat), (uncNameType, scale))
                    #CompareYields(all_histograms, all_samples_list, channel, cat, uncNameType, 'data')
                    all_histograms['QCD'][key] = QCD_Estimation(all_histograms, all_samples_list, channel, cat, uncNameType, scale)

def CreateNamesDict(histNamesDict, sample_types, uncNames, scales, signals, sample_type_config):
    for sample_key in sample_types.keys():
        if sample_key in sample_cfg_dict.keys():
            sample_type = sample_cfg_dict[sample_key]['sampleType']
            if sample_type in signals:
                sample_key = sample_type
        histNamesDict[sample_key] = (sample_key, 'Central','Central')
        if sample_key == 'data': continue
        for uncName in uncNames:
            for scale in scales:
                histName = f"{sample_key}_{uncName}{scale}"
                histKey = (sample_key, uncName, scale)
                histNamesDict[histName] = histKey


def fillHistDict(inFileRoot, all_histograms, channels, QCDregions, categories, sample_type, uncNameTypes, histNamesDict):
    #print(f"filling hist for {sample_type}")
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
                    sample,uncNameType,scale = histNamesDict[key_name]
                    #print(key_split)
                    key_total = ((channel, qcdRegion, cat), (uncNameType, scale))
                    #print(key_total)
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

def GetUncNameTypes(unc_cfg_dict):
    uncNames = []
    uncNames.extend(list(unc_cfg_dict['norm'].keys()))
    uncNames.extend([unc for unc in unc_cfg_dict['shape']])
    return uncNames


if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--histDir', required=True, type=str)
    parser.add_argument('--hists', required=True, type=str)
    parser.add_argument('--sampleConfig', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    args = parser.parse_args()
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
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
        isSignal = False
        if sample_type in signals:
            isSignal = True
            sample_type=sample
        if isSignal: continue
        if sample_type not in all_samples_types.keys() :
            all_samples_types[sample_type] = []
        all_samples_path.append(os.path.join(args.histDir, sample))
        all_samples_types[sample_type].append(sample)
        if sample_type in all_samples_list: continue
        all_samples_list.append(sample_type)

    histNamesDict = {}
    uncNameTypes = GetUncNameTypes(unc_cfg_dict)
    scales = ['Up','Down']
    CreateNamesDict(histNamesDict, all_samples_types, uncNameTypes, scales, signals, sample_cfg_dict)
    all_vars = args.hists.split(',')
    print(all_samples_list)
    categories = list(sample_cfg_dict['GLOBAL']['categories'])
    QCDregions = list(sample_cfg_dict['GLOBAL']['QCDRegions'])
    channels = list(sample_cfg_dict['GLOBAL']['channelSelection'])
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
            #print(sample_type)
            if sample_type not in all_histograms[var].keys():
                all_histograms[var][sample_type] = {}
            for inFileName in all_files[var][sample_type]:
                inFileRoot = ROOT.TFile.Open(inFileName, "READ")
                if inFileRoot.IsZombie():
                    raise RuntimeError(f"{inFile} is Zombie")
                #print(f"filling hist dict for {inFileName}")
                fillHistDict(inFileRoot, all_histograms[var][sample_type], channels, QCDregions, categories, sample_type, uncNameTypes, histNamesDict)
                inFileRoot.Close()
            MergeHistogramsPerType(all_histograms[var][sample_type])
        # Get QCD estimation:
        print("adding QCD ... ")
        AddQCDInHistDict(all_histograms[var], channels, categories, sample_type, uncNameTypes, all_samples_list, scales)

    print("saving files...")
    for var in all_histograms.keys():
        outFileName = os.path.join(args.histDir, f'all_histograms_{var}.root')
        if os.path.exists(outFileName):
            os.remove(outFileName)
        outFile = ROOT.TFile(outFileName, "RECREATE")
        for sample_type in all_histograms[var].keys():
            for key in all_histograms[var][sample_type]:
                (channel, qcdRegion, cat), uncNameType = key
                new_histName = f'{sample_type}_{channel}_{qcdRegion}_{cat}'
                if uncNameType != 'Central' and sample_type!='data':
                    new_histName+=f'_{uncNameType}'
                hist = all_histograms[var][sample_type][key]
                hist.SetTitle(new_histName)
                hist.SetName(new_histName)
                hist.Write()
        outFile.Close()

