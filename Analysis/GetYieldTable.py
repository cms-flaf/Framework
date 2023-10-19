import ROOT
import sys
import os
import math
import pandas as pd
import shutil
from RunKit.sh_tools import sh_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *
from Analysis.hh_bbtautau import *
#all_histograms_inputVar[sample_type][channel][QCDRegion][category][key_name]
# region A = OS_Iso
# region B = OS_ANTI ISO
# region C = SS_Iso
# region D = SS_AntiIso

def CreateNamesDict(histNamesDict, sample_types, uncNames, scales, sample_cfg_dict):
    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    for sample_key in sample_types.keys():
        if sample_key in sample_cfg_dict.keys():
            sample_type = sample_cfg_dict[sample_key]['sampleType']
            #if sample_type in signals:
            #    sample_key = sample_type
        histNamesDict[sample_key] = (sample_key, 'Central','Central')
        if sample_key == 'data': continue
        for uncName in uncNames:
            for scale in scales:
                histName = f"{sample_key}_{uncName}{scale}"
                histKey = (sample_key, uncName, scale)
                histNamesDict[histName] = histKey


def CompareYields(histograms, all_samples_list, channel, category, uncName, scale, table):
    #print(channel, category)
    #print(histograms.keys())key_B_data = ((channel, 'OS_AntiIso', category), ('Central', 'Central'))
    key_A_data = ((channel, 'OS_Iso', category), ('Central', 'Central'))
    key_A = ((channel, 'OS_Iso', category), (uncName, scale))
    key_B_data = ((channel, 'OS_AntiIso', category), ('Central', 'Central'))
    key_B = ((channel, 'OS_AntiIso', category), (uncName, scale))
    key_C_data = ((channel, 'SS_Iso', category), ('Central', 'Central'))
    key_C = ((channel, 'SS_Iso', category), (uncName, scale))
    key_D_data = ((channel, 'SS_AntiIso', category), ('Central', 'Central'))
    key_D = ((channel, 'SS_AntiIso', category), (uncName, scale))
    hist_data = histograms['data']
    #print(hist_data.keys())
    hist_data_A = hist_data[key_A_data]
    hist_data_B = hist_data[key_B_data]
    #if channel != 'tauTau' and category != 'inclusive': return hist_data_B
    hist_data_C = hist_data[key_C_data]
    hist_data_D = hist_data[key_D_data]
    n_data_A = hist_data_A.Integral(0, hist_data_A.GetNbinsX()+1)
    n_data_B = hist_data_B.Integral(0, hist_data_B.GetNbinsX()+1)
    n_data_C = hist_data_C.Integral(0, hist_data_C.GetNbinsX()+1)
    n_data_D = hist_data_D.Integral(0, hist_data_D.GetNbinsX()+1)
    if 'sample' not in table.keys():
        table['sample'] = []
    if 'key' not in table.keys():
        table['key'] = []
    if 'yield' not in table.keys():
        table['yield'] = []
    table['sample'].append('data')
    table['key'].append('OS_Iso')
    table['yield'].append(n_data_A)
    table['sample'].append('data')
    table['key'].append('OS_AntiIso')
    table['yield'].append(n_data_B)
    table['sample'].append('data')
    table['key'].append('SS_Iso')
    table['yield'].append(n_data_C)
    table['sample'].append('data')
    table['key'].append('SS_AntiIso')
    table['yield'].append(n_data_D)
    for sample in all_samples_list:
        #print(sample)
        # find kappa value
        hist_sample = histograms[sample]
        #print(histograms[sample].keys())
        hist_sample_A = hist_sample[key_A]
        hist_sample_B = hist_sample[key_B]
        hist_sample_C = hist_sample[key_C]
        hist_sample_D = hist_sample[key_D]
        n_sample_A = hist_sample_A.Integral(0, hist_sample_A.GetNbinsX()+1)
        n_sample_B = hist_sample_B.Integral(0, hist_sample_B.GetNbinsX()+1)
        n_sample_C = hist_sample_C.Integral(0, hist_sample_C.GetNbinsX()+1)
        n_sample_D = hist_sample_D.Integral(0, hist_sample_D.GetNbinsX()+1)
        table['sample'].append(sample)
        table['key'].append('OS_Iso')
        table['yield'].append(n_sample_A)
        table['sample'].append(sample)
        table['key'].append('OS_AntiIso')
        table['yield'].append(n_sample_B)
        table['sample'].append(sample)
        table['key'].append('SS_Iso')
        table['yield'].append(n_sample_C)
        table['sample'].append(sample)
        table['key'].append('SS_AntiIso')
        table['yield'].append(n_sample_D)



def fillHistDict(inFileRoot, all_histograms, channels, QCDregions, categories, sample_type, uncNameTypes, histNamesDict,signals,onlyCentral=False):
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
                    key_name_split = key_name.split('_')
                    if key_name_split[0] in signals:
                        key_name = f"{sample_type}"
                        if len(key_name_split)>1:
                            key_name+="_"
                            key_name += '_'.join(ks for ks in key_name_split[1:])
                    sample,uncNameType,scale = histNamesDict[key_name]
                    key_total = ((channel, qcdRegion, cat), (uncNameType, scale))
                    if onlyCentral and uncNameType!='Central' and scale!='Central' : continue
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
    parser.add_argument('--onlyCentral', required=False, type=bool, default=False)

    args = parser.parse_args()
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)

    all_samples_list,all_samples_types = GetSamplesStuff(sample_cfg_dict,args.histDir,False)
    histNamesDict = {}
    uncNameTypes = GetUncNameTypes(unc_cfg_dict)
    #scales = ['Up','Down']
    CreateNamesDict(histNamesDict, all_samples_types, uncNameTypes, scales, sample_cfg_dict)

    all_vars = args.hists.split(',')
    #print(all_samples_list)
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
            #print(sample_type)
            if sample_type not in all_histograms[var].keys():
                all_histograms[var][sample_type] = {}
            for inFileName in all_files[var][sample_type]:
                if not os.path.exists(inFileName): continue
                inFileRoot = ROOT.TFile.Open(inFileName, "READ")
                if inFileRoot.IsZombie():
                    raise RuntimeError(f"{inFile} is Zombie")
                #print(f"filling hist dict for {inFileName}")
                fillHistDict(inFileRoot, all_histograms[var][sample_type], channels, QCDregions, categories, sample_type, uncNameTypes, histNamesDict, signals,args.onlyCentral)
                inFileRoot.Close()
            MergeHistogramsPerType(all_histograms[var][sample_type])
            #print(all_histograms[var])
        # Get QCD estimation:
        #print("adding QCD ... ")
        if 'QCD' not in all_histograms[var].keys():
            all_histograms[var]['QCD'] = {}
        table = {}
        for channel in channels:
            for cat in categories:
                key =( (channel, 'OS_Iso', cat), ('Central', 'Central'))
                table[(channel,cat)] = {}
                table[(channel,cat)][('Central', 'Central')] = {}
                all_histograms[var]['QCD'][key] = QCD_Estimation(all_histograms[var], all_samples_list, channel, cat, 'Central', 'Central')
                CompareYields(all_histograms[var], all_samples_list, channel, cat, 'Central', 'Central', table[(channel,cat)][('Central', 'Central')])
                excelTable = pd.DataFrame(table[(channel,cat)][('Central', 'Central')])
                excelTable.to_csv(f'output/csv_files/{channel}_{cat}_Central_Central_{var}.csv',index=False,encoding='utf-8-sig', decimal=',', sep=';')
                for uncNameType in uncNameTypes:
                    #print(f".. and uncNameType {uncNameType}")
                    for scale in scales:
                        #print(f" .. and uncScale {scale}")
                        if args.onlyCentral and uncNameType!='Central' and scale!='Central' : continue
                        key =( (channel, 'OS_Iso', cat), (uncNameType, scale))
                        table[(channel,cat)][(uncNameType, scale)] = {}
                        all_histograms[var]['QCD'][key] = QCD_Estimation(all_histograms[var], all_samples_list, channel, cat, uncNameType, scale)
                        CompareYields(all_histograms[var], all_samples_list, channel, cat, uncNameType, scale, table[(channel,cat)][(uncNameType, scale)])
                        excelTable = pd.DataFrame(table[(channel,cat)][(uncNameType, scale)])
                        excelTable.to_csv(f'output/csv_files/{channel}_{cat}_{uncNameType}_{scale}_{var}.csv',index=False,encoding='utf-8-sig', decimal=',', sep=';')

