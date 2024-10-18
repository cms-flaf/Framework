import ROOT
import sys
import os
import math
import shutil
import time
from RunKit.run_tools import ps_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *

#Import correct analysis
#from Analysis.hh_bbtautau import *
import Analysis.hh_bbww as analysis

def checkLists(list1, list2):
    if len(list1) != len(list2):
        print(f"lists have different length: {list1} and {list2}")
        return False
    for item in list1:
        if item not in list2:
            print(f"{item} in {list1} but not in {list2}")
            return False
    return True

def checkFile(inFileRoot, channels, qcdRegions, categories, var):
    keys_channel = [str(key.GetName()) for key in inFileRoot.GetListOfKeys()]
    if not (checkLists(keys_channel, channels)):
        print("check list not worked for channels")
        return False
    for channel in channels:
        dir_0 = inFileRoot.Get(channel)
        keys_qcdRegions = [str(key.GetName()) for key in dir_0.GetListOfKeys()]
        if not checkLists(keys_qcdRegions, QCDregions):
            print("check list not worked for QCDregions")
            return False
        for qcdRegion in QCDregions:
            dir_1 = dir_0.Get(qcdRegion)
            keys_categories = [str(key.GetName()) for key in dir_1.GetListOfKeys()]
            if 'boosted' in categories and ( var.startswith('b1') or var.startswith('b2') ): categories.remove('boosted')
            if not checkLists(keys_categories, categories):
                    print("check list not worked for categories")
                    return False
            for cat in categories:
                if cat == 'boosted' and (var.startswith('b1') or var.startswith('b2')): continue
                if cat != 'boosted' and var.startswith('SelectedFatJet'): continue
                dir_2 = dir_1.Get(cat)
                keys_histograms = [str(key.GetName()) for key in dir_2.GetListOfKeys()]
                if not keys_histograms: return False
    return True


def getHistDict(var, all_histograms, inFileRoot,channels, QCDregions, categories, uncSource,sample_name,sample_types_to_merge):
    for channel in channels:
        dir_0 = inFileRoot.Get(channel)
        #print(dir_0.GetListOfKeys())
        for qcdRegion in QCDregions:
            dir_1 = dir_0.Get(qcdRegion)
            #print(dir_1.GetListOfKeys())
            for cat in categories:
                if cat == 'boosted' and (var.startswith('b1') or var.startswith('b2')): continue
                if cat != 'boosted' and var.startswith('SelectedFatJet'): continue
                if cat == 'boosted' and uncSource in global_cfg_dict['unc_to_not_consider_boosted']: continue
                #print(cat, var)
                dir_2 = dir_1.Get(cat)
                for key in dir_2.GetListOfKeys():
                    obj = key.ReadObj()
                    key_name = key.GetName()
                    key_name_split = key_name.split('_')
                    obj.SetDirectory(0)
                    if not obj.IsA().InheritsFrom(ROOT.TH1.Class()): continue

                    sample_type = key_name_split[0]
                    name_to_use = sample_name
                    if sample_type in sample_types_to_merge:
                        name_to_use = sample_type
                    if name_to_use not in all_histograms.keys():
                        all_histograms[name_to_use] = {}
                    key_total = ((channel, qcdRegion, cat), ('Central', 'Central'))
                    if uncSource=='Central':
                        key_total = ((channel, qcdRegion, cat), ('Central', 'Central'))
                        if len(key_name_split)>1:continue
                        if key_total not in all_histograms[name_to_use].keys():
                            all_histograms[name_to_use][key_total] = []
                        all_histograms[name_to_use][key_total].append(obj)
                    elif uncSource == 'QCDScale':
                        for scale in ['Up','Down']:
                            key_total = ((channel, qcdRegion, cat), ('QCDScale', scale))
                            if len(key_name_split)>1:continue
                            if key_total not in all_histograms[name_to_use].keys():
                                all_histograms[name_to_use][key_total] = []
                            all_histograms[name_to_use][key_total].append(obj)
                    else:
                        uncName_scale ='' if name_to_use == 'data' else '_'.join(n for n in key_name_split[1:])
                        for scale in ['Up','Down']:
                            if name_to_use != 'data' and uncSource+scale != uncName_scale: continue
                            key_total = ((channel, qcdRegion, cat), (uncSource, scale))
                            if key_total not in all_histograms[name_to_use].keys():
                                all_histograms[name_to_use][key_total] = []
                            all_histograms[name_to_use][key_total].append(obj)

def MergeHistogramsPerType(all_histograms):
    for sample_type in all_histograms.keys():
        if sample_type == 'data': print(f"DURING MERGE HISTOGRAMS, sample_type is {sample_type}")
        for key_name,histlist in all_histograms[sample_type].items():
            final_hist =  histlist[0]
            objsToMerge = ROOT.TList()
            for hist in histlist[1:]:
                objsToMerge.Add(hist)
            final_hist.Merge(objsToMerge)
            all_histograms[sample_type][key_name] = final_hist
            #if len(histlist)!=1:
                #print(f"for {sample_type} the lenght of histlist is {len(histlist)}")



def GetBTagWeightDict(var, all_histograms):
    all_histograms_1D = {}
    for sample_type in all_histograms.keys():
        #print(sample_type)
        all_histograms_1D[sample_type] = {}
        for key_name,histogram in all_histograms[sample_type].items():
            (key_1, key_2) = key_name
            ch, reg, cat = key_1
            uncName,scale = key_2
            key_tuple_num = ((ch, reg, 'btag_shape'), key_2)
            #Why is this hard coded? What is 'btag_shape' here?
            key_tuple_num = ((ch, reg, 'inclusive'), key_2)
            key_tuple_den = ((ch, reg, 'inclusive'), key_2)
            ratio_num_hist = all_histograms[sample_type][key_tuple_num] if key_tuple_num in all_histograms[sample_type].keys() else None
            ratio_den_hist = all_histograms[sample_type][key_tuple_den] if key_tuple_den in all_histograms[sample_type].keys() else None
            
            num = ratio_num_hist.Integral(0,ratio_num_hist.GetNbinsX()+1)
            den = ratio_den_hist.Integral(0,ratio_den_hist.GetNbinsX()+1)
            ratio = 0.
            if ratio_den_hist.Integral(0,ratio_den_hist.GetNbinsX()+1) != 0 :
                ratio = ratio_num_hist.Integral(0,ratio_num_hist.GetNbinsX()+1)/ratio_den_hist.Integral(0,ratio_den_hist.GetNbinsX()+1)
            #if ratio == 0 and hist1D.Integral(0, hist1D.GetNbinsX()+1) ==0 :
            #    continue
            histogram.Scale(ratio)
            all_histograms_1D[sample_type][key_name] = histogram
    return all_histograms_1D


if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('inputFile', nargs='+', type=str)
    #parser.add_argument('datasetFile', nargs='+', type=str)
    parser.add_argument('--outFile', required=True, type=str)
    parser.add_argument('--year', required=True, type=str)
    parser.add_argument('--datasetFile', required=True, type=str)
    parser.add_argument('--var', required=True, type=str)
    parser.add_argument('--sampleConfig', required=True, type=str)
    parser.add_argument('--globalConfig', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--uncSource', required=False, type=str,default='Central')

    args = parser.parse_args()
    startTime = time.time()
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)
    with open(args.globalConfig, 'r') as f:
        global_cfg_dict = yaml.safe_load(f)

    all_samples_list = args.datasetFile.split(',')
    all_samples_types = {}
    uncNameTypes = GetUncNameTypes(unc_cfg_dict)

    if args.uncSource != 'Central' and args.uncSource not in uncNameTypes:
        print("unknown unc source {args.uncSource}")

    categories = list(global_cfg_dict['categories'])
    QCDregions = list(global_cfg_dict['QCDRegions'])
    channels = list(global_cfg_dict['channels_to_consider'])
    signals = list(global_cfg_dict['signal_types'])
    unc_to_not_consider_boosted = list(global_cfg_dict['unc_to_not_consider_boosted'])
    sample_types_to_merge = list(global_cfg_dict['sample_types_to_merge'])
    scales = list(global_cfg_dict['scales'])
    files_separated = {}
    all_histograms ={}
    all_infiles = [ fileName for fileName in args.inputFile ]
    if len(all_infiles) != len(all_samples_list):
        raise RuntimeError(f"all_infiles have len {len(all_infiles)} and all_samples_list have len {len(all_samples_list)}")
    ignore_samples = []
    for (inFileName, sample_name) in zip(all_infiles, all_samples_list):
        if not os.path.exists(inFileName):
            print(f"{inFileName} does not exist")
            continue
            #raise RuntimeError(f"{inFileName} removed")
        #print(sample_name)

        #Sometimes we do not want to stack all samples (signal)
        if 'samples_to_skip_hist' in global_cfg_dict.keys():
            #Add this key check to avoid breaking bbtautau
            if sample_name in global_cfg_dict['samples_to_skip_hist']:
                continue

        inFileRoot = ROOT.TFile.Open(inFileName, "READ")
        if inFileRoot.IsZombie():
            inFileRoot.Close()
            os.remove(inFileName)
            ignore_samples.append(sample_name)
            raise RuntimeError(f"{inFileName} is Zombie")
        if  not checkFile(inFileRoot, channels, QCDregions, categories,args.var):
            print(f"{sample_name} has void file")
            ignore_samples.append(sample_name)
            inFileRoot.Close()
            continue
        getHistDict(args.var,all_histograms, inFileRoot,channels, QCDregions, categories, args.uncSource,sample_name,sample_types_to_merge)
        #print(all_histograms)
        if sample_name == 'data':
            all_samples_types['data'] = ['data']
        else:
            sample_type=sample_cfg_dict[sample_name]['sampleType']
            sample_key = sample_type if sample_type in sample_types_to_merge else sample_name
            if sample_name not in ignore_samples:
                if sample_key not in all_samples_types.keys(): all_samples_types[sample_key] = []
                all_samples_types[sample_key].append(sample_name)
        inFileRoot.Close()
    MergeHistogramsPerType(all_histograms)
    all_histograms_1D=GetBTagWeightDict(args.var,all_histograms)
    print(all_histograms_1D)

    fixNegativeContributions = False
    if args.var != 'kinFit_m':
        fixNegativeContributions=True
    #if args.var == 'kinFit_m':
        #fixNegativeContributions=FatJetObservables
    #    fixNegativeContributions=False
    analysis.AddQCDInHistDict(args.var,all_histograms_1D, channels, categories, args.uncSource, all_samples_types.keys(), scales,unc_to_not_consider_boosted,fixNegativeContributions)


    outFile = ROOT.TFile(args.outFile, "RECREATE")

    for sample_type in all_histograms_1D.keys():

        for key in all_histograms_1D[sample_type]:
            (channel, qcdRegion, cat), (uncNameType, uncScale) = key
            #if qcdRegion != 'OS_Iso': continue
            dirStruct = (channel,qcdRegion, cat)
            dir_name = '/'.join(dirStruct)
            dir_ptr = Utilities.mkdir(outFile,dir_name)
            hist = all_histograms_1D[sample_type][key]
            #print(sample_type, key, hist.GetEntries())
            hist_name =  sample_type
            if uncNameType!=args.uncSource: continue
            if uncNameType != 'Central':
                if sample_type == 'data' : continue
                if uncScale == 'Central': continue
                hist_name+=f"_{uncNameType}_{uncScale}"
            else:
                if uncScale!='Central':continue
            hist.SetTitle(hist_name)
            hist.SetName(hist_name)
            dir_ptr.WriteTObject(hist, hist_name, "Overwrite")
    outFile.Close()
    executionTime = (time.time() - startTime)

    print('Execution time in seconds: ' + str(executionTime))
