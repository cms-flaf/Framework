import ROOT
import sys
import os
import math
import shutil
import json
import time
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



def fillHistDict(var, inFileRoot, all_histograms, unc_source,channels, QCDregions, categories, sample_type, histNamesDict,signals):
    #print(sample_type)
    for channel in channels:
        dir_0 = inFileRoot.Get(channel)
        #print(dir_0.GetListOfKeys())
        for qcdRegion in QCDregions:
            dir_1 = dir_0.Get(qcdRegion)
            #print(dir_1.GetListOfKeys())
            for cat in categories:
                if cat == 'boosted' and var in bjet_vars: continue
                if cat != 'boosted' and var in var_to_add_boosted: continue
                #print(cat, var)
                dir_2 = dir_1.Get(cat)
                #print(dir_2.GetListOfKeys())
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
                    if cat == 'boosted' and uncNameType in unc_to_not_consider_boosted: continue
                    if sample=='data' and uncNameType!='Central':continue
                    if sample!='data' and uncNameType!=unc_source:continue
                    key_total = ((channel, qcdRegion, cat), (uncNameType, scale))
                    if key_total not in all_histograms.keys():
                        all_histograms[key_total] = []
                    all_histograms[key_total].append(obj)

def MergeHistogramsPerType(all_histograms):
    for key_name,histlist in all_histograms.items():
        #print(key_name, histlist)
        final_hist =  histlist[0]
        objsToMerge = ROOT.TList()
        for hist in histlist[1:]:
            objsToMerge.Add(hist)
        final_hist.Merge(objsToMerge)
        all_histograms[key_name] = final_hist

def GetBTagWeightDict(var, all_histograms, sample_name, final_json_dict):
    all_histograms_1D = {}
    for key_name,histogram in all_histograms.items():
        (key_1, key_2) = key_name
        ch, reg, cat = key_1
        uncName,scale = key_2
        key_tuple_num = ((ch, reg, 'btag_shape'), key_2)
        key_tuple_den = ((ch, reg, 'inclusive'), key_2)
        ratio_num_hist = all_histograms[key_tuple_num] if key_tuple_num in all_histograms.keys() else None
        #print(key_name)
        ratio_den_hist = all_histograms[key_tuple_den] if key_tuple_den in all_histograms.keys() else None
        ##print(type(ratio_num_hist))
        #print(type(ratio_den_hist))
        histlist =[]
        if sample_name not in final_json_dict.keys():
            final_json_dict[sample_name]={}
        second_key = f'{ch}_{reg}_{cat}_{uncName}{scale}'
        if sample_name not in final_json_dict[sample_name].keys():
            final_json_dict[sample_name][second_key]={}
        if 'yBin' not in final_json_dict[sample_name][second_key].keys():
            final_json_dict[sample_name][second_key]['yBin']=[]
        if 'nJets' not in final_json_dict[sample_name][second_key].keys():
            final_json_dict[sample_name][second_key]['nJets']=[]
        if 'num' not in final_json_dict[sample_name][second_key].keys():
            final_json_dict[sample_name][second_key]['num']=[]
        if 'den' not in final_json_dict[sample_name][second_key].keys():
            final_json_dict[sample_name][second_key]['den']=[]
        if 'ratio' not in final_json_dict[sample_name][second_key].keys():
            final_json_dict[sample_name][second_key]['ratio']=[]
        for yBin in range(0, histogram.GetNbinsY()):
            histName = f"{sample_name}_{ch}_{reg}_{cat}_{uncName}{scale}"
            hist1D = histogram.ProjectionX(f"{histName}_pfx_{yBin}", yBin, yBin)
            if sample_type != "data" and cat not in ['boosted','inclusive'] and var not in var_to_add_boosted:
                if (ratio_num_hist.GetYaxis().GetBinCenter(yBin) != ratio_den_hist.GetYaxis().GetBinCenter(yBin)):
                    print(f"bin centers are different, for num it's: {ratio_num_hist.GetYaxis().GetBinCenter(yBin)} and for den it's {ratio_den_hist.GetYaxis().GetBinCenter(yBin)}")

                histName_num = f"{sample_name}_{ch}_{reg}_{cat}_{uncName}{scale}_num"
                hist1DProjection_num = ratio_num_hist.ProjectionX(f"{histName_num}_pfx", yBin, yBin)
                histName_den = f"{sample_name}_{ch}_{reg}_{cat}_{uncName}{scale}_den"
                hist1DProjection_den = ratio_den_hist.ProjectionX(f"{histName_den}_pfx", yBin, yBin)

                num = hist1DProjection_num.Integral(0,hist1DProjection_num.GetNbinsX()+1)
                den = hist1DProjection_den.Integral(0,hist1DProjection_den.GetNbinsX()+1)
                ratio = 0.
                if hist1DProjection_den.Integral(0,ratio_den_hist.GetNbinsX()+1) != 0 :
                    ratio = hist1DProjection_num.Integral(0,ratio_num_hist.GetNbinsX()+1)/hist1DProjection_den.Integral(0,ratio_den_hist.GetNbinsX()+1)
                final_json_dict[sample_name][second_key]['yBin'].append(yBin)
                final_json_dict[sample_name][second_key]['nJets'].append(ratio_num_hist.GetYaxis().GetBinCenter(yBin))
                final_json_dict[sample_name][second_key]['num'].append(num)
                final_json_dict[sample_name][second_key]['den'].append(den)
                final_json_dict[sample_name][second_key]['ratio'].append(ratio)
                if ratio == 0 and hist1D.Integral(0, hist1D.GetNbinsX()+1) ==0 :
                    continue
                hist1D.Scale(ratio)
            histlist.append(hist1D)
        if not histlist:
            all_histograms_1D[key_name]= hist1D
        else:
            final_hist =  histlist[0]
            objsToMerge = ROOT.TList()
            for hist in histlist[1:]:
                objsToMerge.Add(hist)
            final_hist.Merge(objsToMerge)
            all_histograms_1D[key_name] = final_hist
    return all_histograms_1D


def ApplyBTagWeight(all_histograms):
    for key_name,histogram in all_histograms.items():
        (key_1, key_2) = key_name
        ch, reg, cat = key_1
        uncName,scale = key_2
        key_tuple_num = ((ch, reg, 'btag_shape'), key_2)
        key_tuple_den = ((ch, reg, 'inclusive'), key_2)
        ratio_num_hist = all_histograms[key_tuple_num]
        ratio_den_hist = all_histograms[key_tuple_den]
        ratio = ratio_num_hist.Integral(0,ratio_num_hist.GetNbinsX()+1)/ratio_den_hist.Integral(0,ratio_den_hist.GetNbinsX()+1)
        histogram.Scale(ratio)
        all_histograms[key_name] = histogram

if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--histDir', required=True, type=str)
    parser.add_argument('--jsonDir', required=True, type=str)
    parser.add_argument('--suffix', required=False, type=str, default='')
    parser.add_argument('--var', required=True, type=str)
    parser.add_argument('--sampleConfig', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--uncSource', required=False, type=str,default='Central')
    parser.add_argument('--wantBTag', required=False, type=bool, default=False)


    args = parser.parse_args()
    startTime = time.time()
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)

    all_samples_list,all_samples_types = GetSamplesStuff(sample_cfg_dict,args.histDir, True, False)
    histNamesDict = {}
    uncNameTypes = GetUncNameTypes(unc_cfg_dict)
    btag_dir= "bTag_WP" if args.wantBTag else "bTag_shape"
    if args.uncSource != 'Central' and args.uncSource not in uncNameTypes:
        print("unknown unc source {args.uncSource}")
    CreateNamesDict(histNamesDict, all_samples_types, args.uncSource, scales, sample_cfg_dict)

    categories = list(sample_cfg_dict['GLOBAL']['categories'])
    QCDregions = list(sample_cfg_dict['GLOBAL']['QCDRegions'])
    channels = list(sample_cfg_dict['GLOBAL']['channelSelection'])
    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    all_files = {}
    all_histograms ={}
    all_histograms_1D ={}
    final_json_dict = {}

    fileName =  f"{args.var}2D{args.suffix}.root"
    for sample_type in all_samples_types.keys():
        samples = all_samples_types[sample_type]
        histDirs = [os.path.join(args.histDir, sample,btag_dir) for sample in samples]
        all_files[sample_type] = [os.path.join(hist_dir,fileName) for hist_dir in histDirs]
        # 1. get Histograms
        if sample_type not in all_histograms.keys():
            all_histograms[sample_type] = {}
            all_histograms_1D[sample_type] = {}
        for inFileName in all_files[sample_type]:
            if 'VBFToBulkGraviton' in inFileName: continue
            if 'VBFToRadion' in inFileName: continue
            #print(inFileName)
            if not os.path.exists(inFileName):
                print(f"{inFileName} removed")
                continue
            inFileRoot = ROOT.TFile.Open(inFileName, "READ")
            if inFileRoot.IsZombie():
                raise RuntimeError(f"{inFile} is Zombie")
            fillHistDict(args.var, inFileRoot, all_histograms[sample_type],args.uncSource, channels, QCDregions, categories, sample_type, histNamesDict, signals)
            inFileRoot.Close()
        MergeHistogramsPerType(all_histograms[sample_type])
        all_histograms_1D[sample_type]=GetBTagWeightDict(args.var,all_histograms[sample_type],sample_type,final_json_dict)
    #print(all_histograms_1D)
    AddQCDInHistDict(args.var,all_histograms_1D, channels, categories, sample_type, args.uncSource, all_samples_list, scales)
    if not os.path.exists(args.jsonDir):
        os.makedirs(args.jsonDir)
    json_file = os.path.join(args.jsonDir, f'all_ratios_{args.var}_{args.uncSource}{args.suffix}.json')
    with open(f"{json_file}", "w") as write_file:
        json.dump(final_json_dict, write_file, indent=4)

    outDir = os.path.join(args.histDir,'all_histograms',args.var, btag_dir)
    if not os.path.exists(outDir):
        os.makedirs(outDir)
    outFileName = os.path.join(outDir, f'all_histograms_{args.var}_{args.uncSource}{args.suffix}.root')
    if os.path.exists(outFileName):
        os.remove(outFileName)
    outFile = ROOT.TFile(outFileName, "RECREATE")
    for sample_type in all_histograms_1D.keys():
        for key in all_histograms_1D[sample_type]:
            (channel, qcdRegion, cat), (uncNameType, uncScale) = key

            if qcdRegion != 'OS_Iso': continue
            dirStruct = (channel, cat)
            dir_name = '/'.join(dirStruct)
            dir_ptr = mkdir(outFile,dir_name)
            hist = all_histograms_1D[sample_type][key]
            #print(sample_type, key, hist.GetEntries())
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
    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))