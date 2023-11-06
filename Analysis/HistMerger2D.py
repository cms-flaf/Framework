import ROOT
import sys
import os
import math
import shutil
import json
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

def GetBTagWeightDict(all_histograms, sample_name, final_json_dict):
    all_histograms_1D = {}
    #print()
    #if sample_name=='data':
        #print(sample_name)
    print(all_histograms.keys())
    for key_name,histogram in all_histograms.items():
        (key_1, key_2) = key_name
        ch, reg, cat = key_1
        uncName,scale = key_2
        key_tuple_num = ((ch, reg, 'btag_shape'), key_2)
        key_tuple_den = ((ch, reg, 'inclusive'), key_2)
        ratio_num_hist = all_histograms[key_tuple_num]
        #print(key_name)
        ratio_den_hist = all_histograms[key_tuple_den]
        ##print(type(ratio_num_hist))
        #print(type(ratio_den_hist))
        histlist =[]
        final_json_dict[sample_name]={}
        second_key = f'{ch}_{reg}_{cat}_{uncName}{scale}'
        final_json_dict[sample_name][second_key] = {}
        for yBin in range(0, histogram.GetNbinsY()):
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

            #if ch=='tauTau' and cat=='res1b' and sample_name=='GluGluToBulkGravitonToHHTo2B2Tau_M-3000' and reg=='SS_Iso'  :print(f"ratio is {ratio}")
            final_json_dict[sample_name][second_key]['nJets'] = ratio_num_hist.GetYaxis().GetBinCenter(yBin)
            final_json_dict[sample_name][second_key]['num'] =  num
            final_json_dict[sample_name][second_key]['den'] = den
            final_json_dict[sample_name][second_key]['ratio'] = ratio


            histName = f"{sample_name}_{ch}_{reg}_{cat}_{uncName}{scale}"
            hist1D = histogram.ProjectionX(f"{histName}_pfx_{yBin}", yBin, yBin)
            #if ch=='tauTau' and cat=='res1b' and sample_name=='GluGluToBulkGravitonToHHTo2B2Tau_M-3000' and reg=='SS_Iso' :print( ratio_num_hist.GetYaxis().GetBinCenter(yBin))
            #if ch=='tauTau' and cat=='res1b' and sample_name=='GluGluToBulkGravitonToHHTo2B2Tau_M-3000' and reg=='SS_Iso' :print(f" Integral of 1D hist is {hist1D.Integral(0, hist1D.GetNbinsX())}")
            #if ch=='tauTau' and cat=='res1b' and sample_name=='GluGluToBulkGravitonToHHTo2B2Tau_M-3000' and reg=='SS_Iso' :
                #for xBin in range(0, histogram.GetNbinsX()):
                #    print(f" xBin={xBin},yBin={yBin}, content {histogram.GetBinContent(xBin,yBin)}, binNumber= {histogram.GetBin(xBin,yBin)}, integral: {histogram.Integral()}")
            if ratio == 0 and hist1D.Integral(0, hist1D.GetNbinsX()+1) ==0 :
                continue
            if sample_type != "data":
                hist1D.Scale(ratio)
            histlist.append(hist1D)
            #if ch=='tauTau' and cat=='res1b' and sample_name=='GluGluToBulkGravitonToHHTo2B2Tau_M-3000' and reg=='SS_Iso' :print(histlist)
            #if ch=='tauTau' and cat=='res1b' and sample_name=='GluGluToBulkGravitonToHHTo2B2Tau_M-3000' and reg=='SS_Iso' :print()

        #if ch=='tauTau' and cat=='res1b' and sample_name=='GluGluToBulkGravitonToHHTo2B2Tau_M-3000' and reg=='SS_Iso' :print(histlist)
        if not histlist:
            print(ch, cat, sample_name, reg)
            all_histograms_1D[key_name]= hist1D
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
    parser.add_argument('--hists', required=True, type=str)
    parser.add_argument('--sampleConfig', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--uncSource', required=False, type=str,default='Central')
    parser.add_argument('--wantBTag', required=False, type=bool, default=False)


    args = parser.parse_args()
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)

    all_samples_list,all_samples_types = GetSamplesStuff(sample_cfg_dict,args.histDir, False)
    histNamesDict = {}
    uncNameTypes = GetUncNameTypes(unc_cfg_dict)
    btag_dir= "bTag_WP" if args.wantBTag else "bTag_shape"
    if args.uncSource != 'Central' and args.uncSource not in uncNameTypes:
        print("unknown unc source {args.uncSource}")
    CreateNamesDict(histNamesDict, all_samples_types, args.uncSource, scales, sample_cfg_dict)
    all_vars = args.hists.split(',')
    categories = list(sample_cfg_dict['GLOBAL']['categories']) + ['btag_shape']
    QCDregions = list(sample_cfg_dict['GLOBAL']['QCDRegions'])
    channels = list(sample_cfg_dict['GLOBAL']['channelSelection'])
    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    all_files = {}
    for var in all_vars:
        fileName =  f"{var}2D{args.suffix}.root"
        if var not in all_files.keys():
            all_files[var] = {}
        for sample_type in all_samples_types.keys():
            samples = all_samples_types[sample_type]
            histDirs = [os.path.join(args.histDir, sample,btag_dir) for sample in samples]
            all_files[var][sample_type] = [os.path.join(hist_dir,fileName) for hist_dir in histDirs]
    # 1. get Histograms
    all_histograms ={}
    all_histograms_1D ={}
    for var in all_files.keys():
        final_json_dict = {}
        if var not in all_histograms.keys():
            all_histograms[var] = {}
            all_histograms_1D[var] = {}
        for sample_type in all_files[var].keys():
            if sample_type not in all_histograms[var].keys():
                all_histograms[var][sample_type] = {}
                all_histograms_1D[var][sample_type] = {}
            for inFileName in all_files[var][sample_type]:
                print(inFileName)
                if not os.path.exists(inFileName): continue
                inFileRoot = ROOT.TFile.Open(inFileName, "READ")
                if inFileRoot.IsZombie():
                    raise RuntimeError(f"{inFile} is Zombie")
                fillHistDict(inFileRoot, all_histograms[var][sample_type],args.uncSource, channels, QCDregions, categories, sample_type, histNamesDict, signals)
                inFileRoot.Close()
            MergeHistogramsPerType(all_histograms[var][sample_type])
            #if sample_type != 'GluGluToRadionToHHTo2B2Tau_M-250' : continue
            all_histograms_1D[var][sample_type]=GetBTagWeightDict(all_histograms[var][sample_type],sample_type,final_json_dict)
        #print(all_histograms_1D[var].keys())
        #print(all_histograms_1D[var])
        AddQCDInHistDict(all_histograms_1D[var], channels, categories, sample_type, args.uncSource, all_samples_list, scales)
        if not os.path.exists(args.jsonDir):
            os.makedirs(args.jsonDir)
        json_file = os.path.join(args.jsonDir, f'all_rations_{var}_{args.uncSource}{args.suffix}.json')
        with open(f"{json_file}", "w") as write_file:
            json.dump(final_json_dict, write_file, indent=4)
    for var in all_histograms_1D.keys():
        outDir = os.path.join(args.histDir,'all_histograms',var, btag_dir)
        if not os.path.exists(outDir):
            os.makedirs(outDir)
        outFileName = os.path.join(outDir, f'all_histograms_{var}_{args.uncSource}{args.suffix}.root')
        if os.path.exists(outFileName):
            os.remove(outFileName)
        outFile = ROOT.TFile(outFileName, "RECREATE")
        for sample_type in all_histograms_1D[var].keys():
            for key in all_histograms_1D[var][sample_type]:
                (channel, qcdRegion, cat), (uncNameType, uncScale) = key

                if qcdRegion != 'OS_Iso': continue
                dirStruct = (channel, cat)
                dir_name = '/'.join(dirStruct)
                dir_ptr = mkdir(outFile,dir_name)
                hist = all_histograms_1D[var][sample_type][key]

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