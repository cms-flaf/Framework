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
from Analysis.RegionDefinition import *
scales = ['Up', 'Down']

def createCentralQuantities(df_central, central_col_types, central_columns):
    map_creator = ROOT.analysis.MapCreator(*central_col_types)()
    df_central = map_creator.processCentral(ROOT.RDF.AsRNode(df_central), Utilities.ListToVector(central_columns))
    #df_central = map_creator.getEventIdxFromShifted(ROOT.RDF.AsRNode(df_central))
    return df_central


def mkdir(file, path):
    dir_names = path.split('/')
    current_dir = file
    for n, dir_name in enumerate(dir_names):
        dir_obj = current_dir.Get(dir_name)
        full_name = f'{file.GetPath()}' + '/'.join(dir_names[:n])
        if dir_obj:
            if not dir_obj.IsA().InheritsFrom(ROOT.TDirectory.Class()):
                raise RuntimeError(f'{dir_name} already exists in {full_name} and it is not a directory')
        else:
            dir_obj = current_dir.mkdir(dir_name)
            if not dir_obj:

                raise RuntimeError(f'Failed to create {dir_name} in {full_name}')
        current_dir = dir_obj
    return current_dir

def SaveHists(histograms, out_file):
    for key_tuple,hist_list in histograms.items():
        dir_name = '/'.join(key_tuple[0])
        dir_ptr = mkdir(out_file,dir_name)
        merged_hist = hist_list[0].GetValue()
        for hist in hist_list[1:] :
            merged_hist.Add(hist.GetValue())
        isCentral = 'Central' in key_tuple[1]
        hist_name = '_'.join(key_tuple[1][:2]) if isCentral else '_'.join(key_tuple[1])
        dir_ptr.WriteTObject(merged_hist, hist_name, "Overwrite")

def createModels(hist_cfg_dict):
    vars_to_plot = list(hist_cfg_dict.keys())
    models = {}
    for var in vars_to_plot:
        models[var] = GetModel(hist_cfg_dict, var)
    return models

def GetHistogramDictFromDataframes(all_dataframes, key_2 , models, key_filter_dict, unc_cfg_dict):
    dataframes = all_dataframes[key_2]
    sample_type,uncName,scale = key_2
    histograms = {}
    for var in models.keys():
        if var not in histograms.keys():
            histograms[var] = {}
        histograms_var = histograms[var]
        for key_1,key_cut in key_filter_dict.items():
            ch, reg, cat = key_1
            if (key_1, key_2) in histograms_var.keys(): continue
            histograms_var[(key_1, key_2)] = []
            weight_name = unc_cfg_dict[uncName]['expression'].format(scale=scale) if uncName != 'Central' else "final_weight"
            total_weight_expression = GetWeight(ch, cat, "Medium") if sample_type!='data' else "1"

            for dataframe in dataframes:
                dataframe = dataframe.Filter(key_cut)
                histograms_var[(key_1, key_2)].append(dataframe.Define("final_weight", f"{total_weight_expression}").Define("weight_for_hists", weight_name).Histo1D(models[var], var, "weight_for_hists"))
    return histograms

def GetShapeDataFrameDict(all_dataframes, key, key_central, inFile, compute_variations, deepTauVersion, colNames, colTypes ):
    sample_type,uncName,scale=key
    if compute_variations and key!=key_central and sample_type!='data':
        if key not in all_dataframes.keys():
            all_dataframes[key] = []

        fileToOpen = ROOT.TFile(inFile, 'READ')
        file_keys= []
        for keyFile in fileToOpen.GetListOfKeys():
            if keyFile.GetName() == 'Events' : continue
            obj = keyFile.ReadObj()
            if obj.IsA().InheritsFrom(ROOT.TH1.Class()):
                continue
            file_keys.append(keyFile.GetName())
        fileToOpen.Close()
        treeName = f"Events_{uncName}{scale}"
        treeName_noDiff = f"{treeName}_noDiff"
        if treeName_noDiff in file_keys:
            dfWrapped_noDiff = DataFrameBuilder(ROOT.RDataFrame(treeName_noDiff, inFile))
            dfWrapped_noDiff.CreateFromDelta(colNames, colTypes)
            all_dataframes[key].append(PrepareDfWrapped(dfWrapped_noDiff).df)

        treeName_Valid = f"{treeName}_Valid"
        if treeName_Valid in file_keys:
            dfWrapped_Valid = DataFrameBuilder(ROOT.RDataFrame(treeName_Valid, inFile))
            dfWrapped_Valid.CreateFromDelta(colNames, colTypes)
            all_dataframes[key].append(PrepareDfWrapped(dfWrapped_Valid).df)

        treeName_nonValid = f"{treeName}_nonValid"
        if treeName_nonValid in file_keys:
            dfWrapped_nonValid = DataFrameBuilder(ROOT.RDataFrame(treeName_nonValid, inFile))
            all_dataframes[key].append(PrepareDfWrapped(dfWrapped_nonValid).df)


def GetHistograms(inFile,dataset,outfiles,unc_cfg_dict, sample_cfg_dict, models,deepTauVersion, compute_unc_variations, compute_rel_weights):
    sample_type = sample_cfg_dict[dataset]['sampleType'] if dataset != 'data' else 'data'
    key_central = (sample_type, "Central", "Central")

    all_dataframes = {}
    all_histograms = {}
    key_filter_dict = createKeyFilterDict()

    # central hist definition
    dfWrapped_central = dfWrapped_central = DataFrameBuilder(ROOT.RDataFrame('Events', inFile), deepTauVersion)
    col_names_central =  dfWrapped_central.colNames
    col_tpyes_central =  dfWrapped_central.colTypes
    if key_central not in all_dataframes:
        all_dataframes[key_central] = [PrepareDfWrapped(dfWrapped_central).df]
    central_histograms =  GetHistogramDictFromDataframes(all_dataframes, key_central , models, key_filter_dict, unc_cfg_dict['norm'])

    # norm weight histograms
    if compute_rel_weights and dataset!='data':
        for uncName in unc_cfg_dict['norm'].keys():
            for scale in scales:
                key_2 = (sample_type, uncName, scale)
                if key_2 not in all_dataframes.keys():
                    all_dataframes[key_2] = []
                all_dataframes[key_2] = all_dataframes[key_central]
                norm_histograms =  GetHistogramDictFromDataframes(all_dataframes, key_2, models, key_filter_dict,unc_cfg_dict['norm'])
                central_histograms.update(norm_histograms)

    # central quantities definition
    compute_variations = ( compute_unc_variations or compute_rel_weights ) and dataset != 'data'
    if compute_variations:
        all_dataframes[key_central][0] = createCentralQuantities(all_dataframes[key_central][0], col_tpyes_central, col_names_central)
        if all_dataframes[key_central][0].Filter("map_placeholder > 0").Count().GetValue() <= 0 : raise RuntimeError("no events passed map placeolder")

    # save histograms
    for var in central_histograms.keys():
        SaveHists(central_histograms[var], outfiles[var])

    # shape weight  histograms
    if compute_unc_variations and dataset!='data':
        for uncName in unc_cfg_dict['shape'].keys():
            for scale in scales:
                key_2 = (sample_type, uncName, scale)
                #print(key_2)
                #print("before everything")
                #print(f"nRuns for dfCentral are: {all_dataframes[key_central][0].GetNRuns()}")
                GetShapeDataFrameDict(all_dataframes, key_2, key_central, inFile, compute_variations, deepTauVersion, col_names_central, col_tpyes_central )
                #print("after GetShapeDataFrameDict, before GetHistogramDictFromDataframes")
                #print(f"nRuns for dfCentral are: {all_dataframes[key_central][0].GetNRuns()}")
                shape_histograms =  GetHistogramDictFromDataframes(all_dataframes, key_2 , models, key_filter_dict,unc_cfg_dict['shape'])
                #print("after GetHistogramDictFromDataframes, before saving histograms")
                #print(f"nRuns for dfCentral are: {all_dataframes[key_central][0].GetNRuns()}")
                for var in shape_histograms.keys():
                    SaveHists(shape_histograms[var], outfiles[var])
                #print("after saving histograms")
                #print(f"nRuns for dfCentral are: {all_dataframes[key_central][0].GetNRuns()}")

    # for debugging: get number of runs for each dataframe
    for key,dataframes in all_dataframes.items():
        for df in dataframes:
            print(f"nRuns for df {key} are {df.GetNRuns()}")



if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--inFile', required=True, type=str)
    parser.add_argument('--outDir', required=False, type=str)
    parser.add_argument('--dataset', required=True, type=str)
    parser.add_argument('--test', required=False, type=bool, default=False)
    parser.add_argument('--deepTauVersion', required=False, type=str, default='v2p1')
    parser.add_argument('--compute_unc_variations', type=bool, default=False)
    parser.add_argument('--compute_rel_weights', type=bool, default=False)
    parser.add_argument('--histConfig', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--sampleConfig', required=True, type=str)
    parser.add_argument('--furtherCut', required=False, type=str, default = "")
    args = parser.parse_args()
    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    if not os.path.isdir(args.outDir):
        os.makedirs(args.outDir)

    hist_cfg_dict = {}
    unc_cfg_dict = {}
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    with open(args.histConfig, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    vars_to_plot = list(hist_cfg_dict.keys())
    sample_cfg_dict = {}
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)

    models = createModels(hist_cfg_dict)


    inFile_idx_list = args.inFile.split('/')[-1].split('.')[0].split('_')
    inFile_idx = inFile_idx_list[1] if len(inFile_idx_list)>1 else 0

    outfiles = {}
    for var in vars_to_plot:
        finalDir = os.path.join(args.outDir, var)
        if not os.path.isdir(finalDir):
            os.makedirs(finalDir)
        finalFileName =f'{finalDir}/{args.dataset}_{inFile_idx}.root'
        outfiles[var] = ROOT.TFile(finalFileName,'RECREATE')

    #if args.test: print(f"Running on file {args.inFile}")
    GetHistograms(args.inFile, args.dataset, outfiles, unc_cfg_dict, sample_cfg_dict, models,
                                       args.deepTauVersion, args.compute_unc_variations, args.compute_rel_weights)


    for var in vars_to_plot:
        outfiles[var].Close()