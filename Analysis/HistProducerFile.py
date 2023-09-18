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
scales = ['Up', 'Down']

def createCentralQuantities(df_central, central_col_types, central_columns):
    map_creator = ROOT.analysis.MapCreator(*central_col_types)()
    df_central = map_creator.processCentral(ROOT.RDF.AsRNode(df_central), Utilities.ListToVector(central_columns))
    #df_central = map_creator.getEventIdxFromShifted(ROOT.RDF.AsRNode(df_central))
    return df_central


def SaveHists(histograms, out_file):
    for key_tuple,hist_list in histograms.items():
        dir_name = '/'.join(key_tuple[0])
        dir_ptr = mkdir(out_file,dir_name)
        '''
        merged_hist = hist_list[0].GetValue()
        for hist in hist_list[1:] :
            merged_hist.Add(hist.GetValue())
        '''
        for merged_hist in hist_list:
            isCentral = 'Central' in key_tuple[1][0]
            #print(key_tuple[1])
            sample_type,uncName,scale = key_tuple[1][0]
            shift_type= key_tuple[1][1]
            hist_name =  sample_type
            if not isCentral:
                hist_name+=f"_{uncName}{scale}{shift_type}"
            dir_ptr.WriteTObject(merged_hist.GetValue(), hist_name, "Overwrite")

def createModels(hist_cfg_dict):
    return { var : GetModel(hist_cfg_dict, var) for var in hist_cfg_dict.keys() }


def GetHistogramDictFromDataframes(all_dataframes, key_2 , models, key_filter_dict, unc_cfg_dict, furtherCut=''):
    dataframes = all_dataframes[key_2]
    ((sample_type,uncName,scale),variation) = key_2
    isCentral = 'Central' in key_2
    histograms = {}
    for var in models.keys():
        if var not in histograms.keys():
            histograms[var] = {}
        histograms_var = histograms[var]
        for key_1,key_cut in key_filter_dict.items():
            ch, reg, cat = key_1
            if (key_1, key_2) in histograms_var.keys(): continue
            histograms_var[(key_1, key_2)] = []
            weight_name = "final_weight"
            if not isCentral:
                if type(unc_cfg_dict)==dict:
                    if uncName in unc_cfg_dict.keys() and 'expression' in unc_cfg_dict[uncName].keys():
                        weight_name = unc_cfg_dict[uncName]['expression'].format(scale=scale)
            total_weight_expression = GetWeight(ch, cat) if sample_type!='data' else "1"

            for dataframe in dataframes:
                if furtherCut != '' : key_cut += f' && {furtherCut}'
                dataframe = dataframe.Filter(key_cut)
                #print(key_cut)
                #print(f"after cut df has {dataframe.Count().GetValue()}")
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
            if not obj.IsA().InheritsFrom(ROOT.TTree.Class()):
                continue
            file_keys.append(keyFile.GetName())
        fileToOpen.Close()
        treeName = f"Events_{uncName}{scale}"
        treeName_noDiff = f"{treeName}_noDiff"
        if treeName_noDiff in file_keys:
            dfWrapped_noDiff = DataFrameBuilder(ROOT.RDataFrame(treeName_noDiff, inFile))
            dfWrapped_noDiff.CreateFromDelta(colNames, colTypes)
            all_dataframes[(key, 'noDiff')]=[(PrepareDfWrapped(dfWrapped_noDiff).df)]

        treeName_Valid = f"{treeName}_Valid"
        if treeName_Valid in file_keys:
            dfWrapped_Valid = DataFrameBuilder(ROOT.RDataFrame(treeName_Valid, inFile))
            dfWrapped_Valid.CreateFromDelta(colNames, colTypes)
            all_dataframes[(key, 'Valid')]=[(PrepareDfWrapped(dfWrapped_Valid).df)]

        treeName_nonValid = f"{treeName}_nonValid"
        if treeName_nonValid in file_keys:
            dfWrapped_nonValid = DataFrameBuilder(ROOT.RDataFrame(treeName_nonValid, inFile))
            all_dataframes[(key, 'nonValid')]=[(PrepareDfWrapped(dfWrapped_nonValid).df)]


def GetHistograms(inFile,dataset,outfiles,unc_cfg_dict, sample_cfg_dict, models,deepTauVersion, compute_unc_variations, compute_rel_weights, furtherCut=''):
    sample_type = sample_cfg_dict[dataset]['sampleType'] if dataset != 'data' else 'data'
    key_central = ((sample_type, "Central", "Central"), '')

    all_dataframes = {}
    all_histograms = {}
    key_filter_dict = createKeyFilterDict()
    #print(key_filter_dict)

    # central hist definition
    dfWrapped_central = dfWrapped_central = DataFrameBuilder(ROOT.RDataFrame('Events', inFile), deepTauVersion)

    col_names_central =  dfWrapped_central.colNames
    col_tpyes_central =  dfWrapped_central.colTypes
    if key_central not in all_dataframes:
        all_dataframes[key_central] = [PrepareDfWrapped(dfWrapped_central).df]
    central_histograms =  GetHistogramDictFromDataframes(all_dataframes, key_central , models, key_filter_dict, unc_cfg_dict['norm'], furtherCut)
    # norm weight histograms
    if compute_rel_weights and dataset!='data':
        for uncName in unc_cfg_dict['norm'].keys():
            for scale in scales:
                key_2 = ((sample_type, uncName, scale),'')
                if key_2 not in all_dataframes.keys():
                    all_dataframes[key_2] = []
                all_dataframes[key_2] = all_dataframes[key_central]
                norm_histograms =  GetHistogramDictFromDataframes(all_dataframes, key_2, models, key_filter_dict,unc_cfg_dict['norm'], furtherCut)
                for var in central_histograms.keys():
                    if var not in norm_histograms.keys():
                        print(f"norm hist not available for {var}")
                    central_histograms[var].update(norm_histograms[var])

    # central quantities definition
    compute_variations = ( compute_unc_variations or compute_rel_weights ) and dataset != 'data'
    if compute_variations:
        all_dataframes[key_central][0] = createCentralQuantities(all_dataframes[key_central][0], col_tpyes_central, col_names_central)
        if all_dataframes[key_central][0].Filter("map_placeholder > 0").Count().GetValue() <= 0 : raise RuntimeError("no events passed map placeolder")

    # save histograms
    #print(central_histograms)
    for var in central_histograms.keys():
        SaveHists(central_histograms[var], outfiles[var])

    # shape weight  histograms
    if compute_unc_variations and dataset!='data':
        for uncName in unc_cfg_dict['shape']:
            for scale in scales:
                key_2 = ((sample_type, uncName, scale))
                GetShapeDataFrameDict(all_dataframes, key_2, key_central, inFile, compute_variations, deepTauVersion, col_names_central, col_tpyes_central )
                for shift in ['Valid', 'nonValid', 'noDiff']:
                    key_2 = ((sample_type, uncName, scale),shift)
                    if key_2 not in all_dataframes.keys(): continue
                    shape_histograms =  GetHistogramDictFromDataframes(all_dataframes, key_2 , models, key_filter_dict,unc_cfg_dict['shape'], furtherCut)
                    for var in shape_histograms.keys():
                        SaveHists(shape_histograms[var], outfiles[var])


if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--inFile', required=True, type=str)
    parser.add_argument('--outFileName', required=True, type=str)
    parser.add_argument('--outDir', required=False, type=str)
    parser.add_argument('--dataset', required=True, type=str)
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
    print(args.furtherCut)
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


    outfiles = {}
    for var in vars_to_plot:
        finalDir = os.path.join(args.outDir, var)
        if not os.path.isdir(finalDir):
            os.makedirs(finalDir)
        finalFileName =f'{finalDir}/{args.outFileName}'
        outfiles[var] = ROOT.TFile(finalFileName,'RECREATE')

    GetHistograms(args.inFile, args.dataset, outfiles, unc_cfg_dict, sample_cfg_dict, models,
                                       args.deepTauVersion, args.compute_unc_variations, args.compute_rel_weights, args.furtherCut)


    for var in vars_to_plot:
        outfiles[var].Close()