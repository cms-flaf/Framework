import ROOT
import sys
import os
import math
import shutil
import time
from RunKit.sh_tools import sh_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *
from Analysis.hh_bbtautau import *

def createCentralQuantities(df_central, central_col_types, central_columns):
    map_creator = ROOT.analysis.MapCreator(*central_col_types)()
    df_central = map_creator.processCentral(ROOT.RDF.AsRNode(df_central), Utilities.ListToVector(central_columns))
    #df_central = map_creator.getEventIdxFromShifted(ROOT.RDF.AsRNode(df_central))
    return df_central


def SaveHists(histograms, out_file):
    for key_tuple,hist_list in histograms.items():
        (key_1, key_2) = key_tuple
        ch, reg, cat = key_1
        sample_type,uncName,scale = key_2
        #if cat == 'btag_shape': continue
        dir_name = '/'.join(key_1)
        dir_ptr = mkdir(out_file,dir_name)
        merged_hist = hist_list[0].GetValue()
        for hist in hist_list[1:] :
            merged_hist.Add(hist.GetValue())
        isCentral = 'Central' in key_2
        hist_name =  sample_type
        if not isCentral:
            hist_name+=f"_{uncName}{scale}"
        dir_ptr.WriteTObject(merged_hist, hist_name, "Overwrite")


def GetHistogramDictFromDataframes(var, all_dataframes, key_2 , key_filter_dict, unc_cfg_dict,hist_cfg_dict, wantBTag=False, want2D=False, furtherCut=''):
    dataframes = all_dataframes[key_2]
    sample_type,uncName,scale = key_2
    isCentral = 'Central' in key_2
    histograms = {}

    for key_1,key_cut in key_filter_dict.items():
        ch, reg, cat = key_1
        if (key_1, key_2) in histograms.keys(): continue
        if cat == 'boosted' and var in bjet_vars: continue
        if cat == 'boosted' and uncName in unc_to_not_consider_boosted: continue
        if cat != 'boosted' and var in var_to_add_boosted: continue
        #print(var, cat, uncName)
        weight_name = "final_weight"
        if not isCentral:
            if type(unc_cfg_dict)==dict:
                if uncName in unc_cfg_dict.keys() and 'expression' in unc_cfg_dict[uncName].keys():
                    weight_name = unc_cfg_dict[uncName]['expression'].format(scale=scale)
        histograms[(key_1, key_2)] = []
        total_weight_expression = GetWeight(ch,cat) if sample_type!='data' else "1"

        for dataframe in dataframes:
            if furtherCut != '' : key_cut += f' && {furtherCut}'
            dataframe_new = dataframe.Filter(key_cut)
            dataframe_new= dataframe_new.Define(f"final_weight_0_{ch}_{cat}_{reg}", f"{total_weight_expression}")
            final_string_weight = ApplyBTagWeight(cat,applyBtag=wantBTag, finalWeight_name = f"final_weight_0_{ch}_{cat}_{reg}") if sample_type!='data' else "1"
            dataframe_new = dataframe_new.Filter(f"{cat}")
            weight_name = "final_weight"
            if cat == 'btag_shape':
                final_string_weight = f"final_weight_0_{ch}_{cat}_{reg}"
            #print(ch, cat, final_string_weight)
            if want2D and not wantBTag:
                histograms[(key_1, key_2)].append(dataframe_new.Define("weight_for_hists", f"{final_string_weight}").Histo2D(Get2DModel(hist_cfg_dict, var), var, "nBJets", "weight_for_hists"))
            else:
                histograms[(key_1, key_2)].append(dataframe_new.Define("weight_for_hists", f"{final_string_weight}").Histo1D(GetModel(hist_cfg_dict, var), var, "weight_for_hists"))

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


def GetHistograms(var, inFile,dataset,outfile,unc_cfg_dict, sample_cfg_dict, deepTauVersion, compute_unc_variations, compute_rel_weights, furtherCut='', wantBTag=False,want2D=False):
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
    central_histograms = GetHistogramDictFromDataframes(var, all_dataframes, key_central , key_filter_dict, unc_cfg_dict['norm'],hist_cfg_dict, wantBTag,want2D,furtherCut)
    # central quantities definition
    compute_variations = ( compute_unc_variations or compute_rel_weights ) and dataset != 'data'
    if compute_variations:
        all_dataframes[key_central][0] = createCentralQuantities(all_dataframes[key_central][0], col_tpyes_central, col_names_central)
        if all_dataframes[key_central][0].Filter("map_placeholder > 0").Count().GetValue() <= 0 : raise RuntimeError("no events passed map placeolder")

    # norm weight histograms
    if compute_rel_weights and dataset!='data':
        for uncName in unc_cfg_dict['norm'].keys():
            for scale in scales:
                key_2 = (sample_type, uncName, scale)
                if key_2 not in all_dataframes.keys():
                    all_dataframes[key_2] = []
                all_dataframes[key_2] = all_dataframes[key_central]
                norm_histograms =  GetHistogramDictFromDataframes(var,all_dataframes, key_2, key_filter_dict,unc_cfg_dict['norm'], hist_cfg_dict, wantBTag,want2D,furtherCut)
                central_histograms.update(norm_histograms)

    # save histograms
    SaveHists(central_histograms, outfile)

    # shape weight  histograms
    if compute_unc_variations and dataset!='data':
        for uncName in unc_cfg_dict['shape']:
            for scale in scales:
                key_2 = (sample_type, uncName, scale)
                GetShapeDataFrameDict(all_dataframes, key_2, key_central, inFile, compute_variations, deepTauVersion, col_names_central, col_tpyes_central )
                shape_histograms=  GetHistogramDictFromDataframes(var, all_dataframes, key_2 , key_filter_dict,unc_cfg_dict['shape'], hist_cfg_dict, wantBTag,want2D, furtherCut)
                SaveHists(shape_histograms, outfile)

    #for dataframe_key in all_dataframes.keys():
    #    for dataframe in all_dataframes[dataframe_key]:
    #        print(dataframe_key)
    #        print(dataframe.GetNRuns())

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
    parser.add_argument('--var', required=True, type=str)
    parser.add_argument('--sampleConfig', required=True, type=str)
    parser.add_argument('--furtherCut', required=False, type=str, default = "")
    parser.add_argument('--wantBTag', required=False, type=bool, default=False)
    parser.add_argument('--want2D', required=False, type=bool, default=False)
    args = parser.parse_args()

    startTime = time.time()
    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    ROOT.gROOT.ProcessLine('#include "include/AnalysisTools.h"')
    if not os.path.isdir(args.outDir):
        os.makedirs(args.outDir)
    #print(args.furtherCut)
    hist_cfg_dict = {}
    unc_cfg_dict = {}
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    with open(args.histConfig, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    sample_cfg_dict = {}
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)

    #models = createModels(hist_cfg_dict)



    btag_dir= "bTag_WP" if args.wantBTag else "bTag_shape"

    finalDir = os.path.join(args.outDir, args.var, btag_dir)
    if not os.path.isdir(finalDir):
        os.makedirs(finalDir)
    finalFileName =f'{finalDir}/{args.outFileName}'
    outfile  = ROOT.TFile(finalFileName,'RECREATE')

    GetHistograms(args.var,args.inFile, args.dataset, outfile, unc_cfg_dict, sample_cfg_dict,
                                       args.deepTauVersion, args.compute_unc_variations, args.compute_rel_weights, args.furtherCut, args.wantBTag args.want2D)



    outfile.Close()

    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))
