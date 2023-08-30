import ROOT
import sys
import os
import math
import shutil
from RunKit.sh_tools import sh_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])
from datetime import datetime

import Common.Utilities as Utilities
from Analysis.HistHelper import *
from Analysis.RegionDefinition import *
scales = ['Up', 'Down']
'''
def getHistValues(histograms):
    for var in histograms.keys():
        histograms_var = histograms[var]
        for key_tuple in histograms_var:
            final_hist = histograms_var[key_tuple][0].GetValue()
            objsToMerge = ROOT.TList()
            for hist in histograms_var[key_tuple][1:]:
                objsToMerge.Add(hist.GetValue())
                final_hist.Merge(objsToMerge)
            histograms_var[key_tuple] = final_hist
'''

def createCentralQuantities(df_central, central_col_types, central_columns):
    tuple_maker = ROOT.analysis.MapCreator(*central_col_types)()
    tuple_maker.processCentral(ROOT.RDF.AsRNode(df_central), Utilities.ListToVector(central_columns))
    tuple_maker.getEventIdxFromShifted(ROOT.RDF.AsRNode(df_central))

def SaveHists(histograms, out_file):
    for var in histograms.keys():
        histograms_var = histograms[var]
        for key_tuple,hist_list in histograms_var.items():
            dir_ptr = make_dir(out_file, key_tuple[0])
            merged_hist = hist_list[0].GetValue()
            for hist in hist_list[1:] :
                merged_hist.Add(hist.GetValue())
            hist_name = '_'.join(key_tuple[1])
            dir_ptr.WriteObject(merged_hist, hist_name)

def GetHistogramDictFromDataframes(all_dataframes, hist_cfg_dict, dataset):
    histograms = {}
    vars_to_plot = list(hist_cfg_dict.keys())
    key_filter_dict = createKeyFilterDict()
    for var in vars_to_plot :
        histograms[var] = {}
    for var in vars_to_plot:
        model = createModel(hist_cfg_dict, var)
        histograms_var = histograms[var]
        for key_1,key_cut in key_filter_dict.items():
            ch, reg, cat = key_1
            for key_2,dataframes in all_dataframes.items():
                #print(f"running on {key_1}, {key_2}, using cut {key_cut}")
                sample_type,uncName,scale = key_2
                histograms_var[(key_1, key_2)] = []
                weight_name = unc_cfg_dict[uncName]['expression'].format(scale=scale) if uncName != 'Central' else "final_weight"
                total_weight_expression = GetWeight(ch, cat, "Medium") if dataset!='data' else "1"
                if dataset == 'TTToSemiLeptonic':
                    total_weight_expression+="*2"
                for dataframe in dataframes:
                    dataframe = dataframe.Filter(key_cut)
                    histograms_var[(key_1, key_2)].append(dataframe.Define("final_weight", f"{total_weight_expression}").Define("weight_for_hists", weight_name).Histo1D(model, var, "weight_for_hists"))
    return histograms

def GetHistograms(inFile,dataset,unc_cfg_dict, sample_cfg_dict, hist_cfg_dict,deepTauVersion, compute_unc_variations, compute_rel_weights):
    fileToOpen = ROOT.TFile(inFile, 'READ')
    file_keys= []
    for key in fileToOpen.GetListOfKeys():
        if key.GetName() == 'Events' : continue
        obj = key.ReadObj()
        if obj.IsA().InheritsFrom(ROOT.TH1.Class()):
            continue
        file_keys.append(key.GetName())
    fileToOpen.Close()

    sample_type = sample_cfg_dict[dataset]['sampleType'] if dataset != 'data' else 'data'

    all_dataframes={}
    dfWrapped_central = DataFrameBuilder(ROOT.RDataFrame('Events', inFile), deepTauVersion)
    key_central = (sample_type, 'Central', 'Central')
    all_dataframes[key_central] = [PrepareDfWrapped(dfWrapped_central).df]

    #print(f"are we computing unc variations? {(compute_unc_variations or compute_rel_weights ) and dataset != 'data'}")
    if ( compute_unc_variations or compute_rel_weights ) and dataset != 'data':
        createCentralQuantities(dfWrapped_central.df, dfWrapped_central.colTypes, dfWrapped_central.colNames)
        print(f"time to compute Central quantities is {datetime.now() - startTime}")

        for key in unc_cfg_dict.keys():
            for scale in scales:
                treeName = f"{sample_type}{key}{scale}"
                key_2 = (sample_type, key, scale)
                if key_2 not in all_dataframes.keys():
                    all_dataframes[key_2] = []

                if unc_cfg_dict[key]['is_shape'] :
                    treeName = f"Events_{key}{scale}"

                    treeName_noDiff = f"{treeName}_noDiff"
                    if treeName_noDiff in file_keys:
                        dfWrapped_noDiff = DataFrameBuilder(ROOT.RDataFrame(treeName_noDiff, inFile))
                        dfWrapped_noDiff.GetEventsFromShifted(dfWrapped_central.df)
                        all_dataframes[key_2].append(dfWrapped_noDiff.df)

                    treeName_Valid = f"{treeName}_Valid"
                    if treeName_Valid in file_keys:
                        dfWrapped_Valid = DataFrameBuilder(ROOT.RDataFrame(treeName_Valid, inFile))
                        var_list = []
                        dfWrapped_Valid.CreateFromDelta(var_list, dfWrapped_central.colNames, dfWrapped_central.colTypes)
                        PrepareDfWrapped(dfWrapped_Valid)
                        all_dataframes[key_2].append(dfWrapped_Valid.df)

                    treeName_nonValid = f"{treeName}_nonValid"
                    if treeName_nonValid in file_keys:
                        dfWrapped_nonValid = DataFrameBuilder(ROOT.RDataFrame(treeName_nonValid, inFile))
                        PrepareDfWrapped(dfWrapped_nonValid)
                        all_dataframes[key_2].append(dfWrapped_nonValid.df)
                else:
                    all_dataframes[key_2] = all_dataframes[key_central]


    print(f"time to get all dataframes dict is {datetime.now() - startTime}")
    all_histograms = GetHistogramDictFromDataframes(all_dataframes, hist_cfg_dict,dataset)
    print(f"time to get all histograms dict is {datetime.now() - startTime}")
    return all_histograms


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
    parser.add_argument('--furtherCut', required=False, type=str, default = "")
    args = parser.parse_args()
    startTime = datetime.now()
    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    if not os.path.isdir(args.outDir):
        os.makedirs(args.outDir)

    inFile_idx_list = args.inFile.split('/')[-1].split('.')[0].split('_')
    inFile_idx = inFile_idx_list[1] if len(inFile_idx_list)>1 else 0
    hist_cfg_dict = {}
    unc_cfg_dict = {}
    unc_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/weight_definition.yaml")
    with open(unc_cfg, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    with open(args.histConfig, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    vars_to_plot = list(hist_cfg_dict.keys())
    sample_cfg_dict = {}
    sample_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/samples_Run2_2018.yaml")
    with open(sample_cfg, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)


    #if args.test: print(f"Running on file {args.inFile}")
    all_histograms = PrepareDataframes(args.inFile, args.dataset, unc_cfg_dict, sample_cfg_dict, hist_cfg_dict,
                                       args.deepTauVersion, args.compute_unc_variations, args.compute_rel_weights)

    for var in all_histograms.keys():
        finalDir = os.path.join(args.outDir, var)
        if not os.path.isdir(finalDir):
            os.makedirs(finalDir)
        finalFile = ROOT.TFile(f'{finalDir}/tmp_{args.dataset}_{inFile_idx}.root','RECREATE')
        print(f"the final file name will be {finalDir}/tmp_{args.dataset}_{inFile_idx}.root")
        SaveHists(all_histograms[var], finalFile)
        finalFile.Close()
    print(f"the script took {datetime.now() - startTime}")

    #print(all_histograms)