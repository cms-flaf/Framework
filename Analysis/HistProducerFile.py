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


def createCentralQuantities(df_central, central_col_types, central_columns):
    tuple_maker = ROOT.analysis.MapCreator(*central_col_types)()
    tuple_maker.processCentral(ROOT.RDF.AsRNode(df_central), Utilities.ListToVector(central_columns))
    tuple_maker.getEventIdxFromShifted(ROOT.RDF.AsRNode(df_central))

def SaveHisto(outFile, directories_names, histNames, current_path=None):
    if current_path is None:
        current_path = []
    for key, value in directories_names.items():
        current_path.append(key)
        value_name = histNames[key]
        if isinstance(value, dict):
            subdir = outFile.GetDirectory("/".join(current_path))
            if not subdir:
                subdir = outFile.mkdir("/".join(current_path))
            SaveHisto(outFile, value,  value_name, current_path)
        elif isinstance(value, list):
            subdir = outFile.GetDirectory("/".join(current_path))
            if not subdir:
                subdir = outFile.mkdir("/".join(current_path))
            outFile.cd("/".join(current_path))
            print(value_name)
            for val in value:
                val.Write()
        current_path.pop()

def createHistDict(df, histName, histograms, histNames,rel_weights, dataset):
    for qcdRegion in QCDregions:
        df_qcd = df.Filter(qcdRegion)
        for cat in categories :
            if cat != 'inclusive' and cat not in df_qcd.GetColumnNames() : continue
            df_cat = df_qcd if cat=='inclusive' else df_qcd.Filter(cat)
            for channel,channel_code in channels.items():
                df_channel = df_cat.Filter(f"""channelId == {channel_code}""").Filter(triggers[channel]).Filter("""b1_pt > 50 && b2_pt > 50""")
                for var in vars_to_plot:
                    model = createModel(hist_cfg_dict, var)
                    total_weight_expression = GetWeight(cat, channel, "Medium") if dataset!='data' else "1"
                    if dataset == 'TTToSemiLeptonic':
                        total_weight_expression+="*2"
                    hist = df_channel.Define("final_weight", f"{total_weight_expression}").Histo1D(model, var, "final_weight" )#.GetValue()
                    histograms[var][channel][qcdRegion][cat].append(hist)
                    histNames[var][channel][qcdRegion][cat].append(histName)
                    for rel_weight in rel_weights:
                        hist_relative_weight = df_channel.Define(f"final_relative_weight_{rel_weight}", f"{total_weight_expression}*{rel_weight}").Histo1D(model, var, f"final_relative_weight_{rel_weight}" )#.GetValue()
                        histograms[var][channel][qcdRegion][cat].append(hist_relative_weight)
                        histNames[var][channel][qcdRegion][cat].append(f"{histName}_{rel_weight}")

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
    args = parser.parse_args()

    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    #header_path_HistHelper = os.path.join(headers_dir, "include/HistHelper.h")
    #header_path_Utilities = os.path.join(headers_dir, "include/Utilities.h")
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    if not os.path.isdir(args.outDir):
        os.makedirs(args.outDir)
    hist_cfg_dict = {}
    with open(args.histConfig, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    vars_to_plot = list(hist_cfg_dict.keys())


    if args.test: print(f"computing histoMaker for file {args.inFile}")

    inFile_idx_list = args.inFile.split('/')[-1].split('.')[0].split('_')
    inFile_idx = inFile_idx_list[1] if len(inFile_idx_list)>1 else 0
    fileToOpen = ROOT.TFile(args.inFile, 'READ')
    keys= []
    for key in fileToOpen.GetListOfKeys():
        if key.GetName() == 'Events' : continue
        obj = key.ReadObj()
        if obj.IsA().InheritsFrom(ROOT.TH1.Class()):
            continue
        keys.append(key.GetName())
    fileToOpen.Close()

    dfWrapped_central = DataFrameBuilder(ROOT.RDataFrame('Events', args.inFile), args.deepTauVersion)

    all_dataframes={}
    histograms = {}
    histNames = {}
    if args.test: print(f"Running on file {args.inFile}")

    test_idx = 0
    if args.compute_unc_variations and args.dataset != 'data':
        createCentralQuantities(dfWrapped_central.df, dfWrapped_central.colTypes, dfWrapped_central.colNames)
        if args.test: print("Preparing uncertainty variation dataframes")
        for key in keys:
            #print(key)
            #if args.test and test_idx>5:
            #    continue
            dfWrapped_key = DataFrameBuilder(ROOT.RDataFrame(key, args.inFile))
            if(key.endswith('_noDiff')):
                dfWrapped_key.GetEventsFromShifted(dfWrapped_central.df)
                #print(f"nRuns for central noDiff is {dfWrapped_central.df.GetNRuns()}")
            elif(key.endswith('_Valid')):
                var_list = []
                dfWrapped_key.CreateFromDelta(var_list, dfWrapped_central.colNames, dfWrapped_central.colTypes)
            elif(key.endswith('_nonValid')):
                pass
            else:
                print(key)
            keys.remove(key)
            keyName_split = key.split("_")[1:]
            treeName = '_'.join(keyName_split)
            #print(treeName)
            all_dataframes[treeName]= PrepareDfWrapped(dfWrapped_key).df
            test_idx+=1
    all_dataframes['Central'] = PrepareDfWrapped(dfWrapped_central).df
    central_colNames = [str(col) for col in all_dataframes['Central'].GetColumnNames()]
    weights_central = GetRelativeWeights(central_colNames)

    for var in vars_to_plot:
        if not var in all_dataframes['Central'].GetColumnNames() : continue
        histograms[var]={}
        histNames[var]={}
        for channel in channels.keys():
            histograms[var][channel] = {}
            histNames[var][channel] = {}

            for qcdRegion in QCDregions:
                if not qcdRegion in all_dataframes['Central'].GetColumnNames() : continue
                histograms[var][channel][qcdRegion]={}
                histNames[var][channel][qcdRegion]={}
                for cat in categories :
                    if cat != 'inclusive' and cat not in all_dataframes['Central'].GetColumnNames() : continue
                    histograms[var][channel][qcdRegion][cat]= []
                    histNames[var][channel][qcdRegion][cat]= []

    for name in all_dataframes.keys():
        weights_relative = []
        #if args.test:
        #    print(name)
        #    print(all_dataframes[name].Count().GetValue())
        if name == "Central" and args.compute_rel_weights == True and args.dataset != 'data' :
            weights_relative = weights_central
        createHistDict(all_dataframes[name], name, histograms, histNames,weights_relative, args.dataset)




    for var in vars_to_plot:
        finalDir = os.path.join(args.outDir, var)
        if not os.path.isdir(finalDir):
            os.makedirs(finalDir)
        finalDir = os.path.join(args.outDir, var)
        if args.test: print(f"the final file name will be {finalDir}/tmp_{args.dataset}_{inFile_idx}.root")
        finalFile = ROOT.TFile(f'{finalDir}/tmp_{args.dataset}_{inFile_idx}.root','RECREATE')
        SaveHisto(finalFile, histograms[var], histNames[var], current_path=None)
    finalFile.Close()
