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

class DataFrameBuilder:

    def defineSelectionRegions(self):
        self.df = self.df.Define("res1b", f"(b1_idbtagDeepFlavB >= {self.bTagWP} || b2_idbtagDeepFlavB >= {self.bTagWP}) && !(b1_idbtagDeepFlavB >= {self.bTagWP} && b2_idbtagDeepFlavB >= {self.bTagWP}) ")
        self.df = self.df.Define("res2b", f"b1_idbtagDeepFlavB >= {self.bTagWP} && b2_idbtagDeepFlavB >= {self.bTagWP}")

    def defineQCDRegions(self):
        tau2_iso_var = f"tau2_idDeepTau{self.deepTauYear()}{self.deepTauVersion}VSjet"
        self.df = self.df.Define("OS_Iso", f"tau1_charge*tau2_charge < 0 && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.Medium.value}")
        self.df = self.df.Define("SS_Iso", f"tau1_charge*tau2_charge > 0 && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.Medium.value}")
        self.df = self.df.Define("OS_AntiIso", f"tau1_charge*tau2_charge < 0 && {tau2_iso_var} < {Utilities.WorkingPointsTauVSjet.Medium.value} && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.VVVLoose.value}")
        self.df = self.df.Define("SS_AntiIso", f"tau1_charge*tau2_charge > 0 && {tau2_iso_var} < {Utilities.WorkingPointsTauVSjet.Medium.value} && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.VVVLoose.value}")

    def deepTauYear(self):
        return deepTauYears[self.deepTauVersion]

    def selectTrigger(self, trigger='HLT_ditau'):
        self.df = self.df.Filter(trigger)

    def CreateColumnTypes(self):
        colNames = [str(c) for c in self.df.GetColumnNames()]
        entryIndexIdx = colNames.index("entryIndex")
        colNames[entryIndexIdx], colNames[0] = colNames[0], colNames[entryIndexIdx]
        self.colNames = colNames
        self.colTypes = [str(self.df.GetColumnType(c)) for c in self.colNames]

    def __init__(self, df, deepTauVersion='v2p1'):
        self.df = df
        self.colNames=[]
        self.colTypes=[]
        self.deepTauVersion = deepTauVersion
        self.bTagWP = 2
        self.var_list = []
        self.CreateColumnTypes()


    def GetEventsFromShifted(self, df_central):
        df_final = df_central.Filter(""" if( std::find ( analysis::GetEntriesVec().begin(), analysis::GetEntriesVec().end(),
                                     entryIndex ) != analysis::GetEntriesVec().end() ) {return true;}
                                     return false;
                                     """)
        self.df=df_final

    def CreateFromDelta(self,var_list,central_columns,central_col_types):
        for var_idx,var_name in enumerate(self.colNames):
            if not var_name.endswith("Diff"):
                continue
            var_name_forDelta = var_name.split("Diff")[0]
            central_col_idx = central_columns.index(var_name_forDelta)
            if central_columns[central_col_idx]!=var_name_forDelta:
                print("ERRORE!")
            self.df = self.df.Define(f"{var_name_forDelta}", f"""analysis::FromDelta({var_name},
                                     analysis::GetEntriesMap()[entryIndex]->GetValue<{col_type_dict[self.colTypes[var_idx]]}>({central_col_idx}) )""")
            var_list.append(f"{var_name_forDelta}")
        for central_col_idx,central_col in enumerate(central_columns):
            if central_col in var_list or central_col in self.colNames: continue
            if central_col != 'channelId' : continue # this is for a bugfix that I still haven't figured out !!
            if( 'Vec' in central_col_types[central_col_idx]):
                print(f"{central_col} is vec type")
                continue
            self.df = self.df.Define(central_col, f"""analysis::GetEntriesMap()[entryIndex]->GetValue<{central_col_types[central_col_idx]}>({central_col_idx})""")


def createModel(hist_cfg, var):
    hists = {}
    x_bins = hist_cfg[var]['x_bins']
    if type(hist_cfg[var]['x_bins'])==list:
        x_bins_vec = Utilities.ListToVector(x_bins, "double")
        model = ROOT.RDF.TH1DModel("", "", x_bins_vec.size()-1, x_bins_vec.data())
    else:
        n_bins, bin_range = x_bins.split('|')
        start,stop = bin_range.split(':')
        model = ROOT.RDF.TH1DModel("", "",int(n_bins), float(start), float(stop))
    return model

def GetValues(collection):
    for key, value in collection.items():
        if isinstance(value, dict):
            GetValues(value)
        else:
            collection[key] = value.GetValue()
    return collection

def GetKeyNames(filee, dir = "" ):
        filee.cd(dir)
        return [key.GetName() for key in ROOT.gDirectory.GetListOfKeys()]

def merge_sameNameHistos(hist_list,histName):
    uncName = ""
    newHist = ROOT.TH1F()
    new_histList = []
    titlesList = {}
    for hist_ptr in hist_list:
        hist = hist_ptr.GetValue()
        current_uncName = histName[hist_list.index(hist_ptr)]
        #print(f"current uncName is {current_uncName}")
        current_uncName_splitted = current_uncName.split("_")
        current_uncName_noSuffixNoPrefix_splitted = uncName.split("_")[1:len(current_uncName_splitted)-1]
        current_uncName_noSuffix = '_'.join(p for p in current_uncName_splitted[0:len(current_uncName_splitted)-1])
        current_uncName_noPrefix = '_'.join(p for p in current_uncName_splitted[1:])
        current_uncName_noSuffixNoPrefix = '_'.join(p for p in current_uncName_splitted[1:len(current_uncName_splitted)-1])
        hist.SetTitle(current_uncName_noSuffix)
        #print(f"current_uncName_noSuffix is {current_uncName_noSuffix}")
        hist.SetName(current_uncName_noSuffix)
        #print(titlesList)
        if current_uncName_noSuffix in titlesList.keys():
            titlesList[current_uncName_noSuffix].Add(hist)
        else:
            if current_uncName_noSuffix!="":
                titlesList[current_uncName_noSuffix]= hist
            else:
                pass
    for hist_name,hist_key in titlesList.items():
        new_histList.append(hist_key)
    return new_histList


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
            new_values = merge_sameNameHistos(value, value_name)
            for val in new_values:
                val.Write()
        current_path.pop()

def PrepareDfWrapped(dfWrapped):
    dfWrapped.df = defineAllP4(dfWrapped.df)
    dfWrapped.df = createInvMass(dfWrapped.df)
    dfWrapped.defineQCDRegions()
    dfWrapped.defineSelectionRegions()
    return dfWrapped

def createCentralQuantities(df_central, central_col_types, central_columns):
    tuple_maker = ROOT.analysis.MapCreator(*central_col_types)(df_central)
    tuple_maker.CleanCentral()
    tuple_maker.CleanCentralVec()
    tuple_maker.processCentral(Utilities.ListToVector(central_columns))
    tuple_maker.getEventIdxFromShifted()

def GetWeight(cat, channel, btag_wp):
    btag_weight = "1"
    if cat!='inclusive':
        btag_weight = f"weight_bTagSF_{btag_wp}_Central"
    trg_weights_dict = {
        'eTau':["weight_tau1_TrgSF_singleEle_Central","weight_tau2_TrgSF_singleEle_Central"],
        'muTau':["weight_tau1_TrgSF_singleMu_Central","weight_tau2_TrgSF_singleMu_Central"],
        'tauTau':["weight_tau1_TrgSF_ditau_Central","weight_tau2_TrgSF_ditau_Central"]
        }
    weights_to_apply = [ "weight_Jet_PUJetID_Central_b1", "weight_Jet_PUJetID_Central_b2", "weight_TauID_Central", btag_weight, "weight_tau1_EleidSF_Central", "weight_tau1_MuidSF_Central", "weight_tau2_EleidSF_Central", "weight_tau2_MuidSF_Central","weight_total"]
    weights_to_apply.extend(trg_weights_dict[channel])
    total_weight = '*'.join(weights_to_apply)
    return total_weight

def GetRelativeWeights(column_names):
    return [col for col in column_names if "weight" in col and "rel" in col]

def createHistDict(df, histName, histograms, histNames,rel_weights):
    for qcdRegion in QCDregions:
        df_qcd = df.Filter(qcdRegion)
        for cat in categories :
            if cat != 'inclusive' and cat not in df_qcd.GetColumnNames() : continue
            df_cat = df_qcd if cat=='inclusive' else df_qcd.Filter(cat)
            for channel,channel_code in channels.items():
                df_channel = df_cat.Filter(f"""channelId == {channel_code}""").Filter(triggers[channel])
                for var in vars_to_plot:
                    model = createModel(hist_cfg_dict, var)
                    total_weight_expression = GetWeight(cat, channel, "Medium")
                    hist = df_channel.Define("total_total_weight", f"{total_weight_expression}").Histo1D(model, var, "total_total_weight" )#.GetValue()
                    histograms[var][channel][qcdRegion][cat].append(hist)
                    histNames[var][channel][qcdRegion][cat].append(histName)
                    for rel_weight in rel_weights:
                        hist_relative_weight = df_channel.Define(f"total_total_relative_weight_{rel_weight}", f"{total_weight_expression}*{rel_weight}").Histo1D(model, var, f"total_total_relative_weight_{rel_weight}" )#.GetValue()
                        histograms[var][channel][qcdRegion][cat].append(hist_relative_weight)
                        histNames[var][channel][qcdRegion][cat].append(f"{histName}_{rel_weight}")

if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputDir', required=True, type=str)
    parser.add_argument('--dataset', required=True, type=str)
    parser.add_argument('--outDir', required=False, type=str)
    parser.add_argument('--test', required=False, type=bool, default=False)
    parser.add_argument('--nFiles', required=False, type=int, default=-1)
    parser.add_argument('--deepTauVersion', required=False, type=str, default='v2p1')
    parser.add_argument('--compute_unc_variations', type=bool, default=False)
    parser.add_argument('--compute_rel_weights', type=bool, default=False)
    args = parser.parse_args()

    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    header_path_Skimmer = os.path.join(headers_dir, "HistHelper.h")
    ROOT.gInterpreter.Declare(f'#include "{header_path_Skimmer}"')
    inFiles = [os.path.join(args.inputDir, fileIn) for fileIn in os.listdir(args.inputDir)]
    if not os.path.isdir(args.outDir):
        os.makedirs(args.outDir)
    allOutFiles = {}
    # create hist dict
    hist_cfg_dict = {}
    hist_cfg = "config/plot/histograms.yaml"
    with open(hist_cfg, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    vars_to_plot = list(hist_cfg_dict.keys())

    for inFile in os.listdir(args.inputDir):
        inFile_path = os.path.join(args.inputDir,inFile)
        print(f"computing histoMaker for file {inFile_path}")
        inFile_idx_list = inFile.split('.')[0].split('_')
        #print(len(inFile_idx_list))
        inFile_idx = inFile_idx_list[1] if len(inFile_idx_list)>1 else 0
        #print(inFile_idx)
        if args.nFiles > 0 and inFile_idx+1 == args.nFiles:
            break
        fileToOpen = ROOT.TFile(inFile_path, 'READ')
        keys= []
        for key in fileToOpen.GetListOfKeys():
            if key.GetName() == 'Events' : continue
            obj = key.ReadObj()
            if obj.IsA().InheritsFrom(ROOT.TH1.Class()):
                continue
            keys.append(key.GetName())
        fileToOpen.Close()

        dfWrapped_central = DataFrameBuilder(ROOT.RDataFrame('Events', inFile_path), args.deepTauVersion)

        all_dataframes={}
        histograms = {}
        histNames = {}

        # *********************************************************************
        test_idx = 0
        if args.compute_unc_variations:
            #print(f"nRuns for central in general [0] is {dfWrapped_central.df.GetNRuns()}")
            createCentralQuantities(dfWrapped_central.df, dfWrapped_central.colTypes, dfWrapped_central.colNames)
            #print(f"nRuns for central in general [1] is {dfWrapped_central.df.GetNRuns()}")
            print("Preparing uncertainty variation dataframes")
            for key in keys:
                #print(key)
                if args.test and test_idx>5:
                    continue
                dfWrapped_key = DataFrameBuilder(ROOT.RDataFrame(key, inFile_path))
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
            if name == "Central" and args.compute_rel_weights == True:
                weights_relative = weights_central
            createHistDict(all_dataframes[name], name, histograms, histNames,weights_relative)




        for var in vars_to_plot:
            if var not in allOutFiles.keys():
                allOutFiles[var] = []
            finalDir = os.path.join(args.outDir, var)
            allOutFiles[var].append(f'{finalDir}/tmp_{args.dataset}_{inFile_idx}.root')
            if not os.path.isdir(finalDir):
                os.makedirs(finalDir)
            finalDir = os.path.join(args.outDir, var)
            #print(f"the final file name will be {finalDir}/tmp_{args.dataset}_{inFile_idx}.root")
            finalFile = ROOT.TFile(f'{finalDir}/tmp_{args.dataset}_{inFile_idx}.root','RECREATE')
            SaveHisto(finalFile, histograms[var], histNames[var], current_path=None)
        finalFile.Close()
        '''
        for name,df in all_dataframes.items():
            print(name)
            print(df.GetNRuns())
        '''
    for var in vars_to_plot:
        finalDir = os.path.join(args.outDir, var)
        outFileName = f'{finalDir}/{args.dataset}.root'
        hadd_str = f'hadd -f209 -j -O {outFileName} '
        hadd_str += ' '.join(f for f in allOutFiles[var])
        if args.test : print(f'hadd_str is {hadd_str}')
        if len(allOutFiles[var]) > 1:
            sh_call([hadd_str], True)
            if os.path.exists(outFileName):
                for histFile in allOutFiles[var]:# + [outFileCentralName]:
                    if args.test : print(histFile)
                    if histFile == outFileName: continue
                    os.remove(histFile)
        else:
            shutil.move(allOutFiles[var][0],outFileName)