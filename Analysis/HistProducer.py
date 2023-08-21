import ROOT
import sys
import os
import math

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

    def CreateFromDelta(self,var_list,central_columns):
        for var_idx,var_name in enumerate(self.colNames):
            if not var_name.endswith("Diff"): continue
            var_name_forDelta = var_name.split("Diff")[0]
            central_col_idx = central_columns.index(var_name_forDelta)
            if central_columns[central_col_idx]!=var_name_forDelta:
                print("ERRORE!")
            self.df = self.df.Define(f"{var_name_forDelta}", f"""analysis::FromDelta({var_name},
                                     analysis::GetEntriesMap()[entryIndex]->GetValue<{col_type_dict[self.colTypes[var_idx]]}>({central_col_idx}) )""")
            var_list.append(f"{var_name_forDelta}")

    def GetWeightDict(self):
        weight_variables = []
        for var in self.colNames:
            if var.split('_')[0] == 'weight':
                weight_variables.append(var)
        all_weights = {}
        relative_weights = []
        options = ['Central', 'Up','Down', 'total']
        #variations = ['Up','Down']
        for opt in options:
            for weightName in weight_variables:
                if opt not in all_weights.keys():
                    all_weights[opt] = []
                if opt in weightName:
                    all_weights[opt].append(weightName)
            weight_variables=list(set(weight_variables)-set(all_weights[opt]))
        print(weight_variables)
        for opt in options:
            print(opt)
            print(all_weights[opt])
    #def CreateShiftDfForNormWeights(self, dataframe, var):
    #    dataframe = dataframe.Vary(var, f"""RVecF{{ {var}*{weight_variables[0]}}}""", [''])

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

def merge_sameNameHistos(hist_list):
    uncName = ""
    newHist = ROOT.TH1F()
    new_histList = []
    titlesList = {}
    for hist in hist_list:
        current_uncName = hist.GetTitle()
        current_uncName_splitted = current_uncName.split("_")
        current_uncName_noSuffixNoPrefix_splitted = uncName.split("_")[1:len(current_uncName_splitted)-1]
        current_uncName_noSuffix = '_'.join(p for p in current_uncName_splitted[0:len(current_uncName_splitted)-1])
        current_uncName_noPrefix = '_'.join(p for p in current_uncName_splitted[1:])
        current_uncName_noSuffixNoPrefix = '_'.join(p for p in current_uncName_splitted[1:len(current_uncName_splitted)-1])
        hist.SetTitle(current_uncName_noSuffix)
        hist.SetName(current_uncName_noSuffix)
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


def SaveHisto(outFile, directories_names, current_path=None):
    if current_path is None:
        current_path = []
    for key, value in directories_names.items():
        current_path.append(key)
        if isinstance(value, dict):
            subdir = outFile.GetDirectory("/".join(current_path))
            if not subdir:
                subdir = outFile.mkdir("/".join(current_path))
            SaveHisto(outFile, value, current_path)
        elif isinstance(value, list):
            subdir = outFile.GetDirectory("/".join(current_path))
            if not subdir:
                subdir = outFile.mkdir("/".join(current_path))
            outFile.cd("/".join(current_path))
            new_values = merge_sameNameHistos(value)
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
    #tuple_maker.CleanCentral()
    tuple_maker.processCentral(Utilities.ListToVector(central_columns))
    #tuple_maker.CleanCentralVec()
    tuple_maker.getEventIdxFromShifted()


if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputDir', required=True, type=str)
    parser.add_argument('--dataset', required=False, type=str, default='TTTo2L2Nu')
    parser.add_argument('--outDir', required=False, type=str)
    parser.add_argument('--test', required=False, type=bool, default=False)
    parser.add_argument('--deepTauVersion', required=False, type=str, default='v2p1')
    args = parser.parse_args()

    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    header_path_Skimmer = os.path.join(headers_dir, "HistHelper.h")
    ROOT.gInterpreter.Declare(f'#include "{header_path_Skimmer}"')
    inFiles = [os.path.join(args.inputDir, fileIn) for fileIn in os.listdir(args.inputDir)]

    for inFile in os.listdir(args.inputDir):
        inFile_path = os.path.join(args.inputDir,inFile)
        print(inFile_path)
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
        createCentralQuantities(dfWrapped_central.df, dfWrapped_central.colTypes, dfWrapped_central.colNames)
        test_idx = 0
        all_dataframes={}
        for key in keys:

            if args.test and test_idx>5:
                continue
            print(key)
            dfWrapped_key = DataFrameBuilder(ROOT.RDataFrame(key, inFile_path))
            if(key.endswith('_noDiff')):
                dfWrapped_key.GetEventsFromShifted(dfWrapped_central.df)
            elif(key.endswith('_Valid')):
                var_list = []
                dfWrapped_key.CreateFromDelta(var_list, dfWrapped_central.colNames)
            elif(key.endswith('_nonValid')):
                pass
            else:
                print(key)
            keys.remove(key)
            keyName_split = key.split("_")[1:]
            treeName = '_'.join(keyName_split)
            #print(treeName)
            all_dataframes[treeName]= PrepareDfWrapped(dfWrapped_key)
            test_idx+=1
        all_dataframes['Central'] = PrepareDfWrapped(dfWrapped_central)
        # create hist dict
        hist_cfg_dict = {}
        hist_cfg = "config/plot/histograms.yaml"
        with open(hist_cfg, 'r') as f:
            hist_cfg_dict = yaml.safe_load(f)
        vars_to_plot = list(hist_cfg_dict.keys())
        histograms = {}
        for var in hist_cfg_dict.keys():
            if not var in all_dataframes['Central'].GetColumnNames() : continue
            histograms[var]={}
            for qcdRegion in QCDregions:
                if not qcdRegion in all_dataframes['Central'].GetColumnNames() : continue
                histograms[var][qcdRegion]={}
                for cut in cuts :
                    if cut != 'inclusive' and cut not in all_dataframes['Central'].GetColumnNames() : continue
                    histograms[var][qcdRegion][cut]= []
        if not os.path.isdir(args.outDir):
            os.makedirs(args.outDir)

        for var in hist_cfg_dict.keys():
            for name in all_dataframes.keys():
                histName = f"{args.dataset}_{name}"
                print(histName)
                for qcdRegion in QCDregions:
                    df_qcd = all_dataframes[name].Filter(qcdRegion)
                    for cut in cuts :
                        if cut != 'inclusive' and cut not in df_qcd.GetColumnNames() : continue
                        df_cut = df_qcd if cut=='inclusive' else df_qcd.Filter(cut)
                        model = createModel(hist_cfg_dict, var)
                        hist = df_cut.Histo1D(model, var).GetValue()
                        hist.GetXaxis().SetTitle()
                        hist.SetName(histName)
                        hist.SetTitle(histName)
                        histograms[var][qcdRegion][cut].append(hist)
            finalDir = os.path.join(args.outDir, var)
            if not os.path.isdir(finalDir):
                os.makedirs(finalDir)
            inFile_idx = inFile.split('.')[0].split('_')[1]
            finalFile = ROOT.TFile(f'{finalDir}/tmp_{args.dataset}_{inFile_idx}.root','RECREATE')
            SaveHisto(finalFile, histograms[var], current_path=None)
            finalFile.Close()
