import ROOT
import sys
import os
import math

if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Common.HistHelper import *


# 1. apply selection
# 2. QCD regions
# 3. Split in categories (2bTag, 1bTag)
# 4. save as rootfile with the process name (TT, DY,..)
# the subdirectories will be: var_name (pt, mass .. ) -> regions, cuts (2b1t,  QCD,...) -> histogram with datasetName_systName_varName

'''
1. Central tree: produce central histogram and up/down variations for weights. This can be done with df.Vary
2. shifted unique tree: the same as central, but no need to vary
3. shifted same tree: select events from central tree and produce hist
4. shifted delta tree: apply deltas and produce hists

2-4 should be repeated for each shift.
To minimise the amout of readout, I suggest to read central tree only once for all 3-4 into a map. This mean that you don't need to have queue&threads - just fill the map in the same thread.
Also, it would be convinient to have function that takes as an input df and outputs dict of histograms. Like this selection code can be reused for 1-4.
'''

class DataFrameBuilder:

    def defineSelectionRegions(self):
        self.df = self.df.Define("cut_2b1t", f"(b1_idbtagDeepFlavB >= {self.bTagWP} || b2_idbtagDeepFlavB >= {self.bTagWP}) && !(b1_idbtagDeepFlavB >= {self.bTagWP} && b2_idbtagDeepFlavB >= {self.bTagWP}) ")
        self.df = self.df.Define("cut_2b2t", f"b1_idbtagDeepFlavB >= {self.bTagWP} && b2_idbtagDeepFlavB >= {self.bTagWP}")

    def defineQCDRegions(self):
        tau2_iso_var = f"tau2_idDeepTau{self.deepTauYear()}{self.deepTauVersion}VSjet"
        self.df = self.df.Define("region_A", f"tau1_charge*tau2_charge < 0 && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.Medium.value}")
        self.df = self.df.Define("region_B", f"tau1_charge*tau2_charge > 0 && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.Medium.value}")
        self.df = self.df.Define("region_C", f"tau1_charge*tau2_charge < 0 && {tau2_iso_var} < {Utilities.WorkingPointsTauVSjet.Medium.value} && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.VVVLoose.value}")
        self.df = self.df.Define("region_D", f"tau1_charge*tau2_charge > 0 && {tau2_iso_var} < {Utilities.WorkingPointsTauVSjet.Medium.value} && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.VVVLoose.value}")

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
            #print(central_columns[central_col_idx], var_name_forDelta)
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
            for val in value:
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
    parser.add_argument('--inFile', required=True, type=str)
    parser.add_argument('--dataset', required=False, type=str, default='TTTo2L2Nu')
    parser.add_argument('--test', required=False, type=bool, default=False)
    args = parser.parse_args()

    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    header_path_Skimmer = os.path.join(headers_dir, "HistHelper.h")
    ROOT.gInterpreter.Declare(f'#include "{header_path_Skimmer}"')

    fileToOpen = ROOT.TFile(args.inFile, 'READ')
    keys= [key.GetName() for key in fileToOpen.GetListOfKeys()]
    fileToOpen.Close()
    keys.remove('Events')
    dfWrapped_central = DataFrameBuilder(ROOT.RDataFrame('Events', args.inFile))
    createCentralQuantities(dfWrapped_central.df, dfWrapped_central.colTypes, dfWrapped_central.colNames)
    all_dataframes={}
    for key in keys:
        dfWrapped_key = DataFrameBuilder(ROOT.RDataFrame(key, args.inFile))
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
        treeName = key.strip('Events_')
        all_dataframes[treeName]= PrepareDfWrapped(dfWrapped_key).df
    all_dataframes['Central'] = PrepareDfWrapped(dfWrapped_central).df

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
            histograms[var][qcdRegion]={}
            for cut in cuts :
                histograms[var][qcdRegion][cut]= []

    for name in all_dataframes.keys():
        print(name)
        histName = f"{args.dataset}_{name}" if name!='Central' else args.dataset
        for var in hist_cfg_dict.keys():
            for qcdRegion in QCDregions:
                df_qcd = all_dataframes[name].Filter(qcdRegion)
                for cut in cuts :
                    df_cut = df_qcd if cut=='noCut' else df_qcd.Filter(cut)
                    model = createModel(hist_cfg_dict, var)
                    hist = df_cut.Histo1D(model, var).GetValue()
                    hist.GetXaxis().SetTitle()
                    hist.SetName(histName)
                    hist.SetTitle(var)
                    histograms[var][qcdRegion][cut].append(hist)
    finalFile = ROOT.TFile(f'output/outFiles/tmp_{args.dataset}/histograms_{args.dataset}.root','RECREATE')
    SaveHisto(finalFile, histograms, current_path=None)
    finalFile.Close()