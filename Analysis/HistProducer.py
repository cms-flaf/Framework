import ROOT
import sys
import os
import math

if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Common.HistHelper import *
#, ROOT::VecOps::RVec<Int_t>, ROOT::VecOps::RVec<Float_t>,ROOT::VecOps::RVec<UChar_t>


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

#def SaveHistograms():
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

    def __init__(self, df, df_central,central_columns, central_col_types, deepTauVersion='v2p1'):
        self.df = df
        self.deepTauVersion = deepTauVersion
        tuple_maker = ROOT.analysis.MapCreator(*central_col_types)(df_central)
        #self.CentralMap = tuple_maker.processCentral(Utilities.ListToVector(central_columns))
        tuple_maker.processCentral(Utilities.ListToVector(central_columns))
        self.bTagWP = 2
        self.var_list = []

    def CreateFromDelta(self,var_list,colNames, colTypes):
        for var_idx,var_name in enumerate(colNames):
            if not var_name.endswith("Diff"): continue
            var_name_forDelta = var_name.split("Diff")[0]
            #print(var_name_forDelta)
            #print(self.CentralMap)
            #print(type(self.CentralMap))
            self.df = self.df.Define(f"{var_name_forDelta}", f"""analysis::FromDelta({var_name}, ::analysis::MapCreator::GetEntriesMap()[entryIndex][{var_idx}])""")
            print(f"{var_name_forDelta} in colNames ? {var_name_forDelta in df_Diff.GetColumnNames()}")
            var_list.append(f"{var_name_forDelta}")




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

    hist_cfg_dict = {}
    hist_cfg = "config/plot/histograms.yaml"
    with open(hist_cfg, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    vars_to_plot = list(hist_cfg_dict.keys())

    trees_noDiff=[]
    trees_Diff=[]
    trees_nonValid=[]
    fileToOpen = ROOT.TFile(args.inFile, 'READ')
    keys= [key.GetName() for key in fileToOpen.GetListOfKeys()]
    fileToOpen.Close()
    keys.remove('Events')
    #print(keys)
    for key in keys:
        if(key.endswith('_noDiff')):
            trees_noDiff.append(key)
        elif(key.endswith('_Valid')):
            trees_Diff.append(key)
        elif(key.endswith('_nonValid')):
            trees_nonValid.append(key)
        else:
            print(key)
        keys.remove(key)

    histograms = {}

    df_central = ROOT.RDataFrame('Events', args.inFile)
    central_colNames = [str(c) for c in df_central.GetColumnNames()]
    entryIndexIdx = central_colNames.index("entryIndex")
    central_colNames[entryIndexIdx], central_colNames[0] = central_colNames[0], central_colNames[entryIndexIdx]
    central_col_types = [str(df_central.GetColumnType(c)) for c in central_colNames]

    df_shifted = ROOT.RDataFrame(trees_Diff[0], args.inFile)
    colNames = [str(c) for c in df_shifted.GetColumnNames()]
    entryIndexIdx = colNames.index("entryIndex")
    colNames[entryIndexIdx], colNames[0] = colNames[0], colNames[entryIndexIdx]
    col_types = [str(df_shifted.GetColumnType(c)) for c in colNames]
    # for the moment:
    dfWrapped = DataFrameBuilder(df_shifted, df_central, central_colNames,central_col_types)
    var_list = []
    dfWrapped.CreateFromDelta(var_list,colNames, col_types)
    print(var_list)
    print(dfWrapped.df.GetColumnNames())
    #dfWrapped.defineQCDRegions()
    #dfWrapped.defineSelectionRegions()
