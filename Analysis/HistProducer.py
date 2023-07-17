import ROOT
import sys
import os
import math

if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
import Common.HistHelper as HistHelper



# 1. apply selection
# 2. QCD estimation
# 3. Split in categories (2bTag, 1bTag)
# 4. save as rootfile with the process name (TT, DY,..)
# the subdirectories will be: var_name (pt, mass .. ) -> regions, cuts (2b1t,  QCD,...) -> histogram with datasetName_systName_varName

col_type_dict = {
  'Float_t':'float',
  'Bool_t':'bool',
  'Int_t' :'int',
  'ULong64_t' :'unsigned long long',
  'Long_t' :'long',
  'UInt_t' :'unsigned int',
  'ROOT::VecOps::RVec<Float_t>':'ROOT::VecOps::RVec<float>',
  'ROOT::VecOps::RVec<Int_t>':'ROOT::VecOps::RVec<int>',
  'ROOT::VecOps::RVec<UChar_t>':'ROOT::VecOps::RVec<unsigned char>'
  }

regions = ['region_A','region_B','region_C','region_D']
cuts = ['cut_2b1t', 'cut_2b2t']

#def SaveHistograms():
def selectTrigger(df, trigger='HLT_ditau'):
    df = df.Filter(trigger)
    return df

def defineQCDRegions(df):
    tau2_iso_var = f"tau2_idDeepTau2017v2p1VSjet"
    df = df.Define("region_A", f"tau1_charge*tau2_charge < 0 && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.Medium.value}")
    df = df.Define("region_B", f"tau1_charge*tau2_charge > 0 && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.Medium.value}")
    df = df.Define("region_C", f"tau1_charge*tau2_charge < 0 && {tau2_iso_var} < {Utilities.WorkingPointsTauVSjet.Medium.value} && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.VVVLoose.value}")
    df = df.Define("region_D", f"tau1_charge*tau2_charge > 0 && {tau2_iso_var} < {Utilities.WorkingPointsTauVSjet.Medium.value} && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.VVVLoose.value}")
    return df

def defineSelectionRegions(df):
    df = df.Define("cut_2b1t", "(b1_idbtagDeepFlavB >= 2 || b2_idbtagDeepFlavB >= 2) && !(b1_idbtagDeepFlavB >= 2 && b2_idbtagDeepFlavB >= 2) ")
    df = df.Define("cut_2b2t", "b1_idbtagDeepFlavB >= 2 && b2_idbtagDeepFlavB >= 2 ")
    return df

def CreateShiftDf(df_central, df_Diff, var_list):
    colNames = [str(c) for c in df_Diff.GetColumnNames()]
    col_types = [str(df_Diff.GetColumnType(c)) for c in colNames]
    tuple_maker = ROOT.analysis.TupleMaker(*col_types)(df_central, 4)
    df_Diff = tuple_maker.processOut(ROOT.RDF.AsRNode(df_Diff))
    tuple_maker.join()
    for var_idx,var_name in enumerate(colNames):
        if not var_name.endswith("Diff"): continue
        var_name_forDelta = var_name.split("Diff")[0]
        df_Diff = df_Diff.Define(f"{var_name_forDelta}", f"""analysis::FromDelta(_entryCentral->GetValue<{col_type_dict[col_types[var_idx]]}>({var_idx}),{var_name})""")
        #print(f"{var_name_forDelta} in colNames ? {var_name_forDelta in df_Diff.GetColumnNames()}")
        var_list.append(f"{var_name_forDelta}")
    return df_Diff


if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--inFile', required=True, type=str)
    parser.add_argument('--dataset', required=False, type=str, default='TTTo2L2Nu')
    parser.add_argument('--test', required=False, type=bool, default=False)
    args = parser.parse_args()
    df_central = ROOT.RDataFrame('Events', args.inFile)
    fileToOpen = ROOT.TFile(args.inFile, 'READ')
    keys= [key.GetName() for key in fileToOpen.GetListOfKeys()]
    fileToOpen.Close()
    keys.remove('Events')
    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    header_path_Skimmer = os.path.join(headers_dir, "SystSkimmer.h")
    ROOT.gInterpreter.Declare(f'#include "{header_path_Skimmer}"')
    trees_noDiff=[]
    trees_Diff=[]
    trees_nonValid=[]
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
    hist_cfg_dict = {}
    hist_cfg = "config/plot/histograms.yaml"
    with open(hist_cfg, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    #print(hist_cfg_dict)
    vars_to_plot = list(hist_cfg_dict.keys())
    # for the moment:
    var_to_plot = vars_to_plot[0]
    region_to_plot = regions[0]
    cut_to_apply = cuts[0]

    histograms = {}


    # 1. trees without difference --> just take the non-shifted value
    if trees_noDiff:
        #df_noDiff = ROOT.RDataFrame(trees_noDiff[0], args.inFile)
        df_noDiff = defineQCDRegions(df_central)
        df_noDiff = defineSelectionRegions(df_noDiff)
        df_noDiff = df_noDiff.Filter(region_to_plot)
        df_noDiff = df_noDiff.Filter(cut_to_apply)
        hist_noDiff = df_noDiff.Histo1D(var_to_plot)
        histograms[trees_noDiff[0]] = hist_noDiff.GetValue()

    # 2. trees nonValid --> do the same than for events central
    if trees_nonValid:
        df_nonValid = ROOT.RDataFrame(trees_nonValid[0], args.inFile)
        df_nonValid = defineQCDRegions(df_nonValid)
        df_nonValid = defineSelectionRegions(df_nonValid)
        df_nonValid = df_nonValid.Filter(region_to_plot)
        df_nonValid = df_nonValid.Filter(cut_to_apply)
        hist_nonValid = df_nonValid.Histo1D(var_to_plot)
        histograms[trees_nonValid[0]] = hist_nonValid.GetValue()

    # 3. trees with differences --> calculate Deltas
    if trees_Diff:
        df_Diff = ROOT.RDataFrame(trees_Diff[0], args.inFile)
        var_list = []
        df_Diff=CreateShiftDf(df_central,df_Diff,var_list)
        df_Diff = defineQCDRegions(df_Diff)
        df_Diff = defineSelectionRegions(df_Diff)
        df_Diff = df_Diff.Filter(region_to_plot)
        df_Diff = df_Diff.Filter(cut_to_apply)
        hist_Diff = df_Diff.Histo1D(var_to_plot)
        histograms[trees_Diff[0]] = hist_Diff.GetValue()


    df_central = defineQCDRegions(df_central)
    df_central = defineSelectionRegions(df_central)
    df_central = df_central.Filter(region_to_plot)
    df_central = df_central.Filter(cut_to_apply)
    hist_central = df_central.Histo1D(var_to_plot)
    histograms['Central'] = hist_central#.GetValue()

    print(histograms)
    finalFile = ROOT.TFile(f'output/outFiles/tmp_TTTo2L2Nu_100/histograms_TT_100.root','RECREATE')
    for key in histograms.keys():
        histograms[key].Write()
    finalFile.Close()

    tuple_maker.join()
    '''
    directories_names = {
        'fileName': f'{args.dataset}.root',
        'subDir1' : var_to_plot,
        'subDir2_first' : regions,
        'subdir2_second' : cuts,
        'histName' : f'{args.dataset}'
        }

    '''
    #
