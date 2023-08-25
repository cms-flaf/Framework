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

if __name__ == "__main__":
    import argparse
    import PlotKit.Plotter as Plotter
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--inFileName', required=True, type=str, default='all_histograms.root')
    parser.add_argument('--period', required=False, type=str, default = 'Run2_2018')
    parser.add_argument('--version', required=False, type=str, default = 'v2_deepTau_v2p1')
    parser.add_argument('--vars', required=False, type=str, default = 'tau1_pt')
    parser.add_argument('--mass', required=False, type=int, default=500)
    parser.add_argument('--test', required=False, type=bool, default=False)
    #parser.add_argument('--new-weights', required=False, type=bool, default=False)
    args = parser.parse_args()
    histograms_path = os.path.join( os.environ['VDAMANTE_STORAGE'], args.period, args.version) if args.test==False else ''
    page_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/cms_stacked.yaml")
    page_cfg_custom = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/2018.yaml")
    hist_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/histograms.yaml")
    with open(hist_cfg, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    inputs_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/inputs.yaml")
    with open(inputs_cfg, 'r') as f:
        inputs_cfg_dict = yaml.safe_load(f)
    inputVariables_arg = []
    if args.var != 'all':
        inputVariables_arg = args.var.split(',')
        for var in
    else:
        inputVariables_arg = os.listdir(histograms_path)
    inputVariables = []
    vars_to_plot = list(hist_cfg_dict.keys())
    for var in inputVariables_arg:
        if var not in vars_to_plot: continue
        inputVariables.append(var)
    print(inputVariables)

    plotter = Plotter.Plotter(page_cfg=page_cfg, page_cfg_custom=page_cfg_custom, hist_cfg=hist_cfg, inputs_cfg=inputs_cfg_dict)

    for var in inputVariables:
        inFileName = os.path.join(histograms_path, inVar, args.inFileName)
        inFile = ROOT.TFile(inFileName,"READ")
        hists_to_plot = {}
        inFile = ROOT.TFile(inFileName,"READ")
        print(Utilities.GetKeyNames())
        '''
        for var in vars:
            hists_to_plot[var] = {}
            for sample in all_histograms[var].keys():
                for region in regions:
                    hists_to_plot[var][sample] = all_histograms[var][sample]['region_A']
            hists_to_plot[var]['QCD'] = Estimate_QCD(all_histograms[var], all_sums)
            custom1= {'cat_text':'inclusive'}
            plotter.plot(var, hists_to_plot[var], f"output/plots/{var}_XMass{args.mass}_{args.version}.pdf")#, custom=custom1)
            for sample in  hists_to_plot[var].keys():
                print(f"{sample}, {hists_to_plot[var][sample].Integral()}")'''