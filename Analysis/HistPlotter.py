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
    parser.add_argument('--inFile', required=True, type=str)
    #parser.add_argument('--period', required=False, type=str, default = 'Run2_2018')
    #parser.add_argument('--version', required=False, type=str, default = 'v2_deepTau_v2p1')
    #parser.add_argument('--vars', required=False, type=str, default = 'tau1_pt')
    parser.add_argument('--mass', required=False, type=int, default=500)
    parser.add_argument('--test', required=False, type=bool, default=False)
    #parser.add_argument('--new-weights', required=False, type=bool, default=False)
    args = parser.parse_args()
    #print(f"using new weights {args.new_weights}")
    abs_path = os.environ['CENTRAL_STORAGE']
    histograms_path = f"/eos/home-k/kandroso/cms-hh-bbtautau/Histograms/Run2_2018/{args.version}/" if test==False else ''
    page_cfg = "config/plot/cms_stacked.yaml"
    page_cfg_custom = "config/plot/2018.yaml"
    hist_cfg = "config/plot/histograms.yaml"
    inputs_cfg = "config/plot/inputs.yaml"
    with open(inputs_cfg, 'r') as f:
        inputs_cfg_dict = yaml.safe_load(f)

    plotter = Plotter.Plotter(page_cfg=page_cfg, page_cfg_custom=page_cfg_custom, hist_cfg=hist_cfg, inputs_cfg=inputs_cfg_dict)

       hists_to_plot = {}
    all_histograms=GetValues(all_histograms)
    all_sums=GetValues(all_sums)
    for var in vars:
        hists_to_plot[var] = {}
        for sample in all_histograms[var].keys():
            for region in regions:
                hists_to_plot[var][sample] = all_histograms[var][sample]['region_A']
        hists_to_plot[var]['QCD'] = Estimate_QCD(all_histograms[var], all_sums)
        custom1= {'cat_text':'inclusive'}
        plotter.plot(var, hists_to_plot[var], f"output/plots/{var}_XMass{args.mass}_{args.version}.pdf")#, custom=custom1)
        for sample in  hists_to_plot[var].keys():
            print(f"{sample}, {hists_to_plot[var][sample].Integral()}")