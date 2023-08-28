import ROOT
import sys
import os
import mathimport ROOT
import sys
import os
import math
import shutil
from RunKit.sh_tools import sh_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *

all_samples_separated = ["DY","QCD" ,"HHnonRes" ,"W" ,"TT" ,"data" ]
samples_signal =   ["GluGluToRadion", "VBFToRadion",  "GluGluToBulkGraviton",  "VBFToBulkGraviton"]
channel_text = {'eTau': 'bbe#tau_{h}','muTau': 'bb#mu#tau_{h}','tauTau': 'bb#tau_{h}#tau_{h}'}
all_samples_separated.extend(samples_signal)
if __name__ == "__main__":
    import argparse
    import PlotKit.Plotter as Plotter
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--inFileName', required=False, type=str, default='all_histograms.root')
    parser.add_argument('--period', required=False, type=str, default = 'Run2_2018')
    parser.add_argument('--version', required=False, type=str, default = 'v2_deepTau_v2p1')
    parser.add_argument('--vars', required=False, type=str, default = 'tau1_pt')
    parser.add_argument('--histdir', required=False, type=str, default='histograms')
    parser.add_argument('--mass', required=False, type=int, default=2000)
    parser.add_argument('--test', required=False, type=bool, default=True)
    #parser.add_argument('--new-weights', required=False, type=bool, default=False)
    args = parser.parse_args()
    #histograms_path = "/eos/home-v/vdamante/HH_bbtautau_resonant_Run2/histograms_noBjetWeights/Run2_2018/v4p1_deepTau2p1/"
    histograms_path = f"/eos/home-v/vdamante/HH_bbtautau_resonant_Run2/{args.histdir}/Run2_2018/v4p1_deepTau2p1/"
    #histograms_path = os.path.join( os.environ['VDAMANTE_STORAGE'], args.period, args.version) if args.test==False else ''
    page_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/cms_stacked.yaml")
    page_cfg_custom = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/2018.yaml")
    hist_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/histograms.yaml")
    with open(hist_cfg, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    inputs_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/inputs.yaml")
    with open(inputs_cfg, 'r') as f:
        inputs_cfg_dict = yaml.safe_load(f)

    all_histlist = {}
    plotter = Plotter.Plotter(page_cfg=page_cfg, page_cfg_custom=page_cfg_custom, hist_cfg=hist_cfg_dict, inputs_cfg=inputs_cfg_dict)
    vars_to_plot = ['bbtautau_mass']#list(hist_cfg_dict.keys())
    categories = ['inclusive']
    channels = ['tauTau']
    for var in vars_to_plot:
        hists_to_plot = {}
        inDir = os.path.join(histograms_path, var)
        if not os.path.isdir(inDir): continue
        inFileName = os.path.join(inDir, args.inFileName)
        inFile = ROOT.TFile(inFileName,"READ")
        for key in inFile.GetListOfKeys():
            obj = key.ReadObj()
            if obj.IsA().InheritsFrom(ROOT.TH1.Class()):
                obj.SetDirectory(0)
                all_histlist[key.GetName()] = obj
        inFile.Close()

        for channel in channels:
            if channel not in hists_to_plot.keys():
                hists_to_plot[channel] = {}
            for category in categories:
                if category not in hists_to_plot[channel].keys():
                    hists_to_plot[channel][category] = {}
                obj_list = ROOT.TList()
                other_inputs = []
                for hist_name,hist in all_histlist.items():
                    histName = hist.GetName()
                    histName_split = histName.split('_')
                    sample = histName_split[0]
                    if sample in samples_signal:
                        #print(sample)
                        mass = histName_split[1].split('-')[1]
                        if mass != str(args.mass): continue
                        histName_split.pop(1)
                        #print(histName_split)
                    ch = histName_split[1]
                    if ch!=channel: continue
                    SignLeptons = histName_split[2]
                    if SignLeptons != 'OS' : continue
                    Iso = histName_split[3]
                    if Iso != 'Iso' : continue
                    cat = histName_split[4]
                    if cat != category: continue
                    unc_shape_norm = histName_split[5]
                    if unc_shape_norm != 'Central': continue
                    if sample not in all_samples_separated:
                        #print(hist_name)
                        #print(f"{sample} in other backgrounds")
                        #if sample != 'ST': continue
                        sample = 'Other'
                        if histName_split[0] in other_inputs: continue
                        if not other_inputs:
                            other_obj = hist
                        other_inputs.append(histName_split[0])
                        obj_list.Add(hist)
                        other_obj.Merge(obj_list)
                        obj_list=ROOT.TList()
                        continue
                    hists_to_plot[channel][category][sample] = hist
                    #print(hist_name, obj.GetEntries())
                hists_to_plot[channel][category]['Other'] = other_obj
                #print(hist_name, other_obj.GetEntries())
                cat_txt = category if category !='inclusive' else 'incl'
                custom1= {'cat_text':f"{cat_txt} m_{{X}}={args.mass} GeV/c^{{2}}", 'ch_text':channel_text[channel], 'datasim_text':'CMS data/simulation'}
                plotter.plot(var, hists_to_plot[channel][category], f"output/plots/{channel}_{category}_{var}_XMass{args.mass}_{args.version}.pdf", custom=custom1)
