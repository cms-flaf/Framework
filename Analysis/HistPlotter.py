import ROOT
import sys
import os
import math
import sys
import os
import math
import shutil
from RunKit.sh_tools import sh_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *
from Analysis.HistMerger import *

def CreateNamesDict(histNamesDict, channels, QCDRegions, categories, sample_types, uncNames, scales, sample_cfg_dict):
    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    for sample_key in sample_types.keys():
        if sample_key in sample_cfg_dict.keys():
            sample_type = sample_cfg_dict[sample_key]['sampleType']
            #if sample_type in signals:
            #    sample_key = sample_type
        for ch in channels:
            for reg in QCDregions:
                for cat in categories:
                    final_sampleKey=f"{sample_key}_{ch}_{reg}_{cat}"
                    histNamesDict[final_sampleKey] = (sample_key, ch, reg,cat, 'Central','Central')
                    if sample_key == 'data': continue
                    for uncName in uncNames:
                        for scale in scales:
                            histName = f"{final_sampleKey}_{uncName}{scale}"
                            histKey = (sample_key, ch, reg,cat,  uncName, scale)
                            histNamesDict[histName] = histKey


channel_text = {'eTau': 'bbe#tau_{h}','muTau': 'bb#mu#tau_{h}','tauTau': 'bb#tau_{h}#tau_{h}'}
all_samples_separated = ["data", "W","DY",  "TT", "HHnonRes", "QCD"]
unify_samples = ["EWK", "ST", "VV", "VVV", "VH", "H", "ttH", "TTV", "TTVV", "TTT", "TTTT", "TTTV", "TTVH", "TTHH"]

if __name__ == "__main__":
    import argparse
    import PlotKit.Plotter as Plotter
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--histDir', required=True)
    parser.add_argument('--outDir', required=True)
    parser.add_argument('--inFileName', required=True)
    parser.add_argument('--hists', required=False, type=str, default = 'tau1_pt')
    parser.add_argument('--mass', required=False, type=int, default=2000)
    parser.add_argument('--sampleConfig', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--qcdRegion', required=False, type=str, default = 'OS_Iso')
    args = parser.parse_args()

    page_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/cms_stacked.yaml")
    page_cfg_custom = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/2018.yaml")
    hist_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/histograms.yaml")
    with open(hist_cfg, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    inputs_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/inputs.yaml")
    with open(inputs_cfg, 'r') as f:
        inputs_cfg_dict = yaml.safe_load(f)
    samples_list = [ s['name'] for s in inputs_cfg_dict]
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)

    all_histlist = {}
    plotter = Plotter.Plotter(page_cfg=page_cfg, page_cfg_custom=page_cfg_custom, hist_cfg=hist_cfg_dict, inputs_cfg=inputs_cfg_dict)

    all_samples_list,all_samples_types = GetSamplesStuff(sample_cfg_dict,args.histDir)

    all_samples_types.update({"QCD":"QCD"})
    histNamesDict = {}
    uncNameTypes = GetUncNameTypes(unc_cfg_dict)
    #scales = ['Up','Down']

    all_vars = args.hists.split(',')
    categories = list(sample_cfg_dict['GLOBAL']['categories'])
    QCDregions = list(sample_cfg_dict['GLOBAL']['QCDRegions'])
    channels = list(sample_cfg_dict['GLOBAL']['channelSelection'])

    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    CreateNamesDict(histNamesDict, channels, QCDregions, categories, all_samples_types, uncNameTypes, scales, sample_cfg_dict)

    for var in all_vars:
        inFileName=f'{args.inFileName}_{var}.root'

        inFile = ROOT.TFile(os.path.join(args.histDir, inFileName),"READ")
        for key in inFile.GetListOfKeys():
            obj = key.ReadObj()
            if obj.IsA().InheritsFrom(ROOT.TH1.Class()):
                obj.SetDirectory(0)
                key_name = key.GetName()
                sample, ch, reg,cat, uncName, scale = histNamesDict[key_name]
                sample_type = sample if not sample in sample_cfg_dict.keys() else sample_cfg_dict[sample]['sampleType']
                if sample in sample_cfg_dict.keys() and 'mass' in sample_cfg_dict[sample].keys():

                    if sample_type in signals and sample_cfg_dict[sample]['mass']!=args.mass : continue
                if sample_type in signals:
                    sample= sample_type
                if (ch, reg,cat, uncName, scale) not in all_histlist.keys():
                    all_histlist[(ch, reg,cat, uncName, scale)] = {}
                all_histlist[(ch, reg,cat, uncName, scale)][sample] = obj
        inFile.Close()
        for ch in channels:
            for cat in categories:
                reg = args.qcdRegion
                for uncName in uncNameTypes+['Central']:
                    for uncScale in scales+['Central']:
                        hists_to_plot = {}
                        key = (ch, reg,cat, uncName, uncScale )
                        if key not in all_histlist.keys(): continue
                        samples_dict = all_histlist[key]
                        for sample,hist in samples_dict.items():
                            obj_list = ROOT.TList()
                            other_inputs = []
                            if sample not in all_samples_separated+sample_cfg_dict['GLOBAL']['signal_types']:
                                if sample in other_inputs: continue
                                if not other_inputs:
                                    other_obj = hist
                                other_inputs.append(sample)
                                #sample = 'Other'
                                obj_list.Add(hist)
                                other_obj.Merge(obj_list)
                                obj_list=ROOT.TList()
                                continue
                            hists_to_plot[sample] = hist
                            #print(hist_name, obj.GetEntries())
                        hists_to_plot['Other'] = other_obj
                        #print(hist_name, other_obj.GetEntries())
                        cat_txt = cat if cat !='inclusive' else 'incl'
                        custom1= {'cat_text':f"{cat_txt} m_{{X}}={args.mass} GeV/c^{{2}}", 'ch_text':channel_text[ch], 'datasim_text':'CMS data/simulation'}
                        outFile_suffix = '_'.join(k for k in key)
                        outDir = os.path.join(args.outDir, var)
                        if not os.path.isdir(outDir):
                            os.makedirs(outDir)
                        outFileName = f"{outFile_suffix}_XMass{args.mass}.pdf"
                        outFileFullPath = os.path.join(outDir,outFileName)
                        plotter.plot(var, hists_to_plot, outFileFullPath, custom=custom1)