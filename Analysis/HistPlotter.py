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

def CreateNamesDict(histNamesDict, ch, reg, cat, sample_types, uncName, scales, sample_cfg_dict):
    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    for sample_key in sample_types.keys():
        #if sample_key in sample_cfg_dict.keys():
        #    sample_type = sample_cfg_dict[sample_key]['sampleType']
        #    #if sample_type in signals:
        #    #    sample_key = sample_type
        final_sampleKey=f"{sample_key}_{ch}_{reg}_{cat}"
        histNamesDict[final_sampleKey] = (sample_key, ch, reg,cat, 'Central','Central')
        if sample_key == 'data': continue
        if uncName == 'Central': continue
        for scale in scales:
            histName = f"{final_sampleKey}_{uncName}_{scale}"
            histKey = (sample_key, ch, reg,cat,  uncName, scale)
            histNamesDict[histName] = histKey



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
    parser.add_argument('--qcdRegion', required=False, type=str, default = 'OS_Iso')
    parser.add_argument('--channel',required=False, type=str, default = 'OS_Iso')
    parser.add_argument('--category',required=False, type=str, default = 'OS_Iso')
    parser.add_argument('--uncSource',required=False, type=str, default = 'OS_Iso')
    args = parser.parse_args()

    page_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/cms_stacked.yaml")
    page_cfg_custom = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/2018.yaml")
    hist_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/histograms.yaml")
    with open(hist_cfg, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    with open(page_cfg_custom, 'r') as f:
        page_cfg_custom_dict = yaml.safe_load(f)
    inputs_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/inputs.yaml")
    with open(inputs_cfg, 'r') as f:
        inputs_cfg_dict = yaml.safe_load(f)
    samples_list = [ s['name'] for s in inputs_cfg_dict]
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)
    all_samples_separated = [k['name'] for k in inputs_cfg_dict]
    all_histlist = {}
    plotter = Plotter.Plotter(page_cfg=page_cfg, page_cfg_custom=page_cfg_custom, hist_cfg=hist_cfg_dict, inputs_cfg=inputs_cfg_dict)

    all_samples_list,all_samples_types = GetSamplesStuff(sample_cfg_dict,args.histDir)

    all_samples_types.update({"QCD":"QCD"})
    histNamesDict = {}

    all_vars = args.hists.split(',')
    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    CreateNamesDict(histNamesDict, args.channel, args.qcdRegion, args.category, all_samples_types, args.uncSource, scales, sample_cfg_dict)
    for var in all_vars:
        inFileName=f'{args.inFileName}_{var}.root'
        inFile = ROOT.TFile(os.path.join(args.histDir, inFileName),"READ")
        for key in inFile.GetListOfKeys():
            obj = key.ReadObj()
            if obj.IsA().InheritsFrom(ROOT.TH1.Class()):
                obj.SetDirectory(0)
                key_name = key.GetName()
                if key_name not in histNamesDict.keys(): continue
                sample, ch, reg,cat, uncName, scale = histNamesDict[key_name]
                if ch != args.channel: continue
                if reg != args.qcdRegion: continue
                if cat != args.category: continue
                if uncName != args.uncSource :
                    if sample != 'data' : continue
                sample_type = sample if not sample in sample_cfg_dict.keys() else sample_cfg_dict[sample]['sampleType']
                if sample in sample_cfg_dict.keys() and 'mass' in sample_cfg_dict[sample].keys():
                    if sample_type in signals and sample_cfg_dict[sample]['mass']!=args.mass : continue
                if sample_type in signals:
                    sample= sample_type
                if (ch, reg,cat, uncName, scale) not in all_histlist.keys():
                    all_histlist[(ch, reg,cat, uncName, scale)] = {}
                all_histlist[(ch, reg,cat, uncName, scale)][sample] = obj
        inFile.Close()

        hists_to_plot = {}
        for uncScale in scales+['Central']:
            if args.uncSource == 'Central' and uncScale != 'Central': continue
            if args.uncSource != 'Central' and uncScale == 'Central': continue
            key = (args.channel, args.qcdRegion, args.category, args.uncSource, uncScale )
            if key not in all_histlist.keys(): continue
            samples_dict = all_histlist[key]
            for sample,hist in samples_dict.items():
                print(sample)
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
            if 'data' not in hists_to_plot.keys():
                hists_to_plot['data'] = all_histlist[ (args.channel, args.qcdRegion, args.category,'Central','Central' )]['data']
            #print(hist_name, other_obj.GetEntries())
            cat_txt = args.category if args.category !='inclusive' else 'incl'
            custom1= {'cat_text':f"{cat_txt} m_{{X}}={args.mass} GeV/c^{{2}}", 'ch_text':page_cfg_custom_dict['channel_text'][args.channel], 'datasim_text':'CMS data/simulation'}
            outFile_suffix = '_'.join(k for k in key)
            outDir = os.path.join(args.outDir, var)
            if not os.path.isdir(outDir):
                os.makedirs(outDir)
            outFileName = f"{outFile_suffix}_XMass{args.mass}.pdf"
            outFileFullPath = os.path.join(outDir,outFileName)
            plotter.plot(var, hists_to_plot, outFileFullPath, custom=custom1)
