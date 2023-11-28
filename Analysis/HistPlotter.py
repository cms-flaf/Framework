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


if __name__ == "__main__":
    import argparse
    import PlotKit.Plotter as Plotter
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--histDir', required=True)
    parser.add_argument('--outDir', required=True)
    parser.add_argument('--inFileName', required=True)
    parser.add_argument('--var', required=False, type=str, default = 'tau1_pt')
    parser.add_argument('--mass', required=False, type=int, default=1250)
    parser.add_argument('--sampleConfig', required=True, type=str)
    parser.add_argument('--channel',required=False, type=str, default = 'tauTau')
    parser.add_argument('--category',required=False, type=str, default = 'inclusive')
    parser.add_argument('--uncSource',required=False, type=str, default = 'Central')
    parser.add_argument('--wantBTag', required=False, type=bool, default=False)
    parser.add_argument('--want2D', required=False, type=bool, default=False)
    parser.add_argument('--suffix', required=False, type=str, default='')
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
    btag_dir= "bTag_WP" if args.wantBTag else "bTag_shape"

    all_samples_list,all_samples_types = GetSamplesStuff(sample_cfg_dict,args.histDir,False,True,args.mass)

    all_samples_types.update({"QCD":"QCD"})
    histNamesDict = {}

    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    CreateNamesDict(histNamesDict, all_samples_types, args.uncSource, scales, sample_cfg_dict)
    inFileName=f'{args.inFileName}_{args.var}_{args.uncSource}{args.suffix}.root'
    inFileName_prefix =  f"{args.inFileName}_{args.var}2D" if args.want2D else f"{args.inFileName}_{args.var}"
    if args.uncSource !='Central':
        inFileName_prefix+={args.uncSource}
    inFileName = f'{inFileName_prefix}{args.suffix}.root'
    print(os.path.join(args.histDir, f'{args.inFileName}',args.var,btag_dir,inFileName))
    inFile = ROOT.TFile(os.path.join(args.histDir, f'{args.inFileName}',args.var,btag_dir,inFileName),"READ")
    dir_0 = inFile.Get(args.channel)
    dir_1 = dir_0.Get(args.category)
    for key in dir_1.GetListOfKeys():
        obj = key.ReadObj()
        if obj.IsA().InheritsFrom(ROOT.TH1.Class()):
            obj.SetDirectory(0)
            key_name = key.GetName()
            if key_name not in histNamesDict.keys(): continue
            sample, uncName, scale = histNamesDict[key_name]
            sample_type = sample if not sample in sample_cfg_dict.keys() else sample_cfg_dict[sample]['sampleType']
            if sample_type in signals:
                sample= sample_type
            if (uncName, scale) not in all_histlist.keys():
                all_histlist[(uncName, scale)] = {}
            all_histlist[(uncName, scale)][sample] = obj
    inFile.Close()

    hists_to_plot = {}
    for uncScale in scales+['Central']:
        if args.uncSource == 'Central' and uncScale != 'Central': continue
        if args.uncSource != 'Central' and uncScale == 'Central': continue
        key = ( args.uncSource, uncScale )
        if key not in all_histlist.keys(): continue
        samples_dict = all_histlist[key]
        obj_list = ROOT.TList()
        for sample,hist in samples_dict.items():
            other_inputs = []
            #print(sample)
            if sample not in all_samples_separated+sample_cfg_dict['GLOBAL']['signal_types']:
                if sample in other_inputs: continue
                if not other_inputs:
                    other_obj = hist
                other_inputs.append(sample)
                #sample = 'Other'
                print(f'{sample} in other has {hist.Integral(0, hist.GetNbinsX()+1)} entries')
                obj_list.Add(hist)
                #obj_list=ROOT.TList()
            else:
                print(f'{sample} has {hist.Integral(0, hist.GetNbinsX()+1)} entries')
                hists_to_plot[sample] = hist
        other_obj.Merge(obj_list)
        print(f'other have {other_obj.GetEntries()} entries')
        hists_to_plot['Other'] = other_obj
        if 'data' not in hists_to_plot.keys():
            hists_to_plot['data'] = all_histlist[ ('Central','Central' )]['data']
        cat_txt = args.category if args.category !='inclusive' else 'incl'
        custom1= {'cat_text':f"{cat_txt} m_{{X}}={args.mass} GeV/c^{{2}}", 'ch_text':page_cfg_custom_dict['channel_text'][args.channel], 'datasim_text':'CMS data/simulation'}
        outFile_prefix = f'{args.var}_{args.channel}_{args.category}'

        if(args.uncSource != 'Central'):
            outFile_prefix += f'_{args.uncSource}{uncScale}'
        tmpdir = 'from2D' if args.want2D else 'from1D'
        outDir = os.path.join(args.outDir,args.var,btag_dir,tmpdir)
        #print(outDir)
        #print(hists_to_plot.keys())
        if not os.path.isdir(outDir):
            os.makedirs(outDir)
        outFileName = f"{outFile_prefix}_XMass{args.mass}.pdf"
        outFileFullPath = os.path.join(outDir,outFileName)
        plotter.plot(args.var, hists_to_plot, outFileFullPath, custom=custom1)