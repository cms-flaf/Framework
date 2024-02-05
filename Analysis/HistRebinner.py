import ROOT
import sys
import os
import math
import array
import time

from RunKit.sh_tools import sh_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])


from Analysis.HistHelper import *
def GetHisto(channel, category, inFile, inDir, hist_name, uncSource, scale):
    dir_0 = inFile.Get(channel)
    dir_1 = dir_0.Get(category)
    #print([str(key.GetName()) for key in dir_1.GetListOfKeys()])
    #if uncSource != 'Central':
    #    total_histName += f'_{uncSource}{scale}'
    for key in dir_1.GetListOfKeys():
        key_name = key.GetName()
        if key_name != hist_name: continue
        obj = key.ReadObj()
        if obj.IsA().InheritsFrom(ROOT.TH1.Class()):
            obj.SetDirectory(0)
            return obj
    return

def RebinHisto(hist_initial, new_binning):
    new_binning_array = array.array('d', new_binning)
    new_hist = hist_initial.Rebin(len(new_binning)-1, "new_hist", new_binning_array)
    return new_hist

def getNewBins(bins):
    if type(bins) == list:
        final_bins = bins
    else:
        n_bins, bin_range = bins.split('|')
        start,stop = bin_range.split(':')
        bin_width = (int(stop) - int(start))/int(n_bins)
        final_bins = []
        bin_center = int(start)
        while bin_center >= int(start) and bin_center <= int(stop):
            final_bins.append(bin_center)
            bin_center = bin_center + bin_width
    return final_bins

def SaveHists(hist_rebinned_dict, outFileName):
    outfile  = ROOT.TFile(outFileName,'RECREATE')
    for key_1,hist_dict in hist_rebinned_dict.items():
        ch, cat,uncSource, uncScale = key_1
        #if cat == 'btag_shape': continue
        dir_name = '/'.join(key_1)
        dir_ptr = mkdir(outfile,dir_name)
        outfile.WriteTObject(hist_dict["histogram"], hist_dict["name"], "Overwrite")
    outfile.Close()

if __name__ == "__main__":
    import argparse
    import json
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--histDir', required=True)
    parser.add_argument('--inFileName', required=True)
    parser.add_argument('--sampleConfig', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--histConfig', required=True, type=str)
    parser.add_argument('--suffix', required=False, type=str, default='')
    parser.add_argument('--uncSource', required=False, type=str, default='')
    parser.add_argument('--var', required=False, type=str, default='kinFit_m')
    parser.add_argument('--wantBTag', required=False, type=bool, default=False)
    args = parser.parse_args()
    ROOT.gStyle.SetOptFit(0)
    ROOT.gStyle.SetOptStat(0)
    startTime = time.time()
#python3 /afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/Analysis/HistRebinner.py --histDir /eos/home-v/vdamante/HH_bbtautau_resonant_Run2/histograms/Run2_2018/v9_deepTau2p1 --inFileName all_histograms --sampleConfig /afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/config/samples_Run2_2018.yaml --var kinFit_m --uncConfig /afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/config/weight_definition.yaml --histConfig /afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/config/plots/histograms.yaml --uncSource Central

    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)
    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    all_samples_list,all_samples_types = GetSamplesStuff(sample_cfg_dict,args.histDir, True, True, False)
    unc_cfg_dict = {}
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    hist_cfg_dict = {}

    with open(args.histConfig, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    bins_to_compute = hist_cfg_dict[args.var]['x_bins']
    new_bins = getNewBins(bins_to_compute)
    all_uncertainties = list(unc_cfg_dict['norm'].keys())
    all_uncertainties.extend(unc_cfg_dict['shape'])

    categories = list(sample_cfg_dict['GLOBAL']['categories'])
    btag_dir= "bTag_WP" if args.wantBTag else "bTag_shape"
    unc_dict = {}
    inDir = os.path.join(args.histDir,'all_histograms',args.var,btag_dir)
    inFileName=f'{args.inFileName}_{args.var}_{args.uncSource}{args.suffix}.root'

    outFileName=f'{args.inFileName}_{args.var}_{args.uncSource}{args.suffix}_Rebinned.root'
    #print(all_samples_list)
    print(f"{os.path.join(inDir,inFileName)}")
    if not os.path.exists(os.path.join(inDir,inFileName)):
        print(f"{os.path.join(inDir,inFileName)} does not exist")
    else:
        inFile = ROOT.TFile(os.path.join(inDir, inFileName),"READ")
        for sample_type in all_samples_list+['QCD', 'data']:
            #print(sample_type)
            hist_rebinned_dict = {}
            for channel in ['eTau', 'muTau', 'tauTau']:
                for category in categories: #['inclusive','res2b','res1b','boosted']:
                    #print(channel, category)
                    total_histName = sample_type
                    for uncScale in scales:
                        hist_name =  sample_type
                        #print(uncScale)
                        if args.uncSource != 'Central':
                            if sample_type == 'data' : continue
                            if uncScale == 'Central': continue
                            hist_name+=f"_{args.uncSource}{uncScale}"
                        uncScale_str = '-' if args.uncSource=='Central' else uncScale
                        #print(hist_name)
                        hist_initial = GetHisto(channel, category, inFile, inDir, hist_name, args.uncSource,uncScale_str)
                        #print(hist_initial)
                        hist_final = RebinHisto(hist_initial, new_bins)
                        if sample_type == 'QCD':
                            if not FixNegativeContributions(histogram):
                                print(f'for {channel} {category} QCD has negative contributions')

                        hist_rebinned_dict[(channel, category,args.uncSource, uncScale)] = {"name":hist_name,"histogram":hist_final}
                    SaveHists(hist_rebinned_dict, os.path.join(inDir,outFileName))
        inFile.Close()


    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))