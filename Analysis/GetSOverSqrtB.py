import ROOT
import matplotlib.pyplot as plt
import sys
import os
import math
import numpy as np
from scipy import stats
from RunKit.sh_tools import sh_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

all_masses = [250, 260, 270, 280, 300, 320, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 1000, 1250, 1500, 1750, 2000, 2500, 3000]

from Analysis.HistHelper import *
def GetHisto(channel, category, inFile, sample_name, uncSource, scale):
    dir_0 = inFile.Get(channel)
    dir_1 = dir_0.Get(category)
    total_histName = sample_name
    if uncSource != 'Central':
        total_histName += f'_{uncSource}{scale}'
    for key in dir_1.GetListOfKeys():
        key_name = key.GetName()
        if key_name != total_histName: continue
        obj = key.ReadObj()
        if obj.IsA().InheritsFrom(ROOT.TH1.Class()):
            obj.SetDirectory(0)
            return obj
    return

def GetBinValues(histogram, mass):
    mean, sigma, integral = histogram.GetMean(),histogram.GetStdDev(),histogram.Integral(0,histogram.GetNbinsX())


    for x_bin in range(0,histogram.GetNbinsX()):
        bin_center = histogram.GetXaxis().GetBinCenter(x_bin)
        bin_center_prev = histogram.GetXaxis().GetBinCenter(x_bin-1) if x_bin > 0 else 0
        bin_value = histogram.GetBinContent(x_bin)
        bin_value_prev = histogram.GetBinContent(x_bin-1) if x_bin > 0 else 0
        bin_width = (bin_center - bin_center_prev)/2
        #print(bin_center,bin_center_prev,bin_width)
        if mean <= bin_center and mean > bin_center_prev and sigma < bin_width:
            sigma = bin_width
            break
    #print('mean=',mean)
    #print('sigma=',sigma)
    #print(mean-sigma, mean+sigma)
    return mean, sigma, mean-sigma, mean+sigma

def GetBinsForIntegral(histogram,lo_bin, up_bin):
    bins = []
    for x_bin in range(0,histogram.GetNbinsX()):
        center = histogram.GetXaxis().GetBinCenter(x_bin)
        if center > lo_bin and center< up_bin:
            bins.append(x_bin)
    return bins

def fit_function(x, par):
    return par[0] + par[1] * x[0]

def constant_function(x,par):
    return par[0]



if __name__ == "__main__":
    import argparse
    import json
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--histDir', required=True)
    parser.add_argument('--outDir', required=True)
    parser.add_argument('--plotDir', required=True)
    parser.add_argument('--inFileName', required=True)
    parser.add_argument('--sampleConfig', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--wantBTag', required=False, type=bool, default=False)
    parser.add_argument('--want2D', required=False, type=bool, default=False)
    parser.add_argument('--suffix', required=False, type=str, default='')
    parser.add_argument('--var', required=False, type=str, default='bbtautau_mass')
    args = parser.parse_args()
    ROOT.gStyle.SetOptFit(0)
    ROOT.gStyle.SetOptStat(0)


    btag_dir= "bTag_WP" if args.wantBTag else "bTag_shape"
    hist_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/histograms.yaml")
    with open(hist_cfg, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)

    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)

    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    for deepTauVersion in ['2p1', '2p5']:
        outDir = os.path.join(args.outDir,f'v7_deepTau{deepTauVersion}', 's_sqrt_b',args.var, btag_dir)
        if not os.path.exists(outDir):
            os.makedirs(outDir)
        for signal in ['GluGluToRadion', 'GluGluToBulkGraviton']:
            mass_dict = {}
            for channel in ['eTau', 'muTau', 'tauTau']:
                mass_dict[channel] = {}
                for category in ['inclusive','res2b','res1b']:
                    mass_dict[channel][category] = {}
                    s_sqrtb = []
                    mass_dict[channel][category]= {}
                    for mass in all_masses:
                        mass_dict[channel][category][mass] = {}
                        all_samples_list,all_samples_types = GetSamplesStuff(sample_cfg_dict,args.histDir,False,True,mass)
                        sum_signal = 0
                        sum_bckg = 0
                        inDir = os.path.join(args.histDir,f'v7_deepTau{deepTauVersion}', 'all_histograms',args.var,btag_dir)
                        TwoDTrue = args.want2D and not args.wantBTag
                        inFileName_prefix =  f"all_histograms_{args.var}2D" if TwoDTrue else f"all_histograms_{args.var}"
                        inFileName = os.path.join(inDir, f'{inFileName_prefix}{args.suffix}.root')

                        print(os.path.join(inDir,inFileName))
                        if not os.path.exists(os.path.join(inDir,inFileName)): continue
                        #print('mass=',mass)
                        #print('channel=',channel)
                        #print('category=',category)
                        inFile = ROOT.TFile(os.path.join(inDir, inFileName),"READ")
                        hist_signal = GetHisto(channel, category, inFile, f'{signal}ToHHTo2B2Tau_M-{mass}', 'Central','-')
                        mean, sigma, lo_bin, up_bin = GetBinValues(hist_signal, mass)
                        bins_for_integral = GetBinsForIntegral(hist_signal,lo_bin, up_bin)
                        mass_dict[channel][category][mass]['mean'] = mean
                        mass_dict[channel][category][mass]['sigma'] = sigma
                        mass_dict[channel][category][mass]['lo_bin_center'] = lo_bin
                        mass_dict[channel][category][mass]['up_bin_center'] = up_bin
                        #print(bins_for_integral)
                        sum_signal = hist_signal.Integral(bins_for_integral[0],bins_for_integral[-1])
                        for sample in all_samples_types.keys():
                            sample_type = sample if not sample in sample_cfg_dict.keys() else sample_cfg_dict[sample]['sampleType']
                            if sample_type in signals : continue
                            hist_bckg = GetHisto(channel, category, inFile, sample, 'Central','-')
                            integral_central = hist_bckg.Integral(bins_for_integral[0],bins_for_integral[-1])
                            sum_bckg+=integral_central
                        inFile.Close()
                        mass_dict[channel][category][mass]['S'] = sum_signal
                        mass_dict[channel][category][mass]['sqrtB'] = math.sqrt(sum_bckg) if sum_bckg > 0 else 0
                        mass_dict[channel][category][mass]['S/sqrtB'] = sum_signal/math.sqrt(sum_bckg) if mass_dict[channel][category][mass]['sqrtB'] != 0 else 0
            with open(f'{outDir}/sOverB_{signal}.json', 'w') as f:
                json.dump(mass_dict, f, indent=4)



    dict_2p1 = {}
    dict_2p5 = {}

    outDir = os.path.join(args.outDir,f'v7_deepTau2p1',  's_sqrt_b',args.var, btag_dir)
    with open(f'{outDir}/sOverB_{signal}.json', 'r') as f:
        dict_2p1=json.load(f)

    outDir = os.path.join(args.outDir,f'v7_deepTau2p5',  's_sqrt_b',args.var, btag_dir)
    pltDir = os.path.join(args.plotDir,'DeepTau2p1vsDeepTau2p5')
    if not os.path.exists(pltDir):
            os.makedirs(pltDir)
    with open(f'{outDir}/sOverB_{signal}.json', 'r') as f:
        dict_2p5=json.load(f)
    for signal in ['GluGluToRadion', 'GluGluToBulkGraviton']:
        for channel in ['eTau', 'muTau', 'tauTau']:
            for category in ['inclusive','res2b','res1b']:
                all_sqrts_2p1=[]
                all_sqrts_2p5=[]
                for mass in all_masses:
                    all_sqrts_2p1.append(dict_2p1[channel][category][f'{mass}']['S/sqrtB'])

                    all_sqrts_2p5.append(dict_2p5[channel][category][f'{mass}']['S/sqrtB'])

                plt.title(f'{channel} {category} {args.var}')
                plt.xlabel('X_mass')
                plt.ylabel('s/sqrt(b)')
                plt.plot(all_masses, all_sqrts_2p1, "or", label="s/sqrt(b) 2p1")
                plt.plot(all_masses, all_sqrts_2p5, "ob", label="s/sqrt(b) 2p5")
                plt.legend(loc="upper right")
                #plt.ylim(-1.5, 2.0)
                plt.savefig(f'{pltDir}/{channel}_{category}_{signal}_s_sqrtb.png')
                plt.clf()


