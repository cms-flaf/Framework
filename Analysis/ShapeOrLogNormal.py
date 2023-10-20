import ROOT
import sys
import os
import math
from RunKit.sh_tools import sh_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])


from Analysis.HistHelper import *
def GetHisto(channel, category, inFileName, histDir, sample_name, uncSource, scale):
    inFile = ROOT.TFile(os.path.join(histDir, 'all_histograms',var,inFileName),"READ")
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
            inFile.Close()
            return obj
    return

def GetFirstMethod(histogram):
    mean = histogram.GetMean()
    differences = []
    for bin_number in range(0, histogram.GetNbinsX()):
        differences.append(abs(mean-histogram.GetBinContent(bin_number)))
    return differences

def fit_function(x, par):
    return par[0] + par[1] * x[0]

def constant_function(x,par):
    return par[0]

def GetSecondMethod(histogram, sample, ch, cat, unc, updown, unc_dict):
    fit_func = ROOT.TF1("fit_func", fit_function, 0, 10, 2)
    mean = histogram.GetMean()
    range_hist = histogram.GetBinCenter(histogram.GetNbinsX()) - histogram.GetBinCenter(0)
    histogram.Fit(fit_func)
    # Ottieni i parametri del fit
    fit_params = fit_func.GetParameters()
    fit_param_errors = [fit_func.GetParError(i) for i in range(2)]

    # Calcola la compatibilità della pendenza con zero entro gli errori
    slope = fit_params[1]
    slope_error = fit_param_errors[1]
    threshold = 0.01

    if  abs(slope)*range_hist/mean < threshold:
        print("La pendenza del fit è compatibile con zero entro gli errori.")
    else:
        print("La pendenza del fit non è compatibile con zero entro gli errori.")

    # Stampare i parametri del fit e i relativi errori
    #print("Parametro 0 (intercetta):", fit_params[0], "±", fit_param_errors[0])
    #print("Parametro 1 (pendenza):", fit_params[1], "±", fit_param_errors[1])
    unc_dict[f'{unc}_{updown}']= {'intercept':fit_params[0],'intercept_error':fit_param_errors[0],'slope':fit_params[1],'slope_error':fit_param_errors[1], 'mean':mean, 'good':abs(slope) < threshold, 'range':range_hist}
    # Disegna l'istogramma e la funzione di adattamento
    canvas = ROOT.TCanvas("canvas", "Fit Plot")
    histogram.Draw()
    fit_func.Draw("same")
    canvas.Update()

    canvas.SaveAs(f"output/histograms/fit/fit_plot_{sample}_{ch}_{cat}_{unc}_{updown}.png")

def GetChi2Method(histogram, sample, ch, cat, unc, updown, unc_dict):
    fit_func = ROOT.TF1("fit_func", constant_function, 0, 10, 1)

    fit_func.SetParameter(0, 1.0)

    histogram.Fit(fit_func)
    # Ottieni il chi-quadro ridotto
    chi2 = fit_func.GetChisquare() #/ fit_func.GetNDF()

    # Verifica la bontà del fit
    if chi2 < 1.0:
        print("Il fit è buono.")
    else:
        print("Il fit non è buono.")

    # Stampare i parametri del fit e il chi-quadro ridotto
    fit_param = fit_func.GetParameter(0)
    fit_param_error = fit_func.GetParError(0)
    unc_dict[f'{unc}_{updown}']= {'intercept':fit_param,'intercept_error':fit_param_error,'chi2':chi2,'good':chi2<1.}

    # Disegna l'istogramma e la funzione di adattamento
    canvas = ROOT.TCanvas("canvas", "Fit Plot")
    histogram.Draw()
    fit_func.Draw("same")
    canvas.Update()
    canvas.SaveAs(f"output/histograms/fit_chi2/fit_plot_{sample}_{ch}_{cat}_{unc}_{updown}.png")



if __name__ == "__main__":
    import argparse
    import json
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--histDir', required=True)
    parser.add_argument('--outDir', required=True)
    parser.add_argument('--inFileName', required=True)
    #parser.add_argument('--hists', required=False, type=str, default = 'tau1_pt')
    parser.add_argument('--mass', required=False, type=int, default=1250)
    parser.add_argument('--sampleConfig', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    #parser.add_argument('--channel',required=False, type=str, default = 'tauTau')
    #parser.add_argument('--category',required=False, type=str, default = 'inclusive')
    #parser.add_argument('--uncSource',required=False, type=str, default = 'all')
    args = parser.parse_args()
    ROOT.gStyle.SetOptFit(0)
    ROOT.gStyle.SetOptStat(0)

    hist_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/histograms.yaml")
    with open(hist_cfg, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)

    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)
    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    all_samples_list,all_samples_types = GetSamplesStuff(sample_cfg_dict,args.histDir,True,args.mass)

    all_histlist = {}
    histNamesDict = {}
    #all_vars = args.hists.split(',')
    unc_cfg_dict = {}
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    all_uncertainties = list(unc_cfg_dict['norm'].keys())
    all_uncertainties.extend(unc_cfg_dict['shape'])
    all_uncertainties.extend(list(unc_cfg_dict['shape_and_norm'].keys()))
    #print(all_uncertainties)
    #if args.uncSource!='all':
    #    all_uncertainties = all_uncertainties[args.UncSource]
    # before, let's run over 1 channel, 1 sample, 1 uncertainty, 1 var, 1 cat

    var = 'tau1_pt'
    for sample in ['TT']: # all_samples_list+['QCD']:
        if sample == 'data': continue
        for channel in ['tauTau']:#['eTau', 'muTau', 'tauTau']:
            for category in ['inclusive','res2b','res1b']:
                unc_dict = {}
                unc_dict_chi2 = {}
                for uncSource in all_uncertainties:
                    inFileName=f'{args.inFileName}_{var}_{uncSource}.root'
                    if not os.path.exists(os.path.join(args.histDir, 'all_histograms',var,inFileName)): continue
                    inFileName_Central=f'{args.inFileName}_{var}_Central.root'
                    sample_name = sample
                    if sample in signals:
                        sample_name += 'ToHHTo2B2Tau-M'+args.mass
                    hist_central = GetHisto(channel, category, inFileName_Central, args.histDir, sample_name, 'Central','-')
                    hist_source_up = GetHisto(channel, category, inFileName, args.histDir, sample_name, uncSource,'Up')
                    hist_up_ratio = hist_source_up.Clone("hist_ratio_up")
                    hist_up_ratio.Divide(hist_central)
                    hist_source_down = GetHisto(channel, category, inFileName, args.histDir, sample_name, uncSource,'Down')
                    hist_down_ratio = hist_source_down.Clone("hist_ratio_down")
                    hist_down_ratio.Divide(hist_central)
                    GetSecondMethod(hist_up_ratio, sample, channel, category, uncSource, 'Up',unc_dict)
                    GetSecondMethod(hist_down_ratio, sample, channel, category, uncSource, 'Down',unc_dict)
                    GetChi2Method(hist_up_ratio, sample, channel, category, uncSource, 'Up',unc_dict_chi2)
                    GetChi2Method(hist_down_ratio, sample, channel, category, uncSource, 'Down',unc_dict_chi2)
                with open(f'output/fitResults/slopeInfo_{sample}_{channel}_{category}.json', 'w') as f:
                    json.dump(unc_dict, f, indent=4)
                with open(f'output/fitResults/slopeInfo_{sample}_{channel}_{category}_chi2.json', 'w') as f:
                    json.dump(unc_dict_chi2, f, indent=4)