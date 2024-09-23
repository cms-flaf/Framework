import ROOT
import sys
import os
import array

from RunKit.run_tools import ps_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *
from Analysis.HistMerger import *

def GetHistName(sample_name, sample_type, uncName, unc_scale,global_cfg_dict):
    hist_names = []
    sample_namehist = sample_type if sample_type in global_cfg_dict['sample_types_to_merge'] else sample_name
    onlyCentral = sample_name == 'data' or uncName == 'Central'
    scales = ['Central'] if onlyCentral else global_cfg_dict['scales']
    for scale in scales:
        histKey = (sample_namehist,  uncName, scale)
        histName = sample_namehist
        if not onlyCentral:
            histName = f"{sample_namehist}_{uncName}{scale}"
    return histName


#def RebinHisto(hist_initial, new_binning, sample, file_to_save_hist, verbose=False):
def RebinHisto(hist_initial, new_binning, sample, verbose=False):
    new_binning_array = array.array('d', new_binning)
    new_hist = hist_initial.Rebin(len(new_binning)-1, sample, new_binning_array)
    if sample == 'data' : new_hist.SetBinErrorOption(ROOT.TH1.kPoisson)
    #file_to_save_hist.WriteObject(new_hist, sample)
    fix_negative_contributions,debug_info,negative_bins_info = FixNegativeContributions(new_hist)
    #print(fix_negative_contributions)
    if not fix_negative_contributions:
        print(fix_negative_contributions,debug_info,negative_bins_info)
        for nbin in range(0,new_hist.GetNbinsX()+1):
            content=new_hist.GetBinContent(nbin)
            if content<0:
                print(f"for {sample}, bin {nbin} content is < 0:  {content}")
    n_finalbin = new_hist.GetBinContent(new_hist.GetNbinsX())
    n_overflow = new_hist.GetBinContent(new_hist.GetNbinsX()+1)
    new_hist.SetBinContent(new_hist.GetNbinsX(), n_finalbin+n_overflow)
    err_finalbin = new_hist.GetBinContent(new_hist.GetNbinsX())
    err_overflow = new_hist.GetBinContent(new_hist.GetNbinsX()+1)
    if n_finalbin+n_overflow > 0:
        new_hist.SetBinError(new_hist.GetNbinsX(), math.sqrt(n_finalbin+n_overflow))
    else:
        new_hist.SetBinError(new_hist.GetNbinsX(), math.sqrt(err_finalbin*err_finalbin+err_overflow*err_overflow))

    if verbose:
        for nbin in range(0, len(new_binning)):
            print(nbin, new_hist.GetBinContent(nbin))
    return new_hist

def findNewBins(hist_cfg_dict, var, channel, category):
    if 'x_rebin' not in hist_cfg_dict[var].keys():
        return hist_cfg_dict[var]['x_bins']

    if type(hist_cfg_dict[var]['x_rebin']) == list :
        return hist_cfg_dict[var]['x_rebin']

    new_dict = hist_cfg_dict[var]['x_rebin']
    if channel in new_dict.keys():
        if type(new_dict[channel]) == list:
            return new_dict[channel]
        elif type(new_dict[channel]) == dict:
            if category in new_dict[channel].keys():
                if type(new_dict[channel][category]) == list:
                    return new_dict[channel][category]

    if category in new_dict.keys():
        if type(new_dict[category]) == list:
            return new_dict[category]
        elif type(new_dict[category]) == dict:
            if channel in new_dict[category].keys():
                if type(new_dict[category][channel]) == list:
                    return new_dict[category][channel]


    return hist_cfg_dict[var]['x_rebin']['other']

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


def GetHistograms(inFile, channel, qcdregion, category, uncSource, all_sample_types,all_histlist, wantData):
    inFile = ROOT.TFile(inFile,"READ")
    dir_0 = inFile.Get(channel)
    dir_0p1 = dir_0.Get(qcdregion)
    dir_1 = dir_0p1.Get(category)
    for key in dir_1.GetListOfKeys():
        obj = key.ReadObj()
        if obj.IsA().InheritsFrom(ROOT.TH1.Class()):
            obj.SetDirectory(0)
            key_name = key.GetName()
            all_histlist[key_name] = obj
    inFile.Close()

def GetSignalHistogram(inFileSig, channel, category, uncSource, histNamesDict,all_histlist, mass):
    inFileSignal = ROOT.TFile(inFileSig,"READ")
    dir_0Signal = inFileSignal.Get(channel)
    dir_qcdSignal = dir_0Signal.Get('OS_Iso')
    dir_1Signal = dir_qcdSignal.Get(category)
    for key in dir_1Signal.GetListOfKeys():
        objSignal = key.ReadObj()
        if objSignal.IsA().InheritsFrom(ROOT.TH1.Class()):
            objSignal.SetDirectory(0)
            key_name = key.GetName()
            key_name_split = key_name.split('_')
            if uncSource == 'Central' and len(key_name_split)>1 : continue
            else:
                key_name = key_name.split('_')[0]
            sample = key_name
            key_name += f'ToHHTo2B2Tau_M-{mass}'
            #print(key_name)
            #print(histNamesDict.keys())
            #print(key_name in histNamesDict.keys())
            if key_name not in histNamesDict.keys(): continue
            sampleName, uncName, scale = histNamesDict[key_name]
            #sample_type = sample if not sample in sample_cfg_dict.keys() else sample_cfg_dict[sample]['sampleType']
            #if sample_type in signals:
            #    sample= sample_type
            if (uncName, scale) not in all_histlist.keys():
                all_histlist[(uncName, scale)] = {}
            #if args.wantData == False and (sample == 'data' or sample_type == 'data'): continue
            all_histlist[(uncName, scale)][sample] = objSignal
    inFileSignal.Close()

if __name__ == "__main__":
    import argparse
    import PlotKit.Plotter as Plotter
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--outFile', required=True)
    parser.add_argument('--inFile', required=True, type=str)
    #parser.add_argument('--inFileRadion', required=True, type=str)
    #parser.add_argument('--inFileGraviton', required=True, type=str)
    parser.add_argument('--var', required=False, type=str, default = 'tau1_pt')
    #parser.add_argument('--mass', required=False, type=int, default=1250)
    parser.add_argument('--sampleConfig', required=True, type=str)
    parser.add_argument('--globalConfig', required=True, type=str)
    parser.add_argument('--bckgConfig', required=True, type=str)
    parser.add_argument('--channel',required=False, type=str, default = 'tauTau')
    parser.add_argument('--qcdregion',required=False, type=str, default = 'OS_Iso')
    parser.add_argument('--category',required=False, type=str, default = 'inclusive')
    parser.add_argument('--wantData', required=False, type=bool, default=False)
    parser.add_argument('--wantSignals', required=False, type=bool, default=False)
    parser.add_argument('--wantQCD', required=False, type=bool, default=False)
    parser.add_argument('--wantLogScale', required=False, type=bool, default=False)
    parser.add_argument('--uncSource', required=False, type=str,default='Central')
    parser.add_argument('--year', required=False, type=str,default='2018')
    parser.add_argument('--rebin', required=False, type=bool,default=False)

    args = parser.parse_args()

    page_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/cms_stacked.yaml")
    page_cfg_custom = os.path.join(os.environ['ANALYSIS_PATH'],f'config/plot/Run2_{args.year}.yaml') # to be fixed!!
    hist_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/histograms.yaml")

    #### config opening ####
    with open(hist_cfg, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)

    #if "x_rebin" in hist_cfg_dict[args.var].keys():
    #    hist_cfg_dict["x_bins"] = hist_cfg_dict[args.var]["x_rebin"][args.channel][args.category]
    with open(page_cfg_custom, 'r') as f:
        page_cfg_custom_dict = yaml.safe_load(f)
    inputs_cfg = os.path.join(os.environ['ANALYSIS_PATH'],"config/plot/inputs.yaml")
    with open(inputs_cfg, 'r') as f:
        inputs_cfg_dict = yaml.safe_load(f)
    # if args.category == 'boosted':
    #     for input_dict in inputs_cfg_dict:
    #         if input_dict['name'] == 'GluGluToBulkGraviton' or input_dict['name'] == 'GluGluToRadion':
    #             input_dict['scale'] = 0.
    # else:
    #     for input_dict in inputs_cfg_dict:
    #         input_dict['scale'] = 1

    index_to_remove = []
    for dicti in inputs_cfg_dict:
        #print('type' in dicti.keys())
        if args.wantSignals == False and 'type' in dicti.keys() and dicti['type']=='signal':
            #print(dicti['name'])
            #print(inputs_cfg_dict.index(dicti))
            index_to_remove.append(inputs_cfg_dict.index(dicti))
        elif args.wantData == False and dicti['name'] == 'data':
            index_to_remove.append(inputs_cfg_dict.index(dicti))


    index_to_remove.sort(reverse=True)

    if index_to_remove:
        for idx in index_to_remove:
            inputs_cfg_dict.pop(idx)
    #print(inputs_cfg_dict)

    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)

    with open(args.bckgConfig, 'r') as f:
        bckg_cfg_dict = yaml.safe_load(f)

    with open(args.globalConfig, 'r') as f:
        global_cfg_dict = yaml.safe_load(f)


    samples_to_plot = [k['name'] for k in inputs_cfg_dict]
    all_histlist = {}

    signals = list(global_cfg_dict['signal_types'])
    scales = list(global_cfg_dict['scales'])

    all_samples_types = {
        'QCD':
            {
                'type':'QCD',
                'plot':'QCD'
            },
        'data':
            {
                'type':'data',
                'plot':'data'
            },
    }
    if args.qcdregion != 'OS_Iso' or args.wantQCD==False:
        all_samples_types = {
            'data':
            {
                'type':'data',
                'plot':'data'
            },
        }
        for input_dict_idx in range(0, len(inputs_cfg_dict)-1):
            input_dict = inputs_cfg_dict[input_dict_idx]
            if input_dict['name'] == 'QCD':
                del inputs_cfg_dict[input_dict_idx]

    for sample_name in bckg_cfg_dict.keys():
        if 'sampleType' not in sample_cfg_dict[sample_name].keys(): continue
        bckg_sample_type = sample_cfg_dict[sample_name]['sampleType']
        bckg_sample_name = bckg_sample_type if bckg_sample_type in global_cfg_dict['sample_types_to_merge'] else sample_name
        if bckg_sample_name in all_samples_types.keys():
            #print(f"{bckg_sample_name} already in all_samples_types, not including it")
            continue
        all_samples_types[bckg_sample_name] = {}
        all_samples_types[bckg_sample_name]['type']= bckg_sample_type
        for sample_for_plot_dict in inputs_cfg_dict:
            plot_types = sample_for_plot_dict['types']
            if bckg_sample_type in plot_types:
                all_samples_types[bckg_sample_name]['plot'] = sample_for_plot_dict['name']
        if 'plot' not in all_samples_types[bckg_sample_name].keys():
            all_samples_types[bckg_sample_name]['plot'] = 'Other'

    #print(all_samples_types)
    for sig_sample_name in sample_cfg_dict.keys():
        if 'sampleType' not in sample_cfg_dict[sig_sample_name].keys(): continue
        sig_sample_type = sample_cfg_dict[sig_sample_name]['sampleType']
        if sig_sample_type not in global_cfg_dict['signal_types']: continue
        sample_mass = sig_sample_name.split("-")[-1]
        sample_name_plot = sig_sample_type+"_"+sample_mass
        for sample_for_plot_dict in inputs_cfg_dict:
            if sample_for_plot_dict['name']== sample_name_plot:
                all_samples_types[sig_sample_name] = {
                    'type' : sig_sample_type,
                    'plot' : sample_name_plot
                }


    plotter = Plotter.Plotter(page_cfg=page_cfg, page_cfg_custom=page_cfg_custom, hist_cfg=hist_cfg_dict, inputs_cfg=inputs_cfg_dict)
    cat_txt = args.category.replace('_masswindow','')
    cat_txt = cat_txt.replace('_cat2','')
    cat_txt = cat_txt.replace('_cat3','')
    custom1= {'cat_text':cat_txt, 'ch_text':page_cfg_custom_dict['channel_text'][args.channel], 'datasim_text':'CMS data/simulation','scope_text':''}
    #print(hists_to_plot)
    if args.wantData==False:
        custom1= {'cat_text':cat_txt, 'ch_text':page_cfg_custom_dict['channel_text'][args.channel], 'datasim_text':'CMS simulation', 'scope_text':''}
    print(args.inFile)
    inFile_root = ROOT.TFile.Open(args.inFile, "READ")
    dir_0 = inFile_root.Get(args.channel)
    dir_0p1 = dir_0.Get(args.qcdregion)
    dir_1 = dir_0p1.Get(args.category)

    hists_to_plot = {}
    if args.wantLogScale:
        hist_cfg_dict[args.var]['use_log_y'] = True
        hist_cfg_dict[args.var]['max_y_sf'] = 2000.2
    else:
        hist_cfg_dict[args.var]['use_log_y'] = False
        hist_cfg_dict[args.var]['max_y_sf'] = 1.4
    print(hist_cfg_dict[args.var]['max_y_sf'])
    rebin_condition = args.rebin and 'x_rebin' in hist_cfg_dict[args.var].keys()
    bins_to_compute = hist_cfg_dict[args.var]['x_bins']
    if rebin_condition :
        bins_to_compute = findNewBins(hist_cfg_dict,args.var,args.channel,args.category)
        new_bins = getNewBins(bins_to_compute)

    #file_to_save_hist = ROOT.TFile(f"histrebinned_{args.var}_{args.channel}_{args.category}_{args.qcdregion}.root", "RECREATE")
    for sample_name,sample_content in all_samples_types.items():
        sample_type = sample_content['type']
        sample_plot_name = sample_content['plot']
        #print(sample_plot_name)
        if args.uncSource != 'Central': continue # to be fixed
        sample_histname = (GetHistName(sample_name, sample_type, 'Central','Central', global_cfg_dict))
        if sample_histname not in dir_1.GetListOfKeys():
            print(f"ERRORE: {sample_histname} non è nelle keys")
            continue
        obj = dir_1.Get(sample_histname)
        if not obj.IsA().InheritsFrom(ROOT.TH1.Class()):
            print(f"ERRORE: {sample_histname} non è un istogramma")
        obj.SetDirectory(0)
        if sample_plot_name not in hists_to_plot.keys():
            hists_to_plot[sample_plot_name] = RebinHisto(obj, new_bins, sample_name) if rebin_condition else obj
        else:
            if rebin_condition:
                hists_to_plot[sample_plot_name].Add(hists_to_plot[sample_plot_name],RebinHisto(obj, new_bins,sample_name))
            else:
                hists_to_plot[sample_plot_name].Add(hists_to_plot[sample_plot_name],obj)
    #print(hists_to_plot)
    #print(all_samples_types.keys())

    plotter.plot(args.var, hists_to_plot, args.outFile, want_data = args.wantData, custom=custom1)
    inFile_root.Close()
    #file_to_save_hist.Close()
    print(args.outFile)
    #print(args.outFile)
