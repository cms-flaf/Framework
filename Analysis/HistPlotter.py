import ROOT
import sys
import os
import array

from FLAF.RunKit.run_tools import ps_call

if __name__ == "__main__":
    sys.path.append(os.environ["ANALYSIS_PATH"])

import FLAF.Common.Utilities as Utilities
from FLAF.Common.HistHelper import *
from FLAF.Analysis.HistMerger import *


def GetHistName(sample_name, sample_type, uncName, unc_scale, global_cfg_dict):
    hist_names = []
    sample_namehist = (
        sample_type
        if sample_type in global_cfg_dict["sample_types_to_merge"]
        else sample_name
    )
    onlyCentral = sample_name == "data" or uncName == "Central"
    scales = ["Central"] if onlyCentral else global_cfg_dict["scales"]
    for scale in scales:
        histKey = (sample_namehist, uncName, scale)
        histName = sample_namehist
        if not onlyCentral:
            histName = f"{sample_namehist}_{uncName}{scale}"
    return histName


def RebinHisto(hist_initial, new_binning, sample, wantOverflow=True, verbose=False):
    new_binning_array = array.array("d", new_binning)
    new_hist = hist_initial.Rebin(len(new_binning) - 1, sample, new_binning_array)
    if sample == "data":
        new_hist.SetBinErrorOption(ROOT.TH1.kPoisson)
    if wantOverflow:
        n_finalbin = new_hist.GetBinContent(new_hist.GetNbinsX())
        n_overflow = new_hist.GetBinContent(new_hist.GetNbinsX() + 1)
        new_hist.SetBinContent(new_hist.GetNbinsX(), n_finalbin + n_overflow)
        err_finalbin = new_hist.GetBinError(new_hist.GetNbinsX())
        err_overflow = new_hist.GetBinError(new_hist.GetNbinsX() + 1)
        new_hist.SetBinError(
            new_hist.GetNbinsX(),
            math.sqrt(err_finalbin * err_finalbin + err_overflow * err_overflow),
        )

    if verbose:
        for nbin in range(0, len(new_binning)):
            print(
                f"nbin = {nbin}, content = {new_hist.GetBinContent(nbin)}, error {new_hist.GetBinError(nbin)}"
            )
    fix_negative_contributions, debug_info, negative_bins_info = (
        FixNegativeContributions(new_hist)
    )
    if not fix_negative_contributions:
        print("negative contribution not fixed")
        print(fix_negative_contributions, debug_info, negative_bins_info)
        for nbin in range(0, new_hist.GetNbinsX() + 1):
            content = new_hist.GetBinContent(nbin)
            if content < 0:
                print(f"for {sample}, bin {nbin} content is < 0:  {content}")

    return new_hist


def findNewBins(hist_cfg_dict, var, channel, category):
    if "x_rebin" not in hist_cfg_dict[var].keys():
        return hist_cfg_dict[var]["x_bins"]

    if type(hist_cfg_dict[var]["x_rebin"]) == list:
        return hist_cfg_dict[var]["x_rebin"]

    new_dict = hist_cfg_dict[var]["x_rebin"]
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
    return hist_cfg_dict[var]["x_rebin"]["other"]


def getNewBins(bins):
    if type(bins) == list:
        final_bins = bins
    else:
        n_bins, bin_range = bins.split("|")
        start, stop = bin_range.split(":")
        bin_width = (float(stop) - float(start)) / int(n_bins)
        final_bins = []
        bin_center = float(start)
        while bin_center >= float(start) and bin_center <= float(stop):
            final_bins.append(bin_center)
            bin_center = bin_center + bin_width
    return final_bins


def GetHistograms(
    inFile,
    channel,
    qcdregion,
    category,
    uncSource,
    all_sample_types,
    all_histlist,
    wantData,
):
    inFile = ROOT.TFile(inFile, "READ")
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


def GetSignalHistogram(
    inFileSig, channel, category, uncSource, histNamesDict, all_histlist, mass
):
    inFileSignal = ROOT.TFile(inFileSig, "READ")
    dir_0Signal = inFileSignal.Get(channel)
    dir_qcdSignal = dir_0Signal.Get("OS_Iso")
    dir_1Signal = dir_qcdSignal.Get(category)
    for key in dir_1Signal.GetListOfKeys():
        objSignal = key.ReadObj()
        if objSignal.IsA().InheritsFrom(ROOT.TH1.Class()):
            objSignal.SetDirectory(0)
            key_name = key.GetName()
            key_name_split = key_name.split("_")
            if uncSource == "Central" and len(key_name_split) > 1:
                continue
            else:
                key_name = key_name.split("_")[0]
            sample = key_name
            key_name += f"ToHHTo2B2Tau_M-{mass}"
            if key_name not in histNamesDict.keys():
                continue
            sampleName, uncName, scale = histNamesDict[key_name]
            if (uncName, scale) not in all_histlist.keys():
                all_histlist[(uncName, scale)] = {}
            all_histlist[(uncName, scale)][sample] = objSignal
    inFileSignal.Close()


if __name__ == "__main__":
    import argparse
    import FLAF.PlotKit.Plotter as Plotter
    import yaml

    parser = argparse.ArgumentParser()
    parser.add_argument("--outFile", required=True)
    parser.add_argument("--inFile", required=True, type=str)
    parser.add_argument("--var", required=False, type=str, default="tau1_pt")
    parser.add_argument("--sigConfig", required=True, type=str)
    parser.add_argument("--globalConfig", required=True, type=str)
    parser.add_argument("--bckgConfig", required=True, type=str)
    parser.add_argument("--channel", required=False, type=str, default="tauTau")
    parser.add_argument("--qcdregion", required=False, type=str, default="OS_Iso")
    parser.add_argument("--category", required=False, type=str, default="inclusive")
    parser.add_argument("--wantData", required=False, action="store_true")
    parser.add_argument("--wantSignals", required=False, action="store_true")
    parser.add_argument("--wantQCD", required=False, type=bool, default=False)
    parser.add_argument("--wantOverflow", required=False, type=bool, default=False)
    parser.add_argument("--wantLogScale", required=False, type=str, default="")
    parser.add_argument("--uncSource", required=False, type=str, default="Central")
    parser.add_argument("--year", required=False, type=str, default="Run2_2018")
    parser.add_argument("--rebin", required=False, type=bool, default=False)
    parser.add_argument("--analysis", required=False, type=str, default="")

    args = parser.parse_args()

    page_cfg = os.path.join(
        os.environ["ANALYSIS_PATH"], "config", "plot/cms_stacked.yaml"
    )
    page_cfg_custom = os.path.join(
        os.environ["ANALYSIS_PATH"], f"config", f"plot/{args.year}.yaml"
    )  # to be fixed!!
    hist_cfg = os.path.join(
        os.environ["ANALYSIS_PATH"], "config", "plot/histograms.yaml"
    )

    #### config opening ####
    with open(hist_cfg, "r") as f:
        hist_cfg_dict = yaml.safe_load(f)
    with open(page_cfg, "r") as f:
        page_cfg_dict = yaml.safe_load(f)
    with open(page_cfg_custom, "r") as f:
        page_cfg_custom_dict = yaml.safe_load(f)
    inputs_cfg = os.path.join(
        os.environ["ANALYSIS_PATH"], "config", "plot", "inputs.yaml"
    )
    with open(inputs_cfg, "r") as f:
        inputs_cfg_dict = yaml.safe_load(f)

    index_to_remove = []
    for dicti in inputs_cfg_dict:
        if (
            args.wantSignals == False
            and "type" in dicti.keys()
            and dicti["type"] == "signal"
        ):
            index_to_remove.append(inputs_cfg_dict.index(dicti))
        elif args.wantData == False and dicti["name"] == "data":
            index_to_remove.append(inputs_cfg_dict.index(dicti))

    index_to_remove.sort(reverse=True)

    if index_to_remove:
        for idx in index_to_remove:
            inputs_cfg_dict.pop(idx)

    with open(args.sigConfig, "r") as f:
        sig_cfg_dict = yaml.safe_load(f)

    with open(args.bckgConfig, "r") as f:
        bckg_cfg_dict = yaml.safe_load(f)

    with open(args.globalConfig, "r") as f:
        global_cfg_dict = yaml.safe_load(f)

    samples_to_plot = [k["name"] for k in inputs_cfg_dict]
    all_histlist = {}

    signals = list(global_cfg_dict["signal_types"])
    scales = list(global_cfg_dict["scales"])

    all_samples_types = {
        "QCD": {"type": "QCD", "plot": "QCD"},
        "data": {"type": "data", "plot": "data"},
    }
    if args.qcdregion != "OS_Iso" or args.wantQCD == False:
        all_samples_types = {
            "data": {"type": "data", "plot": "data"},
        }
        for input_dict_idx in range(0, len(inputs_cfg_dict) - 1):
            input_dict = inputs_cfg_dict[input_dict_idx]
            if input_dict["name"] == "QCD":
                del inputs_cfg_dict[input_dict_idx]

    for sample_name in bckg_cfg_dict.keys():
        if "sampleType" not in bckg_cfg_dict[sample_name].keys():
            continue
        bckg_sample_type = bckg_cfg_dict[sample_name]["sampleType"]
        bckg_sample_name = (
            bckg_sample_type
            if bckg_sample_type in global_cfg_dict["sample_types_to_merge"]
            else sample_name
        )
        if bckg_sample_name in all_samples_types.keys():
            continue
        all_samples_types[bckg_sample_name] = {}
        all_samples_types[bckg_sample_name]["type"] = bckg_sample_type
        for sample_for_plot_dict in inputs_cfg_dict:
            plot_types = sample_for_plot_dict["types"]
            if bckg_sample_type in plot_types:
                all_samples_types[bckg_sample_name]["plot"] = sample_for_plot_dict[
                    "name"
                ]
        if "plot" not in all_samples_types[bckg_sample_name].keys():
            all_samples_types[bckg_sample_name]["plot"] = "Other"

    for sig_sample_name in sig_cfg_dict.keys():
        if "sampleType" not in sig_cfg_dict[sig_sample_name].keys():
            continue
        sig_sample_type = sig_cfg_dict[sig_sample_name]["sampleType"]
        if sig_sample_type not in global_cfg_dict["signal_types"]:
            continue
        for sample_for_plot_dict in inputs_cfg_dict:
            if sample_for_plot_dict["name"] == sig_sample_name:
                all_samples_types[sig_sample_name] = {
                    "type": sig_sample_type,
                    "plot": sig_sample_name,
                }

    plotter = Plotter.Plotter(
        page_cfg=page_cfg,
        page_cfg_custom=page_cfg_custom,
        hist_cfg=hist_cfg_dict,
        inputs_cfg=inputs_cfg_dict,
    )
    cat_txt = args.category.replace("_masswindow", "")
    cat_txt = cat_txt.replace("_cat2", "")
    cat_txt = cat_txt.replace("_cat3", "")
    custom1 = {
        "cat_text": cat_txt,
        "ch_text": page_cfg_custom_dict["channel_text"][args.channel],
        "datasim_text": "CMS " + page_cfg_dict["scope_text"]["text"],
        "scope_text": "",
    }
    if args.wantData == False:
        custom1 = {
            "cat_text": cat_txt,
            "ch_text": page_cfg_custom_dict["channel_text"][args.channel],
            "datasim_text": "CMS simulation",
            "scope_text": "",
        }
    inFile_root = ROOT.TFile.Open(args.inFile, "READ")
    dir_0 = inFile_root.Get(args.channel)
    keys_0 = [str(k) for k in dir_0.GetListOfKeys()]
    dir_0p1 = dir_0.Get(args.qcdregion)
    keys_0p1 = [str(k) for k in dir_0p1.GetListOfKeys()]
    dir_1 = dir_0p1.Get(args.category)
    keys_1 = [str(k) for k in dir_1.GetListOfKeys()]
    # dir_1 = dir_0.Get(args.category) # --> uncomment if QCD regions are not included in the histograms
    # hist_cfg_dict[args.var]['max_y_sf'] = 1.4
    # hist_cfg_dict[args.var]['use_log_y'] = False
    # hist_cfg_dict[args.var]['use_log_x'] = False

    hists_to_plot_unbinned = {}
    if args.wantLogScale == "y":
        hist_cfg_dict[args.var]["use_log_y"] = True
        hist_cfg_dict[args.var]["max_y_sf"] = 2000.2
    if args.wantLogScale == "x":
        hist_cfg_dict[args.var]["use_log_x"] = True
    if args.wantLogScale == "xy":
        hist_cfg_dict[args.var]["use_log_y"] = True
        hist_cfg_dict[args.var]["max_y_sf"] = 2000.2
        hist_cfg_dict[args.var]["use_log_x"] = True

    rebin_condition = args.rebin and "x_rebin" in hist_cfg_dict[args.var].keys()
    bins_to_compute = hist_cfg_dict[args.var]["x_bins"]

    if rebin_condition:
        bins_to_compute = findNewBins(
            hist_cfg_dict, args.var, args.channel, args.category
        )
    new_bins = getNewBins(bins_to_compute)

    for sample_name, sample_content in all_samples_types.items():
        sample_type = sample_content["type"]
        sample_plot_name = sample_content["plot"]
        if args.uncSource != "Central":
            continue  # to be fixed

        sample_histname = GetHistName(
            sample_name, sample_type, "Central", "Central", global_cfg_dict
        )
        if sample_histname not in dir_1.GetListOfKeys():
            print(f"ERRORE: {sample_histname} non è nelle keys")
            continue
        obj = dir_1.Get(sample_histname)
        if not obj.IsA().InheritsFrom(ROOT.TH1.Class()):
            print(f"ERRORE: {sample_histname} non è un istogramma")
        obj.SetDirectory(0)

        if sample_plot_name not in hists_to_plot_unbinned.keys():
            hists_to_plot_unbinned[sample_plot_name] = obj
        else:
            hists_to_plot_unbinned[sample_plot_name].Add(
                hists_to_plot_unbinned[sample_plot_name], obj
            )
    hists_to_plot_binned = {}
    for hist_key, hist_unbinned in hists_to_plot_unbinned.items():
        old_hist = hist_unbinned
        new_hist = RebinHisto(
            old_hist, new_bins, hist_key, wantOverflow=args.wantOverflow, verbose=False
        )
        hists_to_plot_binned[hist_key] = new_hist if rebin_condition else old_hist

    plotter.plot(
        args.var,
        hists_to_plot_binned,
        args.outFile,
        want_data=args.wantData,
        custom=custom1,
    )
    inFile_root.Close()
    print(args.outFile)
