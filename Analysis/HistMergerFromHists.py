import ROOT
import sys
import os
import math
import shutil
import time
from FLAF.RunKit.run_tools import ps_call

#### for the moment only central variation ####

if __name__ == "__main__":
    sys.path.append(os.environ["ANALYSIS_PATH"])

import FLAF.Common.Utilities as Utilities
import FLAF.Common.Setup as Setup
from FLAF.Common.HistHelper import *
from FLAF.Analysis.QCD_estimation import *

import importlib


def checkFile(inFileRoot, channels, qcdRegions, categories):
    keys_channels = [str(key.GetName()) for key in inFileRoot.GetListOfKeys()]
    for channel in channels:
        if channel not in keys_channels:
            return False
    for channel in channels:
        dir_0 = inFileRoot.Get(channel)
        keys_qcdRegions = [str(key.GetName()) for key in dir_0.GetListOfKeys()]
        if not all(element in keys_qcdRegions for element in qcdRegions):
            print("check list not worked for qcdRegions")
            return False
        for qcdRegion in qcdRegions:
            dir_1 = dir_0.Get(qcdRegion)
            keys_categories = [str(key.GetName()) for key in dir_1.GetListOfKeys()]
            if not all(element in keys_categories for element in categories):
                print("check list not worked for categories")
                return False
            for cat in categories:
                dir_2 = dir_1.Get(cat)
                keys_histograms = [str(key.GetName()) for key in dir_2.GetListOfKeys()]
                if not keys_histograms:
                    return False
    return True


def MergeHistogramsPerType(all_hists_dict):
    old_hist_dict = all_hists_dict.copy()
    all_hists_dict.clear()
    for sample_type in old_hist_dict.keys():
        if sample_type == "data":
            print(f"DURING MERGE HISTOGRAMS, sample_type is data")
        for var in old_hist_dict[sample_type].keys():
            if var not in all_hists_dict.keys():
                all_hists_dict[var] = {}
            if sample_type not in all_hists_dict[var].keys():
                all_hists_dict[var][sample_type] = {}
            for key_name, histlist in old_hist_dict[sample_type][var].items():
                final_hist = histlist[0]
                objsToMerge = ROOT.TList()
                for hist in histlist[1:]:
                    objsToMerge.Add(hist)
                final_hist.Merge(objsToMerge)
                all_hists_dict[var][sample_type][key_name] = final_hist
                # if len(histlist)!=1:
                # print(f"for {sample_type} the lenght of histlist is {len(histlist)}")


def GetBTagWeightDict(
    var, all_hists_dict, categories, boosted_categories, boosted_variables
):
    all_hists_dict_1D = {}
    for sample_type in all_hists_dict.keys():
        # print(sample_type)
        all_hists_dict_1D[sample_type] = {}
        for key_name, histogram in all_hists_dict[sample_type].items():
            (key_1, key_2) = key_name

            if var not in boosted_variables:
                ch, reg, cat = key_1
                uncName, scale = key_2
                key_tuple_num = ((ch, reg, "btag_shape"), key_2)
                key_tuple_den = ((ch, reg, "inclusive"), key_2)
                ratio_num_hist = (
                    all_hists_dict[sample_type][key_tuple_num]
                    if key_tuple_num in all_hists_dict[sample_type].keys()
                    else None
                )
                ratio_den_hist = (
                    all_hists_dict[sample_type][key_tuple_den]
                    if key_tuple_den in all_hists_dict[sample_type].keys()
                    else None
                )
                num = ratio_num_hist.Integral(0, ratio_num_hist.GetNbinsX() + 1)
                den = ratio_den_hist.Integral(0, ratio_den_hist.GetNbinsX() + 1)
                ratio = 0.0
                if ratio_den_hist.Integral(0, ratio_den_hist.GetNbinsX() + 1) != 0:
                    ratio = ratio_num_hist.Integral(
                        0, ratio_num_hist.GetNbinsX() + 1
                    ) / ratio_den_hist.Integral(0, ratio_den_hist.GetNbinsX() + 1)
                if (
                    cat in boosted_categories
                    or cat.startswith("btag_shape")
                    or cat.startswith("baseline")
                ):
                    ratio = 1
                # print(f"for cat {cat} setting ratio is {ratio}")
                histogram.Scale(ratio)
            else:
                print(
                    f"for var {var} no ratio is considered and the histogram is directly saved"
                )

            all_hists_dict_1D[sample_type][key_name] = histogram
            # print(sample_type, key_name, histogram.Integral(0, histogram.GetNbinsX()+1))
    return all_hists_dict_1D


if __name__ == "__main__":
    import argparse
    import yaml

    parser = argparse.ArgumentParser()
    parser.add_argument("--inDir", required=True, type=str)
    parser.add_argument("--outDir", required=True, type=str)
    parser.add_argument("--period", required=True, type=str)
    parser.add_argument("--uncSource", required=False, type=str, default="Central")
    parser.add_argument("--channels", required=False, type=str, default="")

    args = parser.parse_args()
    startTime = time.time()

    setup = Setup.Setup(os.environ["ANALYSIS_PATH"], args.period)
    sample_cfg_dict = setup.samples
    global_cfg_dict = setup.global_params
    bckg_cfg_dict = setup.bckg_config
    sig_cfg_dict = setup.signal_config
    unc_cfg_dict = setup.weights_config

    analysis_import = global_cfg_dict["analysis_import"]
    analysis = importlib.import_module(f"{analysis_import}")

    all_samples_dict = bckg_cfg_dict.copy()
    all_samples_dict.update(sig_cfg_dict)
    data_dict = {"data": {"sampleType": "data"}}
    all_samples_dict.update(data_dict)  # add data to the samples dict
    # data_dict = {"data": {"sampleType": "data"}} if needed other custom dict need to think how to include it. --> maybe in config?

    uncNameTypes = GetUncNameTypes(unc_cfg_dict)

    if args.uncSource != "Central" and args.uncSource not in uncNameTypes:
        print("unknown unc source {args.uncSource}")

    categories = list(global_cfg_dict["categories"])

    # this part is analysis dependent. Need to be put in proper place
    # boosted categories and QCD regions --> e.g. for hmm no boosted categories and no QCD regions but muMu mass regions
    # instead, better to define custom categories/regions
    # boosted_categories = list(
    #     global_cfg_dict.get("boosted_categories", [])
    # )  # list(global_cfg_dict['boosted_categories'])
    # Controlregions = list(global_cfg_dict['ControlRegions']) #Later maybe we want to separate Controls from QCDs

    custom_regions_name = global_cfg_dict.get(
        "custom_regions", None
    )  # can be extended to list of names, if for example adding QCD regions + other control regions
    custom_categories_name = global_cfg_dict.get(
        "custom_categories", None
    )  # can be extended to list of names
    custom_categories = []
    custom_regions = []
    if custom_regions_name:
        custom_regions = list(global_cfg_dict.get(custom_regions_name, []))
        if not custom_regions:
            print("No custom regions found")
    if custom_categories_name:
        custom_categories = list(global_cfg_dict.get(custom_categories_name, []))
        if not custom_categories:
            print("No custom categories found")

    all_categories = categories + custom_categories

    setup.global_params["channels_to_consider"] = (
        args.channels.split(",")
        if args.channels
        else setup.global_params["channelSelection"]
    )
    channels = setup.global_params["channels_to_consider"]

    custom_variables = global_cfg_dict.get(
        "var_only_custom", {}
    )  # e.g. var only boosted. Will be constructed as:
    # { "cat == boosted" : [particleNet.. ], "cat != boosted" : [b1_.. ]  }
    # replacing this part:
    # if args.var.startswith("b1") or args.var.startswith("b2"):
    #     all_categories = categories
    unc_exception = global_cfg_dict.get(
        "unc_exception", {}
    )  # e.g. boosted categories with unc list to not consider
    # { "cat == boosted" : [JER, JES] }
    # unc_to_not_consider_boosted = list(
    #     global_cfg_dict.get("unc_to_not_consider_boosted", [])
    # )

    sample_types_to_merge = list(global_cfg_dict["sample_types_to_merge"])
    scales = list(global_cfg_dict["scales"])

    all_hists_dict = {}
    regions = []
    # file structure : channel - region - category - varName_unc (if not central, else only varName)
    for sample_name in all_samples_dict.keys():
        if unc_exception.keys():
            for unc_condition in unc_exception.keys():
                if unc_condition and args.uncSource in unc_exception[key]:
                    continue
        inFile_path = os.path.join(args.inDir, f"{sample_name}.root")
        if not os.path.exists(inFile_path):
            print(
                f"input file for sample {sample_name} (with path= {inFile_path}) does not exist, skipping"
            )
            continue
        inFile = ROOT.TFile.Open(inFile_path, "READ")
        if inFile.IsZombie():
            inFile.Close()
            os.remove(inFile_path)
            ignore_samples.append(sample_name)
            raise RuntimeError(f"{inFile_path} is Zombie")
        if not checkFile(inFile, channels, custom_regions, all_categories):
            print(f"{sample_name} has void file")
            ignore_samples.append(sample_name)
            inFileRoot.Close()
            continue
        sample_type = all_samples_dict[sample_name]["sampleType"]
        if sample_type not in all_hists_dict.keys():
            all_hists_dict[sample_type] = {}

        for channel in channels:
            dir_0 = inFile.Get(channel)
            for region in custom_regions:
                dir_1 = dir_0.Get(region)
                for cat in all_categories:
                    dir_2 = dir_1.Get(cat)
                    for var in global_cfg_dict["vars_to_save"]:
                        if not dir_2.GetListOfKeys().Contains(var):
                            print(f"var {var} not found in {inFile_path}, skipping")
                            continue
                        full_name = f"{var}"
                        if custom_variables.keys():
                            for condition in custom_variables.keys():
                                if condition and var not in custom_variables[key]:
                                    continue
                        if var not in all_hists_dict[sample_type].keys():
                            all_hists_dict[sample_type][var] = {}
                        if args.uncSource != "Central":
                            full_name += f"_{args.uncSource}"
                            for scale in scales:
                                full_name += f"_{scale}"
                                var_hist = dir_2.Get(full_name)
                                var_hist.SetDirectory(0)
                                if (
                                    channel,
                                    region,
                                    cat,
                                    var,
                                    args.uncSource,
                                    scale,
                                ) not in all_hists_dict[sample_type][var].keys():
                                    all_hists_dict[sample_type][var][
                                        (
                                            (channel, region, cat),
                                            (args.uncSource, scale),
                                        )
                                    ] = []
                                all_hists_dict[sample_type][var][
                                    ((channel, region, cat), (args.uncSource, scale))
                                ].append(var_hist)
                        else:
                            var_hist = dir_2.Get(full_name)
                            var_hist.SetDirectory(0)
                            if (channel, region, cat) not in all_hists_dict[
                                sample_type
                            ][var].keys():
                                all_hists_dict[sample_type][var][
                                    ((channel, region, cat), ("Central", "Central"))
                                ] = []
                            all_hists_dict[sample_type][var][
                                ((channel, region, cat), ("Central", "Central"))
                            ].append(var_hist)

    MergeHistogramsPerType(all_hists_dict)

    # here there should be the custom applications - e.g. GetBTagWeightDict, AddQCDInHistDict, etc.
    # analysis.ApplyMergeCustomisations() # --> here go the QCD and bTag functions
    """
    if global_cfg_dict["ApplyBweight"] == True:
        all_hists_dict_1D = GetBTagWeightDict(
            args.var, all_hists_dict, categories, boosted_categories, boosted_variables
        )
    else:
        all_hists_dict_1D = all_hists_dict

    if not analysis_import == "Analysis.H_mumu":
        fixNegativeContributions = False
        error_on_qcdnorm, error_on_qcdnorm_varied = AddQCDInHistDict(
            args.var,
            all_hists_dict_1D,
            channels,
            all_categories,
            args.uncSource,
            all_samples_types.keys(),
            scales,
            wantNegativeContributions=False,
        )
    """

    for var in all_hists_dict.keys():
        outFileName = os.path.join(args.outDir, f"{var}.root")
        outFile = ROOT.TFile(outFileName, "RECREATE")
        for sample_type in all_hists_dict[var].keys():
            for key in all_hists_dict[var][sample_type].keys():
                (
                    (
                        channel,
                        region,
                        cat,
                    ),
                    (uncName, uncScale),
                ) = key
                # here there can be some custom requirements - e.g. regions / categories to not merge, samples to ignore
                dirStruct = (channel, region, cat)
                dir_name = "/".join(dirStruct)
                dir_ptr = Utilities.mkdir(outFile, dir_name)
                hist = all_hists_dict[var][sample_type][key]
                hist_name = sample_type
                if uncName != args.uncSource:
                    continue
                if uncName != "Central":
                    if sample_type == "data":
                        continue
                    if uncScale == "Central":
                        continue
                    hist_name += f"_{uncName}_{uncScale}"
                else:
                    if uncScale != "Central":
                        continue
                hist.SetTitle(hist_name)
                hist.SetName(hist_name)
                dir_ptr.WriteTObject(hist, hist_name, "Overwrite")
        outFile.Close()
    executionTime = time.time() - startTime

    print("Execution time in seconds: " + str(executionTime))
