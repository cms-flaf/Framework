import datetime
import os
import sys
import ROOT
import shutil
import zlib
# import fastcrc
import json


if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import FLAF.Common.Utilities as Utilities
from FLAF.Common.Setup import Setup
import importlib

#ROOT.EnableImplicitMT(1)
ROOT.EnableThreadSafety()


def DefineBinnedColumn(dfw, hist_cfg_dict, var, weight_name):
    axis_name = f"{var}_axis"
    func_name = f"get_{var}_bin"
    hist_name = f"{var}_hist"

    if isinstance(hist_cfg_dict[var]['x_bins'], list):
        edges = hist_cfg_dict[var]['x_bins']
        edges_cpp = "{" + ",".join(map(str, edges)) + "}"
        nbins = len(edges) - 1

        ROOT.gInterpreter.Declare(f"""
            #include <vector>
            #include "TH1F.h"
            #include "ROOT/RVec.hxx"

            static std::vector<double> {axis_name}_edges = {edges_cpp};
            static TH1F* {hist_name} = new TH1F("{hist_name}", "{hist_name}", {nbins}, {axis_name}_edges.data());

            int {func_name}(double x, double weight) {{
                {hist_name}->Fill(x, weight);
                return {hist_name}->GetXaxis()->FindBin(x) - 1;
            }}

            template<typename T>
            ROOT::VecOps::RVec<int> {func_name}(ROOT::VecOps::RVec<T> xvec, T weight) {{
                ROOT::VecOps::RVec<int> out;
                for (auto& x : xvec) {{
                    {hist_name}->Fill(x, weight);
                    out.push_back({hist_name}->GetXaxis()->FindBin(x) - 1);
                }}
                return out;
            }}
        """)
    else:
        n_bins, bin_range = hist_cfg_dict[var]['x_bins'].split('|')
        start, stop = bin_range.split(':')

        ROOT.gInterpreter.Declare(f"""
            #include <vector>
            #include "TH1F.h"
            #include "ROOT/RVec.hxx"

            static TH1F* {hist_name} = new TH1F("{hist_name}", "{hist_name}", {n_bins}, {start}, {stop});

            int {func_name}(double x, double weight) {{
                {hist_name}->Fill(x, weight);
                return {hist_name}->GetXaxis()->FindBin(x) - 1;
            }}

            template<typename T>
            ROOT::VecOps::RVec<int> {func_name}(ROOT::VecOps::RVec<T> xvec, T weight) {{
                ROOT::VecOps::RVec<int> out;
                for (auto& x : xvec) {{
                    {hist_name}->Fill(x, weight);
                    out.push_back({hist_name}->GetXaxis()->FindBin(x) - 1);
                }}
                return out;
            }}
        """)

    dfw.df = dfw.df.Define(f"{var}_bin", f"{func_name}({var}, {weight_name})")


def createHistTuple(inFile, cacheFile, treeName, outFile, setup, hist_cfg_dict,unc_cfg_dict, snapshotOptions,range, evtIds,  compute_unc_variations, compute_rel_weights, histTupleDef):
    start_time = datetime.datetime.now()
    compression_settings = snapshotOptions.fCompressionAlgorithm * 100 + snapshotOptions.fCompressionLevel
    histTupleDef.Initialize()
    histTupleDef.analysis_setup(setup)
    isData = True if setup.global_params["dataset"]=='data' else False
    isCentral = True # for the moment --> there will be changes when including uncs

    sample_type = 'data' if isData else setup.samples[setup.global_params["dataset"]]["sampleType"]
    snaps = []
    reports = []
    outfilesNames = []

    df_central = ROOT.RDataFrame(treeName, inFile)
    df_cache = None
    if cacheFile:
        df_cache = ROOT.RDataFrame(treeName, cacheFile)

    ROOT.RDF.Experimental.AddProgressBar(df_central)
    if range is not None:
        df_central = df_central.Range(range)
    if len(evtIds) > 0:
        df_central = df_central.Filter(f"static const std::set<ULong64_t> evts = {{ {evtIds} }}; return evts.count(event) > 0;")

    # Central + weights shifting:

    dfw_central = histTupleDef.GetDfw(df_central, df_cache, setup.global_params)
    all_rel_uncs_to_compute = ['Central']
    if compute_rel_weights: all_rel_uncs_to_compute.extend(unc_cfg_dict['norm'].keys())
    for unc in all_rel_uncs_to_compute:
        scales = setup.global_params['scales']
        if unc == 'Central':
            scales = ['Central']
        for scale in scales:
            final_weight_name = "weight_for_hists"
            histTupleDef.DefineWeightForHistograms(dfw_central, unc, scale, sample_type, unc_cfg_dict, hist_cfg_dict, setup.global_params, final_weight_name)
            dfw_central.colToSave.append(final_weight_name)
        vars_to_save = []
        if type(setup.global_params["vars_to_save"]) == list:
            vars_to_save = setup.global_params["vars_to_save"]
        elif type(setup.global_params["vars_to_save"]) == dict:
            vars_to_save = setup.global_params["vars_to_save"].keys()
        for var in vars_to_save:
            DefineBinnedColumn(dfw_central,  hist_cfg_dict, var, final_weight_name)
            dfw_central.colToSave.append(f"{var}_bin")
    if setup.global_params["additional_vars"]:
        additional_vars = []
        if type(setup.global_params["additional_vars"]) == list:
            additional_vars = setup.global_params["additional_vars"]
        elif type(setup.global_params["additional_vars"]) == dict:
            additional_vars = setup.global_params["additional_vars"].keys()
        dfw_central.colToSave.extend(additional_vars)
    varToSave = Utilities.ListToVector(dfw_central.colToSave)
    # reports.append(dfw_central.df.Report()) --> enable if we want reports
    snaps.append(dfw_central.df.Snapshot(treeName, outFile, varToSave, snapshotOptions))

    # scratch to include the shifted trees --> TO BE IMPLEMENTED

    # compute_variations = ( compute_unc_variations or compute_rel_weights ) and dataset != 'data'
    # if compute_unc_variations:
    #     all_dataframes[key_central][0] = createCentralQuantities(all_dataframes[key_central][0], col_types_central, col_names_central)
    #     # If original count is already 0, then you don't need to raise error
    #     if all_dataframes[key_central][0].Filter("map_placeholder > 0").Count().GetValue() <= 0 : raise RuntimeError("no events passed map placeolder")
    # # norm weight histograms



    # if is_central:
    #     nEventsAfterFilter = dfw.df.Count()#.GetValue()
    if snapshotOptions.fLazy == True:
        ROOT.RDF.RunGraphs(snaps)
    end_time = datetime.datetime.now()
    # hist_time = ROOT.TH1D(f"time", f"time", 1, 0, 1)
    # hist_time.SetBinContent(1, (end_time - start_time).total_seconds())
    # for index,fileName in enumerate(outfilesNames):
        # outputRootFile= ROOT.TFile(fileName, "UPDATE", "", compression_settings)
    #     rep = ReportTools.SaveReport(reports[index].GetValue(), reoprtName=f"Report")
    #     outputRootFile.WriteTObject(rep, f"Report", "Overwrite")
    #     if index==0:
    #         outputRootFile.WriteTObject(hist_time, f"runtime", "Overwrite")
    #     outputRootFile.Close()
        # if print_cutflow:
        #     report.Print()




if __name__ == "__main__":
    import argparse
    import os
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--period', required=True, type=str)
    parser.add_argument('--inFile', required=True, type=str)
    parser.add_argument('--outFile', required=True, type=str)
    parser.add_argument('--cacheFile', required=False, type=str, default=None)
    parser.add_argument('--dataset', required=True, type=str)
    parser.add_argument('--histConfig', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--histTupleDef', required=True, type=str)
    parser.add_argument('--compute_unc_variations', type=bool, default=False)
    parser.add_argument('--compute_rel_weights', type=bool, default=False)
    parser.add_argument('--customisations', type=str, default=None)
    parser.add_argument('--compressionLevel', type=int, default=4)
    parser.add_argument('--compressionAlgo', type=str, default="ZLIB")
    parser.add_argument('--channels', type=str, default=None)
    parser.add_argument('--nEvents', type=int, default=None)
    parser.add_argument('--evtIds', type=str, default='')

    args = parser.parse_args()

    setup = Setup.getGlobal(os.environ['ANALYSIS_PATH'], args.period, args.customisations)

    customisations_dict = {}
    if args.customisations:
        customisations_dict = getCustomisationSplit(args.customisations)
        setup.global_params.update(customisations_dict)
    treeName = setup.global_params['treeName'] # treeName should be inside global params if not in customisations
    channels = setup.global_params["channelSelection"]
    setup.global_params["channels_to_consider"] = args.channels.split(',') if args.channels else setup.global_params["channelSelection"]
    setup.global_params['dataset'] = args.dataset
    setup.global_params['compute_rel_weights']  = args.compute_rel_weights and args.dataset!='data'
    histTupleDef = Utilities.load_module(args.histTupleDef)


    dont_create_HistTuple = False
    key_not_exist = False
    df_empty = False
    inFile_root = ROOT.TFile.Open(args.inFile,"READ")
    inFile_keys = [k.GetName() for k in inFile_root.GetListOfKeys()]
    if treeName not in inFile_keys:
        key_not_exist = True
    inFile_root.Close()
    if not key_not_exist and ROOT.RDataFrame(treeName,args.inFile).Count().GetValue() == 0:
        df_empty = True
    dont_create_HistTuple = key_not_exist or df_empty


    hist_cfg_dict = {}
    with open(args.histConfig, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    unc_cfg_dict = {}
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)

    histTupleDef = Utilities.load_module(args.histTupleDef)
    if not dont_create_HistTuple :
        # if os.path.isdir(args.outDir):
        #     shutil.rmtree(args.outDir)
        # os.makedirs(args.outDir, exist_ok=True)
        snapshotOptions = ROOT.RDF.RSnapshotOptions()
        snapshotOptions.fOverwriteIfExists=False
        snapshotOptions.fLazy = True
        snapshotOptions.fMode="RECREATE"
        # snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + args.compressionAlgo)
        snapshotOptions.fCompressionLevel = args.compressionLevel
        createHistTuple(args.inFile, args.cacheFile, treeName, args.outFile, setup, hist_cfg_dict,unc_cfg_dict, snapshotOptions,args.nEvents, args.evtIds,  args.compute_unc_variations, args.compute_rel_weights, histTupleDef)
