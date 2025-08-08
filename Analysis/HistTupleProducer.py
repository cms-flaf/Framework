import datetime
import os
import sys
import ROOT
import shutil
import zlib

# import fastcrc
import json


if __name__ == "__main__":
    sys.path.append(os.environ["ANALYSIS_PATH"])

import FLAF.Common.Utilities as Utilities
from FLAF.Common.Setup import Setup
import importlib
from FLAF.RunKit.run_tools import ps_call

# ROOT.EnableImplicitMT(1)
ROOT.EnableThreadSafety()


def create_file(file_name, times=None):
    with open(file_name, "w"):
        os.utime(file_name, times)


# def DefineBinnedColumn(dfw, hist_cfg_dict, var, weight_name, unc, scale):
#     axis_name = f"{var}_axis_{unc}_{scale}"
#     func_name = f"get_{var}_bin_{unc}_{scale}"
#     hist_name = f"{var}_hist_{unc}_{scale}"

#     if isinstance(hist_cfg_dict[var]["x_bins"], list):
#         edges = hist_cfg_dict[var]["x_bins"]
#         edges_cpp = "{" + ",".join(map(str, edges)) + "}"
#         nbins = len(edges) - 1

#         ROOT.gInterpreter.Declare(
#             f"""
#             #include <vector>
#             #include "TH1F.h"
#             #include "ROOT/RVec.hxx"

#             std::vector<double> {axis_name}_edges = {edges_cpp};
#             static TH1F* {hist_name} = new TH1F("{hist_name}", "{hist_name}", {nbins}, {axis_name}_edges.data());

#             int {func_name}(double x, double weight) {{
#                 {hist_name}->Fill(x, weight);
#                 return {hist_name}->GetXaxis()->FindBin(x) - 1;
#             }}

#             template<typename T>
#             ROOT::VecOps::RVec<int> {func_name}(ROOT::VecOps::RVec<T> xvec, T weight) {{
#                 ROOT::VecOps::RVec<int> out;
#                 for (auto& x : xvec) {{
#                     {hist_name}->Fill(x, weight);
#                     out.push_back({hist_name}->GetXaxis()->FindBin(x) - 1);
#                 }}
#                 return out;
#             }}
#         """
#         )
#     else:
#         n_bins, bin_range = hist_cfg_dict[var]["x_bins"].split("|")
#         start, stop = bin_range.split(":")

#         ROOT.gInterpreter.Declare(
#             f"""
#             #include <vector>
#             #include "TH1F.h"
#             #include "ROOT/RVec.hxx"

#             static TH1F* {hist_name} = new TH1F("{hist_name}", "{hist_name}", {n_bins}, {start}, {stop});

#             int {func_name}(double x, double weight) {{
#                 {hist_name}->Fill(x, weight);
#                 return {hist_name}->GetXaxis()->FindBin(x) - 1;
#             }}

#             template<typename T>
#             ROOT::VecOps::RVec<int> {func_name}(ROOT::VecOps::RVec<T> xvec, double weight) {{
#                 ROOT::VecOps::RVec<int> out(xvec.size());
#                 for (int n = 0; n < xvec.size(); n++) {{
#                     out[n] = ({func_name}(xvec[n], weight));
#                 }}
#                 return out;
#             }}
#         """
#         )

#     dfw.df = dfw.df.Define(f"{var}_bin", f"{func_name}({var}, {weight_name})")

def DefineBinnedColumn(dfw, hist_cfg_dict, var, weight_name, unc, scale):
    # Recupera la configurazione dei bin
    x_bins = hist_cfg_dict[var]["x_bins"]

    # Genera un nome di funzione univoco
    func_name = f"get_{var}_bin"

    # Definizione dell'asse per i bin, comune a scalare e vettoriale
    axis_definition = ""

    if isinstance(x_bins, list):
        # Bin a larghezza variabile
        edges = x_bins
        n_bins = len(edges) - 1
        edges_cpp = "{" + ",".join(map(str, edges)) + "}"
        axis_definition = f"static const double bins[] = {edges_cpp}; static const TAxis axis({n_bins}, bins);"
    else:
        # Bin a larghezza fissa
        n_bins, bin_range = x_bins.split("|")
        start, stop = bin_range.split(":")
        axis_definition = f"static const TAxis axis({n_bins}, {start}, {stop});"

    # Dichiarazione della funzione C++ per il calcolo del bin
    ROOT.gInterpreter.Declare(
        f"""
        #include "ROOT/RVec.hxx"
        #include "TAxis.h"

        // Funzione per il tipo scalare
        int {func_name}(double x) {{
            {axis_definition}
            return axis.FindFixBin(x) - 1;
        }}

        // Funzione per il tipo vettoriale
        template<typename T>
        ROOT::VecOps::RVec<int> {func_name}(ROOT::VecOps::RVec<T> xvec) {{
            {axis_definition}
            ROOT::VecOps::RVec<int> out(xvec.size());
            for (size_t i = 0; i < xvec.size(); ++i) {{
                out[i] = axis.FindFixBin(xvec[i]) - 1;
            }}
            return out;
        }}
        """
    )

    # Applica la funzione al DataFrame, usando la versione vettoriale se la colonna è un RVec
    # La logica di RDataFrame sceglie automaticamente la versione giusta
    dfw.df = dfw.df.Define(f"{var}_bin", f"{func_name}({var})")

def createHistTuple(
    inFile,
    cacheFile,
    treeName,
    setup,
    hist_cfg_dict,
    unc_cfg_dict,
    snapshotOptions,
    range,
    evtIds,
    compute_unc_variations,
    compute_rel_weights,
    histTupleDef,
):
    start_time = datetime.datetime.now()
    # compression_settings = snapshotOptions.fCompressionAlgorithm * 100 + snapshotOptions.fCompressionLevel
    histTupleDef.Initialize()
    histTupleDef.analysis_setup(setup)
    isData = True if setup.global_params["dataset"] == "data" else False
    isCentral = True  # for the moment --> there will be changes when including uncs

    sample_type = (
        "data"
        if isData
        else setup.samples[setup.global_params["dataset"]]["sampleType"]
    )
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
        df_central = df_central.Filter(
            f"static const std::set<ULong64_t> evts = {{ {evtIds} }}; return evts.count(event) > 0;"
        )

    # Central + weights shifting:

    vars_to_save = []
    tmp_fileNames = []
    if type(setup.global_params["vars_to_save"]) == list:
        vars_to_save = setup.global_params["vars_to_save"]
    elif type(setup.global_params["vars_to_save"]) == dict:
        vars_to_save = setup.global_params["vars_to_save"].keys()
    additional_vars = []
    if setup.global_params["additional_vars"]:
        if type(setup.global_params["additional_vars"]) == list:
            additional_vars = setup.global_params["additional_vars"]
        elif type(setup.global_params["additional_vars"]) == dict:
            additional_vars = setup.global_params["additional_vars"].keys()

    # dfw_central = histTupleDef.GetDfw(df_central, df_cache, setup.global_params)
    all_rel_uncs_to_compute = ["Central"]
    if compute_rel_weights:
        all_rel_uncs_to_compute.extend(unc_cfg_dict["norm"].keys())
    for unc in all_rel_uncs_to_compute:
        scales = setup.global_params["scales"]
        if unc == "Central":
            scales = ["Central"]
        for scale in scales:
            df_unc_scale = df_central
            df_unc_scale_cache = df_cache
            dfw_unc_scale = histTupleDef.GetDfw(
                df_unc_scale, df_unc_scale_cache, setup.global_params
            )
            final_weight_name = "weight_for_hists"
            histTupleDef.DefineWeightForHistograms(
                dfw_unc_scale,
                unc,
                scale,
                sample_type,
                unc_cfg_dict,
                hist_cfg_dict,
                setup.global_params,
                final_weight_name,
            )
            dfw_unc_scale.colToSave.append(final_weight_name)
            for var in vars_to_save:
                DefineBinnedColumn(
                    dfw_unc_scale, hist_cfg_dict, var, final_weight_name, unc, scale
                )
                dfw_unc_scale.colToSave.append(f"{var}_bin")
            final_treeName = (
                f"{treeName}_{unc}_{scale}" if unc != "Central" else treeName
            )
            dfw_unc_scale.colToSave.extend(additional_vars)
            varToSave = Utilities.ListToVector(dfw_unc_scale.colToSave)
            # reports.append(dfw_unc_scale.df.Report()) --> enable if we want reports
            tmp_fileName = f"{final_treeName}.root"
            tmp_fileNames.append(tmp_fileName)
            snaps.append(
                dfw_unc_scale.df.Snapshot(
                    final_treeName, tmp_fileName, varToSave, snapshotOptions
                )
            )
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
    return tmp_fileNames
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
    parser.add_argument("--period", required=True, type=str)
    parser.add_argument("--inFile", required=True, type=str)
    parser.add_argument("--outFile", required=True, type=str)
    parser.add_argument("--cacheFile", required=False, type=str, default=None)
    parser.add_argument("--dataset", required=True, type=str)
    parser.add_argument("--histTupleDef", required=True, type=str)
    parser.add_argument("--compute_unc_variations", type=bool, default=False)
    parser.add_argument("--compute_rel_weights", type=bool, default=False)
    parser.add_argument("--customisations", type=str, default=None)
    parser.add_argument("--compressionLevel", type=int, default=4)
    parser.add_argument("--compressionAlgo", type=str, default="ZLIB")
    parser.add_argument("--channels", type=str, default=None)
    parser.add_argument("--nEvents", type=int, default=None)
    parser.add_argument("--evtIds", type=str, default="")

    args = parser.parse_args()

    setup = Setup.getGlobal(
        os.environ["ANALYSIS_PATH"], args.period, args.customisations
    )

    customisations_dict = {}
    if args.customisations:
        customisations_dict = getCustomisationSplit(args.customisations)
        setup.global_params.update(customisations_dict)
    treeName = setup.global_params[
        "treeName"
    ]  # treeName should be inside global params if not in customisations
    channels = setup.global_params["channelSelection"]
    setup.global_params["channels_to_consider"] = (
        args.channels.split(",")
        if args.channels
        else setup.global_params["channelSelection"]
    )
    setup.global_params["dataset"] = args.dataset
    setup.global_params["compute_rel_weights"] = (
        args.compute_rel_weights and args.dataset != "data"
    )
    histTupleDef = Utilities.load_module(args.histTupleDef)

    dont_create_HistTuple = False
    key_not_exist = False
    df_empty = False
    inFile_root = ROOT.TFile.Open(args.inFile, "READ")
    inFile_keys = [k.GetName() for k in inFile_root.GetListOfKeys()]
    if treeName not in inFile_keys:
        key_not_exist = True
    inFile_root.Close()
    if (
        not key_not_exist
        and ROOT.RDataFrame(treeName, args.inFile).Count().GetValue() == 0
    ):
        df_empty = True
    dont_create_HistTuple = key_not_exist or df_empty

    # sample_cfg_dict = setup.samples
    # global_cfg_dict = setup.global_params

    unc_cfg_dict = setup.weights_config
    hist_cfg_dict = setup.hists
    # print(hist_cfg_dict)

    histTupleDef = Utilities.load_module(args.histTupleDef)
    if not dont_create_HistTuple:
        # if os.path.isdir(args.outDir):
        #     shutil.rmtree(args.outDir)
        # os.makedirs(args.outDir, exist_ok=True)
        snapshotOptions = ROOT.RDF.RSnapshotOptions()
        snapshotOptions.fOverwriteIfExists = False
        snapshotOptions.fLazy = True
        snapshotOptions.fMode = "RECREATE"
        # snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + args.compressionAlgo)
        # snapshotOptions.fCompressionLevel = args.compressionLevel
        tmp_fileNames = createHistTuple(
            args.inFile,
            args.cacheFile,
            treeName,
            setup,
            hist_cfg_dict,
            unc_cfg_dict,
            snapshotOptions,
            args.nEvents,
            args.evtIds,
            args.compute_unc_variations,
            args.compute_rel_weights,
            histTupleDef,
        )
        # if args.test : print(f'outFileName is {args.outFile}')
        hadd_str = f"hadd -f -j -O {args.outFile} "  # -f209
        hadd_str += " ".join(f for f in tmp_fileNames)
        print(f"hadd_str is {hadd_str}")
        ps_call([hadd_str], True)
        # if args.test : print(f'hadd_str is {hadd_str}')
        # try: ps_call([hadd_str], True)
        # except:
        # create_file(args.outFile)
        # if args.test : print(f"args.outFile is {args.outFile}")
        # print(syst_files_to_merge)
        if os.path.exists(args.outFile) and len(tmp_fileNames) != 0:
            for file_syst in tmp_fileNames:  # + [outFileCentralName]:
                # if args.test : print(file_syst)
                if file_syst == args.outFile:
                    continue
                os.remove(file_syst)
