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
from FLAF.Common.HistHelper import *

# ROOT.EnableImplicitMT(1)
ROOT.EnableThreadSafety()


def create_file(file_name, times=None):
    with open(file_name, "w"):
        os.utime(file_name, times)


def DefineBinnedColumn(hist_cfg_dict, var):
    x_bins = hist_cfg_dict[var]["x_bins"]
    func_name = f"get_{var}_bin"
    axis_definition = ""

    if isinstance(x_bins, list):
        edges = x_bins
        n_bins = len(edges) - 1
        edges_cpp = "{" + ",".join(map(str, edges)) + "}"
        axis_definition = f"static const double bins[] = {edges_cpp}; static const TAxis axis({n_bins}, bins);"
    else:
        n_bins, bin_range = x_bins.split("|")
        start, stop = bin_range.split(":")
        axis_definition = f"static const TAxis axis({n_bins}, {start}, {stop});"

    ROOT.gInterpreter.Declare(
        f"""
        #include "ROOT/RVec.hxx"
        #include "TAxis.h"

        int {func_name}(double x) {{
            {axis_definition}
            return axis.FindFixBin(x) - 1;
        }}

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
    inFile_keys
):
    start_time = datetime.datetime.now()
    # compression_settings = snapshotOptions.fCompressionAlgorithm * 100 + snapshotOptions.fCompressionLevel
    histTupleDef.Initialize()
    histTupleDef.analysis_setup(setup)
    isData = True if setup.global_params["dataset"] == "data" else False
    isCentral = True
    sample_type = (
        "data"
        if isData
        else setup.samples[setup.global_params["dataset"]]["sampleType"]
    )
    snaps = []
    reports = []
    outfilesNames = []
    vars_to_save = []
    tmp_fileNames = []
    if treeName not in inFile_keys:
        print(f"ERRORE, {treeName} non esiste nel file, ritorno il nulla")
        return tmp_fileNames

    df_central = ROOT.RDataFrame(treeName, inFile)
    df_cache_central = None
    if cacheFile:
        df_cache_central = ROOT.RDataFrame(treeName, cacheFile)

    ROOT.RDF.Experimental.AddProgressBar(df_central)
    if range is not None:
        df_central = df_central.Range(range)
    if len(evtIds) > 0:
        df_central = df_central.Filter(
            f"static const std::set<ULong64_t> evts = {{ {evtIds} }}; return evts.count(event) > 0;"
        )

    # Central + weights shifting:

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


    dfw_central = histTupleDef.GetDfw(df_central, df_cache_central, setup.global_params)

    col_names_central = dfw_central.colNames
    col_types_central = dfw_central.colTypes

    all_rel_uncs_to_compute = []
    if compute_rel_weights:
        all_rel_uncs_to_compute.extend(unc_cfg_dict["norm"].keys())
    all_shifts_to_compute = []
    if compute_unc_variations:
        df_central = createCentralQuantities(
            df_central, col_types_central, col_names_central
        )
        # If original count is already 0, then you don't need to raise error
        if (
            df_central
            .Filter("map_placeholder > 0")
            .Count()
            .GetValue()
            <= 0
        ):
            raise RuntimeError("no events passed map placeolder")
        all_shifts_to_compute.extend(unc_cfg_dict["shape"])



    for unc in ["Central"] + all_rel_uncs_to_compute:
        scales = setup.global_params["scales"] if unc != "Central" else ["Central"]
        for scale in scales:
            final_weight_name = f"weight_{unc}_{scale}" if unc!="Central" else "weight_Central"
            histTupleDef.DefineWeightForHistograms(
                dfw_central,
                unc,
                scale,
                sample_type,
                unc_cfg_dict,
                hist_cfg_dict,
                setup.global_params,
                final_weight_name,
            )
            dfw_central.colToSave.append(final_weight_name)
    dfw_central.colToSave.extend(additional_vars)
    for var in vars_to_save:
        DefineBinnedColumn(hist_cfg_dict, var)
        dfw_central.df = dfw_central.df.Define(f"{var}_bin", f"get_{var}_bin({var})")
        dfw_central.colToSave.append(f"{var}_bin")

    varToSave = Utilities.ListToVector(dfw_central.colToSave)
    # reports.append(dfw_central.df.Report()) --> enable if we want reports
    tmp_fileName = f"{treeName}.root"
    tmp_fileNames.append(tmp_fileName)
    snaps.append(
        dfw_central.df.Snapshot(
            treeName, tmp_fileName, varToSave, snapshotOptions
        )
    )

    #### shifted trees


    for unc in all_shifts_to_compute:
        scales = setup.global_params["scales"]
        for scale in scales:
            treeName = f"Events_{unc}{scale}"
            # Lista delle variazioni
            shifts = ["noDiff", "Valid", "nonValid"]

            for shift in shifts:
                treeName_shift = f"{treeName}_{shift}"
                print(treeName_shift)

                if treeName_shift in inFile_keys:
                    # Cache dataframe se disponibile
                    df_cache = None
                    if cacheFile:
                        df_cache = ROOT.RDataFrame(treeName_shift, cacheFile)
                    print(col_names_central)
                    # Costruzione del dataframe wrapper
                    dfw = histTupleDef.GetDfw(
                        ROOT.RDataFrame(treeName_shift, inFile),
                        df_cache,
                        setup.global_params,
                        shift,
                        col_names_central,
                        col_types_central,
                        f"cache_map_{unc}{scale}_{shift}",
                    )

                    # Definizione pesi
                    histTupleDef.DefineWeightForHistograms(
                        dfw,
                        unc,
                        scale,
                        sample_type,
                        unc_cfg_dict,
                        hist_cfg_dict,
                        setup.global_params,
                        final_weight_name,
                    )

                    # Aggiunta colonne
                    dfw.colToSave.append(final_weight_name)
                    for var in vars_to_save:
                        dfw.df = dfw.df.Define(f"{var}_bin", f"get_{var}_bin({var})")
                        dfw.colToSave.append(f"{var}_bin")
                    dfw.colToSave.extend(additional_vars)

                    # Conversione lista → ROOT::VecOps::RVec
                    varToSave = Utilities.ListToVector(dfw.colToSave)

                    # Output temporaneo
                    tmp_fileName = f"{treeName_shift}.root"
                    tmp_fileNames.append(tmp_fileName)

                    # Snapshot
                    snaps.append(
                        dfw.df.Snapshot(
                            treeName_shift,
                            tmp_fileName,
                            varToSave,
                            snapshotOptions,
                        )
                    )


    if snapshotOptions.fLazy == True:
        ROOT.RDF.RunGraphs(snaps)
    end_time = datetime.datetime.now()
    return tmp_fileNames


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
            inFile_keys
        )
        # if args.test : print(f'outFileName is {args.outFile}')
        if tmp_fileNames:
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
