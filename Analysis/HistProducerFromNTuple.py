import argparse
import os
import sys
import importlib
import ROOT
import yaml

if __name__ == "__main__":
    sys.path.append(os.environ["ANALYSIS_PATH"])

from FLAF.Common.HistHelper import *
import FLAF.Common.Utilities as Utilities
from FLAF.Common.Setup import Setup
from FLAF.RunKit.run_tools import ps_call


ROOT.gInterpreter.Declare(
    """
#include <vector>
#include <cmath>
#include <algorithm>
#include "ROOT/RVec.hxx"

template <typename T>
float GetBinValue(const T& bin, const std::vector<float>& edges) {
    int ibin = static_cast<int>(bin);
    float max_val = *std::max_element(edges.begin(), edges.end());
    if (std::abs(ibin) >= static_cast<int>(edges.size()))
        return std::copysign(max_val, bin);
    else if (ibin <= 0)
        return 0.f;
    else
        return edges.at(ibin);
}

template <typename T>
ROOT::VecOps::RVec<float> GetBinValue(const ROOT::VecOps::RVec<T>& bins, const std::vector<float>& edges) {
    ROOT::VecOps::RVec<float> result;
    float max_val = *std::max_element(edges.begin(), edges.end());
    for (const auto& bin : bins) {
        int ibin = static_cast<int>(bin);
        if (std::abs(ibin) >= static_cast<int>(edges.size()))
            result.push_back(std::copysign(max_val, bin));
        else if (ibin <= 0)
            continue;
        else
            result.push_back(edges.at(ibin));
    }
    return result;
}
"""
)


def SaveHist(key, outFile, histogram, hist_name):
    dir_name = "/".join(key)
    dir_ptr = Utilities.mkdir(outFile, dir_name)
    # merged_hist = hist_list[0].GetValue()
    # for hist in hist_list[1:] :
    #     merged_hist.Add(hist.GetValue())
    # isCentral = 'Central' in key_2
    # hist_name =  sample_type
    # if not isCentral:
    #     hist_name+=f"_{uncName}{scale}"
    # #print(dir_name, hist_name)
    dir_ptr.WriteTObject(histogram.GetValue(), hist_name, "Overwrite")


def GetBinValues(rdf, hist_cfg_dict, var):
    edges_vector = GetBinVec(hist_cfg_dict, var)
    rdf = (rdf.Define(
            f"{var}_edges_vector",
            f"""
                        std::vector<float> edges_vector({edges_vector}); return edges_vector;
                        """,
        )
        .Define(f"{var}", f"GetBinValue({var}_bin, {var}_edges_vector)")
    )
    return rdf

def GetHist(rdf, var, filter_to_apply):
    histo = rdf.Filter(filter_to_apply).Histo1D(GetModel(hist_cfg_dict, f"{var}"), f"{var}", "weight_for_hists")
    return histo




if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("inputFiles", nargs="+", type=str)
    parser.add_argument("--period", required=True, type=str)
    parser.add_argument("--outFile", required=True, type=str)
    parser.add_argument("--customisations", type=str, default=None)
    parser.add_argument("--channels", type=str, default=None)
    parser.add_argument("--vars", type=str, default=None)
    parser.add_argument("--furtherCut", type=str, default=None)
    args = parser.parse_args()

    setup = Setup.getGlobal(
        os.environ["ANALYSIS_PATH"], args.period, args.customisations
    )
    unc_cfg_dict = setup.weights_config
    analysis_import = setup.global_params["analysis_import"]
    analysis = importlib.import_module(f"{analysis_import}")
    all_infiles = [fileName for fileName in args.inputFiles]
    inFiles = Utilities.ListToVector(all_infiles)

    # Customisations
    customisations_dict = {}
    if args.customisations:
        customisations_dict = getCustomisationSplit(args.customisations)
        setup.global_params.update(customisations_dict)

    # Tree name
    treeName = setup.global_params["treeName"]
    rdf = ROOT.RDataFrame(treeName, inFiles)

    hist_cfg_dict = setup.hists

    # Channels
    channels = setup.global_params["channelSelection"]
    if args.channels:
        channels = (
            args.channels.split(",") if isinstance(args.channels, str) else args.channels
        )
    setup.global_params["channels_to_consider"] = channels

    # Key filter dictionary
    key_filter_dict = analysis.createKeyFilterDict(
        setup.global_params, setup.global_params["era"]
    )

    # Further cuts
    further_cuts = {}
    if args.furtherCut:
        furtherCut = args.furtherCut.split(",")
    if "further_cuts" in setup.global_params.keys() and setup.global_params["further_cuts"].keys():
        further_cuts = setup.global_params["further_cuts"]
    print("[DEBUG] further_cuts:", further_cuts)

    # Vars to save
    vars_to_save = setup.global_params["vars_to_save"]
    if args.vars:
        vars_to_save = args.vars.split(",")

    # ------------------------
    # Pre-calcolo variabili e cut
    # ------------------------
    column_names = set(rdf.GetColumnNames())
    vars_needed = set(vars_to_save)

    if further_cuts:
        for var_for_cut, (var_cut, _) in further_cuts.items():
            vars_needed.add(var_cut)

    for var in vars_needed:
        if var not in column_names:
            print(f"[DEBUG] Definisco variabile: {var}")
            rdf = GetBinValues(rdf, hist_cfg_dict, var)
            column_names.add(var)

    if further_cuts:
        for further_cut_name, (var_for_cut, cut_expr) in further_cuts.items():
            if further_cut_name not in column_names:
                print(f"[DEBUG] Definisco cut: {further_cut_name} = {cut_expr}")
                rdf = rdf.Define(further_cut_name, cut_expr)
                column_names.add(further_cut_name)

    # ------------------------
    # Generazione file temporanei per variabile
    # ------------------------
    tmp_fileNames = []

    for var in vars_to_save:
        tmp_file = f"tmp_{var}.root"
        tmp_fileNames.append(tmp_file)
        tmp_outFile = ROOT.TFile(tmp_file, "RECREATE")

        booked_hists = []
        for key, filter_to_apply in key_filter_dict.items():
            if further_cuts:
                for further_cut_name in further_cuts.keys():
                    filter_to_apply_further = f"{filter_to_apply} && {further_cut_name}"
                    histo = GetHist(rdf, var, filter_to_apply_further)
                    booked_hists.append((key + (further_cut_name,), histo, var))
            else:
                histo = GetHist(rdf, var, filter_to_apply)
                booked_hists.append((key, histo, var))

        # Calcolo e salvataggio per questa variabile
        for key, histo, var_name in booked_hists:
            histo.GetValue()  # forza la valutazione
            SaveHist(key, tmp_outFile, histo, var_name)

        tmp_outFile.Close()
        print(f"[DEBUG] Salvato file temporaneo: {tmp_file}")

    # ------------------------
    # Unione con hadd
    # ------------------------
    hadd_str = f"hadd -f -j -O {args.outFile} " + " ".join(tmp_fileNames)
    print(f"[DEBUG] hadd_str is {hadd_str}")
    ps_call([hadd_str], True)

    # Rimozione temporanei
    for f in tmp_fileNames:
        os.remove(f)
        print(f"[DEBUG] Rimosso file temporaneo: {f}")

    print(f"[INFO] File finale salvato in {args.outFile}")
