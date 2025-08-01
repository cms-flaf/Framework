import ROOT
import yaml
import importlib
import os
import sys
import numpy as np
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

from FLAF.Analysis.HistHelper import *
from FLAF.Common.HistHelper import *
import FLAF.Common.Utilities as Utilities
from FLAF.Common.Setup import Setup

ROOT.gInterpreter.Declare("""
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
""")
def SaveHist(key, outFile, histogram, hist_name):
    dir_name = '/'.join(key)
    dir_ptr = Utilities.mkdir(outFile,dir_name)
    # merged_hist = hist_list[0].GetValue()
    # for hist in hist_list[1:] :
    #     merged_hist.Add(hist.GetValue())
    # isCentral = 'Central' in key_2
    # hist_name =  sample_type
    # if not isCentral:
    #     hist_name+=f"_{uncName}{scale}"
    # #print(dir_name, hist_name)
    dir_ptr.WriteTObject(histogram.GetValue(), hist_name, "Overwrite")

def GetHist(rdf, hist_cfg_dict, var, filter_to_apply):
    edges_vector = GetBinVec(hist_cfg_dict, var)
    histo = rdf.Filter(filter_to_apply).\
                Define(f"edges_vector", f"""
                        std::vector<float> edges_vector({edges_vector}); return edges_vector;
                        """).\
                Define(f"{var}_values", f"GetBinValue({var}_bin, edges_vector)").\
                Histo1D(GetModel(hist_cfg_dict, f"{var}"), f"{var}_values", "weight_for_hists")
    return histo

if __name__ == "__main__":
    import argparse
    import os
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--period', required=True, type=str)
    parser.add_argument('--inDir', required=True, type=str)
    parser.add_argument('--inFileName', required=False, type=str, default=None)
    parser.add_argument('--outFile', required=True, type=str)
    parser.add_argument('--histConfig', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str) # currently not needed, maybe in future
    parser.add_argument('--customisations', type=str, default=None)
    parser.add_argument('--treeName', required=False, type=str, default="Events")
    parser.add_argument('--channels', type=str, default=None)
    parser.add_argument('--vars', type=str, default=None)
    parser.add_argument('--furtherCut', type=str, default=None)
    args = parser.parse_args()

    setup = Setup.getGlobal(os.environ['ANALYSIS_PATH'], args.period, args.customisations)

    analysis_import = (setup.global_params['analysis_import'])
    analysis = importlib.import_module(f'{analysis_import}')
    inFiles = f'{args.inDir}/*.root'
    if args.inFileName:
        inFiles = f'{args.inDir}/{args.inFileName}'
    rdf = ROOT.RDataFrame(args.treeName, inFiles)

    hist_cfg_dict = {}
    with open(args.histConfig, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)

    channels = setup.global_params["channelSelection"]
    if args.channels:
        channels = args.channels.split(',') if type(args.channels) == str else args.channels
    setup.global_params["channels_to_consider"] = channels

    key_filter_dict = analysis.createKeyFilterDict(setup.global_params, setup.global_params['era'])
    # print(key_filter_dict)

    vars_to_save = setup.global_params["vars_to_save"]
    if args.vars:
        vars_to_save = args.vars.split(',')
    outFile = ROOT.TFile(args.outFile, "RECREATE")
    for key in key_filter_dict.keys():
        for var in vars_to_save:
            dir_0, dir_1, dir_2 = key
            key_new = key
            filter_to_apply = key_filter_dict[key]
            further_cuts = []
            if args.furtherCut is not None:
                further_cuts = args.furtherCut.split(',')
            if further_cuts:
                for further_cut in further_cuts:
                    filter_to_apply_further = filter_to_apply + f" && {further_cut}"
                    histo = GetHist(rdf, hist_cfg_dict, var, filter_to_apply_further)
                    key_new = key + (further_cut)
                    SaveHist(key_new, outFile, histo, var)

            else:
                histo = GetHist(rdf, hist_cfg_dict, var, filter_to_apply)
                SaveHist(key_new, outFile, histo, var)