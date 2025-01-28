#python3 /afs/cern.ch/work/p/prsolank/private/FLAF_8thJan/AnaProd/NNInterface.py --inModelDir /afs/cern.ch/work/p/prsolank/private/FLAF_8thJan/config/HH_bbtautau/nn_models --inFile /tmp/prsolank/luigi-tmp-416131263.root --outFileName /tmp/prsolank/luigi-tmp-862152055.root --uncConfig /afs/cern.ch/work/p/prsolank/private/FLAF_8thJan/config/Run2_2018/weights.yaml --globalConfig /afs/cern.ch/work/p/prsolank/private/FLAF_8thJan/config/HH_bbtautau/global.yaml --EraName e2018 --Mass 400 --Spin 2 --PairType 2

from __future__ import annotations
import os
import enum
from typing import Any, Dict
import ROOT
import pandas as pd
ROOT.ROOT.EnableImplicitMT()
import numpy as np
import tensorflow as tf
from interface import *
import sys

class Era(enum.Enum):

    Run2_2016H = 0
    Run2_2016 = 1
    Run2_2017 = 2
    Run2_2018 = 3

class DotDict(dict):
    @classmethod
    def wrap(cls, *args, **kwargs) -> 'DotDict':
        wrap = lambda d: cls((k, wrap(v)) for k, v in d.items()) if isinstance(d, dict) else d  
        return wrap(dict(*args, **kwargs))

    def __getattr__(self, attr: str) -> Any:
        try:
            return self[attr]
        except KeyError:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'")

    def __setattr__(self, attr: str, value: Any) -> None:
        self[attr] = value

def convert_kinematics(pt, eta, phi, mass):
    px = pt * np.cos(phi)
    py = pt * np.sin(phi)
    pz = pt * np.sinh(eta)
    energy = np.sqrt(pt**2 * np.cosh(eta)**2 + mass**2)
    return px, py, pz, energy

def convert_to_numpy(event_data, period, mass, spin):
    dau1_px, dau1_py, dau1_pz, dau1_e = convert_kinematics(event_data["tau1_pt"], event_data["tau1_eta"], event_data["tau1_phi"], event_data["tau1_mass"])
    dau2_px, dau2_py, dau2_pz, dau2_e = convert_kinematics(event_data["tau2_pt"], event_data["tau2_eta"], event_data["tau2_phi"], event_data["tau2_mass"])
    bjet1_px, bjet1_py, bjet1_pz, bjet1_e = convert_kinematics(event_data["b1_pt"], event_data["b1_eta"], event_data["b1_phi"], event_data["b1_mass"])
    bjet2_px, bjet2_py, bjet2_pz, bjet2_e = convert_kinematics(event_data["b2_pt"], event_data["b2_eta"], event_data["b2_phi"], event_data["b2_mass"])

    selected_fatjet_pt = np.array(event_data["SelectedFatJet_pt"])
    selected_fatjet_eta = np.array(event_data["SelectedFatJet_eta"])
    selected_fatjet_phi = np.array(event_data["SelectedFatJet_phi"])
    selected_fatjet_mass = np.array(event_data["SelectedFatJet_mass"])
    
    if len(selected_fatjet_pt) != 0:
        max_pt_index = np.argmax(selected_fatjet_pt)
        fatjet_pt = selected_fatjet_pt[max_pt_index]
        fatjet_eta = selected_fatjet_eta[max_pt_index]
        fatjet_phi = selected_fatjet_phi[max_pt_index]
        fatjet_mass = selected_fatjet_mass[max_pt_index]
        fatjet_px, fatjet_py, fatjet_pz, fatjet_e = convert_kinematics(fatjet_pt, fatjet_eta, fatjet_phi, fatjet_mass)
    else:
        fatjet_px, fatjet_py, fatjet_pz, fatjet_e = 0.0, 0.0, 0.0, 0.0

    met_px, met_py, _, _ = convert_kinematics(event_data["met_pt"], 0, event_data["met_phi"], 0)

    def ai(v): return np.array([v], dtype=np.int32)
    def al(v): return np.array([v], dtype=np.int64)
    def af(v): return np.array([v], dtype=np.float32)
    
    pairtype_map = {23: 0, 13: 1, 33: 2}

    inputs = {
        "event_number": al(event_data["event"]),
        "spin": ai(spin),
        "mass": ai(mass),
        "era": Era[period],
        "pair_type": ai(pairtype_map.get(event_data["channelId"], 2)),
        "dau1_dm": ai(event_data["tau1_decayMode"]),
        "dau2_dm": ai(event_data["tau2_decayMode"]),
        "dau1_charge": ai(event_data["tau1_charge"]),
        "dau2_charge": ai(event_data["tau2_charge"]),
        "is_boosted": ai(event_data["boosted_baseline"]),
        "has_bjet_pair": ai(event_data["Hbb_isValid"]),
        "met_px": af(met_px),
        "met_py": af(met_py),
        "met_cov00": af(event_data["met_covXX"]),
        "met_cov01": af(event_data["met_covXY"]),
        "met_cov11": af(event_data["met_covYY"]),
        "dau1_e": af(dau1_e),
        "dau1_px": af(dau1_px),
        "dau1_py": af(dau1_py),
        "dau1_pz": af(dau1_pz),
        "dau2_e": af(dau2_e),
        "dau2_px": af(dau2_px),
        "dau2_py": af(dau2_py),
        "dau2_pz": af(dau2_pz),
        "bjet1_e": af(bjet1_e),
        "bjet1_px": af(bjet1_px),
        "bjet1_py": af(bjet1_py),
        "bjet1_pz": af(bjet1_pz),
        "bjet1_btag_df": af(event_data["b1_btagDeepFlavB"]),
        "bjet1_cvsb": af(event_data["b1_btagPNetCvB"]),
        "bjet1_cvsl": af(event_data["b1_btagPNetCvL"]),
        "bjet1_hhbtag": af(event_data["b1_HHbtag"]),
        "bjet2_e": af(bjet2_e),
        "bjet2_px": af(bjet2_px),
        "bjet2_py": af(bjet2_py),
        "bjet2_pz": af(bjet2_pz),
        "bjet2_btag_df": af(event_data["b2_btagDeepFlavB"]),
        "bjet2_cvsb": af(event_data["b2_btagPNetCvB"]),
        "bjet2_cvsl": af(event_data["b2_btagPNetCvL"]),
        "bjet2_hhbtag": af(event_data["b2_HHbtag"]),
        "fatjet_e": af(np.array(fatjet_e)),
        "fatjet_px": af(np.array(fatjet_px)),
        "fatjet_py": af(np.array(fatjet_py)),
        "fatjet_pz": af(np.array(fatjet_pz))
    }
    return inputs

def run_inference(nn_interface, inputs):
    predictions = nn_interface(**inputs)
    return predictions


def getKeyNames(root_file_name):
    root_file = ROOT.TFile(root_file_name, "READ")
    key_names = [str(k.GetName()) for k in root_file.GetListOfKeys() if root_file.Get(str(k.GetName())).InheritsFrom("TTree")]
    root_file.Close()
    return key_names

def createCentralQuantities(df_central, central_col_types, central_columns):
    map_creator = ROOT.analysis.MapCreator(*central_col_types)()
    df_central = map_creator.processCentral(ROOT.RDF.AsRNode(df_central), Utilities.ListToVector(central_columns))
    return df_central

def load_models(model_dir):
    models = [
        NNInterface(fold_index=fold_index, model_path=os.path.join(model_dir, f'hbtres_PSnew_baseline_LSmulti3_SSdefault_FSdefault_daurot_composite-default_extended_pair_ED10_LU8x128_CTdense_ACTelu_BNy_LT50_DO0_BS4096_OPadamw_LR1.0e-03_YEARy_SPINy_MASSy_RSv6_fi80_lbn_ft_lt20_lr1_LBdefault_daurot_fatjet_composite_FI{fold_index}_SDx5'))
        for fold_index in range(NNInterface.n_folds)
    ]
    return models

def run_inference_on_events_tree(df_begin, models, globalConfig, output_root_file, snapshotOptions, period, mass, spin):
    print(f"Running inference on the Events tree")
    run_inference_for_tree(
        tree_name="Events",
        rdf=df_begin,
        models=models,
        globalConfig=globalConfig,
        output_root_file=output_root_file,
        snapshotOptions=snapshotOptions,
        period=period,
        mass=mass,
        spin=spin
    )
    print(f"NN scores for Events tree saved in the output ROOT file")


def run_inference_on_uncertainty_trees(df_begin, inFileName, models, globalConfig, unc_cfg_dict, scales, output_root_file, snapshotOptions, period, mass, spin):
    dfWrapped_central = Utilities.DataFrameBuilderBase(df_begin)
    colNames = dfWrapped_central.colNames
    colTypes = dfWrapped_central.colTypes
    dfWrapped_central.df = createCentralQuantities(df_begin, colTypes, colNames)

    if dfWrapped_central.df.Filter("map_placeholder > 0").Count().GetValue() <= 0:
        raise RuntimeError("No events passed the map placeholder")

    snapshotOptions.fLazy = False
    for uncName in unc_cfg_dict['shape']:
        for scale in scales:
            treeName = f"Events_{uncName}{scale}"
            for suffix in ["_noDiff", "_Valid", "_nonValid"]:
                treeName_with_suffix = f"{treeName}{suffix}"
                if treeName_with_suffix in getKeyNames(inFileName):
                    print(f"  Processing {treeName_with_suffix}")
                    df_unc = ROOT.RDataFrame(treeName_with_suffix, inFileName)
                    dfWrapped_unc = Utilities.DataFrameBuilderBase(df_unc)
                    if "_nonValid" not in treeName_with_suffix:
                        dfWrapped_unc.CreateFromDelta(colNames, colTypes)
                    dfWrapped_unc.AddMissingColumns(colNames, colTypes)    
                    dfW_unc = Utilities.DataFrameWrapper(dfWrapped_unc.df, defaultColToSave)
                    

                    run_inference_for_tree(
                        tree_name=treeName_with_suffix,
                        rdf=dfW_unc.df,
                        models=models,
                        globalConfig=globalConfig,
                        output_root_file=output_root_file,
                        snapshotOptions=snapshotOptions,
                        period=period,
                        mass=mass,
                        spin=spin
                    )
    
    print(f"NN scores saved to {output_file_name}")

def run_inference_for_tree(tree_name, rdf, models, globalConfig, output_root_file, snapshotOptions, period, mass, spin):

    rdf_setup = PrepareDfForDNN(DataFrameBuilderForHistograms(rdf, globalConfig, period)).df

    columns = [
        "entryIndex", "luminosityBlock", "run", "event", "tau1_pt", "tau1_eta", "tau1_phi", "tau1_mass",
        "tau1_decayMode", "tau1_charge",
        "tau2_pt", "tau2_eta", "tau2_phi", "tau2_mass",
        "tau2_decayMode", "tau2_charge",
        "b1_pt", "b1_eta", "b1_phi", "b1_mass",
        "b1_btagDeepFlavB", "b1_btagPNetCvB", "b1_btagPNetCvL", "b1_HHbtag",
        "b2_pt", "b2_eta", "b2_phi", "b2_mass",
        "b2_btagDeepFlavB", "b2_btagPNetCvB", "b2_btagPNetCvL", "b2_HHbtag",
        "SelectedFatJet_pt", "SelectedFatJet_eta", "SelectedFatJet_phi", "SelectedFatJet_mass",
        "met_pt", "met_phi", "met_covXX", "met_covXY", "met_covYY",
        "Hbb_isValid", "boosted_baseline", "X_mass", "X_spin", "channelId"
    ]

    def run_inference_and_save(rdf_filtered):
        other_columns_dict = rdf_filtered.AsNumpy(columns)
        num_events = len(other_columns_dict["event"])

        if num_events == 0:
            print(f"No events found for {tree_name}, skipping inference.")
            return

        predictions_array = np.zeros((NNInterface.n_folds, num_events, NNInterface.n_out))

        for fold_index, nn_interface in enumerate(models):
            print(f'Processing Fold {fold_index} for {tree_name}')
            for i in range(num_events):
                if i % 100 == 0:
                    print(f'  Event No. {i}')

                event_data = {col: other_columns_dict[col][i] for col in columns}
                inputs = convert_to_numpy(event_data, period, mass, spin)

                predictions = run_inference(nn_interface, inputs)
                predictions_array[fold_index, i, :] = predictions.flatten()

        mean_predictions = np.nanmean(predictions_array, axis=0)

        output_root_file.cd()
        tree = ROOT.TTree(f"{tree_name}", f"NN scores for {tree_name}")
       
        HH_score = np.zeros(1, dtype=float)
        TT_score = np.zeros(1, dtype=float)
        DY_score = np.zeros(1, dtype=float)
        tree.Branch("HH_score", HH_score, "HH_score/D")
        tree.Branch("TT_score", TT_score, "TT_score/D")
        tree.Branch("DY_score", DY_score, "DY_score/D")

        entryIndex = np.zeros(1, dtype=np.int64)
        luminosityBlock = np.zeros(1, dtype=np.int64)
        run_num = np.zeros(1, dtype=np.int64)
        event_num = np.zeros(1, dtype=np.int64)

        tree.Branch("entryIndex", entryIndex, "entryIndex/L")
        tree.Branch("luminosityBlock", luminosityBlock, "luminosityBlock/L")
        tree.Branch("run", run_num, "run/L")
        tree.Branch("event", event_num, "event/L")

        for i in range(num_events):
            HH_score[0] = mean_predictions[i, 0]
            TT_score[0] = mean_predictions[i, 1]
            DY_score[0] = mean_predictions[i, 2]
            entryIndex[0] = other_columns_dict["entryIndex"][i]
            luminosityBlock[0] = other_columns_dict["luminosityBlock"][i]
            run_num[0] = other_columns_dict["run"][i]
            event_num[0] = other_columns_dict["event"][i]
            tree.Fill()

        tree.Write()
        print(f"Tree: {tree_name} saved to the file")

    run_inference_and_save(rdf_setup)


if __name__ == "__main__":

    sys.path.append(os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gInterpreter.Declare(f'#include "include/KinFitInterface.h"')
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    ROOT.gROOT.ProcessLine(f'#include "include/AnalysisTools.h"')
    ROOT.gROOT.ProcessLine(f'#include "include/pnetSF.h"')

    import yaml
    import argparse
    import numpy as np
    from Analysis.HistHelper import *
    from Common.Utilities import *
    from Analysis.hh_bbtautau import *
    from Analysis.GetCrossWeights import *
    parser = argparse.ArgumentParser()
    parser.add_argument('--inFile', required=True, type=str)
    parser.add_argument('--inModelDir', required=True, type=str)
    parser.add_argument('--globalConfig', required=True, type=str)  
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--outFileName', required=True, type=str)
    parser.add_argument('--period', required=True, type=str)
    parser.add_argument('--mass', required=True, type=int)
    parser.add_argument('--spin', required=True, type=int)

    args = parser.parse_args()

    with open(args.globalConfig, 'r') as f:
        globalConfig = yaml.safe_load(f)

    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)

    defaultColToSave = [
        "entryIndex", "luminosityBlock", "run", "event", "sample_type", "sample_name", "period",
        "isData", "tau1_pt", "tau1_eta", "tau1_phi", "tau1_mass",
        "tau1_decayMode", "tau1_charge", "tau2_pt", "tau2_eta", "tau2_phi", "tau2_mass",
        "tau2_decayMode", "tau2_charge", "b1_pt", "b1_eta", "b1_phi", "b1_mass",
        "b1_btagDeepFlavB", "b1_btagPNetCvB", "b1_btagPNetCvL", "b1_HHbtag", "b2_pt", "b2_eta", "b2_phi", "b2_mass",
        "b2_btagDeepFlavB", "b2_btagPNetCvB", "b2_btagPNetCvL", "b2_HHbtag", "SelectedFatJet_pt", "SelectedFatJet_eta",
        "SelectedFatJet_phi", "SelectedFatJet_mass", "met_pt", "met_phi", "met_covXX", "met_covXY", "met_covYY", 
        "Hbb_isValid", "X_mass", "X_spin", "channelId"
    ]
        
    scales = ['Up', 'Down']
    df_begin = ROOT.RDataFrame("Events", args.inFile)
    snapshotOptions = ROOT.RDF.RSnapshotOptions()
    file_keys = getKeyNames(args.inFile)

    print("Loading Models----")
    models = load_models(args.inModelDir)
    print("Models Loaded----")

    output_file_name = f'{args.outFileName}'
    output_root_file = ROOT.TFile(output_file_name, "RECREATE")

# Run inference on the Events tree
    run_inference_on_events_tree(
        df_begin=df_begin,
        models=models,
        globalConfig=globalConfig,
        output_root_file=output_root_file,
        snapshotOptions=snapshotOptions,
        period=args.period,
        mass=args.mass,
        spin=args.spin
    )
    
    # Run inference on uncertainty trees
    run_inference_on_uncertainty_trees(
        df_begin=df_begin,
        inFileName=args.inFile,
        models=models,
        globalConfig=globalConfig,
        unc_cfg_dict=unc_cfg_dict,
        scales=scales,
        output_root_file=output_root_file,
        snapshotOptions=snapshotOptions,
        period=args.period,
        mass=args.mass,
        spin=args.spin
    )
    
    output_root_file.Write()
    output_root_file.Close()

    print(f"Processed and saved NN scores for all trees into {output_file_name}.")


