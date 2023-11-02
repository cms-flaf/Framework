import ROOT
import numpy as np
import Common.BaselineSelection as Baseline
import Common.Utilities as Utilities
import Common.triggerSel as Triggers
import Corrections.Corrections as Corrections


def bTagProdEff(inFile, config, sample_name, range, evtIds):

    period = config["GLOBAL"]["era"]
    mass = -1 if 'mass' not in config[sample_name] else config[sample_name]['mass']
    isHH = True if mass > 0 else False
    Baseline.Initialize(False, False)
    Corrections.Initialize(config=config['GLOBAL'],loadBTagEff=False)
    wpValues = Corrections.btag.getWPValues()

    triggerFile = config['GLOBAL']['triggerFile']
    trigger_class = Triggers.Triggers(triggerFile) if triggerFile is not None else None
    df = ROOT.RDataFrame("Events", inFile)
    if range is not None:
        df = df.Range(range)
    if len(evtIds) > 0:
        df = df.Filter(f"static const std::set<ULong64_t> evts = {{ {evtIds} }}; return evts.count(event) > 0;")
    df = Baseline.applyMETFlags(df, config["GLOBAL"]["MET_flags"])
    df = df.Define("sample_type", f"static_cast<int>(SampleType::{config[sample_name]['sampleType']})")
    df = df.Define("period", f"static_cast<int>(Period::{period})")
    df = df.Define("X_mass", f"static_cast<int>({mass})")

    df = Baseline.CreateRecoP4(df)
    df = Baseline.DefineGenObjects(df, isData=False, isHH=isHH)
    dfw = Utilities.DataFrameWrapper(df)
    dfw.Apply(Baseline.SelectRecoP4, 'nano')
    dfw.Apply(Baseline.RecoLeptonsSelection)
    dfw.Apply(Baseline.RecoHttCandidateSelection, config["GLOBAL"])
    dfw.Apply(Baseline.RecoJetSelection)
    df = dfw.df
    pt_bins = Utilities.ListToVector([20,25,30,35,40,50,60,70,80,100,150,200,300,500,1000], "double")
    eta_bins = Utilities.ListToVector([0,0.6,1.2,2.1,2.5], "double")
    model = ROOT.RDF.TH2DModel("", "", pt_bins.size()-1, pt_bins.data(), eta_bins.size()-1, eta_bins.data())
    hists = {}
    for flav in [ 0, 4, 5]:
        hist_name = f'jet_pt_eta_{flav}'
        df = df.Define(f"Jet_flavour{flav}_sel", f"Jet_bCand && Jet_hadronFlavour=={flav}")
        df = df.Define(f"Jet_eta_flavour{flav}", f"abs(v_ops::eta(Jet_p4[Jet_flavour{flav}_sel]))")
        df = df.Define(f"Jet_pt_flavour{flav}", f"v_ops::pt(Jet_p4[Jet_flavour{flav}_sel])")
        hists[hist_name] = df.Histo2D(model, f"Jet_pt_flavour{flav}", f"Jet_eta_flavour{flav}")
        for wp, thr in wpValues.items():
            df = df.Define(f"Jet_WP{wp.name}_flavour{flav}_sel", f"Jet_flavour{flav}_sel && Jet_btagDeepFlavB > {thr}")
            df = df.Define(f"Jet_eta_WP{wp.name}_flavour{flav}", f"abs(v_ops::eta(Jet_p4[Jet_WP{wp.name}_flavour{flav}_sel]))")
            df = df.Define(f"Jet_pt_WP{wp.name}_flavour{flav}", f"v_ops::pt(Jet_p4[Jet_WP{wp.name}_flavour{flav}_sel])")
            hist_name = f'jet_pt_eta_{flav}_{wp.name}'
            hists[hist_name] = df.Histo2D(model, f"Jet_pt_WP{wp.name}_flavour{flav}", f"Jet_eta_WP{wp.name}_flavour{flav}")
    return hists

if __name__ == "__main__":
    import argparse
    import os
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--configFile', type=str)
    parser.add_argument('--inFile', type=str)
    parser.add_argument('--sample', type=str)
    parser.add_argument('--nEvents', type=int, default=None)
    parser.add_argument('--evtIds', type=str, default='')
    parser.add_argument('--outFile', type=str)

    args = parser.parse_args()
    if os.path.exists(args.outFile):
        os.remove(args.outFile)

    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "include/GenTools.h"')
    isHH=False
    with open(args.configFile, 'r') as f:
        config = yaml.safe_load(f)
    hists = bTagProdEff(args.inFile, config, args.sample, args.nEvents,args.evtIds)
    fileToSave = ROOT.TFile(args.outFile, 'RECREATE')
    for hist_name, hist in hists.items():
        hist.SetTitle(hist_name)
        hist.SetName(hist_name)
        fileToSave.WriteTObject(hist.GetValue(), hist_name)
    fileToSave.Close()
    #for hist_name, hist in hists.items():

