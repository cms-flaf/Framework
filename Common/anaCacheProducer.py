import ROOT
import numpy as np
import Common.BaselineSelection as Baseline

def createAnatuple(inFile, outFile, config, sample, X_mass, snapshotOptions,range, isData, evtIds, isHH, triggerFile,
                   store_noncentral):

    period = config["GLOBAL"]["era"]

    Baseline.Initialize(True, True)
    if not isData:
        Corrections.Initialize(period=period)

    trigger_class = Triggers.Triggers(triggerFile) if triggerFile is not None else None
    df = ROOT.RDataFrame("Events", inFile)
    if range is not None:
        df = df.Range(range)
    if len(evtIds) > 0:
        df = df.Filter(f"static const std::set<ULong64_t> evts = {{ {evtIds} }}; return evts.count(event) > 0;")
    df = Baseline.applyMETFlags(df, config["GLOBAL"]["MET_flags"])
    df = DefineAndAppend(df,"sample_type", f"static_cast<int>(SampleType::{sample})")
    df = DefineAndAppend(df,"period", f"static_cast<int>(Period::{period})")
    df = DefineAndAppend(df,"X_mass", f"static_cast<int>({X_mass})")
    is_data = 'true' if isData else 'false'
    df = DefineAndAppend(df,"is_data", is_data)

    df = Baseline.CreateRecoP4(df)
    df = Baseline.DefineGenObjects(df, isData=isData, isHH=isHH)
    if isData:
        syst_dict = { 'nano' : 'Central' }
    else:
        df, syst_dict = Corrections.applyScaleUncertainties(df)
    #df, weight_list = getWeights(df, df_nonsel, config, sample)
    df,weight_branches = Corrections.getWeights(df)
    for br in weight_branches:
        br_name = f'weight_{br}' if br != "Central" else "weight"
        colToSave.append(br_name)
    for syst_name, source_name in syst_dict.items():
        suffix = '' if syst_name in [ 'Central', 'nano' ] else f'_{syst_name}'
        if len(suffix) and not store_noncentral: continue
        df_syst = addAllVariables(df, syst_name, isData, trigger_class)
        report = df_syst.Report()
        histReport = ReportTools.SaveReport(report.GetValue(), reoprtName=f"Report{suffix}")
        varToSave = Utilities.ListToVector(colToSave)
        df_syst.Snapshot(f"Events{suffix}", outFile, varToSave, snapshotOptions)
        outputRootFile= ROOT.TFile(outFile, "UPDATE")
        outputRootFile.WriteTObject(histReport, f"Report{suffix}", "Overwrite")
        outputRootFile.Close()

if __name__ == "__main__":
    import argparse
    import os
    import yaml
    from yaml.loader import SafeLoader
    parser = argparse.ArgumentParser()
    parser.add_argument('--configFile', type=str)
    parser.add_argument('--inFile', type=str)
    parser.add_argument('--outFile', type=str)
    parser.add_argument('--mass', type=int, default=-1)
    parser.add_argument('--sample_type', type=str)
    parser.add_argument('--compressionLevel', type=int, default=9)
    parser.add_argument('--compressionAlgo', type=str, default="LZMA")
    parser.add_argument('--nEvents', type=int, default=None)
    parser.add_argument('--evtIds', type=str, default='')
    parser.add_argument('--store-noncentral', action="store_true", help="Store ES variations.")
    parser.add_argument('--triggerFile', type=str, default=None)

    args = parser.parse_args()

    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "Common/GenTools.h"')
    isHH=False
    isData = False
    hh_samples = [ "GluGluToRadion", "GluGluToBulkGraviton", "VBFToRadion", "VBFToBulkGraviton", "HHnonRes", "TTHH" ]
    if args.mass>0 and args.sample_type in hh_samples:
        isHH = True
    if args.sample_type =='data':
        isData = True
    if os.path.exists(args.outFile):
        os.remove(args.outFile)



    with open(args.configFile, 'r') as f:
        config = yaml.safe_load(f)


    snapshotOptions = ROOT.RDF.RSnapshotOptions()
    snapshotOptions.fOverwriteIfExists=True
    snapshotOptions.fMode="UPDATE"
    snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + args.compressionAlgo)
    snapshotOptions.fCompressionLevel = args.compressionLevel
    createAnatuple(args.inFile, args.outFile, config, args.sample_type, args.mass, snapshotOptions, args.nEvents,
                   isData, args.evtIds, isHH, args.triggerFile, args.store_noncentral)
