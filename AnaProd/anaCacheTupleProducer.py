import os
import sys
import yaml
import ROOT
import datetime
import time
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')

ROOT.EnableThreadSafety()
import Common.LegacyVariables as LegacyVariables
import Common.Utilities as Utilities
defaultColToSave = ["entryIndex","luminosityBlock", "run","event", "sample_type", "sample_name", "period", "X_mass", "X_spin", "isData"]
scales = ['Up','Down']
from Analysis.HistHelper import *
from Analysis.hh_bbtautau import *

def getKeyNames(root_file_name):
    root_file = ROOT.TFile(root_file_name, "READ")
    key_names = [str(k.GetName()) for k in root_file.GetListOfKeys() ]
    root_file.Close()
    return key_names

def applyLegacyVariables(dfw):
    dfw.DefineAndAppend("entry_valid","((b1_pt > 0) & (b2_pt > 0)) & (((channelId == 13) & (HLT_singleEle)) | ((channelId == 23) & (HLT_singleMu)) | ((channelId == 33) & (HLT_ditau)))")
    MT2Branches = dfw.Apply(LegacyVariables.GetMT2)
    dfw.colToSave.extend(MT2Branches)
    KinFitBranches = dfw.Apply(LegacyVariables.GetKinFit)
    dfw.colToSave.extend(KinFitBranches)
    SVFitBranches = dfw.Apply(LegacyVariables.GetSVFit)
    dfw.colToSave.extend(SVFitBranches)

def createCentralQuantities(df_central, central_col_types, central_columns):
    map_creator = ROOT.analysis.MapCreator(*central_col_types)()
    df_central = map_creator.processCentral(ROOT.RDF.AsRNode(df_central), Utilities.ListToVector(central_columns))
    #df_central = map_creator.getEventIdxFromShifted(ROOT.RDF.AsRNode(df_central))
    return df_central

def createAnaCacheTuple(inFileName, outFileName, unc_cfg_dict, snapshotOptions, compute_unc_variations):
    start_time = datetime.datetime.now()
    snaps = []
    file_keys = getKeyNames(inFileName)
    df = ROOT.RDataFrame('Events', inFileName)
    df_begin = df
    dfw = Utilities.DataFrameWrapper(df_begin,defaultColToSave)
    LegacyVariables.Initialize()
    applyLegacyVariables(dfw)
    varToSave = Utilities.ListToVector(dfw.colToSave)
    snaps.append(dfw.df.Snapshot(f"Events", outFileName, varToSave, snapshotOptions))
    #print("append the central snapshot")
    if compute_unc_variations:
        dfWrapped_central = DataFrameBuilder(df_begin)
        colNames =  dfWrapped_central.colNames
        colTypes =  dfWrapped_central.colTypes
        dfWrapped_central.df = createCentralQuantities(df_begin, colTypes, colNames)
        if dfWrapped_central.df.Filter("map_placeholder > 0").Count().GetValue() <= 0 : raise RuntimeError("no events passed map placeolder")
        print("finished defining central quantities")
        for uncName in unc_cfg_dict['shape']:
            for scale in scales:
                treeName = f"Events_{uncName}{scale}"
                treeName_noDiff = f"{treeName}_noDiff"
                if treeName_noDiff in file_keys:
                    dfWrapped_noDiff = DataFrameBuilder(ROOT.RDataFrame(treeName_noDiff, inFileName))
                    dfWrapped_noDiff.CreateFromDelta(colNames, colTypes)
                    #PrepareDfWrapped(dfWrapped_noDiff)
                    dfW_noDiff = Utilities.DataFrameWrapper(dfWrapped_noDiff.df,defaultColToSave)
                    applyLegacyVariables(dfW_noDiff)
                    varToSave = Utilities.ListToVector(dfW_noDiff.colToSave)
                    snaps.append(dfW_noDiff.df.Snapshot(treeName_Valid, outFileName, varToSave, snapshotOptions))
                treeName_Valid = f"{treeName}_Valid"
                if treeName_Valid in file_keys:
                    df_Valid = ROOT.RDataFrame(treeName_Valid, inFileName)
                    dfWrapped_Valid = DataFrameBuilder(df_Valid)
                    dfWrapped_Valid.CreateFromDelta(colNames, colTypes)
                    dfW_Valid = Utilities.DataFrameWrapper(dfWrapped_Valid.df,defaultColToSave)
                    applyLegacyVariables(dfW_Valid)
                    varToSave = Utilities.ListToVector(dfW_Valid.colToSave)
                    snaps.append(dfW_Valid.df.Snapshot(treeName_Valid, outFileName, varToSave, snapshotOptions))
                treeName_nonValid = f"{treeName}_nonValid"
                if treeName_nonValid in file_keys:
                    dfWrapped_nonValid = DataFrameBuilder(ROOT.RDataFrame(treeName_nonValid, inFileName))
                    dfW_nonValid = Utilities.DataFrameWrapper(dfWrapped_nonValid.df,defaultColToSave)
                    applyLegacyVariables(dfW_nonValid)
                    varToSave = Utilities.ListToVector(dfW_nonValid.colToSave)
                    snaps.append(dfW_nonValid.df.Snapshot(treeName_Valid, outFileName, varToSave, snapshotOptions))
    if snapshotOptions.fLazy == True:
        #print("going to rungraph")
        ROOT.RDF.RunGraphs(snaps)


if __name__ == "__main__":
    import argparse
    import os
    parser = argparse.ArgumentParser()
    parser.add_argument('--inFileName', required=True, type=str)
    parser.add_argument('--outFileName', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--compute_unc_variations', type=bool, default=False)
    parser.add_argument('--compressionLevel', type=int, default=4)
    parser.add_argument('--compressionAlgo', type=str, default="ZLIB")
    args = parser.parse_args()


    snapshotOptions = ROOT.RDF.RSnapshotOptions()
    snapshotOptions.fOverwriteIfExists=True
    snapshotOptions.fLazy = True
    snapshotOptions.fMode="RECREATE"
    snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + args.compressionAlgo)
    snapshotOptions.fCompressionLevel = args.compressionLevel
    unc_cfg_dict = {}
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    startTime = time.time()
    createAnaCacheTuple(args.inFileName, args.outFileName, unc_cfg_dict, snapshotOptions, args.compute_unc_variations)

    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))
