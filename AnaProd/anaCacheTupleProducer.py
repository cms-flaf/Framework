import os
import sys
import yaml
import ROOT
import datetime
import time
import shutil
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')

from RunKit.sh_tools import sh_call
ROOT.EnableThreadSafety()
#ROOT.EnableImplicitMT()
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

def applyLegacyVariables(dfw, is_central=True):
    dfw.DefineAndAppend("entry_valid","((b1_pt > 0) & (b2_pt > 0)) & (((channelId == 13) & (HLT_singleEle)) | ((channelId == 23) & (HLT_singleMu)) | ((channelId == 33) & (HLT_ditau)))")
    MT2Branches = dfw.Apply(LegacyVariables.GetMT2)
    dfw.colToSave.extend(MT2Branches)
    KinFitBranches = dfw.Apply(LegacyVariables.GetKinFit)
    dfw.colToSave.extend(KinFitBranches)
    if is_central:
        SVFitBranches  = dfw.Apply(LegacyVariables.GetSVFit)
        dfw.colToSave.extend(SVFitBranches)

def createCentralQuantities(df_central, central_col_types, central_columns):
    map_creator = ROOT.analysis.MapCreator(*central_col_types)()
    df_central = map_creator.processCentral(ROOT.RDF.AsRNode(df_central), Utilities.ListToVector(central_columns))
    #df_central = map_creator.getEventIdxFromShifted(ROOT.RDF.AsRNode(df_central))
    return df_central

def createAnaCacheTuple(inFileName, outFileName, unc_cfg_dict, snapshotOptions, compute_unc_variations):
    start_time = datetime.datetime.now()
    verbosity = ROOT.Experimental.RLogScopedVerbosity(ROOT.Detail.RDF.RDFLogChannel(), ROOT.Experimental.ELogLevel.kInfo)
    snaps = []
    all_files = []
    file_keys = getKeyNames(inFileName)
    df = ROOT.RDataFrame('Events', inFileName)
    df_begin = df
    dfw = Utilities.DataFrameWrapper(df_begin,defaultColToSave)
    LegacyVariables.Initialize()
    applyLegacyVariables(dfw, True)
    varToSave = Utilities.ListToVector(dfw.colToSave)
    all_files.append(f'{outFileName}_Central.root')
    snaps.append(dfw.df.Snapshot(f"Events", f'{outFileName}_Central.root', varToSave, snapshotOptions))
    print("append the central snapshot")

    if compute_unc_variations:
        dfWrapped_central = DataFrameBuilder(df_begin)
        colNames =  dfWrapped_central.colNames
        colTypes =  dfWrapped_central.colTypes
        dfWrapped_central.df = createCentralQuantities(df_begin, colTypes, colNames)
        if dfWrapped_central.df.Filter("map_placeholder > 0").Count().GetValue() <= 0 : raise RuntimeError("no events passed map placeolder")
        #print("finished defining central quantities")
        snapshotOptions.fLazy=False
        for uncName in unc_cfg_dict['shape']:
            for scale in scales:
                treeName = f"Events_{uncName}{scale}"
                treeName_noDiff = f"{treeName}_noDiff"
                if treeName_noDiff in file_keys:
                    dfWrapped_noDiff = DataFrameBuilder(ROOT.RDataFrame(treeName_noDiff, inFileName))
                    dfWrapped_noDiff.CreateFromDelta(colNames, colTypes)
                    dfW_noDiff = Utilities.DataFrameWrapper(dfWrapped_noDiff.df,defaultColToSave)
                    applyLegacyVariables(dfW_noDiff,False)
                    #print(f"number of events in dfW_noDiff is {dfW_noDiff.df.Count().GetValue()}")
                    varToSave = Utilities.ListToVector(dfW_noDiff.colToSave)
                    all_files.append(f'{outFileName}_{uncName}{scale}_noDiff.root')
                    dfW_noDiff.df.Snapshot(treeName_noDiff, f'{outFileName}_{uncName}{scale}_noDiff.root', varToSave, snapshotOptions)
                treeName_Valid = f"{treeName}_Valid"
                if treeName_Valid in file_keys:
                    df_Valid = ROOT.RDataFrame(treeName_Valid, inFileName)
                    dfWrapped_Valid = DataFrameBuilder(df_Valid)
                    dfWrapped_Valid.CreateFromDelta(colNames, colTypes)
                    dfW_Valid = Utilities.DataFrameWrapper(dfWrapped_Valid.df,defaultColToSave)
                    applyLegacyVariables(dfW_Valid,False)
                    varToSave = Utilities.ListToVector(dfW_Valid.colToSave)
                    #print(f"number of events in dfW_Valid is {dfW_Valid.df.Count().GetValue()}")
                    all_files.append(f'{outFileName}_{uncName}{scale}_Valid.root')
                    dfW_Valid.df.Snapshot(treeName_Valid, f'{outFileName}_{uncName}{scale}_Valid.root', varToSave, snapshotOptions)
                treeName_nonValid = f"{treeName}_nonValid"
                if treeName_nonValid in file_keys:
                    dfWrapped_nonValid = DataFrameBuilder(ROOT.RDataFrame(treeName_nonValid, inFileName))
                    dfW_nonValid = Utilities.DataFrameWrapper(dfWrapped_nonValid.df,defaultColToSave)
                    applyLegacyVariables(dfW_nonValid,False)
                    #print(f"number of events in dfW_nonValid is {dfW_nonValid.df.Count().GetValue()}")
                    varToSave = Utilities.ListToVector(dfW_nonValid.colToSave)
                    all_files.append(f'{outFileName}_{uncName}{scale}_nonValid.root')
                    dfW_nonValid.df.Snapshot(treeName_nonValid, f'{outFileName}_{uncName}{scale}_nonValid.root', varToSave, snapshotOptions)
    print(f"snaps len is {len(snaps)}")
    snapshotOptions.fLazy = True
    if snapshotOptions.fLazy == True:
        print("going to rungraph")
        ROOT.RDF.RunGraphs(snaps)
    print(all_files)
    return all_files


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
    all_files = createAnaCacheTuple(args.inFileName, args.outFileName, unc_cfg_dict, snapshotOptions, args.compute_unc_variations)
    outFileNameFinal = f'{args.outFileName}.root'
    print(outFileNameFinal)
    hadd_str = f'hadd -f209 -n10 {outFileNameFinal} '
    hadd_str += ' '.join(f for f in all_files)
    print(hadd_str)
    if len(all_files) > 1:
        sh_call([hadd_str], True)
    else:
        shutil.copy(all_files[0],outFileNameFinal)
    if os.path.exists(outFileNameFinal):
            for histFile in all_files:
                if histFile == outFileNameFinal: continue
                os.remove(histFile)
    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))
