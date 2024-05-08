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
    ROOT.gInterpreter.Declare(f'#include "include/KinFitInterface.h"')
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')

from RunKit.run_tools import ps_call
ROOT.EnableThreadSafety()
#ROOT.EnableImplicitMT()
import Common.LegacyVariables as LegacyVariables
import Common.Utilities as Utilities
defaultColToSave = ["entryIndex","luminosityBlock", "run","event", "sample_type", "sample_name", "period", "X_mass", "X_spin", "isData"]
scales = ['Up','Down']
from Analysis.HistHelper import *
from Analysis.hh_bbtautau import *

def getKeyNames(root_file_name):
    print(root_file_name)
    root_file = ROOT.TFile(root_file_name, "READ")
    print(root_file.GetListOfKeys())
    key_names = [str(k.GetName()) for k in root_file.GetListOfKeys() ]
    root_file.Close()
    return key_names



def applyLegacyVariables(dfw, is_central=True):
    for channel,ch_value in channels.items():
            dfw.df = dfw.df.Define(f"{channel}", f"channelId=={ch_value}")
            for trigger in trigger_list[channel]:
                if trigger not in dfw.df.GetColumnNames():
                    dfw.df = dfw.df.Define(trigger, "1")
    #entryvalid_stri = '((b1_pt > 0) & (b2_pt > 0)) & ('
    entryvalid_stri = '('
    entryvalid_stri += ' || '.join(f'({ch} & {triggers[ch]})' for ch in channels)
    entryvalid_stri += ')'
    #print(entryvalid_stri)
    dfw.DefineAndAppend("entry_valid",entryvalid_stri)
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

def createAnaCacheTuple(inFileName, outFileName, unc_cfg_dict, snapshotOptions, compute_unc_variations, deepTauVersion):
    start_time = datetime.datetime.now()
    print("1")
    verbosity = ROOT.Experimental.RLogScopedVerbosity(ROOT.Detail.RDF.RDFLogChannel(), ROOT.Experimental.ELogLevel.kInfo)
    print("2")
    snaps = []
    all_files = []
    file_keys = getKeyNames(inFileName)
    print("3")
    df = ROOT.RDataFrame('Events', inFileName)
    print("4")
    df_begin = df
    print("5")
    dfw = Utilities.DataFrameWrapper(df_begin,defaultColToSave)
    print("6")
    LegacyVariables.Initialize()
    print("7")
    applyLegacyVariables(dfw, True)
    print("8")
    varToSave = Utilities.ListToVector(dfw.colToSave)
    print("9")
    all_files.append(f'{outFileName}_Central.root')
    print("10")
    snaps.append(dfw.df.Snapshot(f"Events", f'{outFileName}_Central.root', varToSave, snapshotOptions))
    print("append the central snapshot")

    if compute_unc_variations:
        dfWrapped_central = DataFrameBuilder(df_begin, deepTauVersion)
        print("11")
        colNames =  dfWrapped_central.colNames
        print("12")
        colTypes =  dfWrapped_central.colTypes
        print("13")
        dfWrapped_central.df = createCentralQuantities(df_begin, colTypes, colNames)
        print("14")
        if dfWrapped_central.df.Filter("map_placeholder > 0").Count().GetValue() <= 0 : raise RuntimeError("no events passed map placeolder")
        print("finished defining central quantities")
        snapshotOptions.fLazy=False
        print(file_keys)
        for uncName in unc_cfg_dict['shape']:
            print(uncName)
            for scale in scales:
                print(scale)
                treeName = f"Events_{uncName}{scale}"
                print(treeName)
                treeName_noDiff = f"{treeName}_noDiff"
                if treeName_noDiff in file_keys:
                    print(treeName_noDiff)
                    dfWrapped_noDiff = DataFrameBuilder(ROOT.RDataFrame(treeName_noDiff, inFileName), deepTauVersion)
                    print("15")
                    dfWrapped_noDiff.CreateFromDelta(colNames, colTypes)
                    print("16")
                    dfW_noDiff = Utilities.DataFrameWrapper(dfWrapped_noDiff.df,defaultColToSave)
                    print("17")
                    applyLegacyVariables(dfW_noDiff,False)
                    print("18")
                    #print(f"number of events in dfW_noDiff is {dfW_noDiff.df.Count().GetValue()}")
                    varToSave = Utilities.ListToVector(dfW_noDiff.colToSave)
                    print("19")
                    all_files.append(f'{outFileName}_{uncName}{scale}_noDiff.root')
                    print("20")
                    dfW_noDiff.df.Snapshot(treeName_noDiff, f'{outFileName}_{uncName}{scale}_noDiff.root', varToSave, snapshotOptions)
                    print("21")
                treeName_Valid = f"{treeName}_Valid"
                if treeName_Valid in file_keys:
                    print(treeName_Valid)
                    df_Valid = ROOT.RDataFrame(treeName_Valid, inFileName)
                    print("15")
                    dfWrapped_Valid = DataFrameBuilder(df_Valid, deepTauVersion)
                    print("16")
                    dfWrapped_Valid.CreateFromDelta(colNames, colTypes)
                    print("17")
                    dfW_Valid = Utilities.DataFrameWrapper(dfWrapped_Valid.df,defaultColToSave)
                    print("18")
                    applyLegacyVariables(dfW_Valid,False)
                    print("19")
                    varToSave = Utilities.ListToVector(dfW_Valid.colToSave)
                    print("20")
                    #print(f"number of events in dfW_Valid is {dfW_Valid.df.Count().GetValue()}")
                    all_files.append(f'{outFileName}_{uncName}{scale}_Valid.root')
                    print("21")
                    dfW_Valid.df.Snapshot(treeName_Valid, f'{outFileName}_{uncName}{scale}_Valid.root', varToSave, snapshotOptions)
                    print("22")
                treeName_nonValid = f"{treeName}_nonValid"
                if treeName_nonValid in file_keys:
                    print(treeName_nonValid)
                    dfWrapped_nonValid = DataFrameBuilder(ROOT.RDataFrame(treeName_nonValid, inFileName), deepTauVersion)
                    print("15")
                    dfW_nonValid = Utilities.DataFrameWrapper(dfWrapped_nonValid.df,defaultColToSave)
                    print("16")
                    applyLegacyVariables(dfW_nonValid,False)
                    print("17")
                    #print(f"number of events in dfW_nonValid is {dfW_nonValid.df.Count().GetValue()}")
                    varToSave = Utilities.ListToVector(dfW_nonValid.colToSave)
                    print("18")
                    all_files.append(f'{outFileName}_{uncName}{scale}_nonValid.root')
                    print("19")
                    dfW_nonValid.df.Snapshot(treeName_nonValid, f'{outFileName}_{uncName}{scale}_nonValid.root', varToSave, snapshotOptions)
                    print("20")
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
    parser.add_argument('--deepTauVersion', type=str, default="v2p1")
    args = parser.parse_args()
    print("A")
    snapshotOptions = ROOT.RDF.RSnapshotOptions()
    snapshotOptions.fOverwriteIfExists=True
    snapshotOptions.fLazy = True
    snapshotOptions.fMode="RECREATE"
    snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + args.compressionAlgo)
    snapshotOptions.fCompressionLevel = args.compressionLevel
    unc_cfg_dict = {}
    print("B")

    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    print("C")

    startTime = time.time()

    outFileNameFinal = f'{args.outFileName}'
    print(outFileNameFinal)
    try:
        all_files = createAnaCacheTuple(args.inFileName, args.outFileName.strip('.root'), unc_cfg_dict, snapshotOptions, args.compute_unc_variations, args.deepTauVersion)
        print("D")
        hadd_str = f'hadd -f209 -n10 {outFileNameFinal} '
        hadd_str += ' '.join(f for f in all_files)
        print(hadd_str)
        if len(all_files) > 1:
            ps_call([hadd_str], True)
        else:
            shutil.copy(all_files[0],outFileNameFinal)
        if os.path.exists(outFileNameFinal):
                for histFile in all_files:
                    if histFile == outFileNameFinal: continue
                    os.remove(histFile)
    except:
        print("ffjfjfjfj")
        df = ROOT.RDataFrame(0)
        df=df.Define("test", "return true;")
        df.Snapshot("Events", outFileNameFinal, {"test"})
        #Utilities.create_file(outFileNameFinal)
    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))
