import os
import sys
import yaml
import ROOT
import datetime
import time
import shutil

ROOT.EnableThreadSafety()

from FLAF.Common.Utilities import DeclareHeader
from FLAF.RunKit.run_tools import ps_call
import FLAF.Common.LegacyVariables as LegacyVariables
import FLAF.Common.Utilities as Utilities

defaultColToSave = ["entryIndex","luminosityBlock", "run","event", "sample_type", "sample_name", "period", "X_mass", "X_spin", "isData"]
scales = ['Up','Down']

def getKeyNames(root_file_name):
    root_file = ROOT.TFile(root_file_name, "READ")
    key_names = [str(k.GetName()) for k in root_file.GetListOfKeys() ]
    root_file.Close()
    return key_names

def applyLegacyVariables(dfw, global_cfg_dict, is_central=True):
    channels = global_cfg_dict['channelSelection']
    trg_strings = {}
    for channel in channels :
        ch_value = global_cfg_dict['channelDefinition'][channel]
        dfw.df = dfw.df.Define(f"{channel}", f"channelId=={ch_value}")
        trigger_list = global_cfg_dict['triggers'][channel]
        trg_strings[channel] = "("
        trg_strings[channel] += ' || '.join(f'HLT_{trg}' for trg in trigger_list)
        trg_strings[channel] += ")"
        for trigger in trigger_list:
            trigger_name = 'HLT_'+trigger
            if trigger_name not in dfw.df.GetColumnNames():
                dfw.df = dfw.df.Define(trigger_name, "1")
    entryvalid_stri = '('
    entryvalid_stri += ' || '.join(f'({ch} & {trg_strings[ch]})' for ch in channels)
    entryvalid_stri += ')'
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
    return df_central

def createAnaCacheTuple(inFileName, outFileName, unc_cfg_dict, global_cfg_dict, snapshotOptions, compute_unc_variations, deepTauVersion):
    start_time = datetime.datetime.now()
    verbosity = ROOT.Experimental.RLogScopedVerbosity(ROOT.Detail.RDF.RDFLogChannel(), ROOT.Experimental.ELogLevel.kInfo)
    snaps = []
    all_files = []
    file_keys = getKeyNames(inFileName)
    df = ROOT.RDataFrame('Events', inFileName)
    df_begin = df
    dfw = Utilities.DataFrameWrapper(df_begin,defaultColToSave)
    LegacyVariables.Initialize()
    applyLegacyVariables(dfw, global_cfg_dict, True)
    varToSave = Utilities.ListToVector(dfw.colToSave)
    all_files.append(f'{outFileName}_Central.root')
    snaps.append(dfw.df.Snapshot(f"Events", f'{outFileName}_Central.root', varToSave, snapshotOptions))
    print("append the central snapshot")

    if compute_unc_variations:
        dfWrapped_central = Utilities.DataFrameBuilderBase(df_begin)
        colNames =  dfWrapped_central.colNames
        colTypes =  dfWrapped_central.colTypes
        dfWrapped_central.df = createCentralQuantities(df_begin, colTypes, colNames)
        if dfWrapped_central.df.Filter("map_placeholder > 0").Count().GetValue() <= 0 : raise RuntimeError("no events passed map placeolder")
        print("finished defining central quantities")
        snapshotOptions.fLazy=False
        for uncName in unc_cfg_dict['shape']:
            for scale in scales:
                treeName = f"Events_{uncName}{scale}"
                treeName_noDiff = f"{treeName}_noDiff"
                if treeName_noDiff in file_keys:
                    df_noDiff = ROOT.RDataFrame(treeName_noDiff, inFileName)
                    dfWrapped_noDiff = Utilities.DataFrameBuilderBase(df_noDiff)
                    dfWrapped_noDiff.CreateFromDelta(colNames, colTypes)
                    dfWrapped_noDiff.AddMissingColumns(colNames, colTypes)
                    dfW_noDiff = Utilities.DataFrameWrapper(dfWrapped_noDiff.df,defaultColToSave)
                    applyLegacyVariables(dfW_noDiff,global_cfg_dict, False)
                    varToSave = Utilities.ListToVector(dfW_noDiff.colToSave)
                    all_files.append(f'{outFileName}_{uncName}{scale}_noDiff.root')
                    dfW_noDiff.df.Snapshot(treeName_noDiff, f'{outFileName}_{uncName}{scale}_noDiff.root', varToSave, snapshotOptions)
                treeName_Valid = f"{treeName}_Valid"
                if treeName_Valid in file_keys:
                    df_Valid = ROOT.RDataFrame(treeName_Valid, inFileName)
                    dfWrapped_Valid = Utilities.DataFrameBuilderBase(df_Valid)
                    dfWrapped_Valid.CreateFromDelta(colNames, colTypes)
                    dfWrapped_Valid.AddMissingColumns(colNames, colTypes)
                    dfW_Valid = Utilities.DataFrameWrapper(dfWrapped_Valid.df,defaultColToSave)
                    applyLegacyVariables(dfW_Valid,global_cfg_dict, False)
                    varToSave = Utilities.ListToVector(dfW_Valid.colToSave)
                    all_files.append(f'{outFileName}_{uncName}{scale}_Valid.root')
                    dfW_Valid.df.Snapshot(treeName_Valid, f'{outFileName}_{uncName}{scale}_Valid.root', varToSave, snapshotOptions)
                treeName_nonValid = f"{treeName}_nonValid"
                if treeName_nonValid in file_keys:
                    df_nonValid = ROOT.RDataFrame(treeName_nonValid, inFileName)
                    dfWrapped_nonValid = Utilities.DataFrameBuilderBase(df_nonValid)
                    dfWrapped_nonValid.AddMissingColumns(colNames, colTypes)
                    dfW_nonValid = Utilities.DataFrameWrapper(dfWrapped_nonValid.df,defaultColToSave)
                    applyLegacyVariables(dfW_nonValid,global_cfg_dict, False)
                    varToSave = Utilities.ListToVector(dfW_nonValid.colToSave)
                    all_files.append(f'{outFileName}_{uncName}{scale}_nonValid.root')
                    dfW_nonValid.df.Snapshot(treeName_nonValid, f'{outFileName}_{uncName}{scale}_nonValid.root', varToSave, snapshotOptions)
    print(f"snaps len is {len(snaps)}")
    snapshotOptions.fLazy = True
    if snapshotOptions.fLazy == True:
        print("going to rungraph")
        ROOT.RDF.RunGraphs(snaps)
    return all_files


if __name__ == "__main__":
    import argparse
    import os
    parser = argparse.ArgumentParser()
    parser.add_argument('--inFileName', required=True, type=str)
    parser.add_argument('--outFileName', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--globalConfig', required=True, type=str)
    parser.add_argument('--compute_unc_variations', type=bool, default=False)
    parser.add_argument('--compressionLevel', type=int, default=4)
    parser.add_argument('--compressionAlgo', type=str, default="ZLIB")
    parser.add_argument('--deepTauVersion', type=str, default="v2p1")
    parser.add_argument('--channels', type=str, default=None)
    args = parser.parse_args()

    for header in [ "include/KinFitInterface.h", "FLAF/include/HistHelper.h", "FLAF/include/Utilities.h" ]:
        DeclareHeader(os.environ["ANALYSIS_PATH"]+"/"+header)

    snapshotOptions = ROOT.RDF.RSnapshotOptions()
    snapshotOptions.fOverwriteIfExists=True
    snapshotOptions.fLazy = True
    snapshotOptions.fMode="RECREATE"
    snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + args.compressionAlgo)
    snapshotOptions.fCompressionLevel = args.compressionLevel
    unc_cfg_dict = {}

    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)

    global_cfg_dict = {}
    with open(args.globalConfig, 'r') as f:
        global_cfg_dict = yaml.safe_load(f)

    startTime = time.time()
    if args.channels:
        global_cfg_dict['channelSelection'] = args.channels.split(',') if type(args.channels) == str else args.channels
    outFileNameFinal = f'{args.outFileName}'
    all_files = createAnaCacheTuple(args.inFileName, args.outFileName.split('.')[0], unc_cfg_dict, global_cfg_dict, snapshotOptions, args.compute_unc_variations, args.deepTauVersion)
    hadd_str = f'hadd -f209 -n10 {outFileNameFinal} '
    hadd_str += ' '.join(f for f in all_files)
    if len(all_files) > 1:
        ps_call([hadd_str], True)
    else:
        shutil.copy(all_files[0],outFileNameFinal)
    if os.path.exists(outFileNameFinal):
            for histFile in all_files:
                if histFile == outFileNameFinal: continue
                os.remove(histFile)
    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))
