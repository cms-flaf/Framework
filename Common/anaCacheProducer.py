import ROOT
import numpy as np
import Common.BaselineSelection as Baseline
import Corrections.Corrections as Corrections

#def createAnaCache(inFile, outFile, config, sample_name, snapshotOptions,range, evtIds,
def createAnaCache(inFile, config, sample_name, range, evtIds,
                   store_noncentral, dict):

    period = config["GLOBAL"]["era"]
    print(period)

    isData = True if config[sample_name]['sampleType'] == 'data' else False

    Baseline.Initialize(True, True)
    if not isData:
        Corrections.Initialize(period=period)

    df_sel = ROOT.RDataFrame("Events", inFile)
    if range is not None:
        df_sel = df_sel.Range(range)
    df_nonSel = ROOT.RDataFrame("EventsNotSelected", inFile)
    if range is not None:
        df_nonSel = df_nonSel.Range(range)
    if len(evtIds) > 0:
        df_sel = df_sel.Filter(f"static const std::set<ULong64_t> evts = {{ {evtIds} }}; return evts.count(event) > 0;")
    if len(evtIds) > 0:
        df_nonSel = df_nonSel.Filter(f"static const std::set<ULong64_t> evts = {{ {evtIds} }}; return evts.count(event) > 0;")

    print(dict)
    df_denumerator_sel,syst_names = Corrections.getDenumerator(df_sel)
    df_denumerator_nonSel,syst_names = Corrections.getDenumerator(df_nonSel)
    dict[sample_name]={}
    for syst_name in syst_names:
        dict[sample_name][syst_name] = df_denumerator_sel.Sum(f'weight_denum_{syst_name}').GetValue()+df_denumerator_nonSel.Sum(f'weight_denum_{syst_name}').GetValue()
        print(f'for {syst_name} the denumerator is {dict[sample_name][syst_name]}')
    print(dict)



if __name__ == "__main__":
    import argparse
    import os
    import yaml
    import json
    parser = argparse.ArgumentParser()
    parser.add_argument('--configFile', type=str)
    parser.add_argument('--inFile', type=str)
    parser.add_argument('--outFile', type=str)
    parser.add_argument('--sample', type=str)
    parser.add_argument('--nEvents', type=int, default=None)
    parser.add_argument('--evtIds', type=str, default='')
    parser.add_argument('--store-noncentral', action="store_true", help="Store ES variations.")

    args = parser.parse_args()

    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "Common/GenTools.h"')
    isHH=False
    isData = False
    with open(args.configFile, 'r') as f:
        config = yaml.safe_load(f)
    dict = {}
    if os.path.exists(args.outFile):
        with open(args.outFile, 'r') as file:
            dict = yaml.safe_load(file)
    createAnaCache(args.inFile, config, args.sample, args.nEvents,
                   args.evtIds, args.store_noncentral, dict)
    with open(args.outFile, 'w') as file:
        yaml.dump(dict, file)


