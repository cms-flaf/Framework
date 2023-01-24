import ROOT
import numpy as np
import Common.BaselineSelection as Baseline
import Corrections.Corrections as Corrections
from Corrections.CorrectionsCore import *
from Corrections.pu import *

def createAnaCache(inDir, config, range, dict):

    period = config["GLOBAL"]["era"]

    Baseline.Initialize(True, True)
    Corrections.Initialize(period=period)
    df_sel = ROOT.RDataFrame("Events", os.listdir(inDir))
    df_nonSel = ROOT.RDataFrame("EventsNotSelected", os.listdir(inDir))
    if range is not None:
        df_sel = df_sel.Range(range)
        df_nonSel = df_nonSel.Range(range)

    sources = [ central, puWeightProducer.uncSource ]
    df_denumerator_sel,syst_names = Corrections.getDenumerator(df_sel, sources)
    df_denumerator_nonSel,syst_names = Corrections.getDenumerator(df_nonSel, sources)
    dict['denumerator']={}
    for source in sources:
        dict['denumerator'][source]={}
        for scale in getScales(source):
            syst_name = getSystName(source, scale)
            dict['denumerator'][source][scale] = df_denumerator_sel.Sum(f'weight_denum_{syst_name}').GetValue()+df_denumerator_nonSel.Sum(f'weight_denum_{syst_name}').GetValue()
            print(f'for {syst_name} the denumerator is {dict["denumerator"][source][scale]}')
    print(dict)



if __name__ == "__main__":
    import argparse
    import os
    import yaml
    import json
    parser = argparse.ArgumentParser()
    parser.add_argument('--configFile', type=str)
    parser.add_argument('--inDir', type=str)
    parser.add_argument('--outFile', type=str)
    parser.add_argument('--nEvents', type=int, default=None)

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
    createAnaCache(args.inDir, config, args.nEvents, dict)
    with open(args.outFile, 'w') as file:
        yaml.dump(dict, file)


