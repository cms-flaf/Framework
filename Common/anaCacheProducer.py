import ROOT
import numpy as np
import Common.BaselineSelection as Baseline
import Corrections.Corrections as Corrections
from Corrections.CorrectionsCore import *
from Corrections.pu import *

def createAnaCache(inDir, config, range, dict):

    period = config["GLOBAL"]["era"]

    Baseline.Initialize(False, False)
    Corrections.Initialize(period=period)
    dict['denumerator']={}
    sources = [ central, puWeightProducer.uncSource ]
    for tree in [ 'Events', 'EventsNotSelected']:
        df = ROOT.RDataFrame(tree, os.listdir(inDir))
        if range is not None:
            df = df.Range(range)
        df,syst_names = Corrections.getDenumerator(df, sources)
        for source in sources:
            if source not in dict['denumerator']:
                dict['denumerator'][source]={}
            for scale in getScales(source):
                syst_name = getSystName(source, scale)
                dict['denumerator'][source][scale] = dict['denumerator'][source].get(scale, 0.) + df.Sum(f'weight_denum_{syst_name}').GetValue()
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
    if os.path.exists(args.outFile):
        with open(args.outFile, 'r') as file:
            dict = yaml.safe_load(file)
    else:
        dict = {}
    createAnaCache(args.inDir, config, args.nEvents, dict)
    with open(args.outFile, 'w') as file:
        yaml.dump(dict, file)


