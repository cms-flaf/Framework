import ROOT
import numpy as np
import Common.Utilities as Utilities

import Common.BaselineSelection as Baseline
  

def GetMPV(inFile):
    Baseline.Initialize()

    df = ROOT.RDataFrame("Events", inFile) 

    df = Baseline.ApplyGenBaseline0(df)
    df = Baseline.ApplyGenBaseline1(df) 
    '''
    df = Baseline.ApplyRecoBaseline0(df)
    df = Baseline.ApplyRecoBaseline1(df)
    df = Baseline.ApplyRecoBaseline2(df)
    df = df.Define('genChannel', 'genHttCand.channel()')
    df = df.Define('recoChannel', 'httCand.channel()')

    df = df.Filter("genChannel == recoChannel", "SameGenRecoChannels")
    df = df.Filter("GenRecoMatching(genHttCand, httCand, 0.2)", "SameGenRecoHTT")     
    x_max = Baseline.FindMPV(df) '''
    x_max = Baseline.FindMPV(df)
    return x_max


if __name__ == "__main__":
    import argparse
    import os
    import re 

    parser = argparse.ArgumentParser() 
    parser.add_argument('--inFile', type=str)  
    parser.add_argument('--particleFile', type=str,
                        default=f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt")
    args = parser.parse_args()
    
    
    ROOT.gROOT.SetBatch(True)
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "Common/GenTools.h"')
    ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{args.particleFile}\");")   
    mpv = GetMPV(args.inFile)
    print(f"the mpv is {mpv}")
        