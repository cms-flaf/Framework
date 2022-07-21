# from Visual.HistTools import *
# from Studies.HHBTag.Utils import *
import ROOT
import numpy as np

import Common.BaselineSelection as Baseline
 

def createSkim(inFile, outFile, year, sample, X_mass):
    Baseline.Initialize()

    df = ROOT.RDataFrame("Events", inFile)

    df = Baseline.ApplyGenBaseline(df)
    df = Baseline.ApplyRecoBaseline0(df)
    df = Baseline.ApplyRecoBaseline1(df)
    df = Baseline.ApplyRecoBaseline2(df)
    df = df.Define('genChannel', 'genHttCand.channel()')
    df = df.Define('recoChannel', 'httCand.channel()')

    df = df.Filter("genChannel == recoChannel", "SameGenRecoChannels")
    df = df.Filter("GenRecoMatching(genHttCand, httCand, 0.2)", "SameGenRecoHTT")
    df = Baseline.ApplyRecoBaseline3(df)
    df = Baseline.ApplyGenBaseline1(df)
    #df = Baseline.ApplyGenRecoJetMatching(df)
    df = df.Define("year", f"{year}")
    df = df.Define("sample", f"\"{sample}\"")
    df = df.Define("X_mass", f"{X_mass}")

    report = df.Report()
    report.Print()
    

    
    colToSave = ["year", "event","sample","run","lumi","X_mass", "Jet_p4", "RecoJetMatch", "httCand",
                 "genChannel", "recoChannel", "Jet_btagCMVA", "Jet_btagCSVV2", "Jet_btagDeepB", "Jet_btagDeepC", 
                 "Jet_btagDeepCvB", "Jet_btagDeepCvL", "Jet_btagDeepFlavB", "Jet_btagDeepFlavC", "Jet_btagDeepFlavCvB", 
                 "Jet_btagDeepFlavCvL", "Jet_btagDeepFlavQG", "Jet_qgl"]
    
    '''
    varToSave = ROOT.std.vector("string")()
    for col in colToSave:
        varToSave.push_back(col)
    for trg in triggers[year]: 
        varToSave.push_back(trg)
    df.Snapshot("Event", outFile, varToSave())
    '''
    '''
    channels = df.AsNumpy(['genChannel', 'recoChannel'])
    ch, cnt = np.unique(channels['genChannel'], return_counts=True)
    print(ch)
    print(cnt)
    ch, cnt = np.unique(channels['recoChannel'], return_counts=True)
    print(ch)
    print(cnt)
    '''
    # print(df.Filter('genHttCand.channel() == Channel::eTau').Count().GetValue())
    # print(df.Filter('genHttCand.channel() == Channel::muTau').Count().GetValue())
    # print(df.Filter('genHttCand.channel() == Channel::tauTau').Count().GetValue())
    #df = Baseline.ApplyRecoBaselineL0(df)
    # df = ApplyRecoBaselineL1(df)
    # df = ApplyGenRecoMatch(df)
    # df = DefineDataFrame(df, ch)
    # mpv = findMPV(df)
    # print(f"the mpv is {mpv}")


if __name__ == "__main__":
    import argparse
    import os
    import re 

    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=str, default='2018')
    #parser.add_argument('--inFile', type=str)
    #parser.add_argument('--outFile', type=str)
    parser.add_argument('--particleFile', type=str,
                        default=f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt")
    args = parser.parse_args()

    ROOT.gROOT.SetBatch(True)
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "Common/GenTools.h"')
    ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{args.particleFile}\");")
    inDir = f"{os.environ['CENTRAL_STORAGE']}/prod_v1/nanoAOD/{args.year}"
    outDir = 'output' 
    for file in os.listdir(inDir):
        inFile = f"{inDir}/{file}"
        substring= re.split("-|_|\.|ToHH",file)
        year=args.year
        sample=substring[0]
        X_mass = int(substring[3])
        outFile = f"{outDir}/{file}"
        if(X_mass != 350 or sample!='GluGluToBulkGraviton'): continue # and sample!='GluGluToRadion'): continue
        print(inFile)
        print(outFile)
        print(X_mass)
        print(sample)
        print()
        createSkim(inFile, outFile, year, sample, X_mass)
        