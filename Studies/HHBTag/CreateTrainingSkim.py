import ROOT
import numpy as np
import Common.Utilities as Utilities

import Common.BaselineSelection as Baseline
 
snapshotOptions = ROOT.RDF.RSnapshotOptions()
snapshotOptions.fOverwriteIfExists=True

jetVar_list = [ "pt", "eta", "phi", "mass", "btagCSVV2", "btagDeepB", "btagDeepFlavB", "RecoMatched" ]
def JetSavingCondition(df): 
    df = df.Define('Jet_selIdx', 'ReorderObjects(Jet_btagDeepFlavB, Jet_idx[Jet_B3T])')
    for var in jetVar_list:
        df = df.Define(f"RecoJet_{var}", f"Take(Jet_{var}, Jet_selIdx)") 
    return df

def createSkim(inFile, outFile, period, sample, X_mass, mpv):
    Baseline.Initialize()

    df = ROOT.RDataFrame("Events", inFile)

    df = Baseline.DefineGenObjects(df, mpv)  
    df = Baseline.ApplyGenBaseline0(df)  
    df = Baseline.ApplyGenBaseline1(df)
    df = Baseline.ApplyGenBaseline2(df) 
    df = Baseline.ApplyGenBaseline3(df) 
    is2017 = int(period=="2017")
    df = Baseline.ApplyRecoBaseline0(df)
    df = Baseline.ApplyRecoBaseline1(df,is2017 )
    df = Baseline.ApplyRecoBaseline2(df)
    df = Baseline.ApplyRecoBaseline3(df)

    df = df.Define('genChannel', 'genHttCand.channel()')
    df = df.Define('recoChannel', 'httCand.channel()')

    df = df.Filter("genChannel == recoChannel", "SameGenRecoChannels")
    df = df.Filter("GenRecoMatching(genHttCand, httCand, 0.2)", "SameGenRecoHTT") 
    
    report = df.Report()
    report.Print() 
      
    df = Baseline.ApplyRecoBaseline4(df)   
    
    df = Baseline.ApplyGenRecoJetMatching(df)  
    df = df.Define("sample", f"static_cast<int>(SampleType::{sample})") 
    df = df.Define("period", f"static_cast<int>(Period::Run{period})") 
    df = df.Define("X_mass", f"static_cast<int>({X_mass})")

    df = df.Define("httCand_leg0_pt", "httCand.leg_p4[0].Pt()")
    df = df.Define("httCand_leg0_eta", "httCand.leg_p4[0].Eta()")
    df = df.Define("httCand_leg0_phi", "httCand.leg_p4[0].Phi()")
    df = df.Define("httCand_leg0_mass", "httCand.leg_p4[0].M()")
    df = df.Define("httCand_leg1_pt", "httCand.leg_p4[1].Pt()")
    df = df.Define("httCand_leg1_eta", "httCand.leg_p4[1].Eta()")
    df = df.Define("httCand_leg1_phi", "httCand.leg_p4[1].Phi()")
    df = df.Define("httCand_leg1_mass", "httCand.leg_p4[1].M()")
    df = df.Define("Channel", "static_cast<int>(genChannel)")
    df = JetSavingCondition(df)


     
    report = df.Report()
    report.Print() 
    colToSave = ["event","luminosityBlock", "Jet_selIdx",
                "httCand_leg0_pt", "httCand_leg0_eta", "httCand_leg0_phi", "httCand_leg0_mass", "httCand_leg1_pt", "httCand_leg1_eta", "httCand_leg1_phi","httCand_leg1_mass", 
                "Channel","sample","period","X_mass", "MET_pt", "MET_phi"] 

    colToSave+=[f"RecoJet_{var}" for var in jetVar_list]

    varToSave = Utilities.ListToVector(colToSave) 
    print(type(snapshotOptions)) 
    df.Snapshot("Event", outFile, varToSave, snapshotOptions) 
    

if __name__ == "__main__":
    import argparse
    import os
    import re 

    parser = argparse.ArgumentParser()
    parser.add_argument('--period', type=str)
    parser.add_argument('--inFile', type=str)
    parser.add_argument('--outFile', type=str)
    parser.add_argument('--mass', type=int)
    parser.add_argument('--mpv', type=float, default=122.8) 
    parser.add_argument('--sample', type=str)
    parser.add_argument('--compressionLevel', type=int, default=9)
    parser.add_argument('--compressionAlgo', type=str, default="kLZMA")
    parser.add_argument('--particleFile', type=str,
                        default=f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt")
    args = parser.parse_args()
    
    
    ROOT.gROOT.SetBatch(True)
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "Common/GenTools.h"')
    ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{args.particleFile}\");") 
    snapshotOptions.fCompressionLevel=args.compressionLevel 
    setattr(snapshotOptions, 'fCompressionAlgorithm', Utilities.compression_algorithms[args.compressionAlgo])
    createSkim(args.inFile, args.outFile, args.period, args.sample, args.mass, args.mpv)
        