import ROOT
import numpy as np
import Common.Utilities as Utilities

import Common.BaselineSelection as Baseline
 
snapshotOptions = ROOT.RDF.RSnapshotOptions()
snapshotOptions.fOverwriteIfExists=True
snapshotOptions.fCompressionLevel=9
snapshotOptions.fCompressionAlgorithm=ROOT.kLZMA 

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
    df = Baseline.ApplyRecoBaseline4(df)
    
    df = Baseline.ApplyGenBaseline1(df)
    
      
    df = Baseline.ApplyGenRecoJetMatching(df)  
    df = df.Define("sample", f"static_cast<int>(Sample::{sample})") 
    df = df.Define("year", f"static_cast<int>(Year::Year_{year})") 
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
    df = Baseline.JetSavingCondition(df)


     
    report = df.Report()
    report.Print() 
    colToSave = ["event","luminosityBlock", "RecoJet_n","RecoJet_pt", "RecoJet_eta", "RecoJet_phi", "RecoJet_mass","RecoJet_idx", "RecoJetIndices_DeepFlavour",
                "httCand_leg0_pt", "httCand_leg0_eta", "httCand_leg0_phi", "httCand_leg0_mass", "httCand_leg1_pt", "httCand_leg1_eta", "httCand_leg1_phi","httCand_leg1_mass", 
                "Channel","sample","year","X_mass", "RecoJet_btagCSVV2", "RecoJet_btagDeepB", "RecoJet_btagDeepFlavB", "RecoJet_btagDeepFlavCvB", "RecoJet_btagDeepFlavCvL", "RecoJet_btagDeepFlavQG", "MET_pt", "MET_phi"] 

    varToSave = Utilities.ListToVector(colToSave) 
    df.Snapshot("Event", outFile, varToSave,snapshotOptions) 


if __name__ == "__main__":
    import argparse
    import os
    import re 

    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=str)
    parser.add_argument('--inFile', type=str)
    parser.add_argument('--outFile', type=str)
    parser.add_argument('--mass', type=int)
    parser.add_argument('--sample', type=str)
    parser.add_argument('--particleFile', type=str,
                        default=f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt")
    args = parser.parse_args()

    ROOT.gROOT.SetBatch(True)
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "Common/GenTools.h"')
    ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{args.particleFile}\");") 
    createSkim(args.inFile, args.outFile, args.year, args.sample, args.mass)
        