import ROOT
import numpy as np
import Common.Utilities as Utilities

import Common.BaselineSelection as Baseline
 
snapshotOptions = ROOT.RDF.RSnapshotOptions()
snapshotOptions.fOverwriteIfExists=True
def JetSavingCondition(df): 
    df = df.Define('Jet_selIdx', 'ReorderObjects(RecoJet_btagDeepFlavB, Jet_idx[Jet_B3T])')
    for var in [ "pt", "eta", "phi", "mass", "btagCSVV2", "btagDeepB", "btagDeepFlavB", "JetRecoMatched" ]:
        df = df.Define(f"RecoJet_{var}", f"Take(Jet_{var}, Jet_selIdx)") 
    return df

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
    df = Baseline.ApplyGenBaselineAcceptance(df)
    x_max = Baseline.FindMPV(df)
    df = Baseline.ApplyGenBaseline1(df, x_max)
    
      
    df = Baseline.ApplyGenRecoJetMatching(df)  
    df = df.Define("sample", f"static_cast<int>(SampleType::{sample})") 
    df = df.Define("year", f"static_cast<int>(Period::Run{year})") 
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
    colToSave = ["event","luminosityBlock","RecoJet_pt", "RecoJet_eta", "RecoJet_phi", "RecoJet_mass", 
                "httCand_leg0_pt", "httCand_leg0_eta", "httCand_leg0_phi", "httCand_leg0_mass", "httCand_leg1_pt", "httCand_leg1_eta", "httCand_leg1_phi","httCand_leg1_mass", 
                "Channel","sample","year","X_mass", "RecoJet_btagCSVV2", "RecoJet_btagDeepB", "RecoJet_btagDeepFlavB", "MET_pt", "MET_phi"] 

    remove: RecoJet_n, RecoJet_idx, RecoJetIndices_DeepFlavour, RecoJet_btagDeepFlavCvB, RecoJet_btagDeepFlavCvL, RecoJet_btagDeepFlavQG
reuse list of branches for reco jets used before to avoid duplications

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
    parser.add_argument('--compressionLevel', type=int, default=9)
    parser.add_argument('--compressionAlgo', type=str, default="KLZMA")
    parser.add_argument('--particleFile', type=str,
                        default=f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt")
    args = parser.parse_args()

    snapshotOptions.fCompressionLevel=args.compressionLevel 
    setattr(snapshotOptions, 'fCompressionAlgorithm', args.compressionAlgo)
    ROOT.gROOT.SetBatch(True)
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "Common/GenTools.h"')
    ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{args.particleFile}\");") 
    createSkim(args.inFile, args.outFile, args.year, args.sample, args.mass)
        