import ROOT
import numpy as np 
import Common.BaselineSelection as Baseline
import Common.Utilities as Utilities
import Common.ReportTools as ReportTools

def createAnatuple(inFile, outFile, period, sample, X_mass, snapshotOptions):
    Baseline.Initialize(True, True)
    df = ROOT.RDataFrame("Events", inFile)

    df = df.Define("sample", f"static_cast<int>(SampleType::{sample})")
    df = df.Define("period", f"static_cast<int>(Period::Run{period})") 
    df = df.Define("X_mass", f"static_cast<int>({X_mass})")


    df = Baseline.RecoLeptonsSelection(df)
    df = Baseline.RecoJetAcceptance(df)
    df = Baseline.RecoHttCandidateSelection(df)
    df = Baseline.RecoJetSelection(df) 
    df = Baseline.DefineHbbCand(df) 
    # define taus observables
    df = df.Define("Tau1_pt", "httCand.leg_p4[0].Pt()")
    df = df.Define("Tau1_eta", "httCand.leg_p4[0].Eta()")
    df = df.Define("Tau1_phi", "httCand.leg_p4[0].Phi()")
    df = df.Define("Tau1_mass", "httCand.leg_p4[0].M()")
    df = df.Define("Tau2_pt", "httCand.leg_p4[1].Pt()")
    df = df.Define("Tau2_eta", "httCand.leg_p4[1].Eta()")
    df = df.Define("Tau2_phi", "httCand.leg_p4[1].Phi()")
    df = df.Define("Tau2_mass", "httCand.leg_p4[1].M()") 
    # define bjets observables 
    df = df.Define("Bjet1_pt", "HbbCandidate.leg_p4[0].Pt()")
    df = df.Define("Bjet1_eta", "HbbCandidate.leg_p4[0].Eta()")
    df = df.Define("Bjet1_phi", "HbbCandidate.leg_p4[0].Phi()")
    df = df.Define("Bjet1_mass", "HbbCandidate.leg_p4[0].M()")
    df = df.Define("Bjet1_HHBtagScore", "HbbCandidate.leg_p4[0].M()")
    df = df.Define("Bjet2_pt", "HbbCandidate.leg_p4[1].Pt()")
    df = df.Define("Bjet2_eta", "HbbCandidate.leg_p4[1].Eta()")
    df = df.Define("Bjet2_phi", "HbbCandidate.leg_p4[1].Phi()")
    df = df.Define("Bjet2_mass", "HbbCandidate.leg_p4[1].M()") 
    df = df.Define("Bjet2_HHBtagScore", "HbbCandidate.leg_p4[1].M()")  
    report = df.Report()
    histReport=ReportTools.SaveReport(report.GetValue()) 

    colToSave = ["event","luminosityBlock","sample", "period", "X_mass",\
                "firstTau_pt", "firstTau_eta", "firstTau_phi", "firstTau_mass",\
                "secondTau_pt", "secondTau_eta", "secondTau_phi", "secondTau_mass",\
                "firstBjet_pt", "firstBjet_eta", "firstBjet_phi", "firstBjet_mass","firstBjet_HHBtagScore",\
                "secondBjet_pt", "secondBjet_eta", "secondBjet_phi", "secondBjet_mass","secondBjet_HHBtagScore",\
                "MET_pt", "MET_phi", "PuppiMET_pt", "PuppiMET_phi",\
                "DeepMETResolutionTune_pt", "DeepMETResolutionTune_phi","DeepMETResponseTune_pt", "DeepMETResponseTune_phi"] 

    varToSave = Utilities.ListToVector(colToSave)  
    df.Snapshot("Event", outFile, varToSave, snapshotOptions) 
    outputRootFile= ROOT.TFile(outFile, "UPDATE")
    outputRootFile.WriteTObject(histReport, "Report", "Overwrite")
    outputRootFile.Close()

    

if __name__ == "__main__":
    import argparse
    import os 

    parser = argparse.ArgumentParser()
    parser.add_argument('--period', type=str)
    parser.add_argument('--inFile', type=str)
    parser.add_argument('--outFile', type=str)
    parser.add_argument('--mass', type=int) 
    parser.add_argument('--sample', type=str)
    parser.add_argument('--compressionLevel', type=int, default=9)
    parser.add_argument('--compressionAlgo', type=str, default="LZMA") 
    args = parser.parse_args() 
    
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])     
    
     
    snapshotOptions = ROOT.RDF.RSnapshotOptions()
    snapshotOptions.fOverwriteIfExists=True
    snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + args.compressionAlgo)
    snapshotOptions.fCompressionLevel = args.compressionLevel 
    createAnatuple(args.inFile, args.outFile, args.period, args.sample, args.mass, snapshotOptions) 
