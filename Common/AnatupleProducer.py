import ROOT
import numpy as np 
import Common.BaselineSelection as Baseline
import Common.Utilities as Utilities
import Common.ReportTools as ReportTools

def AddVariablesToDf(df, var_list, idx, part_name, part_number, new_vars):
    for var in var_list:
        partName = "b"
        df = df.Define(f"{partName}{part_number}_{var}", f"{part_name}_{var}[{idx}]")
        new_vars.append(f"{partName}{part_number}_{var}")
    return df 


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
    df = df.Define("channelId","ChannelToHHbTagInput(httCand.channel())")
    df = df.Define("is_data", "1")
    df = df.Define("is_TuneCP5", "0")
    df = df.Define("weight", "1") 
    df = df.Define("lumi", "luminosityBlock")
    df = df.Define("evt", "event")
    deepTauScores= ["rawDeepTau2017v2p1VSe","rawDeepTau2017v2p1VSmu",\
                "rawDeepTau2017v2p1VSjet", "rawDeepTau2018v2p5VSe", "rawDeepTau2018v2p5VSmu",\
                "rawDeepTau2018v2p5VSjet"] 
    colToSave = ["event","lumi","run","sample", "period", "X_mass","channelId", "is_data","is_TuneCP5",\
                "MET_pt", "MET_phi","PuppiMET_pt", "PuppiMET_phi",\
                "DeepMETResolutionTune_pt", "DeepMETResolutionTune_phi","DeepMETResponseTune_pt", "DeepMETResponseTune_phi"] 
 
   
    for leg_idx in [0,1]:
        df = df.Define(f"tau{leg_idx+1}_pt", f"httCand.leg_p4[{leg_idx}].Pt()")
        colToSave.append(f"tau{leg_idx+1}_pt")
        df = df.Define(f"tau{leg_idx+1}_eta", f"httCand.leg_p4[{leg_idx}].Eta()")
        colToSave.append(f"tau{leg_idx+1}_eta")
        df = df.Define(f"tau{leg_idx+1}_phi", f"httCand.leg_p4[{leg_idx}].Phi()")
        colToSave.append(f"tau{leg_idx+1}_phi")
        df = df.Define(f"tau{leg_idx+1}_mass", f"httCand.leg_p4[{leg_idx}].M()")
        colToSave.append(f"tau{leg_idx+1}_mass")
        df = df.Define(f"tau{leg_idx+1}_charge", f"httCand.leg_charge[{leg_idx}]")
        colToSave.append(f"tau{leg_idx+1}_charge")
        for deepTauScore in deepTauScores:
            df = df.Define(f"tau{leg_idx+1}_{deepTauScore}", f"deepTauScore(Tau_{deepTauScore},\
            httCand.leg_type[{leg_idx}],httCand.leg_index[{leg_idx}])")
            colToSave.append(f"tau{leg_idx+1}_{deepTauScore}")
        df = df.Define(f"b{leg_idx+1}_pt", f"HbbCandidate.leg_p4[{leg_idx}].Pt()")
        colToSave.append(f"b{leg_idx+1}_pt")
        df = df.Define(f"b{leg_idx+1}_eta", f"HbbCandidate.leg_p4[{leg_idx}].Eta()")
        colToSave.append(f"b{leg_idx+1}_eta")
        df = df.Define(f"b{leg_idx+1}_phi", f"HbbCandidate.leg_p4[{leg_idx}].Phi()")
        colToSave.append(f"b{leg_idx+1}_phi")
        df = df.Define(f"b{leg_idx+1}_mass", f"HbbCandidate.leg_p4[{leg_idx}].M()")
        colToSave.append(f"b{leg_idx+1}_mass")
        df = df.Define(f"b{leg_idx+1}_HHBtagScore", f"HbbCandidate.leg_HHbTagScore[{leg_idx}]")
        colToSave.append(f"b{leg_idx+1}_HHBtagScore")
        
    df= df.Define("MET_cov00", "MET_covXX")
    colToSave.append("MET_cov00")
    df= df.Define("MET_cov01", "MET_covXY")
    colToSave.append("MET_cov01")
    df= df.Define("MET_cov11", "MET_covYY")
    colToSave.append("MET_cov11")
    df= df.Define("npv", "PV_npvs")
    colToSave.append("npv")
    df = df.Define("lhe_HT", "LHE_HT")
    colToSave.append("lhe_HT")

         
    report = df.Report()
    histReport=ReportTools.SaveReport(report.GetValue()) 

    
    
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
