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


def createAnatuple(inFile, outFile, period, sample, X_mass, snapshotOptions, isData=0):
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
    df = df.Define("channelId","static_cast<int>(httCand.channel())")
    df = df.Define("is_data", f"{isData}") 
    df = df.Define("lumi", "luminosityBlock")
    df = df.Define("evt", "event")
    deepTauScores= ["rawDeepTau2017v2p1VSe","rawDeepTau2017v2p1VSmu",\
                "rawDeepTau2017v2p1VSjet", "rawDeepTau2018v2p5VSe", "rawDeepTau2018v2p5VSmu",\
                "rawDeepTau2018v2p5VSjet", "decayMode"] 
    JetObservables = ["hadronFlavour","partonFlavour"]
    colToSave = ["event","lumi","run","sample", "period", "X_mass","channelId", "is_data",\
                "MET_pt", "MET_phi","PuppiMET_pt", "PuppiMET_phi",\
                "DeepMETResolutionTune_pt", "DeepMETResolutionTune_phi","DeepMETResponseTune_pt", "DeepMETResponseTune_phi"] 
 
   
    for leg_idx in [0,1]:
        df = df.Define(f"tau{leg_idx+1}_pt", f"httCand.leg_p4[{leg_idx}].Pt()")
        df = df.Define(f"tau{leg_idx+1}_eta", f"httCand.leg_p4[{leg_idx}].Eta()")
        df = df.Define(f"tau{leg_idx+1}_phi", f"httCand.leg_p4[{leg_idx}].Phi()")
        df = df.Define(f"tau{leg_idx+1}_mass", f"httCand.leg_p4[{leg_idx}].M()")
        df = df.Define(f"tau{leg_idx+1}_charge", f"httCand.leg_charge[{leg_idx}]")
        colToSave.append(f"tau{leg_idx+1}_pt")
        colToSave.append(f"tau{leg_idx+1}_eta")
        colToSave.append(f"tau{leg_idx+1}_phi")
        colToSave.append(f"tau{leg_idx+1}_mass")
        colToSave.append(f"tau{leg_idx+1}_charge")
        
        df = df.Define(f"tau{leg_idx+1}_iso", f"RVecF iso;\
                                    if(httCand.leg_type[{leg_idx}]==Leg::tau)\
                                        iso.push_back(Tau_iso.at(httCand.leg_index[{leg_idx}]));\
                                    else if(httCand.leg_type[{leg_idx}]==Leg::mu)\
                                        iso.push_back(Muon_iso.at(httCand.leg_index[{leg_idx}]));\
                                    else if(httCand.leg_type[{leg_idx}]==Leg::e)\
                                        iso.push_back(Electron_iso.at(httCand.leg_index[{leg_idx}]));\
                                    return iso;")
        colToSave.append(f"tau{leg_idx+1}_iso")
        
        for deepTauScore in deepTauScores:
            df = df.Define(f"tau{leg_idx+1}_{deepTauScore}", f"RVecF deepTauScore ;\
                                    if(httCand.leg_type[{leg_idx}]!=Leg::tau)\
                                        deepTauScore.push_back(-1);\
                                    else\
                                        deepTauScore.push_back(Tau_{deepTauScore}.at(httCand.leg_index[{leg_idx}]));\
                                    return deepTauScore;") 
            colToSave.append(f"tau{leg_idx+1}_{deepTauScore}")
        
        df = df.Define(f"b{leg_idx+1}_pt", f"HbbCandidate.leg_p4[{leg_idx}].Pt()")
        df = df.Define(f"b{leg_idx+1}_eta", f"HbbCandidate.leg_p4[{leg_idx}].Eta()")
        df = df.Define(f"b{leg_idx+1}_phi", f"HbbCandidate.leg_p4[{leg_idx}].Phi()")
        df = df.Define(f"b{leg_idx+1}_mass", f"HbbCandidate.leg_p4[{leg_idx}].M()")
        colToSave.append(f"b{leg_idx+1}_pt")
        colToSave.append(f"b{leg_idx+1}_eta")
        colToSave.append(f"b{leg_idx+1}_phi")
        colToSave.append(f"b{leg_idx+1}_mass")
        for jetVar in JetObservables:
            df = df.Define(f"b{leg_idx+1}_{jetVar}", f"Jet_{jetVar}.at(HbbCandidate.leg_index[{leg_idx}])")
            colToSave.append(f"b{leg_idx+1}_{jetVar}")
        df = df.Define(f"b{leg_idx+1}_HHbtag", f"Jet_HHBtagScore.at(HbbCandidate.leg_index[{leg_idx}])")
        df = df.Define(f"b{leg_idx+1}_DeepFlavour", f"Jet_btagDeepFlavB.at(HbbCandidate.leg_index[{leg_idx}])")
        df = df.Define(f"b{leg_idx+1}_DeepFlavour_CvsB", f"Jet_btagDeepFlavCvB.at(HbbCandidate.leg_index[{leg_idx}])")
        df = df.Define(f"b{leg_idx+1}_DeepFlavour_CvsL", f"Jet_btagDeepFlavCvL.at(HbbCandidate.leg_index[{leg_idx}])")
        colToSave.append(f"b{leg_idx+1}_HHbtag")
        colToSave.append(f"b{leg_idx+1}_DeepFlavour")
        colToSave.append(f"b{leg_idx+1}_DeepFlavour_CvsL")
        colToSave.append(f"b{leg_idx+1}_DeepFlavour_CvsB")
        
    df= df.Define("MET_cov00", "MET_covXX")
    df= df.Define("MET_cov01", "MET_covXY")
    df= df.Define("MET_cov11", "MET_covYY")
    df= df.Define("npv", "PV_npvs")
    df = df.Define("lhe_HT", "LHE_HT")
    colToSave.append("MET_cov00")
    colToSave.append("MET_cov01")
    colToSave.append("MET_cov11")
    colToSave.append("npv")
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
