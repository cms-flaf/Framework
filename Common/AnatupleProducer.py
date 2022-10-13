import ROOT
import numpy as np 
import Common.BaselineSelection as Baseline
import Common.Utilities as Utilities
import Common.ReportTools as ReportTools

def DefineAndAppend(df, varToDefine, varToCall, colToSave):
    df = df.Define(f"{varToDefine}", f"{varToCall}")
    colToSave.append(varToDefine)
    return df 

def createAnatuple(inFile, outFile, period, sample, X_mass, snapshotOptions, isData=0):
    Baseline.Initialize(True, True)
    df = ROOT.RDataFrame("Events", inFile) 
    df = df.Range(100)
    df = df.Define("sample", f"static_cast<int>(SampleType::{sample})")
    df = df.Define("period", f"static_cast<int>(Period::Run{period})") 
    df = df.Define("X_mass", f"static_cast<int>({X_mass})")
    
    df = df.Define("GenPart_daughters", "GetDaughters(GenPart_genPartIdxMother)")
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
    JetObservables = ["hadronFlavour","partonFlavour", "particleNetAK4_B", "particleNetAK4_CvsB",\
                    "particleNetAK4_CvsL","particleNetAK4_QvsG","particleNetAK4_puIdDisc"] 
    colToSave = ["event","lumi","run","sample", "period", "X_mass","channelId", "is_data",\
                "MET_pt", "MET_phi","PuppiMET_pt", "PuppiMET_phi",\
                "DeepMETResolutionTune_pt", "DeepMETResolutionTune_phi","DeepMETResponseTune_pt", "DeepMETResponseTune_phi"] 
 
    
    for leg_idx in [0,1]:
        df = DefineAndAppend(df, f"tau{leg_idx+1}_pt", f"httCand.leg_p4[{leg_idx}].Pt()", colToSave)
        df = DefineAndAppend(df, f"tau{leg_idx+1}_eta", f"httCand.leg_p4[{leg_idx}].Eta()", colToSave)
        df = DefineAndAppend(df,f"tau{leg_idx+1}_phi", f"httCand.leg_p4[{leg_idx}].Phi()", colToSave)
        df = DefineAndAppend(df,f"tau{leg_idx+1}_mass", f"httCand.leg_p4[{leg_idx}].M()", colToSave)
        df = DefineAndAppend(df,f"tau{leg_idx+1}_charge", f"httCand.leg_charge[{leg_idx}]", colToSave) 
        df = df.Define(f"tau{leg_idx+1}_genMatch_class", f"GenRecoLepMatching(httCand.leg_p4[{leg_idx}],\
                          GenPart_pdgId, GenPart_daughters,\
                          GenPart_pt, GenPart_eta, GenPart_phi,\
                          GenPart_mass, GenPart_statusFlags)")
        df = DefineAndAppend(df,f"tau{leg_idx+1}_genMatch", f"static_cast<int>(tau{leg_idx+1}_genMatch_class);" , colToSave)
        df = df.Define(f"tau{leg_idx+1}_recoJetMatchIdx", f"RecoTauMatching(httCand.leg_p4[{leg_idx}], Jet_p4)")
        df = DefineAndAppend(df, f"tau{leg_idx+1}_iso", f"Float_t iso;\
                                    if(httCand.leg_type[{leg_idx}]==Leg::tau)\
                                        iso=(Tau_iso.at(httCand.leg_index[{leg_idx}]));\
                                    else if(httCand.leg_type[{leg_idx}]==Leg::mu)\
                                        iso=(Muon_iso.at(httCand.leg_index[{leg_idx}]));\
                                    else if(httCand.leg_type[{leg_idx}]==Leg::e)\
                                        iso=(Electron_iso.at(httCand.leg_index[{leg_idx}]));\
                                    return iso;", colToSave)
        for deepTauScore in deepTauScores:
            df = DefineAndAppend(df, f"tau{leg_idx+1}_{deepTauScore}", f"Float_t deepTauScore ;\
                                    if(httCand.leg_type[{leg_idx}]!=Leg::tau)\
                                        deepTauScore=(-1.);\
                                    else\
                                        deepTauScore=(Tau_{deepTauScore}.at(httCand.leg_index[{leg_idx}]));\
                                    return deepTauScore;", colToSave) 
        df = DefineAndAppend(df,f"b{leg_idx+1}_pt", f"HbbCandidate.leg_p4[{leg_idx}].Pt()",colToSave)
        df = DefineAndAppend(df,f"b{leg_idx+1}_eta", f"HbbCandidate.leg_p4[{leg_idx}].Eta()",colToSave)
        df = DefineAndAppend(df,f"b{leg_idx+1}_phi", f"HbbCandidate.leg_p4[{leg_idx}].Phi()",colToSave)
        df = DefineAndAppend(df,f"b{leg_idx+1}_mass", f"HbbCandidate.leg_p4[{leg_idx}].M()",colToSave)
        for jetVar in JetObservables:
            df = DefineAndAppend(df,f"b{leg_idx+1}_{jetVar}", f"Jet_{jetVar}.at(HbbCandidate.leg_index[{leg_idx}])", colToSave)
        df = DefineAndAppend(df,f"b{leg_idx+1}_HHbtag", f"Jet_HHBtagScore.at(HbbCandidate.leg_index[{leg_idx}])", colToSave)
        df = DefineAndAppend(df,f"b{leg_idx+1}_DeepFlavour", f"Jet_btagDeepFlavB.at(HbbCandidate.leg_index[{leg_idx}])", colToSave)
        df = DefineAndAppend(df,f"b{leg_idx+1}_DeepFlavour_CvsB", f"Jet_btagDeepFlavCvB.at(HbbCandidate.leg_index[{leg_idx}])", colToSave)
        df = DefineAndAppend(df,f"b{leg_idx+1}_DeepFlavour_CvsL", f"Jet_btagDeepFlavCvL.at(HbbCandidate.leg_index[{leg_idx}])", colToSave)
        
    
    df =DefineAndAppend(df, "matched_jets_pt", "RVecF matchedJetsPt; \
                                        if(tau1_recoJetMatchIdx>=0)\
                                            { matchedJetsPt.push_back(Jet_p4.at(tau1_recoJetMatchIdx).Pt());}\
                                        if(tau2_recoJetMatchIdx>=0)\
                                            { matchedJetsPt.push_back(Jet_p4.at(tau2_recoJetMatchIdx).Pt());} \
                                        if(matchedJetsPt.empty()){matchedJetsPt.push_back(-10000.); }\
                                        return matchedJetsPt;",\
                                        colToSave)
    df =DefineAndAppend(df, "matched_jets_eta", "RVecF matchedJetsEta; \
                                        if(tau1_recoJetMatchIdx>=0)\
                                            { matchedJetsEta.push_back(Jet_p4.at(tau1_recoJetMatchIdx).Eta());}\
                                        if(tau2_recoJetMatchIdx>=0)\
                                            { matchedJetsEta.push_back(Jet_p4.at(tau2_recoJetMatchIdx).Eta());} \
                                        if(matchedJetsEta.empty()){matchedJetsEta.push_back(-10000.); }\
                                        return matchedJetsEta;",\
                                        colToSave)
    df =DefineAndAppend(df, "matched_jets_phi", "RVecF matchedJetsPhi; \
                                        if(tau1_recoJetMatchIdx>=0)\
                                            { matchedJetsPhi.push_back(Jet_p4.at(tau1_recoJetMatchIdx).Phi());}\
                                        if(tau2_recoJetMatchIdx>=0)\
                                            { matchedJetsPhi.push_back(Jet_p4.at(tau2_recoJetMatchIdx).Phi());} \
                                        if(matchedJetsPhi.empty()){matchedJetsPhi.push_back(-10000.); }\
                                        return matchedJetsPhi;",\
                                        colToSave)
    df =DefineAndAppend(df, "matched_jets_m", "RVecF matchedJetsM; \
                                        if(tau1_recoJetMatchIdx>=0)\
                                            { matchedJetsM.push_back(Jet_p4.at(tau1_recoJetMatchIdx).M());}\
                                        if(tau2_recoJetMatchIdx>=0)\
                                            { matchedJetsM.push_back(Jet_p4.at(tau2_recoJetMatchIdx).M());} \
                                        if(matchedJetsM.empty()){matchedJetsM.push_back(-10000.); }\
                                        return matchedJetsM;",\
                                        colToSave)
    
    df =DefineAndAppend(df, "matched_jets_partonFlavour", "RVecI matchedJetspartonFlavour; \
                                        if(tau1_recoJetMatchIdx>=0)\
                                            { matchedJetspartonFlavour.push_back(Jet_partonFlavour.at(tau1_recoJetMatchIdx));}\
                                        if(tau2_recoJetMatchIdx>=0)\
                                            { matchedJetspartonFlavour.push_back(Jet_partonFlavour.at(tau2_recoJetMatchIdx));} \
                                        if(matchedJetspartonFlavour.empty()){matchedJetspartonFlavour.push_back(-1);}\
                                        return matchedJetspartonFlavour;", \
                                        colToSave)
    
    df =DefineAndAppend(df, "matched_jets_hadronFlavour", "RVecI matchedJetshadronFlavour; \
                                        if(tau1_recoJetMatchIdx>=0)\
                                            { matchedJetshadronFlavour.push_back(Jet_hadronFlavour.at(tau1_recoJetMatchIdx));}\
                                        if(tau2_recoJetMatchIdx>=0)\
                                            { matchedJetshadronFlavour.push_back(Jet_hadronFlavour.at(tau2_recoJetMatchIdx));} \
                                        if(matchedJetshadronFlavour.empty()){matchedJetshadronFlavour.push_back(-1);}\
                                        return matchedJetshadronFlavour;", \
                                        colToSave)
    
    df =DefineAndAppend(df, "matched_jets_idx", "RVecI matchedJetsidx; \
                                        if(tau1_recoJetMatchIdx>=0)\
                                            { matchedJetsidx.push_back(Jet_idx.at(tau1_recoJetMatchIdx));}\
                                        if(tau2_recoJetMatchIdx>=0)\
                                            { matchedJetsidx.push_back(Jet_idx.at(tau2_recoJetMatchIdx));} \
                                        if(matchedJetsidx.empty()){matchedJetsidx.push_back(-1);}\
                                        return matchedJetsidx;", \
                                        colToSave)

    #df.Display({"event","matched_jets_hadronFlavour","matched_jets_partonFlavour", "matched_jets_idx"}).Print()
    #df.Define("Jet_size", "Jet_hadronFlavour.size()").Display({"Jet_hadronFlavour","Jet_partonFlavour","Jet_idx","Jet_size"}, 100).Print() 
    #df.Display({"Jet_hadronFlavour","Jet_partonFlavour"}, 100).Print()  
    #df.Display({"matched_jets_pt","matched_jets_eta", "matched_jets_phi", "matched_jets_m"}).Print() 
    #df.Display({"channelId"}).Print()
    #df.Display({"tau1_genMatch", "tau1_recoMatchIdx", "tau1_recoMatchObj"}).Print()
    #df.Display({"event","matched_jets_hadronFlavour","matched_jets_partonFlavour", "matched_jets_idx"}).Print()
    #df.Display({"event","matched_b_jets_hadronFlavour","matched_b_jets_partonFlavour", "matched_b_jets_idx"}).Print()
    #df.Display({"tau2_genMatch", "tau2_recoMatchIdx", "tau2_recoMatchObj"}).Print()
    '''
    df.Filter("tau1_recoMatchObj == 4 || tau2_recoMatchObj == 4 ").Display({"event","matched_jets_hadronFlavour","matched_jets_partonFlavour", "matched_jets_idx"}).Print()
    df.Filter("tau1_recoMatchObj == 4 || tau2_recoMatchObj == 4 ").Display({"matched_jets_pt","matched_jets_eta", "matched_jets_phi", "matched_jets_m"}).Print() 
    
    df.Filter("tau1_recoMatchObj == 4 || tau2_recoMatchObj == 4 ").Display({"tau1_genMatch", "tau1_recoMatchIdx", "tau1_recoMatchObj"}).Print()
    df.Filter("tau1_recoMatchObj == 4 || tau2_recoMatchObj == 4 ").Display({"tau2_genMatch", "tau2_recoMatchIdx", "tau2_recoMatchObj"}).Print()
    '''
    df= DefineAndAppend(df, "MET_cov00", "MET_covXX", colToSave)
    df= DefineAndAppend(df, "MET_cov01", "MET_covXY", colToSave)
    df= DefineAndAppend(df, "MET_cov11", "MET_covYY", colToSave)
    df= DefineAndAppend(df, "npv", "PV_npvs", colToSave)
    df = DefineAndAppend(df, "lhe_HT", "LHE_HT", colToSave) 

         
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
    parser.add_argument('--particleFile', type=str,
                        default=f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt")
    args = parser.parse_args() 
    
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])     
    ROOT.gROOT.ProcessLine('#include "Common/GenTools.h"')
    ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{args.particleFile}\");")
    
     
    snapshotOptions = ROOT.RDF.RSnapshotOptions()
    snapshotOptions.fOverwriteIfExists=True
    snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + args.compressionAlgo) 
    snapshotOptions.fCompressionLevel = args.compressionLevel 
    createAnatuple(args.inFile, args.outFile, args.period, args.sample, args.mass, snapshotOptions) 
