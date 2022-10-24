import ROOT
import numpy as np 
import Common.BaselineSelection as Baseline
import Common.Utilities as Utilities
import Common.ReportTools as ReportTools



def createAnatuple(inFile, outFile, period, sample, X_mass, snapshotOptions,range, isData):
    Baseline.Initialize(True, True)
    df = ROOT.RDataFrame("Events", inFile)
    if range is not None:
        df = df.Range(range)
    df = df.Define("sample", f"static_cast<int>(SampleType::{sample})")
    df = df.Define("period", f"static_cast<int>(Period::Run{period})") 
    df = df.Define("X_mass", f"static_cast<int>({X_mass})")
    
    df = df.Define("GenPart_daughters", "GetDaughters(GenPart_genPartIdxMother)")
    for leg_idx in[0,1]:

        if isData==0:

        df = df.Define("genLeptons","""reco_tau::gen_truth::GenLepton::fromNanoAOD(GenPart_pt, GenPart_eta,
                                            GenPart_phi, GenPart_mass, GenPart_genPartIdxMother, GenPart_pdgId,
                                            GenPart_statusFlags)""")
            df = df.Define(f"tau{leg_idx+1}_genMatchIdx", f"MatchGenLepton(httCand.leg_p4[{leg_idx}], genLeptons, 0.2)")
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_kind", f"""tau{leg_idx+1}_genMatchIdx>=0 ? static_cast<int>(genLeptons.at(tau{leg_idx+1}_genMatchIdx).kind()) : 
                                                        static_cast<int>(GenLeptonMatch::NoMatch);""")
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_vis_pt", f"""tau{leg_idx+1}_genMatchIdx>=0? static_cast<float>(genLeptons.at(tau{leg_idx+1}_genMatchIdx).visibleP4().Pt()) : 
                                                        -1.;""")
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_vis_eta", f"""tau{leg_idx+1}_genMatchIdx>=0? static_cast<float>(genLeptons.at(tau{leg_idx+1}_genMatchIdx).visibleP4().Eta()) : 
                                                        -1.;""")
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_vis_phi", f"""tau{leg_idx+1}_genMatchIdx>=0? static_cast<float>(genLeptons.at(tau{leg_idx+1}_genMatchIdx).visibleP4().Phi()) : 
                                                        -1.;""")
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_vis_mass", f"""tau{leg_idx+1}_genMatchIdx>=0? static_cast<float>(genLeptons.at(tau{leg_idx+1}_genMatchIdx).visibleP4().M()) : 
                                                        -1.;""")
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_nChHad", f"""tau{leg_idx+1}_genMatchIdx>=0? genLeptons.at(tau{leg_idx+1}_genMatchIdx).nChargedHadrons() : 0;""")
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_nNeutHad", f"""tau{leg_idx+1}_genMatchIdx>=0? genLeptons.at(tau{leg_idx+1}_genMatchIdx).nNeutralHadrons() : 0;""")
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_charge", f"""tau{leg_idx+1}_genMatchIdx>=0? genLeptons.at(tau{leg_idx+1}_genMatchIdx).getCharge() : -10;""")


    df = Baseline.RecoLeptonsSelection(df)
    df = Baseline.RecoJetAcceptance(df)
    df = Baseline.RecoHttCandidateSelection(df)
    df = Baseline.RecoJetSelection(df) 
    df = Baseline.RequestOnlyResolvedRecoJets(df)
    df = Baseline.ThirdLeptonVeto(df)
    df = Baseline.DefineHbbCand(df)
    df = df.Define("channelId","static_cast<int>(httCand.channel())")
    df = df.Define("is_data", f"{isData}")   
    deepTauScores= ["rawDeepTau2017v2p1VSe","rawDeepTau2017v2p1VSmu",
                "rawDeepTau2017v2p1VSjet", "rawDeepTau2018v2p5VSe", "rawDeepTau2018v2p5VSmu",
                "rawDeepTau2018v2p5VSjet",
                "idDeepTau2017v2p1VSe", "idDeepTau2017v2p1VSjet", "idDeepTau2017v2p1VSmu",
                "idDeepTau2018v2p5VSe","idDeepTau2018v2p5VSjet","idDeepTau2018v2p5VSmu",
                "decayMode"] 
    JetObservables = ["hadronFlavour","partonFlavour", "particleNetAK4_B", "particleNetAK4_CvsB",
                    "particleNetAK4_CvsL","particleNetAK4_QvsG","particleNetAK4_puIdDisc",
                    "btagDeepFlavB","btagDeepFlavCvB","btagDeepFlavCvL"] 
    colToSave = ["event","luminosityBlock","run","sample", "period", "X_mass","channelId", "is_data",
                "MET_pt", "MET_phi","PuppiMET_pt", "PuppiMET_phi",
                "DeepMETResolutionTune_pt", "DeepMETResolutionTune_phi","DeepMETResponseTune_pt", "DeepMETResponseTune_phi",
                "MET_covXX", "MET_covXY", "MET_covYY", "PV_npvs","LHE_HT"] 
 
    def DefineAndAppend(df, varToDefine, varToCall):
        df = df.Define(f"{varToDefine}", f"{varToCall}")
        colToSave.append(varToDefine)
        return df 

    for leg_idx in [0,1]:
        df = DefineAndAppend(df, f"tau{leg_idx+1}_pt", f"static_cast<float>(httCand.leg_p4[{leg_idx}].Pt())")
        df = DefineAndAppend(df, f"tau{leg_idx+1}_eta", f"static_cast<float>(httCand.leg_p4[{leg_idx}].Eta())")
        df = DefineAndAppend(df,f"tau{leg_idx+1}_phi", f"static_cast<float>(httCand.leg_p4[{leg_idx}].Phi())")
        df = DefineAndAppend(df,f"tau{leg_idx+1}_mass", f"static_cast<float>(httCand.leg_p4[{leg_idx}].M())")
        df = DefineAndAppend(df,f"tau{leg_idx+1}_charge", f"httCand.leg_charge[{leg_idx}]")
        df = df.Define(f"tau{leg_idx+1}_recoJetMatchIdx", f"FindMatching(httCand.leg_p4[{leg_idx}], Jet_p4, 0.3)")
        df = DefineAndAppend(df, f"tau{leg_idx+1}_iso", f"httCand.leg_rawIso.at({leg_idx})")
        for deepTauScore in deepTauScores:
            df = DefineAndAppend(df, f"tau{leg_idx+1}_{deepTauScore}",
                                     f"httCand.leg_type[{leg_idx}] == Leg::tau ? Tau_{deepTauScore}.at(httCand.leg_index[{leg_idx}]) : -1;")

        df = DefineAndAppend(df, f"tau{leg_idx+1}_seedingJet_pt",
                                    f"tau{leg_idx+1}_recoJetMatchIdx>=0 ? Jet_p4.at(tau{leg_idx+1}_recoJetMatchIdx).Pt() : -1;") 
        df = DefineAndAppend(df, f"tau{leg_idx+1}_seedingJet_eta",
                                    f"tau{leg_idx+1}_recoJetMatchIdx>=0 ? Jet_p4.at(tau{leg_idx+1}_recoJetMatchIdx).Eta() : -1;") 
        df = DefineAndAppend(df, f"tau{leg_idx+1}_seedingJet_phi",
                                    f"tau{leg_idx+1}_recoJetMatchIdx>=0 ? Jet_p4.at(tau{leg_idx+1}_recoJetMatchIdx).Phi() : -1;")
        df = DefineAndAppend(df, f"tau{leg_idx+1}_seedingJet_mass",
                                    f"tau{leg_idx+1}_recoJetMatchIdx>=0 ? Jet_p4.at(tau{leg_idx+1}_recoJetMatchIdx).M() : -1;")
        df = DefineAndAppend(df, f"tau{leg_idx+1}_seedingJet_partonFlavour",
                                    f"tau{leg_idx+1}_recoJetMatchIdx>=0 ? Jet_partonFlavour.at(tau{leg_idx+1}_recoJetMatchIdx) : -1;")
        df = DefineAndAppend(df, f"tau{leg_idx+1}_seedingJet_hadronFlavour",
                                f"tau{leg_idx+1}_recoJetMatchIdx>=0 ? Jet_hadronFlavour.at(tau{leg_idx+1}_recoJetMatchIdx) : -1;")
        
        
        df = DefineAndAppend(df,f"b{leg_idx+1}_pt", f"HbbCandidate.leg_p4[{leg_idx}].Pt()")
        df = DefineAndAppend(df,f"b{leg_idx+1}_eta", f"HbbCandidate.leg_p4[{leg_idx}].Eta()")
        df = DefineAndAppend(df,f"b{leg_idx+1}_phi", f"HbbCandidate.leg_p4[{leg_idx}].Phi()")
        df = DefineAndAppend(df,f"b{leg_idx+1}_mass", f"HbbCandidate.leg_p4[{leg_idx}].M()")
        
        for jetVar in JetObservables:
            df = DefineAndAppend(df,f"b{leg_idx+1}_{jetVar}", f"Jet_{jetVar}.at(HbbCandidate.leg_index[{leg_idx}])")
        df = DefineAndAppend(df,f"b{leg_idx+1}_HHbtag", f"Jet_HHBtagScore.at(HbbCandidate.leg_index[{leg_idx}])")
         
    report = df.Report()
    histReport=ReportTools.SaveReport(report.GetValue()) 

    
    
    varToSave = Utilities.ListToVector(colToSave)   
    df.Snapshot("Events", outFile, varToSave, snapshotOptions) 
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
    parser.add_argument('--range', type=int, default=None)
    parser.add_argument('--isData', type=int, default=0)

    '''
    parser.add_argument('--particleFile', type=str,
                        default=f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt")'''
    args = parser.parse_args() 
    
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])     
    ROOT.gROOT.ProcessLine('#include "Common/GenTools.h"')
    #ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{args.particleFile}\");")
    
     
    snapshotOptions = ROOT.RDF.RSnapshotOptions()
    snapshotOptions.fOverwriteIfExists=True
    snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + args.compressionAlgo) 
    snapshotOptions.fCompressionLevel = args.compressionLevel 
    createAnatuple(args.inFile, args.outFile, args.period, args.sample, args.mass, snapshotOptions, args.range, args.isData) 
