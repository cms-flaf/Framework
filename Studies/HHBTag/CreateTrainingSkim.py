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
    df = Baseline.ApplyGenRecoJetMatching(df)
    df = df.Define("year", f"static_cast<std::string>(\"{year}\")")
    df = df.Define("sample", f"static_cast<std::string>(\"{sample}\")") 
    df = df.Define("X_mass", f"static_cast<int>({X_mass})")

    
 
    df = df.Define("httCand_leg0_pt", "httCand.leg_p4[0].Pt()")
    df = df.Define("httCand_leg0_eta", "httCand.leg_p4[0].Eta()")
    df = df.Define("httCand_leg0_phi", "httCand.leg_p4[0].Phi()")
    df = df.Define("httCand_leg0_mass", "httCand.leg_p4[0].M()")
    df = df.Define("httCand_leg1_pt", "httCand.leg_p4[1].Pt()")
    df = df.Define("httCand_leg1_eta", "httCand.leg_p4[1].Eta()")
    df = df.Define("httCand_leg1_phi", "httCand.leg_p4[1].Phi()")
    df = df.Define("httCand_leg1_mass", "httCand.leg_p4[1].M()")
    df = df.Define("GenChannel", "static_cast<int>(genChannel)")
    df = df.Define("RecoChannel", "static_cast<int>(recoChannel)")
    
    
    df = df.Define("jet_pt", "std::vector<float> jet_pt(Jet_p4.size()); for (size_t i = 0 ; i < Jet_p4.size(); i++) {jet_pt[i]=Jet_p4[i].Pt();} return jet_pt;")
    df = df.Define("jet_eta", "std::vector<float> jet_eta(Jet_p4.size()); for (size_t i = 0 ; i < Jet_p4.size(); i++) {jet_eta[i]=Jet_p4[i].Eta();} return jet_eta;")
    df = df.Define("jet_phi", "std::vector<float> jet_phi(Jet_p4.size()); for (size_t i = 0 ; i < Jet_p4.size(); i++) {jet_phi[i]=Jet_p4[i].Phi();} return jet_phi;")
    df = df.Define("jet_mass", "std::vector<float> jet_mass(Jet_p4.size()); for (size_t i = 0 ; i < Jet_p4.size(); i++) {jet_mass[i]=Jet_p4[i].M();} return jet_mass;")
    #df.Filter("n_matched>2").Display({"event", "n_matched", "JetRecoMatched"}).Print()

    df = df.Filter("n_matched>=2", "AtLeastTwoDifferentMatches") 
    #print(df.GetColumnNames())
    report = df.Report()
    report.Print() 
    colToSave = ["event","luminosityBlock", "n_matched", "GenJetAssociated","nJet","Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass", "httCand_leg0_pt", "httCand_leg0_eta",
                "httCand_leg0_phi", "httCand_leg0_mass", "httCand_leg1_pt", "httCand_leg1_eta", "httCand_leg1_phi","httCand_leg1_mass", "GenChannel", "RecoChannel","sample","year","X_mass",
                "Jet_btagCSVV2", "Jet_btagDeepB", "Jet_btagDeepFlavB", "Jet_btagDeepFlavCvB", "Jet_btagDeepFlavCvL", "Jet_btagDeepFlavQG" ] 

    varToSave = ROOT.std.vector("string")()
    for col in colToSave:
        varToSave.push_back(col) 
    df.Snapshot("Event", outFile, varToSave)
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
    outDir = 'output'   
    createSkim(inFile, args.outFile, args.year, args.sample, args.mass)
        