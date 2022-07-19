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
    
                 "genChannel", "recoChannel", "Jet_btagDeepFlavB","Jet_btagDeepFlavCvB"]
    triggers ={
        "2018": ["HLT_Ele32_WPTight_Gsf", "HLT_Ele35_WPTight_Gsf", 
                    "HLT_Ele24_eta2p1_WPTight_Gsf_LooseChargedIsoPFTauHPS30_eta2p1_CrossL1", "HLT_Ele28_eta2p1_WPTight_Gsf_HT150", 
                    "HLT_Ele32_WPTight_Gsf_L1DoubleEG", "HLT_PFMET120_PFMHT120_IDTight", 
                    "HLT_Diphoton30_18_R9IdL_AND_HE_AND_IsoCaloId_NoPixelVeto", 
                    "HLT_MediumChargedIsoPFTau180HighPtRelaxedIso_Trk50_eta2p1", "HLT_Ele50_CaloIdVT_GsfTrkIdT_PFJet165", 
                    "HLT_DoubleMediumChargedIsoPFTauHPS35_Trk1_eta2p1_Reg", 
                    "HLT_PFHT330PT30_QuadPFJet_75_60_45_40_TriplePFBTagDeepCSV_4p5", "HLT_IsoMu24", "HLT_Mu50", "HLT_TkMu100",
                    "HLT_OldMu100", "HLT_IsoMu20_eta2p1_LooseChargedIsoPFTauHPS27_eta2p1_CrossL1", 
                    "HLT_MonoCentralPFJet80_PFMETNoMu120_PFMHTNoMu120_IDTight",
                    "HLT_MediumChargedIsoPFTau180HighPtRelaxedIso_Trk50_eta2p1", "HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8", 
                    "HLT_Mu17_Photon30_IsoCaloId", "HLT_PFMETNoMu120_PFMHTNoMu120_IDTight", "HLT_DoubleMu4_Mass3p8_DZ_PFHT350", 
                    "HLT_DoubleMu3_DCA_PFMET50_PFMHT60", "HLT_DoubleMediumChargedIsoPFTauHPS35_Trk1_eta2p1_Reg", 
                    "HLT_MediumChargedIsoPFTau180HighPtRelaxedIso_Trk50_eta2p1", "HLT_PFMETNoMu120_PFMHTNoMu120_IDTight", 
                    "HLT_MediumChargedIsoPFTau50_Trk30_eta2p1_1pr_MET100", "HLT_AK8PFJet330_TrimMass30_PFAK8BoostedDoubleB_np4", 
                    "HLT_QuadPFJet103_88_75_15_DoublePFBTagDeepCSV_1p3_7p7_VBF1", 
                    "HLT_PFHT330PT30_QuadPFJet_75_60_45_40_TriplePFBTagDeepCSV_4p5", "HLT_Photon35_TwoProngs35"],
    }
    
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
        