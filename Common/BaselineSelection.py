import ROOT
import os
from scipy import stats
import numpy as np
import enum

initialized = False

def Initialize():
    global initialized
    if not initialized:
        import os
        header_path_Gen = f"{os.environ['ANALYSIS_PATH']}/Common/BaselineGenSelection.h"
        header_path_Reco = f"{os.environ['ANALYSIS_PATH']}/Common/BaselineRecoSelection.h"
        ROOT.gInterpreter.Declare(f'#include "{header_path_Gen}"')
        ROOT.gInterpreter.Declare(f'#include "{header_path_Reco}"')
        initialized = True

leg_names = [ "Electron", "Muon", "Tau" ]

channels = [ 'muMu', 'eMu', 'eE', 'muTau', 'eTau', 'tauTau' ] # in order of importance during the channel selection

channelLegs = {
    "eTau": [ "Electron", "Tau" ],
    "muTau": [ "Muon", "Tau" ],
    "tauTau": [ "Tau", "Tau" ],
    "muMu": [ "Muon", "Muon" ],
    "eMu": [ "Electron", "Muon" ],
    "eE": [ "Electron", "Electron" ],
}

class WorkingPointsTauVSmu:
    VLoose = 1
    Loose = 2
    Medium = 4
    Tight = 8

class WorkingPointsTauVSjet:
   VVVLoose =1
   VVLoose= 2
   VLoose= 4
   Loose= 8
   Medium= 16
   Tight= 32
   VTight= 64
   VVTight= 128

class WorkingPointsTauVSe:
    VVVLoose = 1
    VVLoose = 2
    VLoose = 4
    Loose = 8
    Medium = 16
    Tight = 32
    VTight = 64
    VVTight = 128
 

def ApplyGenBaseline(df):
    df = df.Define("GenPart_daughters", "GetDaughters(GenPart_genPartIdxMother )")
    df = df.Define("genHttCand", """GetGenHTTCandidate(event, GenPart_pdgId, GenPart_daughters, GenPart_statusFlags,
                                                       GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass)""")
    return df.Filter("PassGenAcceptance(genHttCand)", "GenAcceptance")

def ApplyRecoBaseline0(df):
    for obj in [ "Electron", "Muon", "Tau", "Jet", "FatJet" ]:
        df = df.Define(f"{obj}_idx", f"CreateIndexes({obj}_pt.size())") \
               .Define(f"{obj}_p4", f"GetP4({obj}_pt, {obj}_eta, {obj}_phi, {obj}_mass, {obj}_idx)")

    df = df.Define("Electron_B0", """
        Electron_pt > 20 && abs(Electron_eta) < 2.1 && abs(Electron_dz) < 0.2 && abs(Electron_dxy) < 0.045
        && (Electron_mvaFall17V2Iso_WP90 || (Electron_mvaFall17V2noIso_WP90 && Electron_pfRelIso03_all < 0.5))
    """)

    df = df.Define("Muon_B0", """
        Muon_pt > 20 && abs(Muon_eta) < 2.3 && abs(Muon_dz) < 0.2 && abs(Muon_dxy) < 0.045
        && ( ((Muon_tightId || Muon_mediumId) && Muon_pfRelIso04_all < 0.5) || (Muon_highPtId && Muon_tkRelIso < 0.5) )
    """)

    df = df.Define("Tau_B0", f"""
        Tau_pt > 20 && abs(Tau_eta) < 2.3 && abs(Tau_dz) < 0.2 && Tau_decayMode != 5 && Tau_decayMode != 6
        && (Tau_idDeepTau2017v2p1VSe & {WorkingPointsTauVSe.VVLoose})
        && (Tau_idDeepTau2017v2p1VSmu & {WorkingPointsTauVSmu.VLoose})
        && (Tau_idDeepTau2017v2p1VSjet & {WorkingPointsTauVSjet.VVVLoose})
    """)

    df = df.Define("Electron_B0T", """
        Electron_B0 && (Electron_mvaFall17V2Iso_WP80
                        || (Electron_mvaFall17V2noIso_WP80 && Electron_pfRelIso03_all < 0.15))
    """)

    df = df.Define("Muon_B0T", """
        Muon_B0 && ( ((Muon_tightId || Muon_mediumId) && Muon_pfRelIso04_all < 0.15)
                    || (Muon_highPtId && Muon_tkRelIso < 0.15) )
    """)

    df = df.Define("Tau_B0T", f"""
        Tau_B0 && (Tau_idDeepTau2017v2p1VSjet & {WorkingPointsTauVSjet.Medium})
    """)


    ch_filters = []
    for leg1_idx in range(len(leg_names)):
        for leg2_idx in range(max(1, leg1_idx), len(leg_names)):
            leg1, leg2 = leg_names[leg1_idx], leg_names[leg2_idx]
            ch_filter = f"{leg1}{leg2}_B0"
            ch_filters.append(ch_filter)
            if leg1 == leg2:
                ch_filter_def = f"{leg1}_idx[{leg1}_B0].size() > 1 && {leg1}_idx[{leg1}_B0T].size() > 0"
            else:
                ch_filter_def = f"""
                    ({leg1}_idx[{leg1}_B0].size() > 0 && {leg2}_idx[{leg2}_B0T].size() > 0)
                    || ({leg1}_idx[{leg1}_B0T].size() > 0 && {leg2}_idx[{leg2}_B0].size() > 0)
                """
            df = df.Define(ch_filter, ch_filter_def)

    return df.Filter(" || ".join(ch_filters), "Reco Baseline 0")

def ApplyRecoBaseline1(df, is2017=0):
    df = df.Define("Jet_B1", f"Jet_pt>20 && abs(Jet_eta) < 2.5 && ( (Jet_jetId & 2) || {is2017} == 1 )")
    df = df.Define("FatJet_B1", "FatJet_msoftdrop > 30 && abs(FatJet_eta) < 2.5")

    df = df.Define("Lepton_p4_B0", "std::vector<RVecLV>{Electron_p4[Electron_B0], Muon_p4[Muon_B0], Tau_p4[Tau_B0]}")
    df = df.Define("Jet_B1T", "RemoveOverlaps(Jet_p4, Jet_B1, Lepton_p4_B0, 2, 0.5)")
    df = df.Define("FatJet_B1T", "RemoveOverlaps(FatJet_p4, FatJet_B1, Lepton_p4_B0, 2, 0.5)")

    return df.Filter("Jet_idx[Jet_B1T].size() >= 2 || FatJet_idx[FatJet_B1T].size() >= 1", "Reco Baseline 1")

def ApplyRecoBaseline2(df):
    df = df.Define("Electron_iso", "Electron_pfRelIso03_all") \
           .Define("Muon_iso", "Muon_pfRelIso04_all") \
           .Define("Tau_iso", "-Tau_rawDeepTau2017v2p1VSjet")

    df = df.Define("Electron_B2_eTau_1", "Electron_B0 && Electron_mvaFall17V2Iso_WP80")
    df = df.Define("Tau_B2_eTau_2", f"""
        Tau_B0
        && (Tau_idDeepTau2017v2p1VSe & {WorkingPointsTauVSe.VLoose})
        && (Tau_idDeepTau2017v2p1VSmu & {WorkingPointsTauVSmu.Tight})
    """)

    df = df.Define("Muon_B2_muTau_1", """
        Muon_B0 && ( (Muon_tightId && Muon_pfRelIso04_all < 0.15)
                     || (Muon_highPtId && Muon_tkRelIso < 0.15) )
    """)
    df = df.Define("Tau_B2_muTau_2", f"""
        Tau_B0
        && (Tau_idDeepTau2017v2p1VSe & {WorkingPointsTauVSe.VLoose})
        && (Tau_idDeepTau2017v2p1VSmu & {WorkingPointsTauVSmu.Tight})
    """)

    df = df.Define("Tau_B2_tauTau_1", f"""
        Tau_B0
        && (Tau_idDeepTau2017v2p1VSe & {WorkingPointsTauVSe.VVLoose})
        && (Tau_idDeepTau2017v2p1VSmu & {WorkingPointsTauVSmu.VLoose})
        && (Tau_idDeepTau2017v2p1VSjet & {WorkingPointsTauVSjet.Medium})
    """)

    df = df.Define("Tau_B2_tauTau_2", f"""
        Tau_B0
        && (Tau_idDeepTau2017v2p1VSe & {WorkingPointsTauVSe.VVLoose})
        && (Tau_idDeepTau2017v2p1VSmu & {WorkingPointsTauVSmu.VLoose})
    """)

    df = df.Define("Muon_B2_muMu_1", """
        Muon_B0 && ( (Muon_tightId && Muon_pfRelIso04_all < 0.15)
                     || (Muon_highPtId && Muon_tkRelIso < 0.15) )
    """)
    df = df.Define("Muon_B2_muMu_2", """
        Muon_B0 && ( (Muon_tightId && Muon_pfRelIso04_all < 0.3) || (Muon_highPtId && Muon_tkRelIso < 0.3) )
    """)

    df = df.Define("Electron_B2_eMu_1", """
        Electron_B0 && Electron_mvaFall17V2noIso_WP80 && Electron_pfRelIso03_all < 0.3
    """)
    df = df.Define("Muon_B2_eMu_2", """
        Muon_B0 && ( (Muon_tightId && Muon_pfRelIso04_all < 0.15) || (Muon_highPtId && Muon_tkRelIso < 0.15) )
    """)

    df = df.Define("Electron_B2_eE_1", """
        Electron_B0
        && (Electron_mvaFall17V2Iso_WP80 || Electron_mvaFall17V2noIso_WP80 && Electron_pfRelIso03_all < 0.15)
    """)
    df = df.Define("Electron_B2_eE_2", """
        Electron_B0 && Electron_mvaFall17V2noIso_WP80 && Electron_pfRelIso03_all < 0.3
    """)

    cand_columns = []
    for ch in channels:
        leg1, leg2 = channelLegs[ch]
        cand_column = f"httCands_{ch}"
        df = df.Define(cand_column, f"""
            GetHTTCandidates(Channel::{ch}, 0.4, {leg1}_B2_{ch}_1, {leg1}_p4, {leg1}_iso, {leg1}_charge,
                                                 {leg2}_B2_{ch}_2, {leg2}_p4, {leg2}_iso, {leg2}_charge)
        """)
        cand_columns.append(cand_column)
    cand_filters = [ f'{c}.size() > 0' for c in cand_columns ]
    df = df.Filter(" || ".join(cand_filters), "Reco Baseline 2")
    cand_list_str = ', '.join([ '&' + c for c in cand_columns])
    return df.Define('httCand', f'GetBestHTTCandidate({{ {cand_list_str} }})')


def FindMPV(df): 
    df = df.Define("bJetp4", " GenJet_p4[GenJet_b_PF]").Filter("bJetp4.size()==2").Define("Two_bGenJets_invMass", "(bJetp4[0]+bJetp4[1]).M()")  
    histo = df.Histo1D(("Two_bGenJets_invMass", "Two_bGenJets_invMass", 400, -0.5, 199.5),"Two_bGenJets_invMass").GetValue() 
    y_max = histo.GetMaximumBin()
    x_max = histo.GetXaxis().GetBinCenter(y_max) 
    return x_max

def ApplyGenBaselineAcceptance(df):
    df = df.Define("GenJet_idx", f"CreateIndexes(GenJet_pt.size())") \
            .Define("GenJet_p4", f"GetP4(GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass, GenJet_idx)")
    df = df.Filter("GenJet_pt > 20 && abs(GenJet_eta) < 2.5", "GenJet Acceptance")
    return df

def ApplyGenBaseline1(df, x_max):
    df = df.Define("GenJet_b_PF", "abs(GenJet_partonFlavour)==5").Filter("GenJet_p4[GenJet_b_PF].size()>=2", "Two b-parton jets at least") 
    
    #print(f"the mpv is {x_max}")
    df = df.Define("TwoClosestJetToMPV",f"FindTwoJetsClosestToMPV({x_max}, GenJet_p4, GenJet_b_PF)")
    return df 

def ApplyGenBaseline2(df):
     df = df.Define("GenJetAK8_idx", f"CreateIndexes(GenJetAK8_pt.size())") \
            .Define("GenJetAK8_p4", f"GetP4(GenJetAK8_pt, GenJetAK8_eta, GenJetAK8_phi, GenJetAK8_mass, GenJetAK8_idx)")

def ApplyRecoBaseline3(df):
    df = df.Define("Jet_B3T", "RemoveOverlaps(Jet_p4, Jet_B1T,{{httCand.leg_p4[0], httCand.leg_p4[1]},}, 2, 0.5)") 
    df = df.Define("FatJet_B3T", "RemoveOverlaps(FatJet_p4, FatJet_B1T,{{httCand.leg_p4[0], httCand.leg_p4[1]},}, 2, 0.5)") 
    return df.Filter("Jet_idx[Jet_B3T].size()>=2 || FatJet_idx[FatJet_B3T].size()>=1", "Reco Baseline 3")
 
def ApplyRecoBaseline4(df):
    return df.Filter("Jet_idx[Jet_B3T].size()>=2", "Reco Baseline 4")


def ApplyGenRecoJetMatching(df):
    df = df.Define("JetRecoMatched", "GenRecoJetMatching(Jet_genJetIdx, TwoClosestJetToMPV)") 
    return df.Filter("Jet_p4[JetRecoMatched].size()>=2", "Two different gen-reco jet matches at least") 
    