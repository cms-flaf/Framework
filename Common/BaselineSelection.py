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

def selectChannel(df, channel):
    df_daughters = GetDaughters(df)
    df_channel = df_daughters.Define("leptons_indices", "GetLeptonIndices(event, GenPart_pdgId, GenPart_genPartIdxMother, GenPart_statusFlags)").Define("HTT_Cand", "GetHTTCand(event,leptons_indices, GenPart_pdgId, GenPart_genPartIdxMother, GenPart_status, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, GenPart_daughters )").Filter(("HTT_Cand.GetChannel()==Channel::{}").format(channel))
    #print(channel, df.Count().GetValue(), df_channel.Count().GetValue())
    return df_channel

def passAcceptance(df):
    return df.Filter('PassAcceptance(HTT_Cand)')

def SelectBestPair(df, channel):
    nTaus = 1 if channel != 'tauTau' else 2
    df = df.Define("Tau_idx", "CreateIndexes(Tau_pt.size())").\
            Define("Tau_selectedIdx", f"Tau_idx[Tau_pt > 20 && abs(Tau_eta)<2.3 && abs(Tau_dz)<0.2 && (Tau_decayMode==0 || Tau_decayMode==1 || Tau_decayMode==10 || Tau_decayMode==11) && (Tau_idDeepTau2017v2p1VSjet)&({channelWPs_baseline[channel][2]}) && (Tau_idDeepTau2017v2p1VSmu)&({channelWPs_baseline[channel][1]}) && \
            (Tau_idDeepTau2017v2p1VSe)&({channelWPs_baseline[channel][0]}) ]").\
            Filter(f"Tau_selectedIdx.size()>={nTaus}").\
            Define("Muon_idx", "CreateIndexes(Muon_pt.size())").\
            Define("Electron_idx", "CreateIndexes(Electron_pt.size())")

    if(channel=='eTau'):
        df_pair = df.Define("Electron_selectedIdx", f"Electron_idx[ Electron_pt > 20 && abs(Electron_eta) < 2.1 && abs(Electron_dz)<0.2  &&  abs(Electron_dxy)<0.045 && Electron_mvaFall17V2Iso_WP80 ]").\
                Filter("Electron_selectedIdx.size()>=1").\
                Define("eTau_pairs","GetPotentialHTTCandidates(HTT_Cand.GetChannel(),Electron_selectedIdx, Tau_selectedIdx, Electron_pt, Electron_phi, Electron_eta, Electron_mass, Electron_charge, Tau_pt, Tau_phi, Tau_eta, Tau_mass, Tau_charge, 0.4)").\
                Define("HTTCandidates_p4_10", "eTau_pairs[0].leg_p4[0]").\
                Define("HTTCandidates_p4_11", "eTau_pairs[0].leg_p4[1]").\
                Define("BestHTTCand","GetBestHTTCand(eTau_pairs, Electron_pfRelIso03_all, Electron_pt, Tau_rawDeepTau2017v2p1VSjet, Tau_pt)")
    elif(channel=='muTau'):
        df_pair = df.Define("Muon_selectedIdx", f"Muon_idx[ Muon_pt > 20 && abs(Muon_eta)<2.3 && abs(Muon_dz)<0.2 && abs(Muon_dxy)<0.045  && ( ( Muon_tightId==1  && Muon_pfRelIso04_all<0.15) || (Muon_highPtId==1 && Muon_tkRelIso<0.1)  ) ];").\
                Filter("Muon_selectedIdx.size()>=1").\
                Define("muTau_pairs","GetPotentialHTTCandidates(HTT_Cand.GetChannel(),Muon_selectedIdx, Tau_selectedIdx, Muon_pt, Muon_phi, Muon_eta, Muon_mass, Muon_charge, Tau_pt, Tau_phi, Tau_eta, Tau_mass, Tau_charge, 0.4)").\
                Define("BestHTTCand","GetBestHTTCand(muTau_pairs, Muon_pfRelIso03_all, Muon_pt, Tau_rawDeepTau2017v2p1VSjet, Tau_pt)")
    elif(channel=='tauTau'):
        df_pair =  df.Define("tauTau_pairs","GetPotentialHTTCandidates( HTT_Cand.GetChannel(),Tau_selectedIdx, Tau_selectedIdx, Tau_pt, Tau_phi, Tau_eta, Tau_mass, Tau_charge, Tau_pt, Tau_phi, Tau_eta, Tau_mass, Tau_charge) ").Define("BestHTTCand", "GetBestHTTCand(tauTau_pairs, Tau_rawDeepTau2017v2p1VSjet, Tau_pt, Tau_rawDeepTau2017v2p1VSjet, Tau_pt)")

    else:
        print("this channel is not considered")
        df_pair = df.Define("BestHTTCand","HTTCand BestHTTCand; return BestHTTCand;")
    return df_pair


def JetSelection(df, channel):
    is2017=0
    nJets = 2 #if channel!='tauTau' else 1
    nFatJets = 1 #if channel!='tauTau' else 1
    df_AK4Jet = df.Define("Jet_idx", "CreateIndexes(Jet_pt.size())").\
                Define("Jet_selectedIdx", f"Jet_idx[Jet_pt>20 && abs(Jet_eta) < 2.5 && ( (Jet_jetId)&(1<<1) || {is2017} == 1) ];").\
                Define("Jet_p4", "GetP4(Jet_pt, Jet_eta, Jet_phi, Jet_mass, Jet_selectedIdx)").\
                Define("BestHTTCand_p4_0", "BestHTTCand.leg_p4[0]").\
                Define("AK4JetFilter", f"JetFilter( BestHTTCand, Jet_p4,{nJets})")
    df_AK8Jet = df_AK4Jet.Define("FatJet_idx", "CreateIndexes(FatJet_pt.size())").\
                        Define("FatJet_selectedIdx", f"FatJet_idx[ FatJet_msoftdrop>30 && abs(FatJet_eta) < 2.5 ];").\
                        Define("FatJet_p4", "GetP4(FatJet_pt, FatJet_eta, FatJet_phi, FatJet_mass, FatJet_selectedIdx)").\
                        Define("AK8JetFilter", f"JetFilter( BestHTTCand, FatJet_p4,{nFatJets})")
    df_Jet = df_AK4Jet.Filter("AK4JetFilter==1")
    return df_Jet

def ThirdLeptonVeto(df):
    df_electronVeto=df.Define("signalElectron_idx","if(BestHTTCand.channel==Channel::eTau) return BestHTTCand.leg_index[0]; return -100;").\
                    Define("ElectronVeto_idx", "Electron_idx[Electron_idx!=signalElectron_idx && Electron_pt >10 && abs(Electron_eta) < 2.5 && abs(Electron_dz) < 0.2 && abs(Electron_dxy) < 0.045 && ( Electron_mvaFall17V2Iso_WP90 == true || ( Electron_mvaFall17V2noIso_WP90 == true && Electron_pfRelIso03_all<0.3 ))]").\
                    Filter("ElectronVeto_idx.size()==0")
    df_muonVeto = df_electronVeto.Define("signalMuon_idx","if(BestHTTCand.channel==Channel::muTau) return BestHTTCand.leg_index[0]; else return -100;").\
    Define("MuonVeto_idx", "Muon_idx[ Muon_idx!=signalMuon_idx &&  Muon_pt >10 && abs(Muon_eta) < 2.4 && abs(Muon_dz) < 0.2 && abs(Muon_dxy) < 0.045 && ( Muon_mediumId == true ||  Muon_tightId == true ) && Muon_pfRelIso04_all<0.3  ]").\
    Filter("MuonVeto_idx.size()==0")
    #Define("muonVeto","MuonVeto(BestHTTCand, Muon_pt, Muon_dz, Muon_dxy, Muon_eta, Muon_tightId, Muon_mediumId, Muon_pfRelIso04_all)").Filter("muonVeto==true")
    #return df.Filter("MuonVeto(BestHTTCand, Muon_pt, Muon_dz, Muon_dxy, Muon_eta, Muon_tightId, Muon_mediumId, Muon_pfRelIso04_all)").Filter("ElectronVeto(HTT_Cand, Electron_pt, Electron_dz, Electron_dxy, Electron_eta, Electron_mvaFall17V2Iso_WP90, Electron_mvaFall17V2noIso_WP90 ,  Electron_pfRelIso03_all)")
    #df_muVeto = df_eleVeto.Define("signalMuon_idx","if(BestHTTCand.channel==Channel::muTau) return BestHTTCand.leg_index[0]; else return -100;").\
    #            Define("MuonVeto_idx", "Muon_idx[ Muon_idx!=signalMuon_idx &&  Muon_pt >10 && abs(Muon_eta) < 2.4 && abs(Muon_dz) < 0.2 && abs(Muon_dxy) < 0.045 && ( Muon_mediumId == true ||  Muon_tightId == true ) && Muon_pfRelIso04_all<0.3  ]").\
    #            Filter("MuonVeto_idx.size()==0")

    return df_muonVeto

def GenMatching(df, channel):
    df_matched = df.Filter("GenRecoMatching(HTT_Cand, BestHTTCand)")
    return df_matched


'''
def JetLepSeparation(df, channel):
    df = df.Define("jet_p4", "return LorentzVectorM(Jet_pt,Jet_eta,Jet_phi,Jet_mass);").Define("tau_p4", "return BestHTTCand.leg_p4[1]").Filter("JetLepSeparation()==true")
    '''
def FindInvMass(df, index_vec):
    # 1. define most two energetic jets
    df = df.Define("genJet_idx", f"ReorderObjects(GenJet_pt, {index_vec},2)")
    for n in range(2):
        df = df.Define(f"jet{n+1}_p4", f"LorentzVectorM(GenJet_pt[genJet_idx[{n}]],GenJet_eta[genJet_idx[{n}]],GenJet_phi[genJet_idx[{n}]],GenJet_mass[genJet_idx[{n}]])")
    df_invMass = df.Define("mjj", "(jet1_p4+jet2_p4).M()")
    return df_invMass



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

    return df.Filter(" || ".join(ch_filters), "RecoBaseline0")

def ApplyRecoBaseline1(df, is2017=0):
    df = df.Define("Jet_B1", f"Jet_pt>20 && abs(Jet_eta) < 2.5 && ( (Jet_jetId & 2) || {is2017} == 1 )")
    df = df.Define("FatJet_B1", "FatJet_msoftdrop > 30 && abs(FatJet_eta) < 2.5")

    df = df.Define("Lepton_p4_B0", "std::vector<RVecLV>{Electron_p4[Electron_B0], Muon_p4[Muon_B0], Tau_p4[Tau_B0]}")
    df = df.Define("Jet_B1T", "RemoveOverlaps(Jet_p4, Jet_B1, Lepton_p4_B0, 2, 0.5)")
    df = df.Define("FatJet_B1T", "RemoveOverlaps(FatJet_p4, FatJet_B1, Lepton_p4_B0, 2, 0.5)")

    return df.Filter("Jet_idx[Jet_B1T].size() >= 2 || FatJet_idx[FatJet_B1T].size() >= 1", "RecoBaseline1")

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
    df = df.Filter(" || ".join(cand_filters), "RecoBaseline2")
    cand_list_str = ', '.join([ '&' + c for c in cand_columns])
    return df.Define('httCand', f'GetBestHTTCandidate({{ {cand_list_str} }})')


def FindMPV(df):
    dfCols = df.GetColumnNames()
    if('genHttCand' not in dfCols):
        df = ApplyGenBaseline(df)
    if('Tau_B0T' not in dfCols):
        df.ApplyRecoBaseline0(df)
    if('Jet_B1T' not in dfCols):
        df = ApplyRecoBaseline1(df)
    if('httCand'not in dfCols):
        df = ApplyRecoBaseline2(df)
    if('Jet_B2T' not in dfCols):
        df = ApplyRecoBaseline3(df)
    if('GenJet_idx' not in dfCols):
        df = df.Define("GenJet_idx", f"CreateIndexes(GenJet_pt.size())") \
            .Define("GenJet_p4", f"GetP4(GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass, GenJet_idx)")
    df = df.Filter("genChannel==recoChannel")
    df = df.Filter("genChannel==Channel::eTau || genChannel == Channel::muTau || genChannel == Channel::tauTau")
    if('GenJet_b_PF' not in dfCols):
        df = df.Define("GenJet_b_PF", "abs(GenJet_partonFlavour)==5").Define("GenJet_b_PF_idx", "GenJet_idx[GenJet_b_PF]")
    df = df.Filter("GenJet_b_PF_idx.size()==2")
    df = df.Define("Two_bGenJets_invMass", "InvMassByFalvour(GenJet_p4, GenJet_partonFlavour, true)")
    histo = df.Histo1D(("Two_bGenJets_invMass", "Two_bGenJets_invMass", 400, -0.5, 199.5),"Two_bGenJets_invMass").GetValue()
    y_max = histo.GetMaximumBin()
    x_max = histo.GetXaxis().GetBinCenter(y_max)
    return x_max


def ApplyGenBaseline1(df):
    df = df.Define("GenJet_idx", f"CreateIndexes(GenJet_pt.size())") \
            .Define("GenJet_p4", f"GetP4(GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass, GenJet_idx)")
    
    df = df.Define("GenJet_b_PF", "abs(GenJet_partonFlavour)==5")
    df = df.Define("GenJet_b_PF_idx", "GenJet_idx[GenJet_b_PF]").Filter("GenJet_b_PF_idx.size()>=2", "AtLeastTwoPartonJets")
    x_max = FindMPV(df)
    print(f"the mpv is {x_max}")
    df = df.Define("TwoClosestJetToMPV",f"FindTwoJetsClosestToMPV({x_max}, GenJet_p4, GenJet_b_PF,GenJet_partonFlavour, true,5)")
    return df 


def ApplyRecoBaseline3(df):
    df = df.Define("Jet_B2T", "RemoveOverlaps(Jet_p4, Jet_B1T,{{{{httCand.leg_p4[0]}}, {{httCand.leg_p4[1]}}}}, 2, 0.5)") 
    return df.Filter("Jet_idx[Jet_B2T].size()>=2", "ApplyRecoBaseline3")
 
def ApplyGenRecoJetMatching(df):
    df = df.Define("JetRecoMatched","GenRecoJetMatching( event,Jet_p4,  GenJet_p4, TwoClosestJetToMPV,0.2)") 
    
    df = df.Define("isMatched", "std::vector<int> isMatched(JetRecoMatched.size()) ; for (size_t i =0 ; i<JetRecoMatched.size(); i++){isMatched[i]=JetRecoMatched[i].first; }return isMatched; ")
    df = df.Define("GenJetAssociated", "std::vector<int> GenJetAss(JetRecoMatched.size()) ; for (size_t i =0 ; i<JetRecoMatched.size(); i++){GenJetAss[i]=JetRecoMatched[i].second;} return GenJetAss; ") 
    df = df.Define("n_matched","std::accumulate(isMatched.begin(), isMatched.end(), 0)") 
    #df = df.Define("n_matched","AtLeastTwoCorrespondence(JetRecoMatched)") 
    #df.Display({"JetRecoMatched", "n_matched"}).Print()
    return df#.Filter("n_matched>=2", "AtLeastTwoDifferentMatches")
    