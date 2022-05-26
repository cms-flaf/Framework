import ROOT
import os
from scipy import stats
import numpy as np
#higgs_header_path = os.path.join(os.sep, str(ROOT.gROOT.GetTutorialDir()) + os.sep, "dataframe" + os.sep,
header_path = "SkimmerHeader.h"

ROOT.gInterpreter.Declare('#include "{}"'.format(header_path))
channelLegs = {
    "eTau": [ "Electron", "Tau" ],
    "muTau": [ "Muon", "Tau" ],
    "tauTau": [ "Tau", "Tau" ],
}
hlt_columns = {
    "eTau":["HLT_Ele32_WPTight_Gsf","HLT_Ele35_WPTight_Gsf","HLT_Ele24_eta2p1_WPTight_Gsf_LooseChargedIsoPFTauHPS30_eta2p1_CrossL1","HLT_Ele28_eta2p1_WPTight_Gsf_HT150","HLT_Ele32_WPTight_Gsf_L1DoubleEG","HLT_PFMET120_PFMHT120_IDTight","HLT_Diphoton30_18_R9IdL_AND_HE_AND_IsoCaloId_NoPixelVeto","HLT_MediumChargedIsoPFTau180HighPtRelaxedIso_Trk50_eta2p1","HLT_Ele50_CaloIdVT_GsfTrkIdT_PFJet165","HLT_DoubleMediumChargedIsoPFTauHPS35_Trk1_eta2p1_Reg","HLT_PFHT330PT30_QuadPFJet_75_60_45_40_TriplePFBTagDeepCSV_4p5"],
    "muTau":["HLT_IsoMu24", "HLT_Mu50", "HLT_TkMu100", "HLT_OldMu100", "HLT_IsoMu20_eta2p1_LooseChargedIsoPFTauHPS27_eta2p1_CrossL1", "HLT_MonoCentralPFJet80_PFMETNoMu120_PFMHTNoMu120_IDTight", "HLT_MediumChargedIsoPFTau180HighPtRelaxedIso_Trk50_eta2p1", "HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8", "HLT_Mu17_Photon30_IsoCaloId", "HLT_PFMETNoMu120_PFMHTNoMu120_IDTight", "HLT_DoubleMu4_Mass3p8_DZ_PFHT350", "HLT_DoubleMu3_DCA_PFMET50_PFMHT60"],
    "tauTau": ["HLT_DoubleMediumChargedIsoPFTauHPS35_Trk1_eta2p1_Reg","HLT_MediumChargedIsoPFTau180HighPtRelaxedIso_Trk50_eta2p1","HLT_PFMETNoMu120_PFMHTNoMu120_IDTight","HLT_MediumChargedIsoPFTau50_Trk30_eta2p1_1pr_MET100","HLT_AK8PFJet330_TrimMass30_PFAK8BoostedDoubleB_np4","HLT_QuadPFJet103_88_75_15_DoublePFBTagDeepCSV_1p3_7p7_VBF1","HLT_PFHT330PT30_QuadPFJet_75_60_45_40_TriplePFBTagDeepCSV_4p5","HLT_Photon35_TwoProngs35"]
}

def selectChannel(df, channel):
    df_channel = df.Define("leptons_indices", "GetLeptonIndices(event, GenPart_pdgId, GenPart_genPartIdxMother, GenPart_statusFlags)").Define("event_info", "GetEventInfo(event,leptons_indices, GenPart_pdgId, GenPart_genPartIdxMother, GenPart_status, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass )").Filter(("event_info.channel==Channel::{}").format(channel))
    #print(channel, df.Count().GetValue(), df_channel.Count().GetValue())
    return df_channel

def passAcceptance(df):
    return df.Filter('PassAcceptance(event_info)')

def SelectBestPair(df, channel):
    if(channel=='tauTau'):
        #RecoEleSelectedIndices
        #RecoMuSelectedIndices
        df_pair = df.Define("tau_indices", "RecoTauSelectedIndices(event, event_info, Tau_dz, Tau_eta, Tau_phi, Tau_pt, Tau_idDeepTau2017v2p1VSjet,  Tau_idDeepTau2017v2p1VSmu, Tau_idDeepTau2017v2p1VSe, Tau_decayMode)").Define("tauTau_pairs","GetPairs(tau_indices, tau_indices, Tau_phi, Tau_eta,Tau_phi, Tau_eta)").Define("final_indices", "GetFinalIndices_tauTau(tauTau_pairs, event,Tau_rawDeepTau2017v2p1VSjet,Tau_pt, Tau_charge)")

    elif(channel=='muTau'):
        df_pair = df.Define("tau_indices", "RecoTauSelectedIndices(event, event_info, Tau_dz, Tau_eta, Tau_phi, Tau_pt, Tau_idDeepTau2017v2p1VSjet,  Tau_idDeepTau2017v2p1VSmu, Tau_idDeepTau2017v2p1VSe, Tau_decayMode)").Define("mu_indices", "RecoMuSelectedIndices(event, Muon_dz,  Muon_dxy, Muon_eta, Muon_phi, Muon_pt, Muon_tightId, Muon_highPtId,Muon_tkRelIso, Muon_pfRelIso04_all)").Define("muTau_pairs","GetPairs(mu_indices, tau_indices, Muon_phi, Muon_eta,Tau_phi, Tau_eta)").Define("final_indices","GetFinalIndices(muTau_pairs, event,Muon_pfRelIso04_all, Muon_pt, Tau_rawDeepTau2017v2p1VSjet, Tau_pt, Muon_charge, Tau_charge)")

    elif(channel=='eTau'):
        df_pair = df.Define("tau_indices", "RecoTauSelectedIndices(event, event_info, Tau_dz, Tau_eta, Tau_phi, Tau_pt, Tau_idDeepTau2017v2p1VSjet,  Tau_idDeepTau2017v2p1VSmu, Tau_idDeepTau2017v2p1VSe, Tau_decayMode)").Define("e_indices", "RecoEleSelectedIndices(event, Electron_dz, Electron_dxy, Electron_eta, Electron_phi, Electron_pt, Electron_mvaFall17V2Iso_WP80)").Define("eTau_pairs","GetPairs(e_indices, tau_indices, Electron_phi, Electron_eta,Tau_phi, Tau_eta)").Define("final_indices","GetFinalIndices(eTau_pairs, event,Electron_pfRelIso03_all, Electron_pt, Tau_rawDeepTau2017v2p1VSjet, Tau_pt, Electron_charge, Tau_charge)")
    else:
        print("this channel is not considered")
        df_pair = df.Define("final_indices","vec_f final_indices; return final_indices;")
    df_pair=df_pair.Filter('final_indices.size()==2')
    return df_pair

def DefineLegP4(df, channel):
    for n in range(2):
        df = df.Define('leg{}_p4'.format(n+1), 'LorentzVectorM({0}_pt[final_indices[{1}]], {0}_eta[final_indices[{1}]],{0}_phi[final_indices[{1}]], {0}_mass[final_indices[{1}]])'.format(channelLegs[channel][n],n)).Define('genLep{}_p4'.format(n+1), ('event_info.leg_p4[{}]').format(n))
    return df

def ThirdLeptonVeto(df):
    df_eleVeto = df.Filter("ElectronVeto(event_info, final_indices, Electron_pt, Electron_dz, Electron_dxy, Electron_eta, Electron_mvaFall17V2Iso_WP90, Electron_mvaFall17V2noIso_WP90 ,  Electron_pfRelIso03_all)")
    df_muVeto = df_eleVeto.Filter("MuonVeto(event_info,final_indices, Muon_pt, Muon_dz, Muon_dxy, Muon_eta, Muon_tightId, Muon_mediumId ,  Muon_pfRelIso04_all)")
    return df_muVeto

def ApplyHLTRequirements(df, channel):
    filter_str=" || ".join([ '( {} == 1 )'.format(p) for p in hlt_columns[channel]])
    #print(filter_str)
    df_filtered= df.Filter(filter_str)
    return df_filtered


def JetSelection(df, channel):
    is2017=0
    df_Jet = df.Define("AK4JetFilter",f"AK4JetFilter(event_info, final_indices, leg1_p4, leg2_p4, Jet_eta,  Jet_phi,  Jet_pt,  Jet_jetId, {is2017})").Define("AK8JetFilter",f"AK8JetFilter(event_info, final_indices, leg1_p4, leg2_p4,  FatJet_pt, FatJet_eta, FatJet_phi, FatJet_msoftdrop, {is2017})").Filter("AK4JetFilter==1")
    return df_Jet

def GenMatching(df, channel):
    if(channel != 'tauTau'):
        df_matched = df.Define("GenRecoMatching","GenRecoMatching(leg1_p4, leg2_p4, genLep1_p4, genLep2_p4)").Filter("GenRecoMatching==true")
    else:
        df_matched = df.Define("GenRecoMatching_1","GenRecoMatching(leg1_p4, leg2_p4, genLep1_p4, genLep2_p4)").Define("GenRecoMatching_2","GenRecoMatching(leg2_p4, leg1_p4, genLep1_p4, genLep2_p4)").Filter("GenRecoMatching_1==true || GenRecoMatching_2==true")
    return df_matched

def FindInvMass(df, index_vec):
    # 1. define most two energetic jets
    df = df.Define("genJet_idx", f"ReorderJets(GenJet_pt, {index_vec})")
    for n in range(2):
        df = df.Define(f"jet{n+1}_p4", f"LorentzVectorM(GenJet_pt[genJet_idx[{n}]],GenJet_eta[genJet_idx[{n}]],GenJet_phi[genJet_idx[{n}]],GenJet_mass[genJet_idx[{n}]])")
    df_invMass = df.Define("mjj", "(jet1_p4+jet2_p4).M()")
    return df_invMass


def DefineDataFrame(df, ch):
    df_channel=selectChannel(df,ch)
    df_acceptance = passAcceptance(df_channel)
    df_pairs = SelectBestPair(df_acceptance, ch)
    df_legP4 = DefineLegP4(df_pairs, ch)

    df_JetFiltered= JetSelection(df_legP4, ch)
    df_lepVeto = ThirdLeptonVeto(df_JetFiltered)
    df_matched = GenMatching(df_lepVeto, ch)
    return df_matched
    #print(f"for the channel {ch}:\nthe number of initial events is {df_channel.Count().GetValue()}\nthe number of events that pass the acceptance is {df_acceptance.Count().GetValue()}\nthe number of events with final best pair with size 2 is {df_pairs.Count().GetValue()}\nafter applying the Jet filter {df_JetFiltered.Count().GetValue()}\nafter applying the third lepton veto {df_lepVeto.Count().GetValue()}\nafter the filter on gen-reco matching {df_matched.Count().GetValue()} \n\n")
