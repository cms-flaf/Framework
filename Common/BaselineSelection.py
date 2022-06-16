import ROOT
import os
from scipy import stats
import numpy as np
import enum
_rootpath = os.path.abspath(os.path.dirname(__file__)+"/../../..")
ROOT.gROOT.ProcessLine(".include "+_rootpath)
header_path_Gen = f"{os.environ['ANALYSIS_PATH']}/Common/BaselineGenSelection.h"
header_path_Reco = f"{os.environ['ANALYSIS_PATH']}/Common/BaselineRecoSelection.h"

ROOT.gInterpreter.Declare('#include "{}"'.format(header_path_Gen))
ROOT.gInterpreter.Declare('#include "{}"'.format(header_path_Reco))
channelLegs = {
    "eTau": [ "Electron", "Tau" ],
    "muTau": [ "Muon", "Tau" ],
    "tauTau": [ "Tau", "Tau" ],
}

class WorkingPointsTauVSMu(enum.Enum):
    VLoose = 1
    Loose = 2
    Medium = 4
    Tight = 8

class WorkingPointsTauVSJet(enum.Enum):
   VVVLoose =1
   VVLoose= 2
   VLoose= 4
   Loose= 8
   Medium= 16
   Tight= 32
   VTight= 64
   VVTight= 128

class WorkingPointsTauVSe(enum.Enum):
    VVVLoose = 1
    VVLoose = 2
    VLoose = 4
    Loose = 8
    Medium = 16
    Tight = 32
    VTight = 64
    VVTight = 128


channelWPs_baseline = {
    "eTau": [ WorkingPointsTauVSe.VLoose.value,WorkingPointsTauVSMu.Tight.value, WorkingPointsTauVSJet.VVVLoose.value ],
    "muTau": [ WorkingPointsTauVSe.VLoose.value,WorkingPointsTauVSMu.Tight.value, WorkingPointsTauVSJet.VVVLoose.value ],
    "tauTau": [ WorkingPointsTauVSe.VVLoose.value,WorkingPointsTauVSMu.VLoose.value, WorkingPointsTauVSJet.VVVLoose.value ],
}
threshold_baseline = {
    "eTau":
        {
        "electron":
            {
            "pT" : 20,
            "eta": 2.1,
            "dz" : 0.2,
            "dxy":0.045
            },
        "tau":
            {
            "dz" : 0.2,
            "pT" : 20,
            "eta": 2.3,

            }

        },
    "muTau":
        {
        "muon":
            {
            "pT" : 20,
            "eta": 2.1,
            "dz" : 0.2,
            "dxy":0.045
            },
        "tau":
            {
            "dz" : 0.2,
            "pT" : 20,
            "eta": 2.3,
            "isolation":0.15,
            "tkIso":0.1,
            }

        },
    "tauTau":
        {
        "tau":
            {
            "dz" : 0.2,
            "pT" : 20,
            "eta": 2.3,

            }

        },
}




def GetDaughters(df):
    df_daughters = df.Define("GenPart_daughters","GetDaughters(GenPart_genPartIdxMother )")
    return df_daughters

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
'''
def DefineLegP4(df, channel):
    df = df.Define('final_indices', 'RVecI final_indices; final_indices.push_back(BestHTTCand.leg_index.first); final_indices.push_back(BestHTTCand.leg_index.second); return final_indices;')
    for n in range(2):
        df = df.Define('leg{}_p4'.format(n+1), 'LorentzVectorM({0}_pt[final_indices[{1}]], {0}_eta[final_indices[{1}]],{0}_phi[final_indices[{1}]], {0}_mass[final_indices[{1}]])'.format(channelLegs[channel][n],n)).Define('genLep{}_p4'.format(n+1), ('HTT_Cand.leg_p4[{}]').format(n))
    df = df.Define(f"BestHTTCand", f"RecoHTTCand BestHTTCand; BestHTTCand.channel=BestHTTCand.channel; BestHTTCand.leg_index=BestHTTCand.leg_index; BestHTTCand.leg_p4[0]=leg1_p4;BestHTTCand.leg_p4[1]=leg2_p4; return BestHTTCand;")
    return df
'''


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

def DefineDataFrame(df, ch):
    df_channel=selectChannel(df,ch)
    df_acceptance = passAcceptance(df_channel)
    df_pairs = SelectBestPair(df_acceptance, ch)
    df_JetFiltered= JetSelection(df_pairs, ch)
    df_lepVeto = ThirdLeptonVeto(df_JetFiltered)
    #df_matched = GenMatching(df_lepVeto, ch)
    print(df_JetFiltered.Count().GetValue())
    return df_JetFiltered

    #return df_pairs
    #print(f"for the channel {ch}:\nthe number of initial events is {df_channel.Count().GetValue()}\nthe number of events that pass the acceptance is {df_acceptance.Count().GetValue()}\nthe number of events with final best pair with size 2 is {df_pairs.Count().GetValue()}\nafter applying the Jet filter {df_JetFiltered.Count().GetValue()}\nafter applying the third lepton veto {df_lepVeto.Count().GetValue()}\nafter the filter on gen-reco matching {df_matched.Count().GetValue()} \n\n")
