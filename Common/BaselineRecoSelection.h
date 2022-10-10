#pragma once
#include "AnalysisTools.h"
#include "HHCore.h"

ROOT::VecOps::RVec<HTTCand> GetHTTCandidates(Channel channel, double dR_thr,
                                             const RVecB& leg1_sel, const RVecLV& leg1_p4,
                                             const RVecF& leg1_rawIso, const RVecI& leg1_charge,
                                             const RVecB& leg2_sel, const RVecLV& leg2_p4,
                                             const RVecF& leg2_rawIso, const RVecI& leg2_charge)
{
  const double dR2_thr = std::pow(dR_thr, 2);
  ROOT::VecOps::RVec<HTTCand> httCands;
  const auto [leg1_type, leg2_type] = ChannelToLegs(channel);
  for(size_t leg1_idx = 0; leg1_idx < leg1_sel.size(); ++leg1_idx) {
    if(!leg1_sel[leg1_idx]) continue;
    for(size_t leg2_idx = 0; leg2_idx < leg2_sel.size(); ++leg2_idx) {
      if(!(leg2_sel[leg1_idx] && (leg1_type != leg2_type || leg1_idx != leg2_idx))) continue;
      const double dR2 = ROOT::Math::VectorUtil::DeltaR2(leg1_p4.at(leg1_idx), leg2_p4.at(leg2_idx));
      if(dR2 > dR2_thr) {
        HTTCand cand;
        cand.leg_type[0] = leg1_type;
        cand.leg_type[1] = leg2_type;
        cand.leg_index[0] = leg1_idx;
        cand.leg_index[1] = leg2_idx;
        cand.leg_p4[0] = leg1_p4.at(leg1_idx);
        cand.leg_p4[1] = leg2_p4.at(leg2_idx);
        cand.leg_charge[0] = leg1_charge.at(leg1_idx);
        cand.leg_charge[1] = leg2_charge.at(leg2_idx);
        cand.leg_rawIso[0] = leg1_rawIso.at(leg1_idx);
        cand.leg_rawIso[1] = leg2_rawIso.at(leg2_idx);
        httCands.push_back(cand);
      }
    }
  }

  return httCands;
}

HTTCand GetBestHTTCandidate(const std::vector<const ROOT::VecOps::RVec<HTTCand>*> httCands)
{
  const auto& comparitor = [&](const HTTCand& cand1, const HTTCand& cand2) -> bool {
    if(cand1 == cand2) return false;
    if(cand1.channel() != cand1.channel()) {
      throw analysis::exception("ERROR: different channels considered for HTT candiate choice!! %1% VS %2%")
      % static_cast<int>(cand1.channel()) % static_cast<int>(cand2.channel());
    }
    for(size_t idx = 0; idx < cand1.leg_index.size(); ++idx) {
      if(cand1.leg_rawIso[idx] != cand2.leg_rawIso[idx]) return cand1.leg_rawIso[idx] < cand2.leg_rawIso[idx];
      if(cand1.leg_p4[idx].pt() != cand2.leg_p4[idx].pt()) return cand1.leg_p4[idx].pt() > cand2.leg_p4[idx].pt();
    }
    throw analysis::exception("ERROR: criteria for best tau pair selection is not found.");
  };

  for(auto cands : httCands) {
    if(!cands->empty())
      return *std::min_element(cands->begin(), cands->end(), comparitor);
  }

  throw analysis::exception("ERROR: no siutable HTT candidate");
}

bool GenRecoMatching(const HTTCand& genHttCand, const HTTCand& recoHttCand, double dR_thr)
{
  const double dR2_thr = std::pow(dR_thr, 2);
  std::vector<bool> matching(HTTCand::n_legs * 2, false);
  for(size_t gen_idx = 0; gen_idx < HTTCand::n_legs; ++gen_idx) {
    for(size_t reco_idx = 0; reco_idx < HTTCand::n_legs; ++reco_idx) {
      if(genHttCand.leg_type[gen_idx] == recoHttCand.leg_type[reco_idx]
          && genHttCand.leg_charge[gen_idx] == recoHttCand.leg_charge[reco_idx]) {
        const double dR2 =  ROOT::Math::VectorUtil::DeltaR2(genHttCand.leg_p4[gen_idx], recoHttCand.leg_p4[reco_idx]);
        if(dR2 < dR2_thr)
          matching[gen_idx * HTTCand::n_legs + reco_idx] = true;
      }
    }
  }
  return (matching[0] && matching[3]) || (matching[1] && matching[2]);
}
 
GenLeptonMatch GenRecoLepMatching(const LorentzVectorM& tau_p4,
                          const RVecI& GenPart_pdgId, const RVecVecI& GenPart_daughters,
                          const RVecF& GenPart_pt, const RVecF& GenPart_eta, const RVecF& GenPart_phi,
                          const RVecF& GenPart_mass, const RVecI& GenPart_statusFlags)
{ 
   
  const float deltaR_thr= 0.2;
  float deltaR_min = 100;
  
  for(int genIdx =0; genIdx<GenPart_pt.size(); genIdx++){
    const GenStatusFlags status(GenPart_statusFlags.at(genIdx));
    if( ! status.isLastCopy()) { continue;}
    
    const LorentzVectorM genLep_p4 = GetVisibleP4(genIdx, GenPart_pdgId, GenPart_daughters,\
                                                GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass);
    auto dR_tauGenPart= ROOT::Math::VectorUtil::DeltaR(tau_p4, genLep_p4);
    if ( dR_tauGenPart > deltaR_thr ){continue;}
    if ( dR_tauGenPart > deltaR_min ) {continue;}
    else { deltaR_min = dR_tauGenPart ; }

    if(std::abs(GenPart_pdgId.at(genIdx))==11) { //electron
      if(genLep_p4.Pt()<8){
        continue;
      }
      return GenLeptonMatch::Electron;
    }
     if(std::abs(GenPart_pdgId.at(genIdx))==13) { //muon
      if(genLep_p4.Pt()<8){
        continue;
      }
      return GenLeptonMatch::Muon;
    }
    if(std::abs(GenPart_pdgId.at(genIdx))==15) { // tau --> need to check what are doughters isDirectPromptTauDecayProduct
        if(genLep_p4.Pt()<15){
          continue;
        }
        int nElectron =0 ;
        int nMuon = 0 ; 
        int nNeutrinos = 0; 
        int nChHad =0; 
        int nNeutHad =0 ;
        for(int daughterIdx = 0 ; daughterIdx<GenPart_daughters[genIdx].size(); daughterIdx++){
          auto daughter_pdgID = std::abs(GenPart_pdgId[GenPart_daughters[genIdx][daughterIdx]]);
          if(daughter_pdgID==11) {
            nElectron+=1;
          }
          else if(daughter_pdgID==12 || daughter_pdgID==14 || daughter_pdgID==16 ) {
            nNeutrinos+=1;
          }
          else if(daughter_pdgID==13){
            nMuon+=1;
          }
          else if(daughter_pdgID==211 || daughter_pdgID==321){
            nChHad+=1;
          }
          else if(daughter_pdgID==111 || daughter_pdgID==311){
            nNeutHad+=1;
          }
        }
        if(nChHad>=1 || nNeutHad>=1){
          return GenLeptonMatch::Tau;
        }
        if(genLep_p4.Pt()<8){
          continue;
        }
        if(nNeutrinos==2 && nElectron==1){
          return GenLeptonMatch::TauElectron;
        }
        if(nNeutrinos==2 && nMuon==1){
          return GenLeptonMatch::TauMuon;
        } 
      } 
    }
  return GenLeptonMatch::NoMatch;
}

std::pair<int, Leg> RecoTauMatching(const LorentzVectorM& tau_p4, const RVecLV& Jet_p4,const RVecLV& Electron_p4, const RVecLV& Muon_p4 ){
   const float deltaR_thr= 0.5;
  float deltaR_min = 100;
  
  int current_idx = -1;
  Leg current_leg = Leg::jet;

  for(int jetIdx =0; jetIdx<Jet_p4.size(); jetIdx++){  
    auto dR_tauJet= ROOT::Math::VectorUtil::DeltaR(tau_p4, Jet_p4.at(jetIdx));
    if ( dR_tauJet > deltaR_thr ){continue;}
    if ( dR_tauJet > deltaR_min ) {continue;}
    else { 
      deltaR_min = dR_tauJet ; 
      current_idx = jetIdx;
    }
  }
  for(int muonIdx =0; muonIdx<Muon_p4.size(); muonIdx++){  
    auto dR_tauMu= ROOT::Math::VectorUtil::DeltaR(tau_p4, Muon_p4.at(muonIdx));
    if ( dR_tauMu > deltaR_thr ){continue;}
    if ( dR_tauMu > deltaR_min ) {continue;}
    else { 
      deltaR_min = dR_tauMu ; 
      current_idx = muonIdx;
      current_leg = Leg::mu;
    }
  }
  for(int electronIdx =0; electronIdx<Electron_p4.size(); electronIdx++){  
    auto dR_tauEle= ROOT::Math::VectorUtil::DeltaR(tau_p4, Electron_p4.at(electronIdx));
    if ( dR_tauEle > deltaR_thr ){continue;}
    if ( dR_tauEle > deltaR_min ) {continue;}
    else { 
      deltaR_min = dR_tauEle ; 
      current_idx = electronIdx;
      current_leg = Leg::e;
    }
  }
  return std::make_pair(current_idx, current_leg);
  
}

RVecI GenRecoJetMatching(int event,const RVecI& Jet_idx, const RVecI& GenJet_idx,  const RVecB& Jet_sel, const RVecB& GenJet_sel,   const RVecLV& GenJet_p4, const RVecLV& Jet_p4 , float DeltaR_thr)
{
  RVecI recoJetMatched (Jet_idx.size(), -1);
  std::set<size_t> taken_jets;
  for(size_t gen_idx = 0; gen_idx < GenJet_p4.size(); ++gen_idx) {
    if(GenJet_sel[gen_idx]!=1) continue;
    size_t best_jet_idx = Jet_p4.size();
    float deltaR_min = std::numeric_limits<float>::infinity(); 
    for(size_t reco_idx = 0; reco_idx < Jet_p4.size(); ++reco_idx) {
      if(Jet_sel[reco_idx]!=1 || taken_jets.count(reco_idx)) continue;
      auto deltaR = ROOT::Math::VectorUtil::DeltaR(Jet_p4[reco_idx], GenJet_p4[gen_idx]);
      if(deltaR<deltaR_min && deltaR<DeltaR_thr){
        best_jet_idx = reco_idx;
        deltaR_min=deltaR;
      }
    }
    if(best_jet_idx<Jet_p4.size()) {
      taken_jets.insert(best_jet_idx);
      recoJetMatched.at(best_jet_idx) = gen_idx;
    }
  }
  return recoJetMatched;
}


HbbCand GetHbbCandidate(const RVecF& HHbTagScores, const RVecB& JetSel,  const RVecLV& Jet_p4, const RVecI& Jet_idx)
{
  RVecI JetIdxOrdered = ReorderObjects(HHbTagScores, Jet_idx);
  HbbCand HbbCandidate; 
  
  int leg_idx = 0;
  for(int i=0; i<JetIdxOrdered.size(); i++){
    auto jet_idx = JetIdxOrdered[i];  
    if(!JetSel[jet_idx]) continue;
    HbbCandidate.leg_index[leg_idx] =  jet_idx;
    HbbCandidate.leg_p4[leg_idx] = Jet_p4.at(jet_idx);
    leg_idx++;
    if(leg_idx == HbbCandidate.n_legs) break;
  } 
  
  return HbbCandidate;
}