#pragma once
#include "AnalysisTools.h"
#include "HHCore.h"

ROOT::VecOps::RVec<HTTCand> GetHTTCandidates(Channel channel, double dR_thr,
                                             const RVecB& leg1_sel, const RVecLV& leg1_p4,
                                             const RVecF& leg1_rawIso, const RVecI& leg1_charge,
                                             const RVecI& leg1_genMatchIdx,
                                             const RVecB& leg2_sel, const RVecLV& leg2_p4,
                                             const RVecF& leg2_rawIso, const RVecI& leg2_charge,
                                             const RVecI& leg2_genMatchIdx)
{
  const double dR2_thr = std::pow(dR_thr, 2);
  ROOT::VecOps::RVec<HTTCand> httCands;
  const auto [leg1_type, leg2_type] = ChannelToLegs(channel);
  for(size_t leg1_idx = 0; leg1_idx < leg1_sel.size(); ++leg1_idx) {
    if(!leg1_sel[leg1_idx]) continue;
    for(size_t leg2_idx = 0; leg2_idx < leg2_sel.size(); ++leg2_idx) {
      if(!(leg2_sel[leg2_idx] && (leg1_type != leg2_type || leg1_idx != leg2_idx))) continue;
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
        cand.leg_genMatchIdx[0] = leg1_genMatchIdx.at(leg1_idx);
        cand.leg_genMatchIdx[1] = leg2_genMatchIdx.at(leg2_idx);
        httCands.push_back(cand);
      }
    }
  }

  return httCands;
}

HTTCand GetBestHTTCandidate(const std::vector<const ROOT::VecOps::RVec<HTTCand>*> httCands, unsigned long long event)
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
    throw analysis::exception("ERROR: criteria for best tau pair selection is not found in channel %1% and event %2%" )
    % static_cast<int>(cand1.channel()) % event ;
  };

  for(auto cands : httCands) {
    if(!cands->empty())
      return *std::min_element(cands->begin(), cands->end(), comparitor);
  }

  throw analysis::exception("ERROR: no siutable HTT candidate ");
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
  for(int i=0; i<Jet_idx.size(); i++){
    int jet_idx = JetIdxOrdered[i];
    if(!JetSel[jet_idx]) continue;
    HbbCandidate.leg_index[leg_idx] =  jet_idx;
    HbbCandidate.leg_p4[leg_idx] = Jet_p4.at(jet_idx);
    leg_idx++;
    if(leg_idx == HbbCandidate.n_legs) break;
  }

  return HbbCandidate;
}

RVecB FindMatching(const RVecB& pre_sel_offline, const RVecB& pre_sel_online, const RVecF& TrigObj_eta,
    const RVecF& TrigObj_phi, const RVecF& offlineObj_eta, const RVecF& offlineObj_phi, const float dR_thr)
    {
        RVecB findMatching(pre_sel_offline.size(), false);
        for(size_t online_idx = 0 ; online_idx < pre_sel_online.size() ; online_idx ++ ){
            if(pre_sel_online[online_idx]==0) continue;
            float dR_min = dR_thr;
            for(size_t offline_idx = 0 ; offline_idx < pre_sel_offline.size() ; offline_idx ++ ){
                if(pre_sel_offline[offline_idx]==0) continue;
                auto dR_current = DeltaR( TrigObj_eta[online_idx], TrigObj_phi[online_idx],  offlineObj_eta[offline_idx], offlineObj_phi[offline_idx]);
                if(dR_current < dR_min ){
                    dR_min = dR_current;
                    findMatching[offline_idx] = true;
                }
            }
        }
        return findMatching;
    }


  bool HasHttMatching(const HTTCand& httCand, const std::vector<std::pair<Leg, RVecB>> legVector ){
    RVecI already_considered_indices;
    RVecB hasHttMatchingVector(legVector.size(), false);
    for(size_t legHtt_idx = 0; legHtt_idx < HTTCand::n_legs; legHtt_idx++){
      if(std::find(already_considered_indices.begin(), already_considered_indices.end(), legHtt_idx)!=already_considered_indices.end()) continue;
      for(size_t leg_idx=0; leg_idx<legVector.size(); leg_idx++){
        std::pair<Leg, RVecB> leg = legVector[leg_idx];
          for(size_t obj_idx=0; obj_idx<leg.second.size(); obj_idx++){
            if(leg.second[httCand.leg_index[legHtt_idx]]!=0) {
              already_considered_indices.push_back(legHtt_idx);
              hasHttMatchingVector[leg_idx]=true;
            }
          }
      }
    }
    bool hasHttMatching = true;
    for (const auto & match : hasHttMatchingVector){
      hasHttMatching = hasHttMatching && match;
    }
    return hasHttMatching;
  }