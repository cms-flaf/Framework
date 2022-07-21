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


RVecI  GenRecoJetMatching(const int event, const RVecLV& Jet_p4, const RVecLV& GenJet_p4, const RVecB& GenJet_ClosestToMPV,double dR_thr=0.2)
{
  const double dR2_thr = std::pow(dR_thr, 2); 
  RVecI RecoJetMatched (Jet_p4.size(),-1);
  std::vector<size_t> already_gen_counted;
  for(size_t gen_idx = 0; gen_idx < GenJet_p4.size(); ++gen_idx) { 
    /*if(event == 23022){
        std::cout << "gen_idx = " << gen_idx << std::endl;
        std::cout << "is closest to MPV ? " << GenJet_ClosestToMPV[gen_idx] << std::endl;
        bool already_found = (std::find(already_gen_counted.begin(),already_gen_counted.end(),gen_idx)!=already_gen_counted.end());
        std::cout << "is already counted ? " << already_found << std::endl;
    }*/
    if(GenJet_ClosestToMPV[gen_idx]==false) continue;
    if(gen_idx!=0 && std::find(already_gen_counted.begin(),already_gen_counted.end(),gen_idx)!=already_gen_counted.end()) continue;
    for(size_t reco_idx = 0; reco_idx < Jet_p4.size(); ++reco_idx) {  
        const double dR2 =  ROOT::Math::VectorUtil::DeltaR2(GenJet_p4[gen_idx], Jet_p4[reco_idx]);
        /*if(event == 23022){
            std::cout << "gen_idx = " << gen_idx << std::endl;
            std::cout << "reco_idx = " << reco_idx << std::endl;
            std::cout << "deltaR ? " << dR2 << std::endl;
            bool below_thr=dR2<dR2_thr;
            std::cout << "is dR2<dR2_thr?  " << below_thr << std::endl;
        }*/
        if(dR2 < dR2_thr){
          RecoJetMatched[reco_idx] = gen_idx;  
          already_gen_counted.push_back(gen_idx);
      } 
      //if(event == 23022){ std::cout << "\n"<< "reco_idx" << reco_idx << std::endl;}
      //if(event == 23022){ std::cout  << "RecoJetMatched " << RecoJetMatched[reco_idx].first  << std::endl;}
      //if(event == 23022){ std::cout << "RecoJetMatched " << RecoJetMatched[reco_idx].second << "\n" << std::endl;}
    
    }
  }
  return RecoJetMatched;
}

int AtLeastTwoCorrespondence(std::vector<std::pair<int, int>>& RecoGenMatched){
  int nMatched=0;
    for (size_t jet_idx =0 ; jet_idx<(RecoGenMatched.size()); jet_idx++){
      if(RecoGenMatched[jet_idx].first==1){
        nMatched++;
      }
    }
    return nMatched;
}

int map_acc(int lhs, const std::pair<int, int> & rhs)
{
  return lhs + rhs.first;
}


// /*
// bool JetLepSeparation(const LorentzVectorM& tau_p4, const RVecF& Jet_eta, const RVecF& Jet_phi, const RVecI & RecoJetIndices){
//   RVecI JetSeparatedWRTLeptons;
//   for (auto& jet_idx:RecoJetIndices){
//     float dR = DeltaR(static_cast<float>(tau_p4.Phi()), static_cast<float>(tau_p4.Eta()), Jet_phi[jet_idx], Jet_eta[jet_idx]);
//     //auto dR_2 = DeltaR(leg2_p4.Phi(), leg2_p4.Eta(), Jet_phi[jet_idx], Jet_eta[jet_idx]);
//     //if(dR_1>0.5 && dR_2>0.5){
//     if(dR>0.5){
//       JetSeparatedWRTLeptons.push_back(jet_idx);
//     }
//   }
//   return (JetSeparatedWRTLeptons.size()>=2);
// }*/

// bool ElectronVeto(const HTTCand& HTT_Cand, const RVecF& Electron_pt, const RVecF& Electron_dz, const RVecF& Electron_dxy, const RVecF& Electron_eta, const RVecB& Electron_mvaFall17V2Iso_WP90, const RVecB& Electron_mvaFall17V2noIso_WP90 , const RVecF&  Electron_pfRelIso03_all){
// int nElectrons =0 ;
// // do not consider signal electron
// int signalElectron_idx = -1;

// if(HTT_Cand.channel==Channel::eTau){
//   signalElectron_idx =  HTT_Cand.leg_index[0];
// }

//   for (size_t el_idx =0 ; el_idx < Electron_pt.size(); el_idx++){
//       if(Electron_pt.at(el_idx) >10 && std::abs(Electron_eta.at(el_idx)) < 2.5 && std::abs(Electron_dz.at(el_idx)) < 0.2 && std::abs(Electron_dxy.at(el_idx)) < 0.045 && ( Electron_mvaFall17V2Iso_WP90.at(el_idx) == true || ( Electron_mvaFall17V2noIso_WP90.at(el_idx) == true && Electron_pfRelIso03_all.at(el_idx)<0.3 )) ){
//           if(el_idx != signalElectron_idx){
//               nElectrons +=1 ;
//           }
//       }
//   }
//   if(nElectrons>=1){
//       return false;
//   }
//   return true;
// }

// bool MuonVeto(const HTTCand& HTT_Cand, const RVecF& Muon_pt, const RVecF& Muon_dz, const RVecF& Muon_dxy, const RVecF& Muon_eta, const RVecB& Muon_tightId, const RVecB& Muon_mediumId , const RVecF&  Muon_pfRelIso04_all){
// int nMuons =0 ;
// // do not consider signal muon
// int signalMuon_idx = -1;
// if(HTT_Cand.channel ==Channel::muTau){
//   signalMuon_idx =  HTT_Cand.leg_index[0];
// }
//   for (size_t mu_idx =0 ; mu_idx < Muon_pt.size(); mu_idx++){
//       if( Muon_pt.at(mu_idx) >10 && std::abs(Muon_eta.at(mu_idx)) < 2.4 && std::abs(Muon_dz.at(mu_idx)) < 0.2 && std::abs(Muon_dxy.at(mu_idx)) < 0.045 && ( Muon_mediumId.at(mu_idx) == true ||  Muon_tightId.at(mu_idx) == true ) && Muon_pfRelIso04_all.at(mu_idx)<0.3  ){
//           if(mu_idx != signalMuon_idx){
//               nMuons +=1 ;
//           }
//       }
//   }
//   if(nMuons>=1){
//       return false;
//   }
//   return true;
// }


// int JetFilter(HTTCand& HTT_Cand, ROOT::VecOps::RVec<LorentzVectorM> Jet_p4, int n_Jets=2){
//   int nJetsAdd=0;
//   for (int jet_idx =0 ; jet_idx < Jet_p4.size(); jet_idx++){
//         float dr1 = ROOT::Math::VectorUtil::DeltaR(HTT_Cand.leg_p4[0], Jet_p4[jet_idx]);
//         float dr2 = ROOT::Math::VectorUtil::DeltaR(HTT_Cand.leg_p4[1], Jet_p4[jet_idx]);
//         if(dr1 > 0.5 && dr2 >0.5 ){
//             nJetsAdd +=1;
//       }
//   }
//   if(nJetsAdd>=n_Jets){
//       return 1;
//   }
//   return 0;
// }
// RVecI FindRecoGenJetCorrespondence(const RVecI& RecoJet_genJetIdx, const RVecI& GenJet_taggedIndices){
//   RVecI RecoJetIndices;
//   for (int i =0 ; i<RecoJet_genJetIdx.size(); i++){
//     for(auto& genJet_idx : GenJet_taggedIndices ){
//       int recoJet_genJet_idx = RecoJet_genJetIdx[i];
//       if(recoJet_genJet_idx==genJet_idx){
//         RecoJetIndices.push_back(recoJet_genJet_idx);
//       }
//     }
//   }
//   return RecoJetIndices;
// }
