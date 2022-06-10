#pragma once
#include "AnalysisTools.h"
#include "HHCore.h"

RVecS RecoEleSelectedIndices(const RVecF& Electron_dz, const RVecF& Electron_dxy, const RVecF& Electron_eta, const RVecF& Electron_phi, const RVecF& Electron_pt, const RVecUC& Electron_mvaFall17V2Iso_WP80){
  RVecS Electron_indices;
  for(size_t i=0; i< Electron_pt.size(); i++ ){
      if(Electron_mvaFall17V2Iso_WP80[i]==1 && Electron_pt[i] > 20 && std::abs(Electron_eta[i])<2.3 && std::abs(Electron_dz[i])<0.2 &&  std::abs(Electron_dxy[i])<0.045 ){
          Electron_indices.push_back(i);
      }
  }
  return Electron_indices;
}

RVecS RecoMuSelectedIndices(const RVecF& Muon_dz,  const RVecF& Muon_dxy, const RVecF& Muon_eta, const RVecF& Muon_phi, const RVecF& Muon_pt, const RVecUC& Muon_tightId, const RVecUC& Muon_highPtId, const RVecF& Muon_tkRelIso,  const RVecF& isolation_variable_muon){
  RVecS Muon_indices;
  for(size_t i=0; i< Muon_pt.size(); i++ ){
      if(Muon_pt[i] > 20 && std::abs(Muon_eta[i])<2.3 && std::abs(Muon_dz[i])<0.2 &&  std::abs(Muon_dxy[i])<0.045  && ( ( Muon_tightId[i]==1  && isolation_variable_muon[i]<0.15) || (Muon_highPtId[i]==1 && Muon_tkRelIso[i]<0.1) ) ){
          Muon_indices.push_back(i);
      }
  }
  return Muon_indices;
}

RVecS RecoTauSelectedIndices(HTTCand& HTT_Cand, const RVecF& Tau_dz, const RVecF& Tau_eta, const RVecF& Tau_phi, const RVecF& Tau_pt, const RVecUC& Tau_idDeepTau2017v2p1VSjet, const RVecUC&  Tau_idDeepTau2017v2p1VSmu, const RVecUC& Tau_idDeepTau2017v2p1VSe, const RVecI& Tau_decayMode ){
  RVecS tau_indices;
  for(size_t i=0; i< Tau_dz.size(); i++ ){
      if(Tau_decayMode[i]!=0 && Tau_decayMode[i]!=1 && Tau_decayMode[i]!=10 && Tau_decayMode[i]!=11) continue;
      if(Tau_pt[i] > 20 && std::abs(Tau_eta[i])<2.3 && std::abs(Tau_dz[i])<0.2){
          if(HTT_Cand.GetChannel()== Channel::tauTau && ((Tau_idDeepTau2017v2p1VSjet[i])&(1<<4)) &&  ((Tau_idDeepTau2017v2p1VSmu[i])&(1<<0)) && ((Tau_idDeepTau2017v2p1VSe[i])&(1<<1)) ){
              tau_indices.push_back(i);
          }
          else if(HTT_Cand.GetChannel() != Channel::tauTau && ((Tau_idDeepTau2017v2p1VSjet[i])&(1<<4)) &&  ((Tau_idDeepTau2017v2p1VSmu[i])&(1<<3)) && ((Tau_idDeepTau2017v2p1VSe[i])&(1<<2))){
              tau_indices.push_back(i);
          }
      }
  }

  return tau_indices;

}



ROOT::VecOps::RVec<HTTCand> GetPotentialHTTCandidates(int channel, const RVecS &indices_leg1, const  RVecS &indices_leg2, const RVecF& Pt_leg1, const RVecF& Phi_leg1, const RVecF& Eta_leg1,const RVecF& Mass_leg1, const RVecS &charge_leg1, const RVecF& Pt_leg2, const RVecF& Phi_leg2, const RVecF& Eta_leg2,const RVecF& Mass_leg2, const RVecS &charge_leg2, float dR_thr = 0.5){
  ROOT::VecOps::RVec<HTTCand> HTTCandidates;
  for(size_t i=0; i< indices_leg1.size(); i++ ){
      for(size_t j=0; j< indices_leg2.size(); j++ ){
          size_t cand1 = indices_leg1.at(i);
          size_t cand2 = indices_leg2.at(j);
          LorentzVectorM vector_1 = LorentzVectorM(Pt_leg1[cand1], Eta_leg1[cand1],Phi_leg1[cand1], Mass_leg1[cand1]);
          LorentzVectorM vector_2 = LorentzVectorM(Pt_leg2[cand2], Eta_leg2[cand2],Phi_leg2[cand2], Mass_leg2[cand2]);
          float current_dR = ROOT::Math::VectorUtil::DeltaR(vector_1, vector_2);
          if(current_dR > dR_thr){
              HTTCand potential_HTTcanditate;
              potential_HTTcanditate.channel=channel;
              potential_HTTcanditate.leg_index[0]=cand1;
              potential_HTTcanditate.leg_index[1]=cand2;
              potential_HTTcanditate.leg_charge[0]=charge_leg1[cand1];
              potential_HTTcanditate.leg_charge[1]=charge_leg2[cand2];
              potential_HTTcanditate.leg_p4[0]=vector_1;
              potential_HTTcanditate.leg_p4[1]=vector_2;
              HTTCandidates.push_back(potential_HTTcanditate);
          }
      }
  }
  return HTTCandidates;
}

HTTCand GetBestHTTCand(const ROOT::VecOps::RVec<HTTCand>& HTTCandidates, int event, const RVecF& isolation_variable_first_cand, const RVecF& first_cand_pt, const RVecF& isolation_variable_second_cand, const RVecF& second_cand_pt){
  HTTCand BestHTTCand;

  const auto& Comparitor = [&](HTTCand HTT_Cand1, HTTCand HTT_Cand2) -> bool
  {

     if(HTT_Cand1.channel!=HTT_Cand2.channel){
        throw analysis::exception("error, different channels considered for HTT candiate choice!! %1% VS %2%") %HTT_Cand1.channel %HTT_Cand2.channel ;
     }
     const std::pair<int, int> pair_1 = HTT_Cand1.GetAsPair(HTT_Cand1.leg_index);
     const std::pair<int, int> pair_2 = HTT_Cand2.GetAsPair(HTT_Cand2.leg_index);
     if(pair_1 == pair_2) return false;
     if(HTT_Cand1.channel!=Channel::tauTau){
       // First prefer the pair with the most isolated candidate 1 (electron for eTau, muon for muTau)

       const size_t first_cand_pair_1 = pair_1.first ;
       const size_t first_cand_pair_2 = pair_2.first ;
       if(first_cand_pair_1 != first_cand_pair_2) {
           auto iso_first_cand_pair_1 = isolation_variable_first_cand.at(first_cand_pair_1);
           auto iso_first_cand_pair_2 = isolation_variable_first_cand.at(first_cand_pair_2);
           int iso_cmp;
           if(iso_first_cand_pair_1 == iso_first_cand_pair_2){ iso_cmp= 0;}
           else {iso_cmp =  iso_first_cand_pair_1 > iso_first_cand_pair_2 ? 1 : -1; }
           if(iso_cmp != 0) {return iso_cmp == 1;}
           // If the isolation of candidate 1 is the same in both pairs, prefer the pair with the highest candidate 1 pt (for cases of genuinely the same isolation value but different possible candidate 1).
           if(first_cand_pt.at(first_cand_pair_1) != first_cand_pt.at(first_cand_pair_2)){
               return first_cand_pt.at(first_cand_pair_1) > first_cand_pt.at(first_cand_pair_2);
           }
           else{
             const size_t second_cand_pair_1 = pair_1.second ;
             const size_t second_cand_pair_2 = pair_2.second ;
             if(second_cand_pair_1 != second_cand_pair_2) {
                 auto iso_second_cand_pair_1 = isolation_variable_second_cand.at(second_cand_pair_1);
                 auto iso_second_cand_pair_2 = isolation_variable_second_cand.at(second_cand_pair_2);
                 int iso_cmp;
                 if(iso_second_cand_pair_1 == iso_second_cand_pair_2){ iso_cmp= 0;}
                 else {iso_cmp =  iso_second_cand_pair_1 > iso_second_cand_pair_2 ? 1 : -1; }
                 if(iso_cmp != 0) {return iso_cmp == 1;}
                 //If the isolation of candidate 2 is the same, prefer the pair with highest candidate 2 pt (for cases of genuinely the same isolation value but different possible candidate 2).
                 if(second_cand_pt.at(second_cand_pair_1) != second_cand_pt.at(second_cand_pair_2)){
                     return second_cand_pt.at(second_cand_pair_1) > second_cand_pt.at(second_cand_pair_2);
                 } // closes if on tau pts
             } // closes if on tau indices
           } // closes else

       } // closes if
       // If the pt of candidate 1 in both pairs is the same (likely because it's the same object) then prefer the pair with the most isolated candidate 2 (tau for eTau and muTau).
       else{
         const size_t second_cand_pair_1 = pair_1.second ;
         const size_t second_cand_pair_2 = pair_2.second ;
         if(second_cand_pair_1 != second_cand_pair_2) {
             auto iso_second_cand_pair_1 = isolation_variable_second_cand.at(second_cand_pair_1);
             auto iso_second_cand_pair_2 = isolation_variable_second_cand.at(second_cand_pair_2);
             int iso_cmp;
             if(iso_second_cand_pair_1 == iso_second_cand_pair_2){ iso_cmp= 0;}
             else {iso_cmp =  iso_second_cand_pair_1 > iso_second_cand_pair_2 ? 1 : -1; }
             if(iso_cmp != 0) {return iso_cmp == 1;}
             //If the isolation of candidate 2 is the same, prefer the pair with highest candidate 2 pt (for cases of genuinely the same isolation value but different possible candidate 2).
             if(second_cand_pt.at(second_cand_pair_1) != second_cand_pt.at(second_cand_pair_2)){
                 return second_cand_pt.at(second_cand_pair_1) > second_cand_pt.at(second_cand_pair_2);
             } // closes if on tau pts
         } // closes if on tau indices
       } // closes else
   }
     else{
       for(size_t leg_id = 0; leg_id < 2; ++leg_id) {
           const size_t h1_leg_id = leg_id == 0 ? pair_1.first : pair_1.second;
           const size_t h2_leg_id = leg_id == 0 ? pair_2.first : pair_2.second;

           if(h1_leg_id != h2_leg_id) {
               // per ora lo faccio solo per i tau ma poi va aggiustato!!
               auto iso_cand1_pair_1 = isolation_variable_second_cand.at(h1_leg_id);
               auto iso_cand1_pair_2 = isolation_variable_second_cand.at(h2_leg_id);
               int iso_cmp;
               if(iso_cand1_pair_1 == iso_cand1_pair_2){ iso_cmp= 0;}
               else {iso_cmp =  iso_cand1_pair_1 > iso_cand1_pair_2 ? 1 : -1; }
               if(iso_cmp != 0) return iso_cmp == 1;

               if(first_cand_pt.at(h1_leg_id) != second_cand_pt.at(h2_leg_id))
                   return first_cand_pt.at(h1_leg_id) > second_cand_pt.at(h2_leg_id);
           }
       }
     }
     //std::cout << event << std::endl;
     throw analysis::exception("not found a good criteria for best tau pair");
     };

   if(!HTTCandidates.empty()){
         BestHTTCand= *std::min_element(HTTCandidates.begin(), HTTCandidates.end(), Comparitor);
         std::cout<<typeid(BestHTTCand.leg_p4[0]).name()<<std::endl;
         //if(first_cand_charge[best_pair.first]!= second_cand_charge[best_pair.second]){
         //const std::pair<int, int> best_pair = HTT_Cand1.leg_index;
         //const std::pair<int, int> pair_2 = HTT_Cand2.leg_index;
         //BestHTTCand.push_back(best_pair.first);
         //BestHTTCand.push_back(best_pair.second);
         //}
     }
   return BestHTTCand;

}


bool GenRecoMatching( const HTTCand& HTT_Cand, const HTTCand RecoHTT_Cand, const float dr_thr=0.2 ){
  float dR_00 = ROOT::Math::VectorUtil::DeltaR(HTT_Cand.leg_p4[0], RecoHTT_Cand.leg_p4[0]);
  float dR_11 = ROOT::Math::VectorUtil::DeltaR(HTT_Cand.leg_p4[1], RecoHTT_Cand.leg_p4[1]);
  float dR_10 = 0 ;
  float dR_01 = 0;
  if(HTT_Cand.GetChannel()==Channel::tauTau){
    dR_10 = ROOT::Math::VectorUtil::DeltaR(HTT_Cand.leg_p4[1], RecoHTT_Cand.leg_p4[0]);
    dR_01 = ROOT::Math::VectorUtil::DeltaR(HTT_Cand.leg_p4[0], RecoHTT_Cand.leg_p4[1]);
  }
    return (dR_00 < dr_thr && dR_01 < dr_thr && dR_10 < dr_thr && dR_11 < dr_thr);
}
/*
bool JetLepSeparation(const LorentzVectorM& tau_p4, const RVecF& Jet_eta, const RVecF& Jet_phi, const RVecI & RecoJetIndices){
  RVecI JetSeparatedWRTLeptons;
  for (auto& jet_idx:RecoJetIndices){
    float dR = DeltaR(static_cast<float>(tau_p4.Phi()), static_cast<float>(tau_p4.Eta()), Jet_phi[jet_idx], Jet_eta[jet_idx]);
    //auto dR_2 = DeltaR(leg2_p4.Phi(), leg2_p4.Eta(), Jet_phi[jet_idx], Jet_eta[jet_idx]);
    //if(dR_1>0.5 && dR_2>0.5){
    if(dR>0.5){
      JetSeparatedWRTLeptons.push_back(jet_idx);
    }
  }
  return (JetSeparatedWRTLeptons.size()>=2);
}*/

bool ElectronVeto(HTTCand& HTT_Cand, const RVecF& Electron_pt, const RVecF& Electron_dz, const RVecF& Electron_dxy, const RVecF& Electron_eta, const RVecB& Electron_mvaFall17V2Iso_WP90, const RVecB& Electron_mvaFall17V2noIso_WP90 , const RVecF&  Electron_pfRelIso03_all){
int nElectrons =0 ;
// do not consider signal electron
int signalElectron_idx = -1;
if(HTT_Cand.GetChannel()==Channel::eTau){
  signalElectron_idx =  HTT_Cand.leg_index[0];
}

  for (size_t el_idx =0 ; el_idx < Electron_pt.size(); el_idx++){
      if(Electron_pt.at(el_idx) >10 && std::abs(Electron_eta.at(el_idx)) < 2.5 && std::abs(Electron_dz.at(el_idx)) < 0.2 && std::abs(Electron_dxy.at(el_idx)) < 0.045 && ( Electron_mvaFall17V2Iso_WP90.at(el_idx) == true || ( Electron_mvaFall17V2noIso_WP90.at(el_idx) == true && Electron_pfRelIso03_all.at(el_idx)<0.3 )) ){
          if(el_idx != signalElectron_idx){
              nElectrons +=1 ;
          }
      }
  }
  if(nElectrons>=1){
      return false;
  }
  return true;
}

bool MuonVeto(HTTCand& HTT_Cand, const RVecF& Muon_pt, const RVecF& Muon_dz, const RVecF& Muon_dxy, const RVecF& Muon_eta, const RVecB& Muon_tightId, const RVecB& Muon_mediumId , const RVecF&  Muon_pfRelIso04_all){
int nMuons =0 ;
// do not consider signal muon
int signalMuon_idx = -1;
if(HTT_Cand.GetChannel()==Channel::muTau){
  signalMuon_idx =  HTT_Cand.leg_index[0];
}
  for (size_t mu_idx =0 ; mu_idx < Muon_pt.size(); mu_idx++){
      if( Muon_pt.at(mu_idx) >10 && std::abs(Muon_eta.at(mu_idx)) < 2.4 && std::abs(Muon_dz.at(mu_idx)) < 0.2 && std::abs(Muon_dxy.at(mu_idx)) < 0.045 && ( Muon_mediumId.at(mu_idx) == true ||  Muon_tightId.at(mu_idx) == true ) && Muon_pfRelIso04_all.at(mu_idx)<0.3  ){
          if(mu_idx != signalMuon_idx){
              nMuons +=1 ;
          }
      }
  }
  if(nMuons>=1){
      return false;
  }
  return true;
}

int JetFilter(HTTCand& HTT_Cand, int nJets=2){

  if(HTT_Cand.GetChannel()==Channel::tauTau)
    return 0;
  return 1;
}
/*
int JetFilter(int n_Jets=2){
  int nJetsAdd=0;
  for (int jet_idx =0 ; jet_idx < Jet_p4.size(); jet_idx++){

        float dr1 = ROOT::Math::VectorUtil::DeltaR(HTT_Cand.leg_p4[0], Jet_p4[jet_idx]);
        float dr2 = ROOT::Math::VectorUtil::DeltaR(HTT_Cand.leg_p4[1], Jet_p4[jet_idx]);
        if(dr1 > 0.5 && dr2 >0.5 ){
            nJetsAdd +=1;
      }
  }
  //int n_Jets = 2;
  //if(HTT_Cand.GetChannel()!=Channel::tauTau){
  //     n_Jets = 1;
  //}
  if(nJetsAdd>=n_Jets){
      return 1;
  }
  return 0;
}*/
/*
int AK8JetFilter(HTTCand& HTT_Cand, const RVecF&  FatJet_pt, const RVecF&  FatJet_eta, const RVecF&  FatJet_phi, const RVecF& FatJet_msoftdrop, const int& is2017){
int nFatJetsAdd=0;
for (size_t fatjet_idx =0 ; fatjet_idx < FatJet_pt.size(); fatjet_idx++){
    if(FatJet_msoftdrop.at(fatjet_idx)>30 && std::abs(FatJet_eta.at(fatjet_idx)) < 2.5) {
        float dr1 = DeltaR(static_cast<float>(HTT_cand.leg_p4[0].phi()), static_cast<float>(HTT_cand.leg_p4[0].eta()),FatJet_phi[fatjet_idx], FatJet_eta[fatjet_idx]);
        float dr2 = DeltaR(static_cast<float>(leg2_p4.phi()), static_cast<float>(leg2_p4.eta()),FatJet_phi[fatjet_idx], FatJet_eta[fatjet_idx]);
        if(dr1 > 0.5 && dr2 >0.5 ){
            nFatJetsAdd +=1;
          }
    }
}
//std::cout << nFatJetsAdd << "\t" <<nJetsAdd << std::endl;
int n_FatJets = 1 ;
//if(HTT_Cand.GetChannel()!=Channel::tauTau){
//     n_Jets = 1;
//}
if(nFatJetsAdd>=n_FatJets ){
    return 1;
}
return 0;
}
*/
RVecI FindRecoGenJetCorrespondence(const RVecI& RecoJet_genJetIdx, const RVecI& GenJet_taggedIndices){
  RVecI RecoJetIndices;
  for (int i =0 ; i<RecoJet_genJetIdx.size(); i++){
    for(auto& genJet_idx : GenJet_taggedIndices ){
      int recoJet_genJet_idx = RecoJet_genJetIdx[i];
      if(recoJet_genJet_idx==genJet_idx){
        RecoJetIndices.push_back(recoJet_genJet_idx);
      }
    }
  }
  return RecoJetIndices;
}
