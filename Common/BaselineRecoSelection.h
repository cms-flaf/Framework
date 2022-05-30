#include "AnalysisTools.h"
#pragma once


vec_s RecoEleSelectedIndices(int event, const vec_f& Electron_dz, const vec_f& Electron_dxy, const vec_f& Electron_eta, const vec_f& Electron_phi, const vec_f& Electron_pt, const vec_uc& Electron_mvaFall17V2Iso_WP80){
  vec_s Electron_indices;
  for(size_t i=0; i< Electron_pt.size(); i++ ){
      if(Electron_mvaFall17V2Iso_WP80[i]==1 && Electron_pt[i] > 20 && std::abs(Electron_eta[i])<2.3 && std::abs(Electron_dz[i])<0.2 &&  std::abs(Electron_dxy[i])<0.045 ){
          Electron_indices.push_back(i);
      }
  }
  return Electron_indices;
}

vec_s RecoMuSelectedIndices(int event, const vec_f& Muon_dz,  const vec_f& Muon_dxy, const vec_f& Muon_eta, const vec_f& Muon_phi, const vec_f& Muon_pt, const vec_uc& Muon_tightId, const vec_uc& Muon_highPtId, const vec_f& Muon_tkRelIso,  const vec_f& isolation_variable_muon){
  vec_s Muon_indices;
  for(size_t i=0; i< Muon_pt.size(); i++ ){
      //std::cout << Muon_pt[i] <<"\t"<< Muon_tightId[i] <<"\t"<< Muon_eta[i] <<"\t"<< Muon_dz[i] <<"\t"<< Muon_dxy[i]<<"\t"<< isolation_variable_muon[i] << std::endl;
      if(Muon_pt[i] > 20 && std::abs(Muon_eta[i])<2.3 && std::abs(Muon_dz[i])<0.2 &&  std::abs(Muon_dxy[i])<0.045  && ( ( Muon_tightId[i]==1  && isolation_variable_muon[i]<0.15) || (Muon_highPtId[i]==1 && Muon_tkRelIso[i]<0.1) ) ){
          Muon_indices.push_back(i);
      }
  }
  return Muon_indices;
}

vec_s RecoTauSelectedIndices(int event, EvtInfo& evt_info, const vec_f& Tau_dz, const vec_f& Tau_eta, const vec_f& Tau_phi, const vec_f& Tau_pt, const vec_uc& Tau_idDeepTau2017v2p1VSjet, const vec_uc&  Tau_idDeepTau2017v2p1VSmu, const vec_uc& Tau_idDeepTau2017v2p1VSe, const vec_i& Tau_decayMode ){
  vec_s tau_indices;
  for(size_t i=0; i< Tau_dz.size(); i++ ){
      if(Tau_decayMode[i]!=0 && Tau_decayMode[i]!=1 && Tau_decayMode[i]!=10 && Tau_decayMode[i]!=11) continue;
      if(Tau_pt[i] > 20 && std::abs(Tau_eta[i])<2.3 && std::abs(Tau_dz[i])<0.2){
          if(evt_info.channel == Channel::tauTau && ((Tau_idDeepTau2017v2p1VSjet[i])&(1<<4)) &&  ((Tau_idDeepTau2017v2p1VSmu[i])&(1<<0)) && ((Tau_idDeepTau2017v2p1VSe[i])&(1<<1)) ){
              tau_indices.push_back(i);
          }
          else if(evt_info.channel != Channel::tauTau && ((Tau_idDeepTau2017v2p1VSjet[i])&(1<<4)) &&  ((Tau_idDeepTau2017v2p1VSmu[i])&(1<<3)) && ((Tau_idDeepTau2017v2p1VSe[i])&(1<<2))){
              tau_indices.push_back(i);
          }
      }
  }

  return tau_indices;

}


std::vector<std::pair<size_t, size_t>> GetPairs(const vec_s &indices_leg1,const  vec_s &indices_leg2, const vec_f& Phi_leg1, const vec_f& Eta_leg1, const vec_f& Phi_leg2, const vec_f& Eta_leg2){
  std::vector<std::pair<size_t, size_t>> pairs;
  for(size_t i=0; i< indices_leg1.size(); i++ ){
      for(size_t j=0; j< indices_leg2.size(); j++ ){
          size_t cand1 = indices_leg1.at(i);
          size_t cand2 = indices_leg2.at(j);
          float current_dR = DeltaR(Phi_leg1[cand1], Eta_leg1[cand1],Phi_leg2[cand2], Eta_leg2[cand2]);
          //std::cout<<"current_dR = " << current_dR << std::endl;
          if(current_dR > 0.5){
              pairs.push_back(std::make_pair(cand1, cand2));
          }
      }
  }
  return pairs;
}

vec_s GetFinalIndices(const std::vector<std::pair<size_t, size_t>>& pairs, int event, const vec_f& isolation_variable_first_cand, const vec_f& first_cand_pt, const vec_f& isolation_variable_second_cand, const vec_f& second_cand_pt, const vec_i& first_cand_charge,const vec_i& second_cand_charge){
  vec_s final_indices;

  const auto Comparitor = [&](std::pair<size_t, size_t> pair_1, std::pair<size_t, size_t> pair_2) -> bool
  {
     if(pair_1 == pair_2) return false;
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
     std::cout << event << std::endl;
     throw std::runtime_error("not found a good criteria for best tau pair");
     };

   if(!pairs.empty()){
    // std::cout << "found " << pairs.size() << "pairs in the event " << event << std::endl;
         const auto best_pair = *std::min_element(pairs.begin(), pairs.end(), Comparitor);
         if(first_cand_charge[best_pair.first]!= second_cand_charge[best_pair.second]){
             final_indices.push_back(best_pair.first);
             //std::cout << "Electron has charge " << Electron_charge[best_pair.first] << " and index " << best_pair.first << std::endl;
             final_indices.push_back(best_pair.second);
             //std::cout << "Tau has charge " << Tau_charge[best_pair.second] << " and index " << best_pair.second << std::endl;
         }
     }
   //std::cout << "final indices size == " << final_indices.size() << std::endl;
   return final_indices;

}


vec_s GetFinalIndices_tauTau(const std::vector<std::pair<size_t, size_t>>& pairs, int event, const vec_f& isolation_variable_tau,const vec_f& Tau_pt, const vec_i& Tau_charge){
  vec_s final_indices;
  // define comparitor for tauTau
  //std::cout << "size tau pairs = " << pairs.size()<< std::endl;

  const auto Comparitor = [&](std::pair<size_t, size_t> pair_1, std::pair<size_t, size_t> pair_2) -> bool
  {
     if(pair_1 == pair_2) return false;
     for(size_t leg_id = 0; leg_id < 2; ++leg_id) {
         const size_t h1_leg_id = leg_id == 0 ? pair_1.first : pair_1.second;
         const size_t h2_leg_id = leg_id == 0 ? pair_2.first : pair_2.second;

         if(h1_leg_id != h2_leg_id) {
             // per ora lo faccio solo per i tau ma poi va aggiustato!!
             auto iso_cand1_pair_1 = isolation_variable_tau.at(h1_leg_id);
             auto iso_cand1_pair_2 = isolation_variable_tau.at(h2_leg_id);
             int iso_cmp;
             if(iso_cand1_pair_1 == iso_cand1_pair_2){ iso_cmp= 0;}
             else {iso_cmp =  iso_cand1_pair_1 > iso_cand1_pair_2 ? 1 : -1; }
             if(iso_cmp != 0) return iso_cmp == 1;

             if(Tau_pt.at(h1_leg_id) != Tau_pt.at(h2_leg_id))
                 return Tau_pt.at(h1_leg_id) > Tau_pt.at(h2_leg_id);
         }
     }
     std::cout << event << std::endl;
     throw std::runtime_error("not found a good criteria for best tau pair");
 };

 if(!pairs.empty()){
     const auto best_pair = *std::min_element(pairs.begin(), pairs.end(), Comparitor);
     //std::cout <<"Tau_charge[best_pair.first] = " << Tau_charge[best_pair.first]<< "\t Tau_charge[best_pair.second] = "<<Tau_charge[best_pair.second]<<std::endl;
     if(Tau_charge[best_pair.first]!=Tau_charge[best_pair.second]){
         final_indices.push_back(best_pair.first);
         final_indices.push_back(best_pair.second);
     }
 }
 return final_indices;
}



bool GenRecoMatching(const LorentzVectorM& leg1_p4, const LorentzVectorM& leg2_p4, const LorentzVectorM& lep1_p4, const LorentzVectorM& lep2_p4){
    auto dR_1 = DeltaR(leg1_p4.Phi(), leg1_p4.Eta(), lep1_p4.Phi(), lep1_p4.Eta());
    auto dR_2 = DeltaR(leg2_p4.Phi(), leg2_p4.Eta(), lep2_p4.Phi(), lep2_p4.Eta());
    //std::cout << "DR_1 = " <<dR_1 <<"\t DR_2 = "<< dR_2<<std::endl;
    //std::cout << " are they smaller than 0.2 ? " <<(dR_1 < 0.2 && dR_2<0.2)<< std::endl;
    return (dR_1 < 0.2 && dR_2<0.2);
    //bool correspondance= (dR_1 < 0.2 && dR_2<0.2);
    //return correspondence;
}

bool JetLepSeparation(const LorentzVectorM& tau_p4, const vec_f& Jet_eta, const vec_f& Jet_phi, const vec_i & RecoJetIndices){
  vec_i JetSeparatedWRTLeptons;
  for (auto& jet_idx:RecoJetIndices){
    auto dR = DeltaR(tau_p4.Phi(), tau_p4.Eta(), Jet_phi[jet_idx], Jet_eta[jet_idx]);
    //auto dR_2 = DeltaR(leg2_p4.Phi(), leg2_p4.Eta(), Jet_phi[jet_idx], Jet_eta[jet_idx]);
    //if(dR_1>0.5 && dR_2>0.5){
    if(dR>0.5){
      JetSeparatedWRTLeptons.push_back(jet_idx);
    }
  }
  return (JetSeparatedWRTLeptons.size()>=2);
}

bool ElectronVeto(EvtInfo& evt_info, const vec_i& indices, const vec_f& Electron_pt, const vec_f& Electron_dz, const vec_f& Electron_dxy, const vec_f& Electron_eta, const vec_b& Electron_mvaFall17V2Iso_WP90, const vec_b& Electron_mvaFall17V2noIso_WP90 , const vec_f&  Electron_pfRelIso03_all){
int nElectrons =0 ;
// do not consider signal electron
int signalElectron_idx = -1;
if(evt_info.channel==Channel::eTau){
  signalElectron_idx = indices[0];
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

bool MuonVeto(EvtInfo& evt_info, const vec_i& indices,const vec_f& Muon_pt, const vec_f& Muon_dz, const vec_f& Muon_dxy, const vec_f& Muon_eta, const vec_b& Muon_tightId, const vec_b& Muon_mediumId , const vec_f&  Muon_pfRelIso04_all){
int nMuons =0 ;
// do not consider signal muon
int signalMuon_idx = -1;
if(evt_info.channel==Channel::muTau){
  signalMuon_idx = indices[0];
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


vec_f ReorderVSJet(EvtInfo& evt_info,const vec_i& final_indices, const vec_f& Tau_rawDeepTau2017v2p1VSjet){

vec_f reordered_vs_jet ;
if(evt_info.channel == Channel::tauTau){
    for(auto& i : final_indices){
        reordered_vs_jet.push_back(Tau_rawDeepTau2017v2p1VSjet.at(i));
    }
}
else{
    reordered_vs_jet.push_back(Tau_rawDeepTau2017v2p1VSjet.at(final_indices[1] ) );
}
return reordered_vs_jet;
}



int AK4JetFilter(EvtInfo& evt_info, const vec_i& indices, const LorentzVectorM& leg1_p4, const LorentzVectorM& leg2_p4, const vec_f&  Jet_eta, const vec_f&  Jet_phi, const vec_f&  Jet_pt, const vec_i&  Jet_jetId, const int& is2017){
int nFatJetsAdd=0;
int nJetsAdd=0;

for (size_t jet_idx =0 ; jet_idx < Jet_pt.size(); jet_idx++){
    if(Jet_pt.at(jet_idx)>20 && std::abs(Jet_eta.at(jet_idx)) < 2.5 && ( (Jet_jetId.at(jet_idx))&(1<<1) || is2017 == 1) ) {
        float dr1 = DeltaR(leg1_p4.phi(), leg1_p4.eta(),Jet_phi[jet_idx], Jet_eta[jet_idx]);
        float dr2 = DeltaR(leg2_p4.phi(), leg2_p4.eta(),Jet_phi[jet_idx], Jet_eta[jet_idx]);
        if(dr1 > 0.5 && dr2 >0.5 ){
            nJetsAdd +=1;
          }
    }
}
int n_Jets = 2;
//if(evt_info.channel!=Channel::tauTau){
//     n_Jets = 1;
//}
if(nJetsAdd>=n_Jets){
    return 1;
}
return 0;
}

int AK8JetFilter(EvtInfo& evt_info, const vec_i& indices, const LorentzVectorM& leg1_p4, const LorentzVectorM& leg2_p4, const vec_f&  FatJet_pt, const vec_f&  FatJet_eta, const vec_f&  FatJet_phi, const vec_f& FatJet_msoftdrop, const int& is2017){
int nFatJetsAdd=0;
for (size_t fatjet_idx =0 ; fatjet_idx < FatJet_pt.size(); fatjet_idx++){
    if(FatJet_msoftdrop.at(fatjet_idx)>30 && std::abs(FatJet_eta.at(fatjet_idx)) < 2.5) {
        float dr1 = DeltaR(leg1_p4.phi(), leg1_p4.eta(),FatJet_phi[fatjet_idx], FatJet_eta[fatjet_idx]);
        float dr2 = DeltaR(leg2_p4.phi(), leg2_p4.eta(),FatJet_phi[fatjet_idx], FatJet_eta[fatjet_idx]);
        if(dr1 > 0.5 && dr2 >0.5 ){
            nFatJetsAdd +=1;
          }
    }
}
//std::cout << nFatJetsAdd << "\t" <<nJetsAdd << std::endl;
int n_FatJets = 1 ;
//if(evt_info.channel!=Channel::tauTau){
//     n_Jets = 1;
//}
if(nFatJetsAdd>=n_FatJets ){
    return 1;
}
return 0;
}

vec_i FindRecoGenJetCorrespondence(const vec_i& RecoJet_genJetIdx, const vec_i& GenJet_taggedIndices){
  vec_i RecoJetIndices;
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
