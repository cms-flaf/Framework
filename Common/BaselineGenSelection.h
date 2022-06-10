#pragma once
#include "AnalysisTools.h"
#include "GenTools.h"
#include "TextIO.h"
#include "GenStatusFlags.h"
#include "HHCore.h"

std::map<std::string,std::set<int>> GetLeptonIndices(int evt, const RVecI& GenPart_pdgId, const RVecI& GenPart_genPartIdxMother, const RVecI& GenPart_statusFlags){
  std::map<std::string,std::set<int>> lep_indices;
  std::set<int> e_indices;
  std::set<int> mu_indices;
  std::set<int> tau_indices;
  GenStatusFlags statusFlags;
  for(size_t i=0; i< GenPart_pdgId.size(); i++ ){
    // check particle mother has pdg index positive, and that isLastCopy is on
      if(GenPart_genPartIdxMother[i]>=0 && ((GenPart_statusFlags[i])&(1<<statusFlags.StatusBits::kIsLastCopy)  )){
        // case 1 : is tau --> need to check that the status on is: isPrompt or isDirectPromptTauDecayProduct or fromHardProcess
          if(std::abs(GenPart_pdgId[i])==PdG::tau()  && ( ( (GenPart_statusFlags[i])&(1<<statusFlags.StatusBits::kIsPrompt) || (GenPart_statusFlags[i])&(1<<statusFlags.StatusBits::kIsDirectPromptTauDecayProduct) )  ) && ((GenPart_statusFlags[i])&(1<<statusFlags.StatusBits::kFromHardProcess)) ){
              tau_indices.insert(i);
          }
        // case 2 : is NOT tau --> is either electron or muon, need to check that the status on is: isDirectPromptTauDecayProduct or isHardProcessTauDecayProduct or isDirectHardProcessTauDecayProduct
          else if((std::abs(GenPart_pdgId[i])==PdG::mu() || std::abs(GenPart_pdgId[i])==PdG::e()) && ( (GenPart_statusFlags[i])&(1<<statusFlags.StatusBits::kIsDirectPromptTauDecayProduct) ) && ((GenPart_statusFlags[i])&(1<<statusFlags.StatusBits::kIsHardProcessTauDecayProduct) || (GenPart_statusFlags[i])&(1<<statusFlags.StatusBits::kIsDirectHardProcessTauDecayProduct)) ) {
              int mother_index = GenPart_genPartIdxMother[i];
              // check that the mother is tau
              if( std::abs(GenPart_pdgId[mother_index]) != PdG::tau()) {
                  //std::cout << "event " << evt << " particle " << i << " is " << GenPart_pdgId[i] <<" particle mother " << GenPart_genPartIdxMother[i] << " is " << GenPart_pdgId[GenPart_genPartIdxMother[i]]  << std::endl;
                  throw analysis::exception("particle mother is not tau");
              }

              // remove the mother from tau indices!!
              tau_indices.erase(GenPart_genPartIdxMother[i]);
              if(std::abs(GenPart_pdgId[i])==PdG::e() ){
                  e_indices.insert(i);
              }
              if(std::abs(GenPart_pdgId[i])==PdG::mu() ){
                  mu_indices.insert(i);

              }
          } // closes if on pdg ids for mu and e
      } // closes if on status flags and have idx mother
  } // closes for on genParticles
  lep_indices["Electron"] = e_indices ;
  lep_indices["Muon"] = mu_indices ;
  lep_indices["Tau"] = tau_indices ;

  return lep_indices;
}


HTTCand GetHTTCand(int evt, std::map<std::string, std::set<int>>& lep_indices, const RVecI& GenPart_pdgId,
                    const RVecI& GenPart_genPartIdxMother, const RVecI& GenPart_status, const RVecF& GenPart_pt,
                    const RVecF& GenPart_eta, const RVecF& GenPart_phi, const RVecF& GenPart_mass, const ROOT::VecOps::RVec<RVecI>& GenPart_daughters){
  HTTCand HTT_Cand;
  // 1. tauTau
  if(lep_indices["Electron"].size()==0 && lep_indices["Muon"].size()==0 && lep_indices["Tau"].size()==2 ){
      int leg_p4_index = 0;
      std::set<int>::iterator it_tau = lep_indices["Tau"].begin();
      while (it_tau != lep_indices["Tau"].end()){
          int tau_idx = *it_tau;
          HTT_Cand.leg_index[leg_p4_index] = tau_idx;
          HTT_Cand.leg_p4[leg_p4_index] = GetGenParticleVisibleP4(tau_idx,GenPart_pt,GenPart_eta,
                                          GenPart_phi, GenPart_mass, GenPart_genPartIdxMother,
                                          GenPart_pdgId, GenPart_status, GenPart_daughters);
          leg_p4_index++;
          it_tau++;
      }
      HTT_Cand.leg_pdg[0] = PdG::tau();
      HTT_Cand.leg_pdg[1] = PdG::tau();

  } // closes tauTau Channel

  // 2. muTau
  if(lep_indices["Electron"].size()==0 && (lep_indices["Muon"].size()==1 && lep_indices["Tau"].size()==1)){
      int leg_p4_index = 0;
      // loop over mu_indices
      std::set<int>::iterator it_mu = lep_indices["Muon"].begin();
      while (it_mu != lep_indices["Muon"].end()){
          int mu_idx = *it_mu;
          HTT_Cand.leg_index[leg_p4_index] = mu_idx;
          ParticleInfo particle_information = ParticleDB::GetParticleInfo(GenPart_pdgId[mu_idx]);
          float mu_mass = particle_information.mass>0? particle_information.mass : GenPart_mass[mu_idx];
          HTT_Cand.leg_p4[leg_p4_index] =  LorentzVectorM(GenPart_pt[mu_idx], GenPart_eta[mu_idx],GenPart_phi[mu_idx], mu_mass);
          leg_p4_index++;
          it_mu++;
      } // closes loop over mu_indices
      // loop over tau_indices
      std::set<int>::iterator it_tau = lep_indices["Tau"].begin();
      while (it_tau != lep_indices["Tau"].end()){
          int tau_idx = *it_tau;
          HTT_Cand.leg_index[leg_p4_index] = tau_idx;
          HTT_Cand.leg_p4[leg_p4_index] = GetGenParticleVisibleP4(tau_idx,GenPart_pt,GenPart_eta, GenPart_phi,
                                            GenPart_mass, GenPart_genPartIdxMother, GenPart_pdgId, GenPart_status, GenPart_daughters);
          leg_p4_index++;
          it_tau++;
      } // closes loop over tau_indices
      HTT_Cand.leg_pdg[0] = PdG::mu();
      HTT_Cand.leg_pdg[1] = PdG::tau();
  } // closes muTau Channel

  // 3. eTau
  if(lep_indices["Muon"].size()==0 && (lep_indices["Electron"].size()==1 && lep_indices["Tau"].size()==1)){
      int leg_p4_index = 0;
      // loop over e_indices
      std::set<int>::iterator it_e = lep_indices["Electron"].begin();
      while (it_e != lep_indices["Electron"].end()){
          int e_idx = *it_e;
          HTT_Cand.leg_index[leg_p4_index] = e_idx;
          ParticleInfo particle_information = ParticleDB::GetParticleInfo(GenPart_pdgId[e_idx]);
          float e_mass = particle_information.mass>0? particle_information.mass : GenPart_mass[e_idx];
          HTT_Cand.leg_p4[leg_p4_index] = LorentzVectorM(GenPart_pt[e_idx], GenPart_eta[e_idx],GenPart_phi[e_idx], e_mass);
          leg_p4_index++;
          it_e++;
      } // closes loop over e_indices
      // loop over tau_indices
      std::set<int>::iterator it_tau = lep_indices["Tau"].begin();
      while (it_tau != lep_indices["Tau"].end()){
          int tau_idx = *it_tau;
          HTT_Cand.leg_index[leg_p4_index] = tau_idx;
          HTT_Cand.leg_p4[leg_p4_index] = GetGenParticleVisibleP4(tau_idx,GenPart_pt,GenPart_eta, GenPart_phi,
                                        GenPart_mass, GenPart_genPartIdxMother, GenPart_pdgId, GenPart_status, GenPart_daughters);
          leg_p4_index++;
          it_tau++;
      } // closes loop over tau_indices
      HTT_Cand.leg_pdg[0] = PdG::e();
      HTT_Cand.leg_pdg[1] = PdG::tau();
  } // closes eTau Channel

  // 4. muMu
  if(lep_indices["Electron"].size()==0 && lep_indices["Tau"].size()==0 && lep_indices["Muon"].size()==2){
      int leg_p4_index = 0;
      // loop over mu_indices
      std::set<int>::iterator it_mu = lep_indices["Muon"].begin();
      while (it_mu != lep_indices["Muon"].end()){
          int mu_idx = *it_mu;
          HTT_Cand.leg_index[leg_p4_index] = mu_idx;
          ParticleInfo particle_information = ParticleDB::GetParticleInfo(GenPart_pdgId[mu_idx]);
          float mu_mass = particle_information.mass>0? particle_information.mass : GenPart_mass[mu_idx];
          HTT_Cand.leg_p4[leg_p4_index] =  LorentzVectorM(GenPart_pt[mu_idx], GenPart_eta[mu_idx],GenPart_phi[mu_idx], mu_mass);
          leg_p4_index++;
          it_mu++;
      } // closes loop over mu_indices
      HTT_Cand.leg_pdg[0] = PdG::mu();
      HTT_Cand.leg_pdg[1] = PdG::mu();
  } // closes muMu Channel


  // 5. eE
  if(lep_indices["Muon"].size()==0 && lep_indices["Tau"].size()==0 && lep_indices["Electron"].size()==2){
      int leg_p4_index = 0;
      // loop over e_indices
      std::set<int>::iterator it_e = lep_indices["Electron"].begin();
      while (it_e != lep_indices["Electron"].end()){
          int e_idx = *it_e;
          HTT_Cand.leg_index[leg_p4_index] = e_idx;
          ParticleInfo particle_information = ParticleDB::GetParticleInfo(GenPart_pdgId[e_idx]);
          float e_mass = particle_information.mass>0? particle_information.mass : GenPart_mass[e_idx];
          HTT_Cand.leg_p4[leg_p4_index] = LorentzVectorM(GenPart_pt[e_idx], GenPart_eta[e_idx],GenPart_phi[e_idx], e_mass);
          leg_p4_index++;
          it_e++;
      } // closes loop over e_indices
      HTT_Cand.leg_pdg[0] = PdG::e();
      HTT_Cand.leg_pdg[1] = PdG::e();
  } // closes eE Channel

  // 6. eMu
  if(lep_indices["Tau"].size()==0 && (lep_indices["Electron"].size()==1 && lep_indices["Muon"].size()==1)){
      int leg_p4_index = 0;
      // loop over mu_indices
      std::set<int>::iterator it_mu = lep_indices["Muon"].begin();
      while (it_mu != lep_indices["Muon"].end()){
          int mu_idx = *it_mu;
          HTT_Cand.leg_index[leg_p4_index] = mu_idx;
          ParticleInfo particle_information = ParticleDB::GetParticleInfo(GenPart_pdgId[mu_idx]);
          float mu_mass = particle_information.mass>0? particle_information.mass : GenPart_mass[mu_idx];
          HTT_Cand.leg_p4[leg_p4_index] =  LorentzVectorM(GenPart_pt[mu_idx], GenPart_eta[mu_idx],GenPart_phi[mu_idx], mu_mass);
          leg_p4_index++;
          it_mu++;
      } // closes loop over mu_indices
      // loop over e_indices
      std::set<int>::iterator it_e = lep_indices["Electron"].begin();
      while (it_e != lep_indices["Electron"].end()){
          int e_idx = *it_e;
          HTT_Cand.leg_index[leg_p4_index] = e_idx;
          ParticleInfo particle_information = ParticleDB::GetParticleInfo(GenPart_pdgId[e_idx]);
          float e_mass = particle_information.mass>0? particle_information.mass : GenPart_mass[e_idx];
          HTT_Cand.leg_p4[leg_p4_index] = LorentzVectorM(GenPart_pt[e_idx], GenPart_eta[e_idx],GenPart_phi[e_idx], e_mass);
          leg_p4_index++;
          it_e++;
      } // closes loop over e_indices
      HTT_Cand.leg_pdg[0] = PdG::e();
      HTT_Cand.leg_pdg[1] = PdG::mu();

  } // closes eMu Channel

  return HTT_Cand;

}


bool PassAcceptance(const HTTCand& HTT_Cand){
    for(size_t i =0; i<HTT_Cand.leg_p4.size(); i++){
        if(!(HTT_Cand.leg_p4.at(i).pt()>20 && std::abs(HTT_Cand.leg_p4.at(i).eta())<2.3 )){
            return false;
        }
    }
    return true;
}
