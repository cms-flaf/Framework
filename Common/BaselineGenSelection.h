#pragma once
#include "AnalysisTools.h"
#include "GenTools.h"



bool isTauDaughter(int tau_idx, int particle_idx, const RVecI& GenPart_genPartIdxMother){
   bool isTauDaughter = false;
   int idx_mother = GenPart_genPartIdxMother[particle_idx];
   while(1){
       if(idx_mother == -1){
           return false;
       }
       else {
           if(idx_mother==tau_idx){
               return true;
           }
           else{
               int newParticle_index = idx_mother;
               idx_mother = GenPart_genPartIdxMother[newParticle_index];
           }
       }
   }
}
LorentzVectorM GetTauP4(int tau_idx, const RVecF& pt, const RVecF& eta, const RVecF& phi, const RVecF& mass, const RVecI& GenPart_genPartIdxMother, const RVecI& GenPart_pdgId, const RVecI& GenPart_status){
    LorentzVectorM sum(0.,0.,0.,0.);
    LorentzVectorM TauP4;

    for(size_t particle_idx=0; particle_idx< GenPart_pdgId.size(); particle_idx++ ){
        if(GenPart_pdgId[particle_idx] == 12 || GenPart_pdgId[particle_idx] == 14 || GenPart_pdgId[particle_idx] == 16) continue;
        if(GenPart_status[particle_idx]!= 1 ) continue;

        bool isRelatedToTau = isTauDaughter(tau_idx, particle_idx, GenPart_genPartIdxMother);
        if(isRelatedToTau){
            LorentzVectorM current_particleP4 ;
            float p_mass ;
            if (particleMasses.find(GenPart_pdgId[particle_idx]) != particleMasses.end()){
                p_mass=particleMasses.at(GenPart_pdgId[particle_idx]);
            }
            else{
                p_mass = mass[particle_idx];
            }
            current_particleP4=LorentzVectorM(pt[particle_idx], eta[particle_idx], phi[particle_idx],p_mass);
            sum = sum + current_particleP4;
        }

    }
    TauP4=LorentzVectorM(sum.Pt(), sum.Eta(), sum.Phi(), sum.M());
    return TauP4;

}
std::map<std::string,std::set<int>> GetLeptonIndices(int evt, const RVecI& GenPart_pdgId, const RVecI& GenPart_genPartIdxMother, const RVecI& GenPart_statusFlags){
  std::map<std::string,std::set<int>> lep_indices;
  //lep_indices["Electron"] = std::set<int>;
  std::set<int> e_indices;
  std::set<int> mu_indices;
  std::set<int> tau_indices;

  // start the loop to look for the indices of e, mu and taus
  for(size_t i=0; i< GenPart_pdgId.size(); i++ ){
    // check particle mother has pdg index positive, and that isLastCopy is on
      if(GenPart_genPartIdxMother[i]>=0 && ((GenPart_statusFlags[i])&(1<<13))  ){
        // case 1 : is tau --> need to check that the status on is: isPrompt or isDirectPromptTauDecayProduct or fromHardProcess
          if(std::abs(GenPart_pdgId[i])==15  && ( ( (GenPart_statusFlags[i])&(1<<0) || (GenPart_statusFlags[i])&(1<<5) )  ) && ((GenPart_statusFlags[i])&(1<<8)) ){
              tau_indices.insert(i);
          }
        // case 2 : is NOT tau --> is either electron or muon, need to check that the status on is: isDirectPromptTauDecayProduct or isHardProcessTauDecayProduct or isDirectHardProcessTauDecayProduct
          else if((std::abs(GenPart_pdgId[i])==13 || std::abs(GenPart_pdgId[i])==11) && ( (GenPart_statusFlags[i])&(1<<5) ) && ((GenPart_statusFlags[i])&(1<<9) || (GenPart_statusFlags[i])&(1<<10)) ) {
              int mother_index = GenPart_genPartIdxMother[i];
              // check that the mother is tau
              if( std::abs(GenPart_pdgId[mother_index]) != 15) {
                  std::cout << "event " << evt << " particle " << i << " is " << GenPart_pdgId[i] <<" particle mother " << GenPart_genPartIdxMother[i] << " is " << GenPart_pdgId[GenPart_genPartIdxMother[i]]  << std::endl;
                  throw std::runtime_error("particle mother is not tau");
              }

              // remove the mother from tau indices!!
              tau_indices.erase(GenPart_genPartIdxMother[i]);
              if(std::abs(GenPart_pdgId[i])==11 ){
                  e_indices.insert(i);
              }
              if(std::abs(GenPart_pdgId[i])==13 ){
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


EvtInfo GetEventInfo(int evt, std::map<std::string, std::set<int>>& lep_indices, const RVecI& GenPart_pdgId, const RVecI& GenPart_genPartIdxMother, const RVecI& GenPart_status, const RVecF& GenPart_pt, const RVecF& GenPart_eta, const RVecF& GenPart_phi, const RVecF& GenPart_mass ){
  EvtInfo evt_info;
  // 1. tauTau
  if(lep_indices["Electron"].size()==0 && lep_indices["Muon"].size()==0 && lep_indices["Tau"].size()==2 ){

      /* uncomment this part to require tau with pt > 10 GeV*/
      /*
      if(lep_indices["Tau"].size()!=2){
          for(size_t t = 0; t <lep_indices["Tau"].size(); t++){
              int tau_idx = *std::next(lep_indices["Tau"].begin(), t);
              if(GenPart_pt[tau_idx]<10.){
                  lep_indices["Tau"].erase(tau_idx);
              }
          }
      } // closes if on tau_indices size
      */

      /* if uncomment the following lines and remove the requirements on tau size, on signal it should be the same */
      /*
      // check that there are two taus
      if(lep_indices["Tau"].size()!=2){
          std::cout << "in this event tauTau = " << evt << " there should be 2 taus but there are "<< lep_indices["Tau"].size()<< " taus" <<std::endl;

          // loop over taus
          for(size_t t = 0; t <lep_indices["Tau"].size(); t++){
              int tau_idx = *std::next(lep_indices["Tau"].begin(), t);
              std::cout << "index = "<< tau_idx<< "\t pdgId = " << GenPart_pdgId[tau_idx] << "\t pt = " << GenPart_pt[tau_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[tau_idx]] << "\t status = " << GenPart_status[tau_idx] << std::endl;
          } // end of loop over taus
          throw std::runtime_error("it should be tautau, but tau indices is not 2 ");
      }
      */
      int leg_p4_index = 0;
      // loop over tau_indices
      std::set<int>::iterator it_tau = lep_indices["Tau"].begin();
      while (it_tau != lep_indices["Tau"].end()){
          int tau_idx = *it_tau;
          evt_info.leg_p4[leg_p4_index] = GetTauP4(tau_idx,GenPart_pt,GenPart_eta, GenPart_phi, GenPart_mass, GenPart_genPartIdxMother, GenPart_pdgId, GenPart_status);
          leg_p4_index++;
          it_tau++;
      } // closes loop over tau_indices

      evt_info.channel = Channel::tauTau;

  } // closes tauTau Channel

  // 2. muTau
  if(lep_indices["Electron"].size()==0 && (lep_indices["Muon"].size()==1 && lep_indices["Tau"].size()==1)){
      /* uncomment this part to require tau with pt > 10 GeV */
      /*
      if(lep_indices["Tau"].size()!=1){
          for(size_t t = 0; t <lep_indices["Tau"].size(); t++){
              int tau_idx = *std::next(lep_indices["Tau"].begin(), t);
              if(GenPart_pt[tau_idx]<10.){
                  lep_indices["Tau"].erase(tau_idx);
              }
          }
      } // closes if on tau_indices size
      */
      /* if uncomment the following lines and change the second && to || , on signal it should be the same */
      /*
      // 2.1: when there is 1 mu and != 1 tau
      if(lep_indices["Tau"].size()!=1){
          std::cout << "in this event muTau = " << evt << " there should be 1 tau but there are "<< lep_indices["Tau"].size()<< " taus" <<std::endl;
          for(size_t t = 0; t <lep_indices["Tau"].size(); t++){
              int tau_idx = *std::next(lep_indices["Tau"].begin(), t);
              std::cout << "index = "<< tau_idx<< "\t pdgId = " << GenPart_pdgId[tau_idx] << "\t pt = " << GenPart_pt[tau_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[tau_idx]] << "\t status = " << GenPart_status[tau_idx] << std::endl;
          }
          throw std::runtime_error("it should be muTau, but tau indices is not 1 ");
      }

      // 2.2: when there is 1 tau and != 1 mu
      if(lep_indices["Muon"].size()!=1){
          std::cout << "in this event muTau = " << evt << " there should be 1 mu but there are "<< lep_indices["Muon"].size()<< " mus" <<std::endl;
          for(size_t t = 0; t <lep_indices["Muon"].size(); t++){
              int mu_idx = *std::next(lep_indices["Muon"].begin(), t);
              std::cout << "index = "<< mu_idx<< "\t pdgId = " << GenPart_pdgId[mu_idx] << "\t pt = " << GenPart_pt[mu_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[mu_idx]] << "\t status = " << GenPart_status[mu_idx] << std::endl;
          }
          throw std::runtime_error("it should be muTau, but mu indices is not 1 ");
      }
      */
      int leg_p4_index = 0;

      // loop over mu_indices
      std::set<int>::iterator it_mu = lep_indices["Muon"].begin();
      while (it_mu != lep_indices["Muon"].end()){
          int mu_idx = *it_mu;
          evt_info.leg_p4[leg_p4_index] =  LorentzVectorM(GenPart_pt[mu_idx], GenPart_eta[mu_idx],GenPart_phi[mu_idx], muon_mass);
          leg_p4_index++;
          it_mu++;
      } // closes loop over mu_indices
      // loop over tau_indices
      std::set<int>::iterator it_tau = lep_indices["Tau"].begin();
      while (it_tau != lep_indices["Tau"].end()){
          int tau_idx = *it_tau;
          evt_info.leg_p4[leg_p4_index] = GetTauP4(tau_idx,GenPart_pt,GenPart_eta, GenPart_phi, GenPart_mass, GenPart_genPartIdxMother, GenPart_pdgId, GenPart_status);
          leg_p4_index++;
          it_tau++;
      } // closes loop over tau_indices
      evt_info.channel = Channel::muTau;
  } // closes muTau Channel


  // eTau
  if(lep_indices["Muon"].size()==0 && (lep_indices["Electron"].size()==1 && lep_indices["Tau"].size()==1)){
      /* uncomment this part to require tau with pt > 10 GeV */
      /*
      if(lep_indices["Tau"].size()!=1){
          for(size_t t = 0; t <lep_indices["Tau"].size(); t++){
              int tau_idx = *std::next(lep_indices["Tau"].begin(), t);
              if(GenPart_pt[tau_idx]<10.){
                  lep_indices["Tau"].erase(tau_idx);
              }
          }
      } // closes if on tau_indices size
      */
      /* if uncomment the following lines and change the second && to || , on signal it should be the same */
      /*
      // case 1: when there is 1 e and != 1 tau
      if(lep_indices["Tau"].size()!=1){
          std::cout << "in this event eTau = " << evt << " there should be 1 tau but there are "<< lep_indices["Tau"].size()<< " taus" <<std::endl;
          for(size_t t = 0; t <lep_indices["Tau"].size(); t++){
              int tau_idx = *std::next(lep_indices["Tau"].begin(), t);
              std::cout << "index = "<< tau_idx<< "\t pdgId = " << GenPart_pdgId[tau_idx] << "\t pt = " << GenPart_pt[tau_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[tau_idx]] << "\t status = " << GenPart_status[tau_idx] << std::endl;
          }
          throw std::runtime_error("it should be eTau, but tau indices is not 1 ");
      }

      // case 2: when there is 1 tau and != 1 e
      if(lep_indices["Electron"].size()!=1){
          std::cout << "in this event eTau = " << evt << " there should be 1 e but there are "<< lep_indices["Electron"].size()<< " es" <<std::endl;
          for(size_t t = 0; t <lep_indices["Electron"].size(); t++){
              int e_idx = *std::next(lep_indices["Electron"].begin(), t);
              std::cout << "index = "<< e_idx<< "\t pdgId = " << GenPart_pdgId[e_idx] << "\t pt = " << GenPart_pt[e_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[e_idx]] << "\t status = " << GenPart_status[e_idx] << std::endl;
          }
          throw std::runtime_error("it should be eTau, but e indices is not 1 ");
      }
      */
      int leg_p4_index = 0;


      // loop over e_indices
      std::set<int>::iterator it_e = lep_indices["Electron"].begin();
      while (it_e != lep_indices["Electron"].end()){
          int e_idx = *it_e;
          evt_info.leg_p4[leg_p4_index] = LorentzVectorM(GenPart_pt[e_idx], GenPart_eta[e_idx],GenPart_phi[e_idx], electron_mass);
          leg_p4_index++;
          it_e++;
      } // closes loop over e_indices

      // loop over tau_indices
      std::set<int>::iterator it_tau = lep_indices["Tau"].begin();
      while (it_tau != lep_indices["Tau"].end()){
          int tau_idx = *it_tau;
          evt_info.leg_p4[leg_p4_index] = GetTauP4(tau_idx,GenPart_pt,GenPart_eta, GenPart_phi, GenPart_mass, GenPart_genPartIdxMother, GenPart_pdgId, GenPart_status);
          leg_p4_index++;
          it_tau++;
      } // closes loop over tau_indices
      evt_info.channel = Channel::eTau;

  } // closes eTau Channel

  // muMu
  if(lep_indices["Electron"].size()==0 && lep_indices["Tau"].size()==0 && lep_indices["Muon"].size()==2){

      /* uncomment this part to require mu with pt > 10 GeV */
      /*
      if(lep_indices["Muon"].size()!=2){
          for(size_t t = 0; t <lep_indices["Muon"].size(); t++){
              int mu_idx = *std::next(lep_indices["Muon"].begin(), t);
              if(GenPart_pt[mu_idx]<10.){
                  lep_indices["Muon"].erase(mu_idx);
              }
          }
      } // closes if on lep_indices["Muon"] size
      */
      /* if uncomment the following lines and remove the requirements on tau size, on signal it should be the same */
      /*
      if(lep_indices["Muon"].size()!=2){
          std::cout << "in this event muTau = " << evt << " there should be 2 mus but there are "<< lep_indices["Muon"].size()<< " mus" <<std::endl;

          // loop over mus
          for(size_t t = 0; t <lep_indices["Muon"].size(); t++){
              int mu_idx = *std::next(lep_indices["Muon"].begin(), t);
              std::cout << "index = "<< mu_idx<< "\t pdgId = " << GenPart_pdgId[mu_idx] << "\t pt = " << GenPart_pt[mu_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[mu_idx]] << "\t status = " << GenPart_status[mu_idx] << std::endl;
          } // end of loop over mus

          throw std::runtime_error("it should be mumu, but mu indices is not 2 ");
      }
      */

      int leg_p4_index = 0;

      // loop over mu_indices

      std::set<int>::iterator it_mu = lep_indices["Muon"].begin();
      while (it_mu != lep_indices["Muon"].end()){
          int mu_idx = *it_mu;
          evt_info.leg_p4[leg_p4_index] =  LorentzVectorM(GenPart_pt[mu_idx], GenPart_eta[mu_idx],GenPart_phi[mu_idx], muon_mass);
          leg_p4_index++;
          it_mu++;
      } // closes loop over mu_indices

      evt_info.channel = Channel::muMu;

  } // closes muMu Channel


  // eE
  if(lep_indices["Muon"].size()==0 && lep_indices["Tau"].size()==0 && lep_indices["Electron"].size()==2){

      /* uncomment this part to require mu with pt > 10 GeV */
      /*
      if(lep_indices["Electron"].size()!=2){
          for(size_t t = 0; t <lep_indices["Electron"].size(); t++){
              int e_idx = *std::next(lep_indices["Electron"].begin(), t);
              if(GenPart_pt[e_idx]<10.){
                  lep_indices["Electron"].erase(e_idx);
              }
          }
      } // closes if on e_indices size
      */
      /* if uncomment the following lines and remove the requirements on tau size, on signal it should be the same */
      /*
      if(lep_indices["Electron"].size()!=2){
          std::cout << "in this event eTau = " << evt << " there should be 2 es but there are "<< lep_indices["Electron"].size()<< " es" <<std::endl;

          // loop over es
          for(size_t t = 0; t <lep_indices["Electron"].size(); t++){
              int e_idx = *std::next(lep_indices["Electron"].begin(), t);
              std::cout << "index = "<< e_idx<< "\t pdgId = " << GenPart_pdgId[e_idx] << "\t pt = " << GenPart_pt[e_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[e_idx]] << "\t status = " << GenPart_status[e_idx] << std::endl;
          } // end of loop over es

          throw std::runtime_error("it should be ee, but e indices is not 2 ");
      }
      */
      int leg_p4_index = 0;

      // loop over lep_indices["Electron"]

      std::set<int>::iterator it_e = lep_indices["Electron"].begin();
      while (it_e != lep_indices["Electron"].end()){
          int e_idx = *it_e;
          evt_info.leg_p4[leg_p4_index] = LorentzVectorM(GenPart_pt[e_idx], GenPart_eta[e_idx],GenPart_phi[e_idx], electron_mass);
          leg_p4_index++;
          it_e++;
      } // closes loop over e_indices

      evt_info.channel = Channel::eE;

  } // closes eE Channel

  // eMu
  if(lep_indices["Tau"].size()==0 && (lep_indices["Electron"].size()==1 && lep_indices["Muon"].size()==1)){
      /* uncomment this part to require tau with pt > 10 GeV */
      /*
      if(lep_indices["Muon"].size()!=1){
          for(size_t t = 0; t <lep_indices["Muon"].size(); t++){
              int mu_idx = *std::next(lep_indices["Muon"].begin(), t);
              if(GenPart_pt[mu_idx]<10.){
                  lep_indices["Muon"].erase(mu_idx);
              }
          }
      } // closes if on lep_indices["Muon"] size
      */
      /* if uncomment the following lines and change the second && to || , on signal it should be the same */
      /*
      // case 1: when there is 1 e and != 1 mu
      if(lep_indices["Muon"].size()!=1){
          std::cout << "in this event eMu = " << evt << " there should be 1 mu but there are "<< lep_indices["Muon"].size()<< " mus" <<std::endl;
          for(size_t t = 0; t <lep_indices["Muon"].size(); t++){
              int mu_idx = *std::next(lep_indices["Muon"].begin(), t);
              std::cout << "index = "<< mu_idx<< "\t pdgId = " << GenPart_pdgId[mu_idx] << "\t pt = " << GenPart_pt[mu_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[mu_idx]] << "\t status = " << GenPart_status[mu_idx] << std::endl;
          }
          throw std::runtime_error("it should be eMu, but mu indices is not 1 ");
      }

      // case 2: when there is 1 mu and != 1 e
      if(lep_indices["Electron"].size()!=1){
          std::cout << "in this event eTau = " << evt << " there should be 1 e but there are "<< lep_indices["Electron"].size()<< " es" <<std::endl;
          for(size_t t = 0; t <lep_indices["Electron"].size(); t++){
              int e_idx = *std::next(lep_indices["Electron"].begin(), t);
              std::cout << "index = "<< e_idx<< "\t pdgId = " << GenPart_pdgId[e_idx] << "\t pt = " << GenPart_pt[e_idx] << "\t mother = " << GenPart_pdgId[GenPart_genPartIdxMother[e_idx]] << "\t status = " << GenPart_status[e_idx] << std::endl;
          }
          throw std::runtime_error("it should be eTau, but e indices is not 1 ");
      }
      */
      int leg_p4_index = 0;

      // loop over mu_indices
      std::set<int>::iterator it_mu = lep_indices["Muon"].begin();
      while (it_mu != lep_indices["Muon"].end()){
          int mu_idx = *it_mu;
          evt_info.leg_p4[leg_p4_index] =  LorentzVectorM(GenPart_pt[mu_idx], GenPart_eta[mu_idx],GenPart_phi[mu_idx], muon_mass);
          leg_p4_index++;
          it_mu++;
      } // closes loop over mu_indices

      // loop over e_indices
      std::set<int>::iterator it_e = lep_indices["Electron"].begin();
      while (it_e != lep_indices["Electron"].end()){
          int e_idx = *it_e;
          evt_info.leg_p4[leg_p4_index] = LorentzVectorM(GenPart_pt[e_idx], GenPart_eta[e_idx],GenPart_phi[e_idx], electron_mass);
          leg_p4_index++;
          it_e++;
      } // closes loop over e_indices

      evt_info.channel = Channel::eMu;

  } // closes eMu Channel

  return evt_info;

}


bool PassAcceptance(const EvtInfo& evt_info){
    for(size_t i =0; i<evt_info.leg_p4.size(); i++){
        if(!(evt_info.leg_p4.at(i).pt()>20 && std::abs(evt_info.leg_p4.at(i).eta())<2.3 )){
            //std::cout << "channel that does not pass acceptance == " << evt_info.channel << std::endl;
            //if(evt_info.channel == Channel::eTau){
            //    std::cout << "pt == " << evt_info.leg_p4.at(i).pt() << "eta == " << evt_info.leg_p4.at(i).eta() << std::endl;
            //}
            return false;
        }
    }
    return true;
}
