#pragma once
#include "Framework/Common/AnalysisTools.h"
#include "Framework/Common/GenTools.h"


float InvMassByFalvour(const RVecF &GenJet_pt,const RVecF &GenJet_eta,const RVecF &GenJet_phi,const RVecF &GenJet_mass, const RVecI& GenJet_partonFlavour, bool wantOnlybPartonFlavour){
    LorentzVectorM genParticle_Tot_momentum;
    for(int part_idx = 0 ;part_idx<GenJet_pt.size(); part_idx++){
      if(wantOnlybPartonFlavour==true && abs(GenJet_partonFlavour[part_idx])!=5)continue;
      ParticleInfo particle_information = ParticleDB::GetParticleInfo(GenJet_partonFlavour[part_idx]);
      const float particleMass = particle_information.mass>0? particle_information.mass : GenJet_mass[part_idx];
      genParticle_Tot_momentum+=LorentzVectorM(GenJet_pt[part_idx], GenJet_eta[part_idx], GenJet_phi[part_idx],particleMass);
    }
    return genParticle_Tot_momentum.M();
}


float InvMassByIndices(const RVecI &indices, const RVecF& GenPart_pt,const RVecF &GenPart_eta,const RVecF &GenPart_phi,const RVecF &GenPart_mass,const RVecI& GenPart_pdgId, bool wantOnlybParticle){
    LorentzVectorM genParticle_Tot_momentum;
    //if(evt!=905) return 0.;
    for(int i = 0 ;i<indices.size(); i++){
      auto part_idx = indices.at(i);
      if(wantOnlybParticle && abs(GenPart_pdgId[part_idx])!=5) continue;
      ParticleInfo particle_information = ParticleDB::GetParticleInfo(GenPart_pdgId[part_idx]);
      const float particleMass = particle_information.mass>0? particle_information.mass : GenPart_mass[part_idx];
      genParticle_Tot_momentum+=LorentzVectorM(GenPart_pt[part_idx], GenPart_eta[part_idx], GenPart_phi[part_idx],particleMass);
    }
    return genParticle_Tot_momentum.M();
}


RVecI FindTwoJetsClosestToMPV(float mpv, const RVecF& GenPart_pt,const RVecF &GenPart_eta,const RVecF &GenPart_phi,const RVecF &GenPart_mass,const RVecI& GenPart_pdgId){
  RVecI indices;
  int i_min, j_min;
  float delta_min = 100;
  for(int i =0 ; i< GenPart_pt.size(); i++){
    for(int j=0; j<i; j++){
      RVecI temporary_indices;
      temporary_indices.push_back(i);
      temporary_indices.push_back(j);
      float inv_mass = InvMassByIndices(temporary_indices, GenPart_pt,GenPart_eta,GenPart_phi,GenPart_mass,GenPart_pdgId, true);
      float delta_mass = abs(inv_mass-mpv);
      if(delta_mass<=delta_min){
        i_min=i;
        j_min=j;
        delta_min = delta_mass;
      }
    }
  }

  indices.push_back(i_min);
  indices.push_back(j_min);
  return indices;
}
