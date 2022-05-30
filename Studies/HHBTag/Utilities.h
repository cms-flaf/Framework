// includere cose


float InvMassByFalvour(const vec_f &GenJet_pt,const vec_f &GenJet_eta,const vec_f &GenJet_phi,const vec_f &GenJet_mass, const vec_i& GenJet_partonFlavour, bool wantOnlybPartonFlavour){
    LorentzVectorM genParticle_Tot_momentum;
    for(int part_idx = 0 ;part_idx<GenJet_pt.size(); part_idx++){
      if(wantOnlybPartonFlavour==true && abs(GenJet_partonFlavour[part_idx])!=5)continue;
      const float particleMass = (particleMasses.find(GenJet_partonFlavour[part_idx]) != particleMasses.end()) ? particleMasses.at(GenJet_partonFlavour[part_idx]) : GenJet_mass[part_idx];
      genParticle_Tot_momentum+=LorentzVectorM(GenJet_pt[part_idx], GenJet_eta[part_idx], GenJet_phi[part_idx],particleMass);
    }
    return genParticle_Tot_momentum.M();
}


float InvMassByIndices(const vec_i &indices, const vec_f& GenPart_pt,const vec_f &GenPart_eta,const vec_f &GenPart_phi,const vec_f &GenPart_mass,const vec_i& GenPart_pdgId, bool wantOnlybParticle){
    LorentzVectorM genParticle_Tot_momentum;
    //if(evt!=905) return 0.;
    for(int i = 0 ;i<indices.size(); i++){
      auto part_idx = indices.at(i);
      //std::cout << part_idx<<"\n";
      //std::cout << GenPart_pt.size()<<"\n";
      if(wantOnlybParticle && abs(GenPart_pdgId[part_idx])!=5) continue;
      //if(wantOnlybPartonFlavour==true && abs(GenJet_pdgId[part_idx])!=5)continue;
      const float particleMass = (particleMasses.find(GenPart_pdgId[part_idx]) != particleMasses.end()) ? particleMasses.at(GenPart_pdgId[part_idx]) : GenPart_mass[part_idx];
      //std::cout << GenPart_pt[part_idx]<<"\t"<< GenPart_eta[part_idx]<<"\t"<< GenPart_phi[part_idx]<<"\t"<<particleMass<< std::endl;
      genParticle_Tot_momentum+=LorentzVectorM(GenPart_pt[part_idx], GenPart_eta[part_idx], GenPart_phi[part_idx],particleMass);
    }
    return genParticle_Tot_momentum.M();
}


vec_i FindTwoJetsClosestToMPV(float mpv, const vec_f& GenPart_pt,const vec_f &GenPart_eta,const vec_f &GenPart_phi,const vec_f &GenPart_mass,const vec_i& GenPart_pdgId){
  vec_i indices;
  int i_min, j_min;
  float delta_min = 100;
  //float closest_value=10.*mpv;
  for(int i =0 ; i< GenPart_pt.size(); i++){
    for(int j=0; j<i; j++){
      vec_i temporary_indices;
      temporary_indices.push_back(i);
      temporary_indices.push_back(j);
      float inv_mass = InvMassByIndices(temporary_indices, GenPart_pt,GenPart_eta,GenPart_phi,GenPart_mass,GenPart_pdgId, true);
      float delta_mass = abs(inv_mass-mpv);
      if(delta_mass<=delta_min){
        i_min=i;
        j_min=j;
        delta_min = delta_mass;
        //closest_value = inv_mass
      }
    }
  }

  indices.push_back(i_min);
  indices.push_back(j_min);
  return indices;
}
