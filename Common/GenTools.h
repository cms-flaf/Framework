#pragma once
#include "AnalysisTools.h"
#include "exception.h"
#include "TextIO.h"


struct ParticleInfo{
  int pdgId;
  int charge;
  std::string name;
  std::string type;
  int mass{-1};
};


class ParticleDB {
public:
  static void Initialize(const std::string_view inputFile) {
    std::ifstream file (inputFile, ios::in );
     std::string line;
     while (getline(file, line)){
       auto values= analysis::SplitValueList(line,true,",",true);
       if(values.size()!=4 && values.size()!=5){
         throw analysis::exception("invalid line %1%")%line;
       }
       ParticleInfo currentInfo;
       currentInfo.pdgId = analysis::Parse<int>(values.at(0));
       currentInfo.name = values.at(1);
       currentInfo.type= values.at(2);
       currentInfo.charge= analysis::Parse<int>(values.at(3));
       if(values.size()>4){
         currentInfo.mass = analysis::Parse<float>(values.at(4));
       }
       particles()[currentInfo.pdgId]= currentInfo ;
     }


  }

  static const ParticleInfo& GetParticleInfo(int pdgId)
  {
    if(particles().empty())
      throw analysis::exception("ParticleDB is not initialized.");
    auto iter = particles().find(pdgId);
    if(iter == particles().end()){
      throw analysis::exception("ParticleInfo not found for particle ID");}
    return iter->second;
  }
  static const ParticleInfo& GetParticleInfo(std::string_view particle_name)
  {
    for(const auto& info : particles()) {
      if(info.second.name == particle_name)
        return info.second;
    }
    throw analysis::exception("Particle with name '%1%' not found.") % particle_name;
  }


  static float GetMass(int pdgId, float mass){
    static const std::set<int> pdgId_noMass {11,  12,  13,  14,  15,  16,  22,  111,     211,    311,     321,     421,     411   };
    if(pdgId_noMass.count(std::abs(pdgId))){
      return GetParticleInfo(pdgId).mass;
    }
    return mass;
  }

private:
  static std::map<int, ParticleInfo>& particles()
  {
    static std::map<int, ParticleInfo> p;
    return p;
  }


};
struct PdG {
  static int tau()
  {
    static const int pdg = ParticleDB::GetParticleInfo("tau-").pdgId;
    return pdg;
  }
  static int e()
  {
    static const int pdg = ParticleDB::GetParticleInfo("e-").pdgId;
    return pdg;
  }
  static int mu()
  {
    static const int pdg = ParticleDB::GetParticleInfo("mu-").pdgId;
    return pdg;
  }
};

bool isRelated(int potential_mother, int particle_idx, const RVecI& GenPart_genPartIdxMother)
{
  if(potential_mother == particle_idx) return true;
  const int mother_idx = GenPart_genPartIdxMother.at(particle_idx);
  if(mother_idx < 0) return false;
  return isRelated(potential_mother, mother_idx, GenPart_genPartIdxMother);
}

LorentzVectorM GetGenParticleVisibleP4(int tau_idx, const RVecF& pt, const RVecF& eta, const RVecF& phi,
                                      const RVecF& GenPart_mass, const RVecI& GenPart_genPartIdxMother,
                                      const RVecI& GenPart_pdgId, const RVecI& GenPart_status, const ROOT::VecOps::RVec<RVecI>& GenPart_daughters){
    LorentzVectorM sum(0.,0.,0.,0.);
    LorentzVectorM TauP4;
    RVecI tau_daughters = GenPart_daughters.at(tau_idx);

    for (int idx = 0; idx<tau_daughters.size(); idx++){
        int daughter_idx = tau_daughters[idx];
        if(GenPart_pdgId[daughter_idx] == 12 || GenPart_pdgId[daughter_idx] == 14 || GenPart_pdgId[daughter_idx] == 16) continue;
        if(GenPart_status[daughter_idx]!= 1 ) continue;
        LorentzVectorM current_particleP4 ;
        ParticleInfo particle_information = ParticleDB::GetParticleInfo(GenPart_pdgId[daughter_idx]);
        float particleMass = particle_information.mass>0? particle_information.mass : GenPart_mass[daughter_idx];
        current_particleP4=LorentzVectorM(pt[daughter_idx], eta[daughter_idx], phi[daughter_idx],particleMass);
        sum = sum + current_particleP4;
    }

    /*
    for(size_t particle_idx=0; particle_idx< GenPart_pdgId.size(); particle_idx++ ){
        if(GenPart_pdgId[particle_idx] == 12 || GenPart_pdgId[particle_idx] == 14 || GenPart_pdgId[particle_idx] == 16) continue;
        if(GenPart_status[particle_idx]!= 1 ) continue;

        bool isRelatedToTau = isRelated(tau_idx, particle_idx, GenPart_genPartIdxMother);
        if(isRelatedToTau){
            LorentzVectorM current_particleP4 ;
            ParticleInfo particle_information = ParticleDB::GetParticleInfo(GenPart_pdgId[particle_idx]);
            float particleMass = particle_information.mass>0? particle_information.mass : GenPart_mass[particle_idx];
            current_particleP4=LorentzVectorM(pt[particle_idx], eta[particle_idx], phi[particle_idx],particleMass);
            sum = sum + current_particleP4;
        }

    }
    */
    TauP4=LorentzVectorM(sum.Pt(), sum.Eta(), sum.Phi(), sum.M());
    return TauP4;

}



ROOT::VecOps::RVec<RVecI> GetDaughters(const RVecI& GenPart_genPartIdxMother ){
  ROOT::VecOps::RVec<RVecI> daughters(GenPart_genPartIdxMother.size());
  for (int part_idx =0; part_idx<GenPart_genPartIdxMother.size(); part_idx++){
    if(GenPart_genPartIdxMother[part_idx]>=0){
      daughters.at(GenPart_genPartIdxMother[part_idx]).push_back(part_idx);
    }
  }
  return daughters;
}
RVecI GetMothers(const int &part_idx, const RVecI& GenPart_genPartIdxMother ){
  RVecI mothers;
  int new_idx = part_idx;
  int mother_idx = GenPart_genPartIdxMother[new_idx];
  while(mother_idx >=0 ){
    mothers.push_back(mother_idx);
    new_idx = mother_idx;
    mother_idx = GenPart_genPartIdxMother[new_idx];

  }
  return mothers;

}

RVecI GetLastHadrons(const RVecI& GenPart_pdgId, const RVecI& GenPart_genPartIdxMother, const ROOT::VecOps::RVec<RVecI>& GenPart_daughters){

  RVecI lastHadrons;
  //if(evt!=905) return lastHadrons;

  for(int part_idx =0; part_idx<GenPart_pdgId.size(); part_idx++){
    RVecI mothers = GetMothers(part_idx, GenPart_genPartIdxMother);
    RVecI daughters = GenPart_daughters.at(part_idx);
    bool comesFrom_b = false;
    bool comesFrom_H = false;
    bool hasHadronsDaughters = false;

    for (auto& mother : mothers){
      if(abs(GenPart_pdgId[mother])==5) comesFrom_b=true;
      if(abs(GenPart_pdgId[mother])==25) comesFrom_H=true;
    }
    for (auto& daughter:daughters){
      ParticleInfo daughterInformation = ParticleDB::GetParticleInfo(GenPart_pdgId[daughter]);
      if(daughterInformation.type == "baryon" || daughterInformation.type == "meson"){
        hasHadronsDaughters = true;
      }
    }
    if(comesFrom_b && comesFrom_H && !hasHadronsDaughters){
      ParticleInfo lastHadronInformation = ParticleDB::GetParticleInfo(GenPart_pdgId[part_idx]);
      if(lastHadronInformation.type == "baryon" || lastHadronInformation.type == "meson"){
        lastHadrons.push_back(part_idx);
      }
    }
  }
  return lastHadrons;
}



void PrintDecayChainParticle(const ULong64_t evt, const int& mother_idx, const RVecI& GenPart_pdgId, const RVecI& GenPart_genPartIdxMother, const RVecI& GenPart_statusFlags, const RVecF& GenPart_pt, const RVecF& GenPart_eta, const RVecF& GenPart_phi, const RVecF& GenPart_mass, const RVecI& GenPart_status, const std::string pre, const ROOT::VecOps::RVec<RVecI>& GenPart_daughters, std::ostream& os)
{
  ParticleInfo particle_information = ParticleDB::GetParticleInfo(GenPart_pdgId[mother_idx]);
  RVecI daughters = GenPart_daughters.at(mother_idx);
  const float particleMass = particle_information.mass>0? particle_information.mass : GenPart_mass[mother_idx];
  const LorentzVectorM genParticle_momentum = LorentzVectorM(GenPart_pt[mother_idx], GenPart_eta[mother_idx], GenPart_phi[mother_idx],particleMass);
  int mother_mother_index = GenPart_genPartIdxMother[mother_idx];
  const auto flag = GenPart_statusFlags[mother_idx];

  os << particle_information.name      << " <" << GenPart_pdgId[mother_idx]
     << "> pt = " << genParticle_momentum.Pt()      << " eta = " << genParticle_momentum.Eta()
     << " phi = " << genParticle_momentum.Phi()     << " E = " << genParticle_momentum.E()
     << " m = "   << genParticle_momentum.M()       << " index = " << mother_idx
     << " flag = " << GetBinaryString(flag)     << " particleStatus = " << GenPart_status[mother_idx]
     << " charge = " << particle_information.charge << " type = " << particle_information.type
     << " mother_idx = " << mother_mother_index;
  os << "\n";

    if(daughters.size()==0 ) return;
    for(int d_idx =0; d_idx<daughters.size(); d_idx++) {
      int n = daughters[d_idx];
      os << pre << "+-> ";
      const char pre_first = d_idx == daughters.size() -1 ?  ' ' : '|';
      const std::string pre_d = pre + pre_first ;//+ "  ";
      PrintDecayChainParticle(evt, n, GenPart_pdgId, GenPart_genPartIdxMother, GenPart_statusFlags, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, GenPart_status, pre_d, GenPart_daughters, os);

  }
}
int PrintDecayChain(const ULong64_t evt, const RVecI& GenPart_pdgId, const RVecI& GenPart_genPartIdxMother, const RVecI& GenPart_statusFlags, const RVecF& GenPart_pt, const RVecF& GenPart_eta, const RVecF& GenPart_phi, const RVecF& GenPart_mass, const RVecI& GenPart_status,const ROOT::VecOps::RVec<RVecI>& GenPart_daughters,const std::string& outFile)
{
    std::ofstream out_file(outFile);
    for(int mother_idx =0; mother_idx<GenPart_pdgId.size(); mother_idx++){
      bool isStartingParticle = ( GenPart_genPartIdxMother[mother_idx] == -1);//&& (GenPart_pdgId[mother_idx] == 21 || GenPart_pdgId[mother_idx] == 9);
      if(!isStartingParticle) continue;
      PrintDecayChainParticle(evt, mother_idx, GenPart_pdgId, GenPart_genPartIdxMother, GenPart_statusFlags, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, GenPart_status, "", GenPart_daughters, out_file);
    }

return 0;
}
