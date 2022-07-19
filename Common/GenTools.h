#pragma once

#include "AnalysisTools.h"
#include "exception.h"
#include "TextIO.h"
#include "GenStatusFlags.h"


struct ParticleInfo {
  int pdgId;
  int charge;
  std::string name;
  std::string type;
  int mass{-1};
};

class ParticleDB {
public:
  static void Initialize(const std::string_view inputFile) {
    std::ifstream file (std::string(inputFile).c_str(), ios::in );
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
       particles()[currentInfo.pdgId] = currentInfo;
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

  static float GetMass(int pdgId, float mass)
  {
    static const std::set<int> pdgId_noMass { 11, 12, 13, 14, 15, 16, 22, 111, 211, 311, 321, 421, 411 };
    if(pdgId_noMass.count(std::abs(pdgId)))
      return GetParticleInfo(pdgId).mass;
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
  static int e() { static const int pdg = ParticleDB::GetParticleInfo("e-").pdgId; return pdg; }
  static int mu() { static const int pdg = ParticleDB::GetParticleInfo("mu-").pdgId; return pdg; }
  static int tau() { static const int pdg = ParticleDB::GetParticleInfo("tau-").pdgId; return pdg; }
  static int Higgs() { static const int pdg = ParticleDB::GetParticleInfo("h0").pdgId; return pdg; }
  static int nu_e() { static const int pdg = ParticleDB::GetParticleInfo("nu_e").pdgId; return pdg; }
  static int nu_mu() { static const int pdg = ParticleDB::GetParticleInfo("nu_mu").pdgId; return pdg; }
  static int nu_tau() { static const int pdg = ParticleDB::GetParticleInfo("nu_tau").pdgId; return pdg; }

  static bool isNeutrino(int pdg)
  {
    static const std::set<int> neutrinos = { nu_e(), nu_mu(), nu_tau() };
    return neutrinos.count(std::abs(pdg)) > 0;
  }
};

inline Leg PdGToLeg(int pdg)
{
    static const std::map<int, Leg> pdg_to_leg = {
      { PdG::e(), Leg::e },
      { PdG::mu(), Leg::mu },
      { PdG::tau(), Leg::tau },
    };
    const auto iter = pdg_to_leg.find(std::abs(pdg));
    if(iter == pdg_to_leg.end())
      throw analysis::exception("Unknown leg type. leg_pdg = %1%.") % pdg;
    return iter->second;
}

bool isRelated(int potential_mother, int particle_idx, const RVecI& GenPart_genPartIdxMother)
{
  if(potential_mother == particle_idx) return true;
  const int mother_idx = GenPart_genPartIdxMother.at(particle_idx);
  if(mother_idx < 0) return false;
  return isRelated(potential_mother, mother_idx, GenPart_genPartIdxMother);
}

int GetLastCopy(int genPart, const RVecI& GenPart_pdgId, const RVecI& GenPart_statusFlags,
                const RVecVecI& GenPart_daughters)
{
  GenStatusFlags pFlags(GenPart_statusFlags.at(genPart));
  int genPart_copy = genPart;
  while(true) {
    GenStatusFlags flags(GenPart_statusFlags.at(genPart_copy));
    if(flags.isLastCopy())
      return genPart_copy;
    std::vector<int> copies;
    for(int daughter : GenPart_daughters.at(genPart_copy)) {
       GenStatusFlags daughter_flags(GenPart_statusFlags.at(daughter));
      if(GenPart_pdgId.at(daughter) == GenPart_pdgId.at(genPart_copy) && daughter_flags.fromHardProcess())
        copies.push_back(daughter);
    }
    if(copies.size() != 1) break;
    genPart_copy = copies[0];
  }
  throw analysis::exception("Last copy not found.");
}

LorentzVectorM GetVisibleP4(int genPart, const RVecI& GenPart_pdgId, const RVecVecI& GenPart_daughters,
                            const RVecF& GenPart_pt, const RVecF& GenPart_eta, const RVecF& GenPart_phi,
                            const RVecF& GenPart_mass) {
    LorentzVectorXYZ sum(0.,0.,0.,0.);
    std::function<void(int)> addParticle;
    addParticle = [&](int p) {
      if(GenPart_daughters.at(p).empty() && !PdG::isNeutrino(p)) {
        sum += LorentzVectorM(GenPart_pt.at(p), GenPart_eta.at(p), GenPart_phi.at(p),
                              ParticleDB::GetMass(p, GenPart_mass.at(p)));
      }
      for(int daughter : GenPart_daughters.at(p))
        addParticle(daughter);
    };
    addParticle(genPart);
    return LorentzVectorM(sum);
}


LorentzVectorM GetGenParticleVisibleP4(int tau_idx, const RVecF& pt, const RVecF& eta, const RVecF& phi,
                                      const RVecF& GenPart_mass, const RVecI& GenPart_genPartIdxMother,
                                      const RVecI& GenPart_pdgId, const RVecI& GenPart_status, const RVecVecI& GenPart_daughters){
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



void PrintDecayChainParticle(ULong64_t evt, int genPart_idx, const RVecI& GenPart_pdgId,
                             const RVecI& GenPart_genPartIdxMother, const RVecI& GenPart_statusFlags,
                             const RVecF& GenPart_pt, const RVecF& GenPart_eta, const RVecF& GenPart_phi,
                             const RVecF& GenPart_mass, const RVecI& GenPart_status, const std::string pre,
                             const ROOT::VecOps::RVec<RVecI>& GenPart_daughters, std::ostream& os)
{
  const ParticleInfo& particle_information = ParticleDB::GetParticleInfo(GenPart_pdgId[genPart_idx]);
  const float particleMass = ParticleDB::GetMass(GenPart_pdgId[genPart_idx], GenPart_mass[genPart_idx]);
  const LorentzVectorM genParticle_momentum(GenPart_pt[genPart_idx], GenPart_eta[genPart_idx],
                                            GenPart_phi[genPart_idx], particleMass);

  os << particle_information.name
     << " <" << GenPart_pdgId[genPart_idx] << '>'
     << " pt = " << genParticle_momentum.pt()
     << " eta = " << genParticle_momentum.eta()
     << " phi = " << genParticle_momentum.phi()
     << " E = " << genParticle_momentum.energy()
     << " m = "   << genParticle_momentum.mass()
     << " index = " << genPart_idx
     << " flag = " << GetBinaryString<int, 15>(GenPart_statusFlags[genPart_idx])
     << " status = " << GenPart_status[genPart_idx]
     << " charge = " << particle_information.charge
     << " type = " << particle_information.type
     << '\n';

  const RVecI& daughters = GenPart_daughters.at(genPart_idx);
  for(int d_idx = 0; d_idx < daughters.size(); ++d_idx) {
    const int n = daughters[d_idx];
    os << pre << "+-> ";
    const char pre_first = d_idx == daughters.size() - 1 ?  ' ' : '|';
    const std::string pre_d = pre + pre_first ;
    PrintDecayChainParticle(evt, n, GenPart_pdgId, GenPart_genPartIdxMother, GenPart_statusFlags, GenPart_pt,
                            GenPart_eta, GenPart_phi, GenPart_mass, GenPart_status, pre_d, GenPart_daughters, os);
  }
}

int PrintDecayChain(ULong64_t evt, const RVecI& GenPart_pdgId, const RVecI& GenPart_genPartIdxMother,
                    const RVecI& GenPart_statusFlags, const RVecF& GenPart_pt, const RVecF& GenPart_eta,
                    const RVecF& GenPart_phi, const RVecF& GenPart_mass, const RVecI& GenPart_status,
                    const RVecVecI& GenPart_daughters, const std::string& outFile)
{
  std::ofstream out_file(outFile, std::ios_base::app);
  out_file << "event=" << evt << '\n';
  for(int genPart_idx = 0; genPart_idx < GenPart_pdgId.size(); ++genPart_idx) {
    if(GenPart_genPartIdxMother[genPart_idx] == -1)
      PrintDecayChainParticle(evt, genPart_idx, GenPart_pdgId, GenPart_genPartIdxMother, GenPart_statusFlags, GenPart_pt,
                              GenPart_eta, GenPart_phi, GenPart_mass, GenPart_status, "", GenPart_daughters, out_file);
  }
  return 0;
}

float InvMassByIndices(const RVecI &indices, const RVecLV& GenPart_p4,const RVecI& GenPart_pdgId,bool WantOnlySpecificParticle, int ParticleType=5){
    LorentzVectorM genParticle_Tot_momentum;
    //if(evt!=905) return 0.;
    for(size_t i = 0 ;i<indices.size(); i++){
      auto part_idx = indices.at(i);
      if(WantOnlySpecificParticle && abs(GenPart_pdgId[part_idx])!=ParticleType) continue;
      ParticleInfo particle_information = ParticleDB::GetParticleInfo(GenPart_pdgId[part_idx]);
      const float particleMass = particle_information.mass>0? particle_information.mass : GenPart_p4[part_idx].M();
      genParticle_Tot_momentum+=LorentzVectorM(GenPart_p4[part_idx].Pt(), GenPart_p4[part_idx].Eta(), GenPart_p4[part_idx].Phi(),particleMass);
    }
    return genParticle_Tot_momentum.M();
}


float InvMassByFalvour(const RVecLV& GenPart_p4,const RVecI& GenJet_partonFlavour,bool WantOnlySpecificParticle, int ParticleType=5){
    LorentzVectorM genParticle_Tot_momentum;
    //if(evt!=905) return 0.;
    for(size_t i = 0 ;i<GenPart_p4.size(); i++){
      auto part_idx = i;
      if(WantOnlySpecificParticle && abs(GenJet_partonFlavour[part_idx])!=ParticleType) continue;
      ParticleInfo particle_information = ParticleDB::GetParticleInfo(GenJet_partonFlavour[part_idx]);
      const float particleMass = particle_information.mass>0? particle_information.mass : GenPart_p4[part_idx].M();
      genParticle_Tot_momentum+=LorentzVectorM(GenPart_p4[part_idx].Pt(), GenPart_p4[part_idx].Eta(), GenPart_p4[part_idx].Phi(),particleMass);
    }
    return genParticle_Tot_momentum.M();
}
