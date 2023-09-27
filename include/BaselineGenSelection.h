#pragma once

#include "AnalysisTools.h"
#include "GenTools.h"
#include "TextIO.h"
#include "HHCore.h"

std::shared_ptr<HTTCand<2>> GetGenHTTCandidate(int evt, const RVecI& GenPart_pdgId,
                                            const RVecVecI& GenPart_daughters, const RVecI& GenPart_statusFlags,
                                            const RVecF& GenPart_pt, const RVecF& GenPart_eta,
                                            const RVecF& GenPart_phi, const RVecF& GenPart_mass,
                                            bool throw_error_if_not_found)
{
  try {
    std::set<int> htt_indices;
    for(int n = 0; n < GenPart_pdgId.size(); ++n) {
        const GenStatusFlags status(GenPart_statusFlags.at(n));
        if(!(GenPart_pdgId[n] == PdG::Higgs() && status.isLastCopy())) continue;
        const auto& daughters = GenPart_daughters.at(n);
        int n_tau_daughters = std::count_if(daughters.begin(), daughters.end(), [&](int idx) {
        return std::abs(GenPart_pdgId.at(idx)) == PdG::tau();
        });
        if(n_tau_daughters == 0) continue;
        if(n_tau_daughters != 2)
        throw analysis::exception("Invalid H->tautau decay. n_tau_daughters = %1%, higgs_idx = %2%")
            % n_tau_daughters % n;
        htt_indices.insert(n);
    }
    if(htt_indices.empty())
        throw analysis::exception("H->tautau not found.");
    if(htt_indices.size() != 1)
        throw analysis::exception("Multiple H->tautau candidates.");
    const int htt_index = *htt_indices.begin();
    HTTCand<2> htt_cand;
    int leg_idx = 0;
    for(int tau_idx : GenPart_daughters.at(htt_index)) {
        if(std::abs(GenPart_pdgId.at(tau_idx)) != PdG::tau()) continue;
        tau_idx = GetLastCopy(tau_idx, GenPart_pdgId, GenPart_statusFlags, GenPart_daughters);

        int lepton_idx = tau_idx;
        size_t n_neutrinos = 0;
        for(int tau_daughter : GenPart_daughters.at(tau_idx)) {
            if(PdG::isNeutrino(GenPart_pdgId[tau_daughter]))
                ++n_neutrinos;
            const GenStatusFlags status(GenPart_statusFlags.at(tau_daughter));
            const int tau_daughter_pdg = std::abs(GenPart_pdgId[tau_daughter]);
            if(!((tau_daughter_pdg == PdG::e() || tau_daughter_pdg == PdG::mu())
                && status.isDirectPromptTauDecayProduct())) continue;
            if(lepton_idx != tau_idx)
                throw analysis::exception("Invalid tau decay. tau_idx = %1%") % tau_idx;
            lepton_idx = tau_daughter;
        }

        if(!((lepton_idx == tau_idx && n_neutrinos == 1) || (lepton_idx != tau_idx && n_neutrinos == 2)))
            throw analysis::exception("Invalid number of neutrinos = %1% in tau decay. tau_idx = %2%")
                % n_neutrinos % tau_idx;

        lepton_idx = GetLastCopy(lepton_idx, GenPart_pdgId, GenPart_statusFlags, GenPart_daughters);
        htt_cand.leg_index.at(leg_idx) = lepton_idx;
        ++leg_idx;
    }
    int leg0_pdg = std::abs(GenPart_pdgId.at(htt_cand.leg_index.at(0)));
    int leg1_pdg = std::abs(GenPart_pdgId.at(htt_cand.leg_index.at(1)));
    if(leg0_pdg > leg1_pdg || (leg0_pdg == leg1_pdg
            && GenPart_pt.at(htt_cand.leg_index.at(0)) < GenPart_pt.at(htt_cand.leg_index.at(1))))
        std::swap(htt_cand.leg_index.at(0), htt_cand.leg_index.at(1));

    for(leg_idx = 0; leg_idx < htt_cand.leg_index.size(); ++leg_idx) {
        const int genPart_index = htt_cand.leg_index.at(leg_idx);
        const int genPart_pdg = GenPart_pdgId.at(genPart_index);
        const auto& genPart_info = ParticleDB::GetParticleInfo(genPart_pdg);
        htt_cand.leg_type[leg_idx] = PdGToLeg(genPart_pdg);
        htt_cand.leg_charge[leg_idx] = genPart_info.charge;
        htt_cand.leg_p4[leg_idx] = GetVisibleP4(genPart_index, GenPart_pdgId, GenPart_daughters,
                                                GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass);
    }

    return std::make_shared<HTTCand<2>>(htt_cand);
  } catch(analysis::exception& e) {
    if(throw_error_if_not_found)
      throw analysis::exception("GetGenHTTCandidate (event=%1%): %2%") % evt % e.message();
    return std::make_shared<HTTCand<2>>();
  }
}

int GetGenHBBIndex(int evt, const RVecI& GenPart_pdgId,
                           const RVecVecI& GenPart_daughters, const RVecI& GenPart_statusFlags)
{
  try {
    std::set<int> hbb_indices;
    for(int n = 0; n < GenPart_pdgId.size(); ++n) {
        const GenStatusFlags status(GenPart_statusFlags.at(n));
        if(!(GenPart_pdgId[n] == PdG::Higgs() && status.isLastCopy())) continue;
        const auto& daughters = GenPart_daughters.at(n);
        int n_b_daughters = std::count_if(daughters.begin(), daughters.end(), [&](int idx) {
        return std::abs(GenPart_pdgId.at(idx)) == PdG::b();
        });
        if(n_b_daughters == 0) continue;
        if(n_b_daughters != 2)
        throw analysis::exception("Invalid H->bb decay. n_b_daughters = %1%, higgs_idx = %2%")
            % n_b_daughters % n;
        hbb_indices.insert(n);
    }
    if(hbb_indices.empty())
        throw analysis::exception("H->bb not found.");
    if(hbb_indices.size() != 1)
        throw analysis::exception("Multiple H->bb candidates.");
    const int hbb_index = *hbb_indices.begin();
    return hbb_index;
  }
  catch(analysis::exception& e) {
      throw analysis::exception("GetGenHBBCandidate (event=%1%): %2%") % evt % e.message();
    }
}



bool PassGenAcceptance(const HTTCand<2>& HTT_Cand){
    for(size_t i = 0; i < HTT_Cand.leg_p4.size(); ++i){
        if(!(HTT_Cand.leg_p4.at(i).pt()>20 && std::abs(HTT_Cand.leg_p4.at(i).eta())<2.3 )){
            return false;
        }
    }
    return true;
}

RVecB FindTwoJetsClosestToMPV(float mpv, const RVecLV& GenJet_p4, const RVecB& pre_sel)
{
  int i_min=-1, j_min = -1;
  float delta_min = std::numeric_limits<float>::infinity();
  for(int i = 0; i < GenJet_p4.size(); i++) {
    if(!pre_sel[i]) continue;
    for(int j = 0; j < i; j++) {
      if(!pre_sel[j]) continue;
      const float inv_mass = (GenJet_p4[i]+GenJet_p4[j]).M();
      const float delta_mass = std::abs(inv_mass - mpv);
      if(delta_mass < delta_min) {
        i_min = i;
        j_min = j;
        delta_min = delta_mass;
      }
    }
  }
  RVecB result(pre_sel.size(), false);
  if(i_min >= 0 && j_min>=0) {
    result[i_min] = true;
    result[j_min] = true;
  }
  return result;
}
RVecB FindGenJetAK8(const RVecF& GenJetAK8_mass, const RVecB& pre_sel){

  int i_max = -1;
  float max_mass = -1.;
  for(int i = 0; i < GenJetAK8_mass.size(); i++) {
    if(!pre_sel[i]) continue;
    if(GenJetAK8_mass[i]>max_mass){
      i_max=i;
      max_mass = GenJetAK8_mass[i];
    }
  }
  RVecB result(pre_sel.size(), false);
  if(i_max >= 0 ) {
    result[i_max] = true;
  }
  return result;
}

