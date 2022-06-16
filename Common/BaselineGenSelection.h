#pragma once

#include "AnalysisTools.h"
#include "GenTools.h"
#include "TextIO.h"
#include "HHCore.h"

HTTCand GetGenHTTCandidate(int evt, const RVecI& GenPart_pdgId,
                           const RVecVecI& GenPart_daughters, const RVecI& GenPart_statusFlags,
                           const RVecF& GenPart_pt, const RVecF& GenPart_eta,
                           const RVecF& GenPart_phi, const RVecF& GenPart_mass)
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
    HTTCand htt_cand;
    int leg_idx = 0;
    for(int tau_idx : GenPart_daughters.at(htt_index)) {
        if(std::abs(GenPart_pdgId.at(tau_idx)) != PdG::tau()) continue;
        tau_idx = GetLastCopy(tau_idx, GenPart_pdgId, GenPart_statusFlags, GenPart_daughters);

        int lepton_idx = tau_idx;
        for(int tau_daughter : GenPart_daughters.at(tau_idx))
        {
        const GenStatusFlags status(GenPart_statusFlags.at(tau_daughter));
        if(!((GenPart_pdgId[tau_daughter] == PdG::e() || GenPart_pdgId[tau_daughter] == PdG::mu())
            && status.isDirectPromptTauDecayProduct())) continue;
        if(lepton_idx != tau_idx)
            throw analysis::exception("Invalid tau decay. tau_idx = %1%") % tau_idx;
        lepton_idx = tau_daughter;
        }
        lepton_idx = GetLastCopy(lepton_idx, GenPart_pdgId, GenPart_statusFlags, GenPart_daughters);
        htt_cand.leg_index.at(leg_idx) = lepton_idx;
        ++leg_idx;
    }
    if(std::abs(GenPart_pdgId.at(htt_cand.leg_index.at(0))) > std::abs(GenPart_pdgId.at(htt_cand.leg_index.at(1))))
        std::swap(htt_cand.leg_index.at(0), htt_cand.leg_index.at(1));

    for(leg_idx = 0; leg_idx < htt_cand.leg_index.size(); ++ leg_idx) {
        const int genPart_index = htt_cand.leg_index.at(leg_idx);
        const int genPart_pdg = GenPart_pdgId.at(genPart_index);
        const auto& genPart_info = ParticleDB::GetParticleInfo(genPart_pdg);
        htt_cand.leg_type[leg_idx] = PdGToLeg(genPart_pdg);
        htt_cand.leg_charge[leg_idx] = genPart_info.charge;
        htt_cand.leg_p4[leg_idx] = GetVisibleP4(genPart_index, GenPart_pdgId, GenPart_daughters,
                                                GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass);
    }

    return htt_cand;
  } catch(analysis::exception& e) {
    throw analysis::exception("GetGenHTTCandidate (event=%1%): %2%") % evt % e.message();
  }
}

bool PassAcceptance(const HTTCand& HTT_Cand){
    for(size_t i =0; i<HTT_Cand.leg_p4.size(); i++){
        if(!(HTT_Cand.leg_p4.at(i).pt()>20 && std::abs(HTT_Cand.leg_p4.at(i).eta())<2.3 )){
            return false;
        }
    }
    return true;
}
