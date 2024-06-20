#pragma once

#include "AnalysisTools.h"
#include "GenTools.h"
#include "TextIO.h"
#include "HHCore.h"


void AssignHadronicWCand(WCand& cand, int cand_idx, const RVecI& GenPart_pdgId, const RVecVecI& GenPart_daughters,
                         const RVecF& GenPart_pt, const RVecF& GenPart_eta,
                         const RVecF& GenPart_phi, const RVecF& GenPart_mass)
{
  cand.leg_kind[0] = Wleg::Jet;
  cand.leg_kind[1] = Wleg::Jet;

  RVecI daughters = GenPart_daughters.at(cand_idx);
  cand.leg_index[0] = daughters[0];
  cand.leg_index[1] = daughters[1];

  cand.cand_p4 = GetP4(GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, cand_idx);

  cand.leg_p4[0] = GetP4(GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, cand.leg_index[0]);
  cand.leg_p4[1] = GetP4(GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, cand.leg_index[1]);

  cand.leg_vis_p4[0] = LorentzVectorM{};
  cand.leg_vis_p4[1] = LorentzVectorM{};
}

void AssignLeptonicWCand(WCand& cand, int cand_idx,
                         const RVecI& GenPart_pdgId, const RVecVecI& GenPart_daughters, const RVecI& GenPart_statusFlags,
                         const RVecF& GenPart_pt, const RVecF& GenPart_eta, const RVecF& GenPart_phi, const RVecF& GenPart_mass)
{
  cand.cand_p4 = GetP4(GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, cand_idx);
  cand.leg_vis_p4[0] = LorentzVectorM{}; // quick solution for now because we don't care about matching or whatever
  cand.leg_vis_p4[1] = LorentzVectorM{};

  RVecI daughters = GenPart_daughters.at(cand_idx);
  std::sort(daughters.begin(), daughters.end(), [&GenPart_pdgId](int x, int y){ return std::abs(GenPart_pdgId[x]) < std::abs(GenPart_pdgId[y]); });

  int lep = daughters[0];
  int nu = daughters[1];

  cand.leg_index[1] = nu;
  cand.leg_kind[1] = Wleg::Nu;
  cand.leg_p4[1] = GetP4(GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, nu);

  Wleg lep_leg = Wleg::None;
  lep = GetLastCopy(lep, GenPart_pdgId, GenPart_statusFlags, GenPart_daughters);
  int lep_pdgId = std::abs(GenPart_pdgId[lep]);
  RVecI lep_daughters = GenPart_daughters.at(lep);
  if (lep_daughters.empty())
  {
    if (lep_pdgId == PdG::e())
    {
      lep_leg = Wleg::PromptElectron;
    }
    else if (lep_pdgId == PdG::mu())
    {
      lep_leg = Wleg::PromptMuon;
    }
  }
  else
  {
    for (auto d: lep_daughters)
    {
      int daughter_pdgId = GenPart_pdgId[d];
      if(PdG::isNeutrino(daughter_pdgId))
      {
        continue;
      }
      else if (std::abs(daughter_pdgId) == PdG::e())
      {
        lep = d;
        lep_leg = Wleg::TauDecayedToElectron;
        break;
      }
      else if (std::abs(daughter_pdgId) == PdG::mu())
      {
        lep = d;
        lep_leg = Wleg::TauDecayedToMuon;
        break;
      }
      else if (std::abs(daughter_pdgId) <= PdG::b())
      {
        lep_leg = Wleg::TauDecayedToHadrons;
        break;
      }
      else
      {
        throw analysis::exception("Unknown Tau lepton decay type");
      }
    }
  }

  if (lep_leg == Wleg::None)
  {
    throw analysis::exception("Could not identify type of leptonic leg in W decay");
  }

  cand.leg_index[0] = lep;
  cand.leg_kind[0] = lep_leg;
  cand.leg_p4[0] = GetP4(GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, lep);
}

WCand GetGenWCand(int evt, int w_idx, const RVecI& GenPart_pdgId,
                  const RVecVecI& GenPart_daughters, const RVecI& GenPart_statusFlags,
                  const RVecF& GenPart_pt, const RVecF& GenPart_eta,
                  const RVecF& GenPart_phi, const RVecF& GenPart_mass)
{
  WCand res;
  RVecI daughters = GenPart_daughters.at(w_idx);
  if (daughters.size() != 2)
  {
    throw analysis::exception("W candidate wrong number of daughters");
  }

  // potentially problematic: what if W->gamma W ?
  bool decays_to_hadrons = std::all_of(daughters.begin(), daughters.end(), [&](int id){ return std::abs(id) <= PdG::b(); });
  if (decays_to_hadrons)
  {
    AssignHadronicWCand(res, w_idx, GenPart_pdgId, GenPart_daughters,
                        GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass);
  }

  bool decays_to_leptons = std::all_of(daughters.begin(), daughters.end(), [&](int id){ return PdG::isNeutrino(id) || PdG::isLepton(id); });
  if (decays_to_leptons)
  {
    AssignLeptonicWCand(res, w_idx, GenPart_pdgId, GenPart_daughters, GenPart_statusFlags,
                        GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass);
  }

  if (!decays_to_hadrons && !decays_to_leptons)
  {
    throw analysis::exception("Encountered neither leptonic nor hadronic W decay");
  }

  return res;
}

std::shared_ptr<HWWCand> GetGenHWWCandidate(int evt, const RVecI& GenPart_pdgId,
                                            const RVecVecI& GenPart_daughters, const RVecI& GenPart_statusFlags,
                                            const RVecF& GenPart_pt, const RVecF& GenPart_eta,
                                            const RVecF& GenPart_phi, const RVecF& GenPart_mass,
                                            bool throw_error_if_not_found)
{
  try
  {
    int sz = GenPart_pdgId.size();
    std::set<int> HWW_indices;
    for (int i = 0; i < sz; ++i)
    {
      const GenStatusFlags status(GenPart_statusFlags.at(i));
      bool is_higgs = GenPart_pdgId[i] == PdG::Higgs();
      if (!(is_higgs && status.isLastCopy()))
      {
        continue;
      }

      auto const& daughters = GenPart_daughters.at(i);
      auto IsW = [&](int id){ return id == PdG::Wplus() || id == PdG::Wminus(); };
      int n_W_from_H = std::count_if(daughters.begin(), daughters.end(), IsW);
      if (n_W_from_H != 2)
      {
        throw analysis::exception("Invalid H->WW decay: n_W_from_H = %2%") % n_W_from_H;
      }

      HWW_indices.insert(i);
    }

    if(HWW_indices.empty())
    {
      throw analysis::exception("H->WW not found.");
    }
    if(HWW_indices.size() != 1)
    {
      throw analysis::exception("Multiple H->WW candidates.");
    }

    int const HWW_index = *HWW_indices.begin();
    HWWCand HWW_cand;

    HWW_cand.cand_p4 = GetP4(GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, HWW_index);

    RVecI W_from_H = GenPart_daughters.at(HWW_index);
    int W1_idx = W_from_H[0];
    int W2_idx = W_from_H[2];

    HWW_cand.legs[0] = GetGenWCand(evt, W1_idx, GenPart_pdgId, GenPart_daughters, GenPart_statusFlags,
                                   GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass);
    HWW_cand.legs[1] = GetGenWCand(evt, W2_idx, GenPart_pdgId, GenPart_daughters, GenPart_statusFlags,
                                   GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass);

    return std::make_shared<HWWCand>(HWW_cand);
  }
  catch (analysis::exception& e)
  {
    if(throw_error_if_not_found)
    {
      throw analysis::exception("GetGenHWWCandidate (event=%1%): %2%") % evt % e.message();
    }
    return std::make_shared<HWWCand>();
  }
}

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

