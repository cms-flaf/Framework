#pragma once
#include "AnalysisTools.h"
#include "GenTools.h"



struct HTTCand {
  std::array<LorentzVectorM, 2> leg_p4;
  std::array<Leg, 2> leg_type;
  std::array<int, 2> leg_index;
  std::array<int, 2> leg_charge;

  Channel GetChannel() const
  {
    return LegsToChannel(leg_type[0], leg_type[1]);
    static const std::map<std::pair<int, int>, int> pdg_to_channel = {
      { { PdG::e(), PdG::tau() }, Channel::eTau },
      { { PdG::mu(), PdG::tau() }, Channel::muTau },
      { { PdG::tau(), PdG::tau() }, Channel::tauTau },
      { { PdG::mu(), PdG::mu() }, Channel::muMu },
      { { PdG::e(), PdG::mu() }, Channel::eMu },
      { { PdG::e(), PdG::e() }, Channel::eE },
    };
    const auto key = std::make_pair(std::abs(leg_pdg[0]), std::abs(leg_pdg[1]));
    const auto iter = pdg_to_channel.find(key);
    if(iter == pdg_to_channel.end())
      throw analysis::exception("Unknown channel. leg_pdg = %1%, %2%") % leg_pdg[0] % leg_pdg[1];
    return iter->second;
  }

};
