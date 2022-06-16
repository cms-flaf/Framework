#pragma once
#include "AnalysisTools.h"
#include "GenTools.h"



struct HTTCand {
  std::array<LorentzVectorM, 2> leg_p4;
  std::array<int, 2> leg_pdg;
  std::array<int, 2> leg_index;
  std::array<int, 2> leg_charge;
  int channel=-1;

  int GetChannel() const
  {
    //std::cout<<"channel is "<<channel<<std::endl;
    if(channel>=0){
      return channel;
    }
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
  std::pair<int, int> GetAsPair(std::array<int, 2> legs){
    return std::make_pair(legs[0], legs[1]);
  }

};
