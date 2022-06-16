#pragma once
#include "AnalysisTools.h"
#include "GenTools.h"



struct HTTCand {
  std::array<LorentzVectorM, 2> leg_p4;
  std::array<Leg, 2> leg_type;
  std::array<int, 2> leg_index;
  std::array<int, 2> leg_charge;

  Channel channel() const { return LegsToChannel(leg_type[0], leg_type[1]); }
};
