#pragma once

#include "AnalysisTools.h"
#include "GenTools.h"

struct HTTCand {
  static constexpr size_t n_legs = 2;
  std::array<Leg, n_legs> leg_type;
  std::array<int, n_legs> leg_index;
  std::array<LorentzVectorM, n_legs> leg_p4;
  std::array<int, n_legs> leg_charge; 
  std::array<float, n_legs> leg_rawIso;

  Channel channel() const { return LegsToChannel(leg_type[0], leg_type[1]); }

  bool operator==(const HTTCand& other) const
  {
    for(size_t idx = 0; idx < n_legs; ++idx) {
      if(leg_type[idx] != other.leg_type[idx] || leg_index[idx] != other.leg_index[idx])
        return false;
    }
    return true;
  }
  bool isLegS(int obj_index, Leg leg) const { 
    for(size_t idx = 0; idx < n_legs; ++idx){ 
      if(leg_type[idx] == leg && leg_index[idx]==obj_index){
        return true;
      }
    }
    return false;
  }

  RVecB isLeg(const RVecI &obj_vec, Leg leg) const {
    RVecB isLeg_vector(obj_vec.size(), false);
    for(size_t obj_idx=0; obj_idx<obj_vec.size(); obj_idx++){
      isLeg_vector[obj_idx] = isLegS(obj_vec[obj_idx], leg);
    }
    return isLeg_vector;
  }
};

struct HbbCand {
  static constexpr size_t n_legs = 2; 
  std::array<int, n_legs> leg_index;
  std::array<LorentzVectorM, n_legs> leg_p4; 
};


std::ostream& operator<<(std::ostream& os, const HTTCand& cand)
{
  for(size_t n = 0; n < HTTCand::n_legs; ++n) {
    os << "leg" << n+1 << ":"
       << " type=" << static_cast<int>(cand.leg_type[n])
       << " index=" << cand.leg_index[n]
       << " (pt,eta,phi,m)=" << cand.leg_p4[n]
       << " charge=" << cand.leg_charge[n]
       << " rawIso=" << cand.leg_rawIso[n] << "\n";
  }
  return os;
}
