#include <math.h>
#include <iostream>
#include <fstream>
#include <string>
#pragma once

using LorentzVectorM = ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>;
using vec_i = ROOT::VecOps::RVec<int>;
using vec_s = ROOT::VecOps::RVec<size_t>;
using vec_f = ROOT::VecOps::RVec<float>;
using vec_b = ROOT::VecOps::RVec<bool>;
using vec_uc = ROOT::VecOps::RVec<unsigned char>;

namespace Channel{
    enum {
        eTau = 0,
        muTau = 1,
        tauTau = 2,
        eMu = 3,
        eE =4,
        muMu = 5
    } ;
}


struct EvtInfo{
    int channel;
    std::array<LorentzVectorM,2> leg_p4;
};

struct ParticleInfo{
  int pdgId;
  int charge;
  std::string name;
  std::string type;
};

float DeltaPhi(Float_t phi1, Float_t phi2){
    static constexpr float pi = M_PI;
    float dphi = phi1 - phi2;
    if(dphi > pi){
        dphi -= 2*pi;
    }
    else if(dphi <= -pi){
        dphi += 2*pi;
    }
    return dphi;
}

float DeltaEta(Float_t eta1, Float_t eta2){
  return (eta1-eta2);
}

float DeltaR(Float_t phi1,Float_t eta1,Float_t phi2,Float_t eta2) {
  float dphi = DeltaPhi(phi1, phi2);
  float deta = DeltaEta(eta1, eta2);
  return (std::sqrt(deta * deta + dphi * dphi));
}

vec_i ReorderObjects(const vec_f& VarToOrder, const vec_i& index_vec, const unsigned nMax=std::numeric_limits<unsigned>::max()){

  vec_i reordered_jet_indices ;
  while(reordered_jet_indices.size()<nMax){
    float pt_max = 0;
    int i_max = -1;
    for(auto&i :index_vec){
      //std::cout << "i " << i << "\t i max "<< i_max<< "\tVarToOrder " << VarToOrder.at(i) << "\tpt_max " << pt_max << std::endl;
      if(std::find(reordered_jet_indices.begin(), reordered_jet_indices.end(), i)!=reordered_jet_indices.end()) continue;
      if(VarToOrder.at(i)>pt_max){
        pt_max=VarToOrder.at(i);
        i_max = i;
      }
    }
    if(i_max>=0){
      reordered_jet_indices.push_back(i_max);
      pt_max = 0;
      i_max = -1;
    }
  }
  return reordered_jet_indices;
}

std::string FromDecimalToBinary(const int& decimalNumber)
{
    int n=decimalNumber;
    std::string r;
    while(n!=0) {r=(n%2==0 ?"0":"1")+r; n/=2;}
    return r;
}
