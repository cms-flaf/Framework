#pragma once
#include <cmath>
#include <iostream>
#include <fstream>
#include <string>


using LorentzVectorM = ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>;
using RVecI = ROOT::RVecI;
using RVecS = ROOT::VecOps::RVec<size_t>;
using RVecUC = ROOT::VecOps::RVec<UChar_t>;
using RVecF = ROOT::RVecF;
using RVecB = ROOT::RVecB;
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


template<typename T>
T DeltaPhi(T phi1, T phi2) {
  return ROOT::Math::VectorUtil::Phi_mpi_pi(phi2 - phi1);
}
template<typename T>
T DeltaEta(T eta1, T eta2){
  return (eta1-eta2);
}

template<typename T>
T DeltaR(T eta1, T phi1, T eta2, T phi2) {
  T dphi = DeltaPhi(phi1, phi2);
  T deta = DeltaEta(eta1, eta2);
  return std::hypot(dphi, deta);
}


template<typename V>
RVecI ReorderObjects(const V& varToOrder, const RVecI& indices, size_t nMax=std::numeric_limits<size_t>::max())
{
  RVecI ordered_indices = indices;
  std::sort(ordered_indices.begin(), ordered_indices.end(), [&](int a, int b) {
    return varToOrder.at(a) > varToOrder.at(b);
  });
  const size_t n = std::min(ordered_indices.size(), nMax);
  ordered_indices.resize(n);
  return ordered_indices;
}/*
RVecI ReorderObjects(const RVecF& VarToOrder, const RVecI& index_vec, const unsigned nMax=std::numeric_limits<unsigned>::max()){

  RVecI reordered_jet_indices ;
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
}*/

template<typename T, int n_binary_places=std::numeric_limits<T>::digits>
std::string GetBinaryString(T x)
{
  std::bitset<n_binary_places> bs(x);
  std::ostringstream ss;
  ss << bs;
  return ss.str();
}
