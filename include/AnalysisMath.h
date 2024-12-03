#pragma once
#include <cmath>
#include <iostream>
#include <fstream>
#include <string>

using LorentzVectorXYZ = ROOT::Math::LorentzVector<ROOT::Math::PxPyPzE4D<double>>;
using LorentzVectorM = ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>;
using LorentzVectorE = ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiE4D<double>>;
using RVecI = ROOT::VecOps::RVec<int>;
using RVecS = ROOT::VecOps::RVec<size_t>;
using RVecUC = ROOT::VecOps::RVec<UChar_t>;
using RVecF = ROOT::VecOps::RVec<float>;
using RVecB = ROOT::VecOps::RVec<bool>;
using RVecVecI = ROOT::VecOps::RVec<RVecI>;
using RVecLV = ROOT::VecOps::RVec<LorentzVectorM>;
using RVecSetInt = ROOT::VecOps::RVec<std::set<int>>;



//see AN-13-178
template<typename LVector1, typename LVector2>
double Calculate_MT(const LVector1& lepton_p4, const LVector2& met_p4)
{
    const double delta_phi = TVector2::Phi_mpi_pi(lepton_p4.Phi() - met_p4.Phi());
    return std::sqrt( 2.0 * lepton_p4.Pt() * met_p4.Pt() * ( 1.0 - std::cos(delta_phi) ) );
}

template<typename LVector1, typename LVector2, typename LVector3>
double Calculate_TotalMT(const LVector1& lepton1_p4, const LVector2& lepton2_p4, const LVector3& met_p4)
{
    const double mt_1 = Calculate_MT(lepton1_p4, met_p4);
    const double mt_2 = Calculate_MT(lepton2_p4, met_p4);
    const double mt_ll = Calculate_MT(lepton1_p4, lepton2_p4);
    return std::sqrt(std::pow(mt_1, 2) + std::pow(mt_2, 2) + std::pow(mt_ll, 2));
}
