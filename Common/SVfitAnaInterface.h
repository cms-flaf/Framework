/*! A wrapper for SVfit code. */

#pragma once

#include "RootExt.h"
#include "TauAnalysis/ClassicSVfit/interface/ClassicSVfit.h"
#include "TauAnalysis/ClassicSVfit/interface/MeasuredTauLepton.h"
#include "TauAnalysis/ClassicSVfit/interface/svFitHistogramAdapter.h"
#include "AnalysisTools.h"
namespace sv_fit {
struct FitResults {
    bool has_valid_momentum;
    LorentzVectorM momentum;
    LorentzVectorM momentum_error;
    double transverseMass;
    double transverseMass_error;
    FitResults() :
        has_valid_momentum(false), transverseMass(std::numeric_limits<double>::lowest()),
        transverseMass_error(std::numeric_limits<double>::lowest()) {}
    FitResults(bool _has_valid_momentum, LorentzVectorM _momentum, LorentzVectorM _momentum_error,
               double _transverseMass, double _transverseMass_error) :
        has_valid_momentum(_has_valid_momentum), momentum(_momentum), momentum_error(_momentum_error),
        transverseMass(_transverseMass), transverseMass_error(_transverseMass_error) {}
};
class FitProducer {
public:
    static classic_svFit::MeasuredTauLepton CreateMeasuredLepton(const LorentzVectorM& momentum, Leg leg_type,
                                                                 int decay_mode)
    {
        double preciseVisMass = momentum.mass();
        classic_svFit::MeasuredTauLepton::kDecayType decay_type;
        if(leg_type == Leg::e) {
            static const double minVisMass = classic_svFit::electronMass, maxVisMass = minVisMass;
            preciseVisMass = std::clamp(preciseVisMass, minVisMass, maxVisMass);
            decay_type = classic_svFit::MeasuredTauLepton::kTauToElecDecay;
            decay_mode = -1;
        } else if(leg_type == Leg::mu) {
            decay_type = classic_svFit::MeasuredTauLepton::kTauToMuDecay;
            decay_mode = -1;
        } else if(leg_type == Leg::tau){
            const double minVisMass = decay_mode == 0 ? classic_svFit::chargedPionMass : 0.3;
            const double maxVisMass = decay_mode == 0 ? classic_svFit::chargedPionMass : 1.5;
            preciseVisMass = std::clamp(preciseVisMass, minVisMass, maxVisMass);
            decay_type = classic_svFit::MeasuredTauLepton::kTauToHadDecay;
        } else {
            throw std::runtime_error("Leg Type not supported for SVFitAnaInterface.");
        }
        return classic_svFit::MeasuredTauLepton(decay_type, momentum.Pt(), momentum.Eta(), momentum.Phi(),
                                                preciseVisMass, decay_mode);
    }
    static FitResults Fit(const LorentzVectorM& tau1_p4, Leg tau1_leg_type, int tau1_decay_mode,
                          const LorentzVectorM& tau2_p4, Leg tau2_leg_type, int tau2_decay_mode,
                          const LorentzVectorM& met_p4, double MET_cov_00, double MET_cov_01, double MET_cov_11,
                          int verbosity = 0)
    {
        static const auto init = []() { TH1::AddDirectory(false); return true; };
        static const bool initialized = init();
        (void) initialized;
        const std::vector<classic_svFit::MeasuredTauLepton> measured_leptons = {
            CreateMeasuredLepton(tau1_p4, tau1_leg_type, tau1_decay_mode),
            CreateMeasuredLepton(tau2_p4, tau2_leg_type, tau2_decay_mode)
        };
        TMatrixD met_cov_t(2, 2);
        met_cov_t(0, 0) = MET_cov_00;
        met_cov_t(0, 1) = MET_cov_01;
        met_cov_t(1, 0) = MET_cov_01;
        met_cov_t(1, 1) = MET_cov_11;
        ClassicSVfit algo(verbosity);
        algo.addLogM_fixed(false);
        algo.addLogM_dynamic(false);
        // algo.setDiTauMassConstraint(-1.0);
        algo.integrate(measured_leptons, met_p4.Px(), met_p4.Py(), met_cov_t);
        FitResults result;
        if(algo.isValidSolution()) {
            auto histoAdapter = dynamic_cast<classic_svFit::DiTauSystemHistogramAdapter*>(algo.getHistogramAdapter());
            result.momentum = LorentzVectorM(histoAdapter->getPt(), histoAdapter->getEta(), histoAdapter->getPhi(), histoAdapter->getMass());
            result.momentum_error = LorentzVectorM(histoAdapter->getPtErr(), histoAdapter->getEtaErr(), histoAdapter->getPhiErr(), histoAdapter->getMassErr());
            result.transverseMass = histoAdapter->getTransverseMass();
            result.transverseMass_error = histoAdapter->getTransverseMassErr();
            result.has_valid_momentum = true;
        }
        return result;
    }
};
} // namespace sv_fit_ana