/*! Definition of wrappers for KinFit.*/

#pragma once

#include "TextIO.h"
#include "RootExt.h"
#include "HHKinFit2/interface/HHKinFitMasterHeavyHiggs.h"


namespace kin_fit {
    template<typename LVector>
    TLorentzVector ConvertVector(const LVector& v)
    {
        return TLorentzVector(v.Px(), v.Py(), v.Pz(), v.E());
    }

    struct FitResults {
        double mass, chi2, probability;
        int convergence;
        bool HasValidMass() const { return convergence > 0; }
        FitResults() : convergence(std::numeric_limits<int>::lowest()) {}
        FitResults(double _mass, double _chi2, double _probability, int _convergence) :
            mass(_mass), chi2(_chi2), probability(_probability), convergence(_convergence) {}
    };

    class FitProducer {
    public:
        template<typename LVector1, typename LVector2, typename LVector3, typename LVector4, typename LVector5>
        static FitResults Fit(const LVector1& lepton1_p4, const LVector2& lepton2_p4,
                            const LVector3& jet1_p4, const LVector4& jet2_p4,
                            const LVector5& met_p4, double MET_cov_00, double MET_cov_01, double MET_cov_11,
                            double resolution_1, double resolution_2, int verbosity = 0)
        {
            TMatrixD met_cov(2, 2);
            met_cov(0, 0) = MET_cov_00;
            met_cov(0, 1) = MET_cov_01;
            met_cov(1, 0) = MET_cov_01;
            met_cov(1, 1) = MET_cov_11;
            auto Met_p4_converted = ConvertVector(met_p4);
            return FitImpl(ConvertVector(lepton1_p4), ConvertVector(lepton2_p4), ConvertVector(jet1_p4),
                        ConvertVector(jet2_p4), TVector2(Met_p4_converted.Px(), Met_p4_converted.Py()), met_cov,
                        resolution_1, resolution_2, verbosity);
        }
        static FitResults FitImpl(const TLorentzVector& lepton1_p4, const TLorentzVector& lepton2_p4,
                                const TLorentzVector& jet1_p4, const TLorentzVector& jet2_p4, const TVector2& met,
                                const TMatrixD& met_cov, double resolution_1, double resolution_2, int verbosity)
        {
        FitResults result;
        try {


            HHKinFit2::HHKinFitMasterHeavyHiggs hh_kin_fit(jet1_p4, jet2_p4, lepton1_p4, lepton2_p4, met, met_cov,
                                                            resolution_1, resolution_2);
            hh_kin_fit.verbosity = verbosity;
            hh_kin_fit.addHypo(125,125);
            hh_kin_fit.fit();
            result.convergence = hh_kin_fit.getConvergence();
            if(result.HasValidMass()) {
                result.mass = hh_kin_fit.getMH();
                result.chi2 = hh_kin_fit.getChi2();
                result.probability = hh_kin_fit.getFitProb();
            }
            if(verbosity > 0) {
                std::cout << "Convergence = " << result.convergence << std::endl;
                if(result.HasValidMass()) {
                    std::cout << std::setprecision(6);
                    std::cout << "Mass = " << result.mass << std::endl;
                    std::cout << "chi2 = " << result.chi2 << std::endl;
                    std::cout << "probability = " << result.probability << std::endl;
                }
            }
        } catch(std::exception&) {}
        return result;
        }
    };
}