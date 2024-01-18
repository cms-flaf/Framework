#pragma once 
namespace kin_fit {
  struct FitResults {
      double mass, chi2, probability;
      int convergence;
      bool HasValidMass() const { return convergence > 0; }
      FitResults() : convergence(std::numeric_limits<int>::lowest()) {}
      FitResults(double _mass, double _chi2, double _probability, int _convergence) :
          mass(_mass), chi2(_chi2), probability(_probability), convergence(_convergence) {}
  };
}
