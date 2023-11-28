#pragma once

#include <any>
#include <iostream>
#include <tuple>
#include <thread>
#include <string>
#include <variant>
#include <typeinfo>
#include <typeindex>

#include "EntryQueue.h"
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

using RVecF = ROOT::VecOps::RVec<float>;
using RVecI = ROOT::VecOps::RVec<int>;
using RVecUC = ROOT::VecOps::RVec<unsigned char>;

namespace analysis {
typedef std::variant<int,float,bool, unsigned long,unsigned long long,long long, long,unsigned int, RVecI, RVecF,RVecUC,double, kin_fit::FitResults> MultiType;

struct Entry {
  std::vector<MultiType> var_values;

  explicit Entry(size_t size) : var_values(size) {}

  template <typename T>
  void Add(int index, const T& value)
  {
      var_values.at(index)= value;
  }

  void Add(int index, const kin_fit::FitResults& value)
  {
    kin_fit::FitResults toAdd(value.mass, value.chi2, value.probability, value.convergence) ;
    var_values.at(index)= toAdd;
  }

template<typename T>
  const T& GetValue(int idx) const
  {
    return std::get<T>(var_values.at(idx));
  }
};

struct StopLoop {};
static std::map<std::tuple<int, unsigned int,unsigned long long ,unsigned int>, std::shared_ptr<Entry>>& GetEntriesMap(){
      static std::map<std::tuple<int, unsigned int,unsigned long long ,unsigned int>, std::shared_ptr<Entry>> entries;
      return entries;
    }


template<typename ...Args>
struct MapCreator {


  ROOT::RDF::RNode processCentral(ROOT::RDF::RNode df_in, const std::vector<std::string>& var_names)
  {
      auto df_node = df_in.Define("_entry", [=](const Args& ...args) {
              auto entry = std::make_shared<Entry>(var_names.size());
              int index = 0;
              (void) std::initializer_list<int>{ (entry->Add(index++, args), 0)... };
              return entry;
          }, var_names).Define("map_placeholder", [&](const std::shared_ptr<Entry>& entry) {
              const auto idx = entry->GetValue<int>(0);
              const auto run = entry->GetValue<unsigned int>(1);
              const auto evt = entry->GetValue<unsigned long long>(2);
              const auto lumi = entry->GetValue<unsigned int>(3);
              std::tuple<int,unsigned int,unsigned long long ,unsigned int> tupletofind = {idx,run, evt, lumi};
              if(GetEntriesMap().find(tupletofind)!=GetEntriesMap().end()) {
                throw std::runtime_error("Duplicate entry for index " + std::to_string(idx));
              }
              GetEntriesMap().emplace(tupletofind,entry);
              return true;
              }, {"_entry"});
      return df_node;
  }
};

} // namespace analysis