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

using RVecF = ROOT::VecOps::RVec<float>;
using RVecI = ROOT::VecOps::RVec<int>;
using RVecUC = ROOT::VecOps::RVec<unsigned char>;

namespace analysis {
typedef std::variant<int,float,bool, unsigned long long,long long, long,unsigned int, RVecI, RVecF,RVecUC> MultiType;

struct Entry {
  std::vector<MultiType> var_values;

  explicit Entry(size_t size) : var_values(size) {}

  template <typename T>
  void Add(int index, const T& value)
  {
      var_values.at(index)= value;
  }

template<typename T>
  const T& GetValue(int idx) const
  {
    return std::get<T>(var_values.at(idx));
  }
};

struct StopLoop {};
static std::map<int, std::shared_ptr<Entry>>& GetEntriesMap(){
      static std::map<int, std::shared_ptr<Entry>> entries;
      return entries;
    }

static std::vector<int>& GetEntriesVec(){
      static std::vector<int> eventVec;
      return eventVec;
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
              if(GetEntriesMap().count(idx)) {
                throw std::runtime_error("Duplicate entry for index " + std::to_string(idx));
              }
              GetEntriesMap()[idx] = entry;
              return true;
              }, {"_entry"});
      return df_node;
  }
};

} // namespace analysis