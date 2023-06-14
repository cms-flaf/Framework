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


namespace analysis {
typedef std::variant<int,float,bool, unsigned long long,long,unsigned int, RVecI, RVecF> MultiType;

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

template<typename ...Args>
struct TupleMaker {
  TupleMaker(const std::string& tree_name, const std::string& in_file, size_t queue_size)
    : df_in(tree_name, in_file), queue(queue_size)
  {
  }

  TupleMaker(const TupleMaker&) = delete;
  TupleMaker& operator= (const TupleMaker&) = delete;

  void processIn(const std::vector<std::string>& var_names)
  {
    auto df_node = df_in.Define("_entry", [=](const Args& ...args) {
      auto entry = std::make_shared<Entry>(var_names.size());
      int index = 0;
      (void) std::initializer_list<int>{ (entry->Add(index++, args), 0)... };
      return entry;
    }, var_names);
    thread = std::make_unique<std::thread>([=]() {
      std::cout << "TupleMaker::processIn: thread started." << std::endl;
      {
        std::unique_lock<std::mutex> lock(mutex);
        cond_var.wait(lock);
      }
      std::cout << "TupleMaker::processIn: starting foreach." << std::endl;
      try {
        ROOT::RDF::RNode df = df_node;
        df.Foreach([&](const std::shared_ptr<Entry>& entry) {
          if(!queue.Push(entry)) {
            throw StopLoop();
          }
        }, {"_entry"});
      } catch(StopLoop) {
      } catch(std::exception& e) {
        std::cout << "TupleMaker::processIn: exception: " << e.what() << std::endl;
        throw;
      }
      queue.SetAllDone();
    });
  }




  ROOT::RDF::RNode processOut(ROOT::RDF::RNode df_out)
  {
    auto notify = [&]() {
      std::cout << "TupleMaker::processOut: notifying" << std::endl;
      std::unique_lock<std::mutex> lock(mutex);
      cond_var.notify_all();
      return true;
    };
    df_out = df_out.Define("_entryCentral", [=](Int_t entryIndexShifted) {
      std::shared_ptr<Entry> entryCentral;
      try {
        static bool notified = notify();
        static std::shared_ptr<Entry> entry;
        static std::set<int> processedEntries;
        if(processedEntries.count(entryIndexShifted))
          throw std::runtime_error("Entry already processed");
        while(!entry || entry->GetValue<int>(0)<entryIndexShifted){
          if(entry){
            processedEntries.insert(entry->GetValue<int>(0));
            entry.reset();
          }
          if (!queue.Pop(entry)) {
            break;
          }
        }
        if(entry && entry->GetValue<int>(0)==entryIndexShifted){
          entryCentral=entry;
        }
      } catch (const std::exception& e) {
        std::cout << "TupleMaker::processOut: exception: " << e.what() << std::endl;
        throw;
      }
      return entryCentral;
    }, { "entryIndex" });

    return df_out;
  }

  void join()
  {
    if(thread)
      thread->join();
  }

  ROOT::RDataFrame df_in;
  EntryQueue<std::shared_ptr<Entry>> queue;
  std::unique_ptr<std::thread> thread;
  std::mutex mutex;
  std::condition_variable cond_var;
};

namespace detail {
  template<typename T>
  struct DeltaImpl {
    static T Delta(const T& shifted, const T& central) {
      return shifted - central;
    }
    static T FromDelta(const T& delta, const T& central){
      return delta + central;
    }

  };

  template<typename T>
  struct DeltaImpl<ROOT::VecOps::RVec<T>> {
    static ROOT::VecOps::RVec<T> Delta(const ROOT::VecOps::RVec<T>& shifted, const ROOT::VecOps::RVec<T>& central)
    {
      ROOT::VecOps::RVec<T> delta = shifted;
      size_t n_max = std::min(shifted.size(), central.size());
      for(size_t n = 0; n < n_max; ++n)
        delta[n] -= central[n];
      return delta;
    }
    static ROOT::VecOps::RVec<T> FromDelta(const ROOT::VecOps::RVec<T>& delta, const ROOT::VecOps::RVec<T>& central){
      ROOT::VecOps::RVec<T> fromDeltaVec = delta;
      size_t n_max = std::min(delta.size(), central.size());
      for (size_t n =0 ; n < n_max; ++n){
        fromDeltaVec[n]+= central[n];
      }
      return fromDeltaVec;
    }
  };

  template<typename T>
  struct IsSameImpl {
    static bool IsSame(T shifted, T central) {
      return shifted == central;
    }
  };

  template<typename T>
  struct IsSameImpl<ROOT::VecOps::RVec<T>> {
    static bool IsSame(const ROOT::VecOps::RVec<T>& shifted, const ROOT::VecOps::RVec<T>& central)
    {
      const size_t n_shifted = shifted.size();
      if(n_shifted != central.size())
        return false;
      for(size_t n = 0; n < n_shifted; ++n)
        if(!IsSameImpl<T>::IsSame(shifted[n], central[n]))
          return false;
      return true;
    }
  };
}

template<typename T>
bool IsSame(const T& shifted, const T& central)
{
  return detail::IsSameImpl<T>::IsSame(shifted, central);
}
template<typename T>
T Delta(const T& shifted, const T& central)
{
  return detail::DeltaImpl<T>::Delta(shifted, central);
}
template<>
bool Delta<bool>(const bool& shifted, const bool& central)
{
  return shifted == central;
}
template<typename T>
T FromDelta(const T& shifted, const T& central)
{
  return detail::DeltaImpl<T>::FromDelta(shifted, central);
}
template<>
bool FromDelta<bool>(const bool& delta, const bool& central)
{
  return delta ? central : !central ;
}


} // namespace analysis