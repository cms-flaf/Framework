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
using RVecB = ROOT::VecOps::RVec<bool>;
using RVecI = ROOT::VecOps::RVec<int>;
  using RVecUL = ROOT::VecOps::RVec<unsigned long>;

namespace analysis {
typedef std::variant<int,float,double,bool,unsigned long long, long, unsigned long, unsigned int, RVecI, RVecF, RVecB, RVecUL> MultiType;

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

namespace detail {

template<typename ...Args>
inline void fillEntry(Entry& entry, Args&& ...args)
{
  int index = 0;
  (void) std::initializer_list<int>{ (entry.Add(index++, std::forward<Args>(args)), 0)... };
}

} // namespace detail

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

      //detail::fillEntry(*entry, args...);
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
          // std::cout << "Pushing" <<std::endl;
          if(!queue.Push(entry)) {
            throw StopLoop();
          }
          // std::cout << "Pushed" <<std::endl;
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
    df_out = df_out.Define("_entryCentral", [=](ULong64_t entryIndexShifted) {
      std::shared_ptr<Entry> entryCentral;
      try {
        static bool notified = notify();
        static std::shared_ptr<Entry> entry;
        static std::set<unsigned long long> processedEntries;
        if(processedEntries.count(entryIndexShifted))
          throw std::runtime_error("Entry already processed");
        // std::cout << "Poping" <<std::endl;
        while(!entry || entry->GetValue<unsigned long long>(0)<entryIndexShifted){
          if(entry){
            processedEntries.insert(entry->GetValue<unsigned long long>(0));
            entry.reset();
          }
          if (!queue.Pop(entry)) {
            break;
          }
        }
        // std::cout << "Poped" <<std::endl;
        //std::cout << entryIndexShifted << "\t"<< entry->GetValue<unsigned long long>(0)<<std::endl;
        if(entry && entry->GetValue<unsigned long long>(0)==entryIndexShifted){
          entryCentral=entry;
          // std::cout << "Set" <<std::endl;
        }
        //std::cout << "sono uguali "<< entryIndexShifted << "\t"<< entryCentral->GetValue<unsigned long long>(0)<<std::endl;
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

} // namespace analysis