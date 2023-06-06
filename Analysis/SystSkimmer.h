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
typedef std::variant<int,float,double,bool,unsigned long long, long, unsigned long, unsigned int, RVecI, RVecF> MultiType;

struct Entry {
  std::vector<MultiType> var_values;

  explicit Entry(size_t size) : var_values(size) {}

  template <typename T>
  void Add(int index, T& value)
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
  TupleMaker(size_t queue_size)
    : queue(queue_size)
  {
  }

  TupleMaker(const TupleMaker&) = delete;
  TupleMaker& operator= (const TupleMaker&) = delete;

  ROOT::RDF::RNode process(ROOT::RDF::RNode df_in, ROOT::RDF::RNode df_out, const std::vector<std::string>& var_names)
  {
    df_in = df_in.Define("_entry", [=](const Args& ...args) {
      auto entry = std::make_shared<Entry>(var_names.size());
      detail::fillEntry(*entry, args...);
      return entry;
    }, var_names);
    thread = std::make_unique<std::thread>([=]() {
      //std::cout << "TupleMaker::process: foreach started." << std::endl;
      try {
        ROOT::RDF::RNode df = df_in;
        df.Foreach([&](const std::shared_ptr<Entry>& entry) {
          if(!queue.Push(entry)) {
            throw StopLoop();
          }
        }, {"_entry"});
      } catch(StopLoop) {
      }
      queue.SetAllDone();
    });
    //std::cout << "starting defining entryCentral" << std::endl;

    df_out = df_out.Define("_entryCentral", [=](ULong64_t entryIndexShifted) {


      std::shared_ptr<Entry> entryCentral;
      //entryCentral->ResizeVarValues(var_names.size());

      try {
        static std::shared_ptr<Entry> entry;
          while(!entry || entry->GetValue<unsigned long long>(0)<entryIndexShifted){
          entry.reset();
          if (!queue.Pop(entry)) {
            break;
          }
        }
        //std::cout << entryIndexShifted << "\t"<< entry->GetValue<unsigned long long>(0)<<std::endl;
        if(entry && entry->GetValue<unsigned long long>(0)==entryIndexShifted){
          entryCentral=entry;
        }
        //std::cout << "sono uguali "<< entryIndexShifted << "\t"<< entryCentral->GetValue<unsigned long long>(0)<<std::endl;
      } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << std::endl;
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

  EntryQueue<std::shared_ptr<Entry>> queue;
  std::unique_ptr<std::thread> thread;
};

} // namespace analysis