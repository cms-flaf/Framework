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

namespace analysis {
typedef std::variant<int,float,double,bool,unsigned long long, long, unsigned long, unsigned int> MultiType;

struct Entry {
  std::vector<MultiType> var_values;

  explicit Entry(size_t size) : var_values(size) {}

  template <typename T>
  void Add(int index, T value)
  {
    //std::cout << index << "\t "<< std::to_string(value) <<std::endl;
    //CheckIndex(index);
    var_values.at(index)= value;
  }
template<typename T>
  T GetValue(int idx) const
  {
    return std::get<T>(var_values.at(idx));
  }
};

struct StopLoop {};

namespace detail {
  /*
inline void putEntry(std::shared_ptr<Entry>& entry, int index) {}

template<typename T,typename ...Args>
void putEntry(std::shared_ptr<Entry>& entry, int var_index,
              const T& value, Args&& ...args)
{
  //std::cout << var_index << "\t " << value <<std::endl;
  entry->Add(var_index, value);
  //std::cout << "before incrementing " << var_index << std::endl;
  //std::cout << "after incrementing " << var_index << std::endl;
  putEntry(entry, var_index+1,std::forward<Args>(args)...);
}
*/
template<typename T>
void addEntry(Entry& entry, int index, T& value)
{
  entry.Add(index, value);
}

template<typename ...Args>
void fillEntry(Entry& entry, Args&& ...args)
{
    int index = 0;
    (void) std::initializer_list<int>{ (addEntry(entry, index++, std::forward<Args>(args)), 0)... };
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
    thread = std::make_unique<std::thread>([df_in, this, var_names]() {
      std::cout << "TupleMaker::process: foreach started." << std::endl;
      try {
        ROOT::RDF::RNode df = df_in;
        df.Foreach([&](const Args& ...args) {
          auto entry = std::make_shared<Entry>(var_names.size());
          //std::cout << "TupleMaker::process: running detail::putEntry->" << std::endl;
          detail::fillEntry(*entry, args...);
          //detail::putEntry(entry, 0,args...);
          //std::cout << "TupleMaker::process: push entry->" << std::endl;
          //std::cout << "push entry is "<< queue.Push(entry) << std::endl;
          if(!queue.Push(entry)) {
            //std::cout << "TupleMaker::process: queue is full." << std::endl;
            throw StopLoop();
          }
          //std::cout << "finished to push entry " << std::endl;
        }, var_names);
      } catch(StopLoop) {
        //std::cout << "stop loop catched " << std::endl;
      }
      queue.SetAllDone();
      std::cout << "TupleMaker::process: foreach done." << std::endl;
    });
    std::cout << "starting defining entryCentral" << std::endl;

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