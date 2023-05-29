#pragma once

#include <map>
#include <vector>
#include <thread>
#include <typeinfo>

#include "EntryQueue.h"

namespace analysis {

struct Entry {
  bool valid{false};
  int index;
  std::map<int, int> int_values;
  std::map<int, bool> bool_values;
  std::map<int,unsigned long long> ulong_values;
  std::map<int, float> float_values;
  std::map<int, double> double_values; // to be removed

  void Add(int index, float value)
  {
    CheckIndex(index);
    float_values[index] = value;
  }

  void Add(int index, int value)
  {
    CheckIndex(index);
    int_values[index] = value;
  }

  void Add(int index, unsigned long long value)
  {
    CheckIndex(index);
    ulong_values[index] = value;
  }

  void Add(int index, bool value)
  {
    CheckIndex(index);
    bool_values[index] = value;
  }

  void Add(int index, double value) // to be removed
  {
    CheckIndex(index);
    double_values[index] = value;
  }

private:

  void CheckIndex(int index) const
  {
    if (int_values.count(index) || float_values.count(index) || ulong_values.count(index) || double_values.count(index) || bool_values.count(index) )
      throw std::runtime_error("Entry::Add: index already exists");
  }
};

struct StopLoop {};

namespace detail {
inline void putEntry(Entry& entry, int index) {}

template<typename T, typename ...Args>
void putEntry(Entry& entry, int var_index,
              const T& value, Args&& ...args)
{
  entry.Add(var_index, value);
  std::cout << var_index << "\t " << value <<std::endl;
  putEntry(entry, var_index + 1, std::forward<Args>(args)...);
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
    thread = std::make_unique<std::thread>([=]() {
      std::cout << "TupleMaker::process: foreach started." << std::endl;
      try {
        ROOT::RDF::RNode df = df_in;
        df.Foreach([&](const Args& ...args) {
          Entry entry;
          std::cout << "TupleMaker::process: running detail::putEntry." << std::endl;
          detail::putEntry(entry, 0, args...);
          std::cout << "TupleMaker::process: push entry." << std::endl;
          entry.valid = true;
          std::cout << "push entry is "<< queue.Push(entry) << std::endl;
          if(!queue.Push(entry)) {
            std::cout << "TupleMaker::process: queue is full." << std::endl;
            throw StopLoop();
          }
        }, var_names);
      } catch(StopLoop) {
        //std::cout << "stop loop catched " << std::endl;
      }
      queue.SetAllDone();
      std::cout << "TupleMaker::process: foreach done." << std::endl;
    });
    /*
    df_out = df_out.Define("_entryCentral", [=](ULong64_t entryIndexShifted) {
      static Entry entry;
      std::cout << "cacca " <<std::endl;
      while(!entry->valid || entry->ulong_values.at(0)<entryIndexShifted){
        entry = Entry();
        if (!queue.Pop(entry)) {
          std::cout << "entry popped " <<std::endl;
        }
      }
      Entry entryCentral;
      if(entry->valid && entry->ulong_values.at(0)==entryIndexShifted){
        entryCentral=entry;
      }
      return entryCentral;
    }, { "entryIndex" });*/
    return df_out;
  }

  void join()
  {
    if(thread)
      thread->join();
  }

  EntryQueue<Entry> queue;
  std::unique_ptr<std::thread> thread;
};

} // namespace analysis