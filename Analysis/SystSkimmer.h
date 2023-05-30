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
              T& value, Args&& ...args)
{
  entry.Add(var_index, value);
  //std::cout << var_index << "\t " << value <<std::endl;
  putEntry(entry, var_index + 1, std::forward<Args>(args)...);
}

bool isDifferentEvent(Entry& entry1, Entry& entry2, int index){
  return (entry1.int_values[index] != entry2.int_values[index] || entry1.bool_values[index]!=entry2.bool_values[index] || entry1.ulong_values[index]!=entry2.ulong_values[index] || entry1.float_values[index]!=entry2.float_values[index] || entry1.double_values[index]!=entry2.double_values[index] );
};
bool CompareEntries(Entry& entry_central, Entry& entry_shifted,int var_index)
{
  bool is_different_event = isDifferentEvent(entry_central, entry_shifted,var_index);
  std::cout << "var_index " << var_index << std::endl;
  std::cout<<"isDifferentEvent? " <<is_different_event << std::endl;
  //CompareEntries(entry_central,entry_shifted,var_index+1);
  return is_different_event;

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
      //std::cout << "TupleMaker::process: foreach started." << std::endl;
      try {
        ROOT::RDF::RNode df = df_in;
        df.Foreach([&](const Args& ...args) {
          Entry entry;
          //std::cout << "TupleMaker::process: running detail::putEntry." << std::endl;
          detail::putEntry(entry, 0, args...);
          //std::cout << "TupleMaker::process: push entry." << std::endl;
          entry.valid = true;
          //std::cout << "push entry is "<< queue.Push(entry) << std::endl;
          if(!queue.Push(entry)) {
            //std::cout << "TupleMaker::process: queue is full." << std::endl;
            throw StopLoop();
          }
        }, var_names);
      } catch(StopLoop) {
        //std::cout << "stop loop catched " << std::endl;
      }
      queue.SetAllDone();
      //std::cout << "TupleMaker::process: foreach done." << std::endl;
    });
    //std::cout << "starting defining entryCentral" << std::endl;

    df_out = df_out.Define("_entryCentral", [=](ULong64_t entryIndexShifted) {
      static Entry entry;
      while(!entry.valid || entry.ulong_values.at(0)<entryIndexShifted){
        entry = Entry();
        //std::cout << "entry popped? " << queue.Pop(entry) << std::endl;
        if (!queue.Pop(entry)) {
          //std::cout << "entry popped " <<std::endl;
        }
      }
      Entry entryCentral;
      if(entry.valid && entry.ulong_values.at(0)==entryIndexShifted){
        entryCentral=entry;
      }
      return entryCentral;
    }, { "entryIndex" });
    df_out = df_out.Define("compareEntries", [=](Entry entryCentralShiftedSame){
      bool areCoincidentEvents = false;
      static Entry entry;
      while(!entry.valid){
        entry = Entry();
        //std::cout << "entry popped? " << queue.Pop(entry) << std::endl;
        if (!queue.Pop(entry)) {
          //std::cout << "entry popped " <<std::endl;
        }
      }
      int idx=0;
      while(entry.valid && idx < var_names.size()){
        areCoincidentEvents = !(detail::CompareEntries(entry, entryCentralShiftedSame,idx));
        idx+=1;
        std::cout << "var_names size = " << var_names.size() << "\t idx = "<<idx << std::endl;
      }
      std::cout << "are coincident events?" << areCoincidentEvents<< std::endl;
      return areCoincidentEvents;
    },{"_entryCentral"});
    std::cout << "finished defining entryCentral" << std::endl;

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