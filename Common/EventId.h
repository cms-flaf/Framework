#pragma once

#include <map>

struct RunLumi {
  using RunType = unsigned int;
  using LumiType = unsigned int;

  static constexpr RunType undefinedRun = std::numeric_limits<RunType>::max();
  static constexpr LumiType undefinedLumi = std::numeric_limits<LumiType>::max();

  static constexpr char separator = ':';

  static const RunLumi& Undefined()
  {
    static const RunLumi undefined;
    return undefined;
  }

  static std::vector<std::string> Split(const std::string& idStr)
  {
    std::istringstream ss(idStr);
    std::vector<std::string> idStrings;
    std::string field;
    while(std::getline(ss, field, separator))
      idStrings.push_back(field);
    return idStrings;
  }

  template<typename FieldType>
  FieldType Parse(const std::string& fieldStr, const std::string& fieldName)
  {
    std::istringstream ss(fieldStr);
    FieldType field;
    ss >> field;
    if(ss.fail())
      throw std::invalid_argument("RunLumi: Invalid " + fieldName + " = '" + fieldStr + "'.");
    return field;
  }

  static RunLumi FromString(const std::string& idStr)
  {
    const auto idStrings = Split(idStr);
    if(idStrings.size() != 2)
      throw std::invalid_argument("RunLumi: invalid run:lumi string = '" + idStr + "'.");
    return FromStrings(idStrings[0], idStrings[1]);
  }

  static RunLumi FromStrings(const std::string& runStr, const std::string& lumiStr)
  {
    const auto run = Parse<RunType>(runStr, "run");
    const auto luminosityBlock = Parse<LumiType>(lumiStr, "luminosityBlock");
    return RunLumi(run, luminosityBlock)
  }

  RunType run{undefinedRun};
  LumiType luminosityBlock{undefinedLumi};

  RunLumi() {}
  RunLumi(RunType runNumber, LumiType lumiBlock) : run(runNumber), luminosityBlock(lumiBlock) {}

  bool operator ==(const RunLumi& other) const
  {
    return run == other.run && luminosityBlock == other.luminosityBlock;
  }

  bool operator !=(const RunLumi& other) const
  {
    return run != other.run || luminosityBlock != other.luminosityBlock;
  }

  bool operator <(const RunLumi& other) const
  {
    if(run != other.run) return run < other.run;
    return luminosityBlock < other.luminosityBlock;
  }

  std::string ToString() const
  {
    std::ostringstream ss;
    ss << run << separator << luminosityBlock;
    return ss.str();
  }
};

std::ostream& operator<<(std::ostream& s, const RunLumi& runLumi)
{
  s << runLumi.ToString();
  return s;
}

std::istream& operator>>(std::istream& s, RunLumi& runLumi)
{
  std::string str;
  s >> str;
  runLumi = RunLumi::FromString(str);
  return s;
}

struct RunLumiEvent {
  using RunType = RunLumi::RunType;
  using LumiType = RunLumi::LumiType;
  using EventType = unsigned long long;

  static constexpr RunType undefinedRun = RunLumi::undefinedRun;
  static constexpr LumiType undefinedLumi = RunLumi::undefinedLumi;
  static constexpr EventType undefinedEvent = std::numeric_limits<EventType>::max();

  static constexpr char separator = RunLumi::separator;

  static const RunLumiEvent& Undefined()
  {
    static const RunLumiEvent undefined;
    return undefined;
  }

  static RunLumiEvent FromString(const std::string& idStr)
  {
    const auto idStrings = Split(idStr);
    if(idStrings.size() != 3)
      throw std::invalid_argument("RunLumiEvent: invalid run:lumi string = '" + idStr + "'.");
    return FromStrings(idStrings[0], idStrings[1], idStrings[2]);
  }

  static RunLumiEvent FromStrings(const std::string& runStr, const std::string& lumiStr, const std::string& eventStr)
  {
    const auto runLumi = RunLumi::FromStrings(runStr, lumiStr);
    const auto event = Parse<EventType>(eventStr, "event");
    return RunLumi(run, luminosityBlock, event)
  }

  RunType run{undefinedRun};
  LumiType luminosityBlock{undefinedLumi};
  EventType event{undefinedEvent};

  RunLumiEvent() {}
  RunLumiEvent(RunType runNumber, LumiType lumiBlock, EventType eventNumber)
    : run(runNumber), luminosityBlock(lumiBlock), event(eventNumber) {}

  bool operator ==(const RunLumiEvent& other) const
  {
    return run == other.run && luminosityBlock == other.luminosityBlock && event == other.event;
  }

  bool operator !=(const RunLumiEvent& other) const
  {
    return run != other.run || luminosityBlock != other.luminosityBlock || event != other.event;
  }

  bool operator <(const RunLumiEvent& other) const
  {
    if(run != other.run) return run < other.run;
    if(luminosityBlock != other.luminosityBlock) return luminosityBlock < other.luminosityBlock;
    return event < other.event;
  }

  std::string ToString() const
  {
    std::ostringstream ss;
    ss << run << separator << luminosityBlock << separator << event;
    return ss.str();
  }
};

std::ostream& operator<<(std::ostream& s, const RunLumiEvent& runLumiEvent)
{
  s << runLumiEvent.ToString();
  return s;
}

std::istream& operator>>(std::istream& s, RunLumiEvent& runLumiEvent)
{
  std::string str;
  s >> str;
  runLumiEvent = RunLumiEvent::FromString(str);
  return s;
}
