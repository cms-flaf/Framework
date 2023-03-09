import ROOT
ROOT.gInterpreter.Declare(
    """
    using lumiEventMapType = std::map < unsigned int, std::set <unsigned long long > >;
    using eventMapType =  std::map < unsigned int ,  lumiEventMapType >;
    using RunLumiEventSet = std::set < std::tuple < unsigned int, unsigned int, unsigned long long > > ;

    bool saveEvent(unsigned int run, unsigned int lumi, unsigned long long event){
        static eventMapType eventMap;
        static std::mutex eventMap_mutex;
        const std::lock_guard<std::mutex> lock(eventMap_mutex);
        auto& events = eventMap[run][lumi];
        if(events.find(event) != events.end())
            return false;
        events.insert(event);
        return true;
        }
    """
)

def merge_ntuples(df):
    print(df.Count().GetValue())
    df = df.Filter("saveEvent(run, luminosityBlock, event)")
    print(df.Count().GetValue())
    return df