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
        if (eventMap.find(run)!= eventMap.end()){
            lumiEventMapType evtMapLumi = eventMap.at(run);
            if(evtMapLumi.find(lumi) != evtMapLumi.end()){
                if( eventMap.at(run).at(lumi).find(event) != eventMap.at(run).at(lumi).end()){
                    //std::cout<< "found event " << event << " for lumi " << lumi << " for run " << run << std::endl;
                    return false;
                }
                else{
                    eventMap.at(run).at(lumi).insert(event);
                    }
            }
            else{
                std::set<unsigned long long> s;
                s.insert(event);
                eventMap[run][lumi]=s;
            }
        }
        else{
            std::set<unsigned long long> s;
            s.insert(event);
            static lumiEventMapType lumiEventMap;
            lumiEventMap[lumi] = s ;
            eventMap[run]= lumiEventMap;
        }
            return true;
        }
    """
)

def merge_ntuples(df):
    print(df.Count().GetValue())
    df = df.Filter("saveEvent(run, luminosityBlock, event)")
    print(df.Count().GetValue())
    return df