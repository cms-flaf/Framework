import ROOT
ROOT.gInterpreter.Declare(
    """
    using lumiEventMapType = std::map < unsigned int, std::set <unsigned long long > >;
    using eventMapType =  std::map < unsigned int ,  lumiEventMapType >;

    bool saveEvent(unsigned long event, unsigned int run, unsigned int lumi, bool isData){
        if(!isData) return true;
        static eventMapType eventMap;
        static std::mutex eventMap_mutex;
            if (eventMap.find(run)!= eventMap.end()){
                if(eventMap.at(run).find(lumi) != eventMap.at(run).end() ) {
                    if( eventMap.at(run).at(lumi).find(event) != eventMap.at(run).at(lumi).end()){
                        std::cout << "event " << event << " lumi " << lumi << " run " << run << " already present in df "<< std::endl;
                        return false;
                    }
                    else{
                        eventMap.at(run).at(lumi).insert(event);
                    }
                }
                /*else{
                    std::set<unsigned long long> s;
                    s.insert(event);
                    lumiEventMapType lumiEventMap;
                    lumiEventMap[lumi] = s ;
                    eventMap[run]= lumiEventMap;
                }*/
            }
            eventMap_mutex.lock();
            static std::set<unsigned long long> s;
            s.insert(event);
            static lumiEventMapType lumiEventMap;
            lumiEventMap[lumi] = s ;
            eventMap[run]= lumiEventMap;
            eventMap_mutex.unlock();
            return true;
        }
    """
)

def merge_ntuples(df):
    print(df.Count().GetValue())
    df = df.Filter("saveEvent(event, run, luminosityBlock, isData)")
    print(df.Count().GetValue())
    return df