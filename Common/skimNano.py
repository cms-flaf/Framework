from . import BaselineSelection as Baseline

def skim_RecoLeptons(df):
    Baseline.Initialize()
    df = Baseline.CreateRecoP4(df, '')
    df = Baseline.RecoLeptonsSelection(df)
    return df

def skim_failed_RecoLeptons(df):
    Baseline.Initialize()
    df = Baseline.CreateRecoP4(df, '')
    df, b0_filter = Baseline.RecoLeptonsSelection(df, apply_filter=False)
    df = df.Filter(f'!({b0_filter})')
    return df

def skim_RecoLeptonsJetAcceptance(df):
    Baseline.Initialize()
    df = Baseline.CreateRecoP4(df, '')
    df = Baseline.RecoLeptonsSelection(df)
    df = Baseline.RecoJetAcceptance(df)
    return df

def skim_failed_RecoLeptonsJetAcceptance(df):
    Baseline.Initialize()
    df = Baseline.CreateRecoP4(df, '')
    df, b0_filter = Baseline.RecoLeptonsSelection(df, apply_filter=False)
    df, b1_filter = Baseline.RecoJetAcceptance(df, apply_filter=False)
    df = df.Filter(f'!(({b0_filter}) && ({b1_filter}))')
    return df

import ROOT
ROOT.gInterpreter.Declare(
    """
    static std::map < unsigned long std::pair < unsigned int, unsigned int > > eventMap;
    std::mutex eventMap_mutex;
    bool saveEvent(unsigned long event, unsigned int run, unsigned int lumi){
            if (eventMap.find(event)!= eventMap.end()){
                    if(eventMap.at(event).first == run && eventMap.at(event).second == lumi  )
                    return false;
                }
            eventMap_mutex.lock();
            eventMap.insert(static std::map < unsigned long std::pair < unsigned int, unsigned int > > (event, std::make_pair<unsigned int, unsigned int > (run, lumi)));
            eventMap_mutex.unlock();
            return true;
        }
    """)

def merge_ntuples(df):
    return df.Filter("saveEvent(event, run, luminosityBlock)")