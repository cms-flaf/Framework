import copy
import time
import os
import sys
import ROOT
import shutil
import zlib
import json
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.BaselineSelection as Baseline
import Common.Utilities as Utilities
import Common.ReportTools as ReportTools
import Common.triggerSel as Triggers
import Corrections.Corrections as Corrections
from Corrections.lumi import LumiFilter

#ROOT.EnableImplicitMT(1)
ROOT.EnableThreadSafety()
defaultColToSave = []

dict_files_BulkGraviton = {
    'GluGluToBulkGravitonToHHTo2B2Tau_M-1000':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-1000/nano.root',
        'mass': 1000
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-1250':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-1250/nano.root',
        'mass': 1250
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-1500':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-1500/nano.root',
        'mass': 1500
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-1750':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-1750/nano.root',
        'mass': 1750
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-2000':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-2000/nano.root',
        'mass': 2000
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-250':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-250/nano.root',
        'mass': 250
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-2500':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-2500/nano.root',
        'mass': 2500
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-260':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-260/nano.root',
        'mass': 260
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-270':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-270/nano.root',
        'mass': 270
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-280':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-280/nano.root',
        'mass': 280
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-300':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-300/nano.root',
        'mass': 300
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-3000':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-3000/nano.root',
        'mass': 3000
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-320':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-320/nano.root',
        'mass': 320
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-350':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-350/nano.root',
        'mass': 350
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-400':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-400/nano.root',
        'mass': 400
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-450':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-450/nano.root',
        'mass': 450
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-500':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-500/nano.root',
        'mass': 500
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-550':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-550/nano.root',
        'mass': 550
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-600':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-600/nano.root',
        'mass': 600
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-650':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-650/nano.root',
        'mass': 650
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-700':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-700/nano.root',
        'mass': 700
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-750':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-750/nano.root',
        'mass': 750
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-800':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-800/nano.root',
        'mass': 800
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-850':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-850/nano.root',
        'mass': 850
        },
    'GluGluToBulkGravitonToHHTo2B2Tau_M-900':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToBulkGravitonToHHTo2B2Tau_M-900/nano.root',
        'mass': 900
        }
}
dict_files_Radion = {
    'GluGluToRadionToHHTo2B2Tau_M-1000':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-1000/nano.root',
        'mass': 1000
        },
    'GluGluToRadionToHHTo2B2Tau_M-1250':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-1250/nano.root',
        'mass': 1250
        },
    'GluGluToRadionToHHTo2B2Tau_M-1500':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-1500/nano.root',
        'mass': 1500
        },
    'GluGluToRadionToHHTo2B2Tau_M-1750':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-1750/nano.root',
        'mass': 1750
        },
    'GluGluToRadionToHHTo2B2Tau_M-2000':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-2000/nano.root',
        'mass': 2000
        },
    'GluGluToRadionToHHTo2B2Tau_M-250':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-250/nano.root',
        'mass': 250
        },
    'GluGluToRadionToHHTo2B2Tau_M-2500':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-2500/nano.root',
        'mass': 2500
        },
    'GluGluToRadionToHHTo2B2Tau_M-260':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-260/nano.root',
        'mass': 260
        },
    'GluGluToRadionToHHTo2B2Tau_M-270':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-270/nano.root',
        'mass': 270
        },
    'GluGluToRadionToHHTo2B2Tau_M-280':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-280/nano.root',
        'mass': 280
        },
    'GluGluToRadionToHHTo2B2Tau_M-300':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-300/nano.root',
        'mass': 300
        },
    'GluGluToRadionToHHTo2B2Tau_M-3000':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-3000/nano.root',
        'mass': 3000
        },
    'GluGluToRadionToHHTo2B2Tau_M-320':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-320/nano.root',
        'mass': 320
        },
    'GluGluToRadionToHHTo2B2Tau_M-350':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-350/nano.root',
        'mass': 350
        },
    'GluGluToRadionToHHTo2B2Tau_M-400':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-400/nano.root',
        'mass': 400
        },
    'GluGluToRadionToHHTo2B2Tau_M-450':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-450/nano.root',
        'mass': 450
        },
    'GluGluToRadionToHHTo2B2Tau_M-500':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-500/nano.root',
        'mass': 500
        },
    'GluGluToRadionToHHTo2B2Tau_M-550':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-550/nano.root',
        'mass': 550
        },
    'GluGluToRadionToHHTo2B2Tau_M-600':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-600/nano.root',
        'mass': 600
        },
    'GluGluToRadionToHHTo2B2Tau_M-650':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-650/nano.root',
        'mass': 650
        },
    'GluGluToRadionToHHTo2B2Tau_M-700':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-700/nano.root',
        'mass': 700
        },
    'GluGluToRadionToHHTo2B2Tau_M-750':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-750/nano.root',
        'mass': 750
        },
    'GluGluToRadionToHHTo2B2Tau_M-800':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-800/nano.root',
        'mass': 800
        },
    'GluGluToRadionToHHTo2B2Tau_M-850':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-850/nano.root',
        'mass': 850
        },
    'GluGluToRadionToHHTo2B2Tau_M-900':{
        'fileName':'/eos/home-k/kandroso/cms-hh-bbtautau/nanoAOD/Run2_2018/GluGluToRadionToHHTo2B2Tau_M-900/nano.root',
        'mass': 900
        }
}


deepTauScores= ["rawDeepTau2017v2p1VSe","rawDeepTau2017v2p1VSmu",
            "rawDeepTau2017v2p1VSjet", "rawDeepTau2018v2p5VSe", "rawDeepTau2018v2p5VSmu",
            "rawDeepTau2018v2p5VSjet",
            "idDeepTau2017v2p1VSe", "idDeepTau2017v2p1VSjet", "idDeepTau2017v2p1VSmu",
            "idDeepTau2018v2p5VSe","idDeepTau2018v2p5VSjet","idDeepTau2018v2p5VSmu",
            "decayMode"]
def createDenumDictEntry(treeName, inFile, sample_dict, config,nEvents, evtIds):

    df = ROOT.RDataFrame(treeName, inFile)
    if nEvents is not None:
        df = df.Range(nEvents)
    if len(evtIds) > 0:
        df = df.Filter(f"static const std::set<ULong64_t> evts = {{ {evtIds} }}; return evts.count(event) > 0;")
    sample_dict['beginning'] = df.Count().GetValue()
    period = config["GLOBAL"]["era"]
    df = df.Define("period", f"static_cast<int>(Period::{period})")
    df = Baseline.applyMETFlags(df, config["GLOBAL"]["MET_flags"])
    sample_dict['MET_flags'] = df.Count().GetValue()
    #print(f"MET flags {df.Count().GetValue()}")
    is_data = 'false'
    isData = False

    df = df.Define("isData", is_data)
    df = Baseline.CreateRecoP4(df)
    df = Baseline.DefineGenObjects(df, isData=isData, isHH=True)
    df, syst_dict = Corrections.applyScaleUncertainties(df)
    syst_dict = {'Central': 'Central'}
    dfw = Utilities.DataFrameWrapper(df,defaultColToSave)
    df = Baseline.SelectRecoP4(df, 'Central')
    #sample_dict["SelectRecoP4"]=df.Count().GetValue()
    #df = df.Define("n_GenJet", "GenJet_idx.size()")
    #df = Baseline.PassGenAcceptance(df)
    #sample_dict["PassGenAcceptance"]=df.Count().GetValue()
    #df = Baseline.GenJetSelection(df)
    #sample_dict["GenJetSelection"]=df.Count().GetValue()
    #df = Baseline.GenJetHttOverlapRemoval(df)
    #sample_dict["GenJetHttOverlapRemoval"]=df.Count().GetValue()
    #df = Baseline.RequestOnlyResolvedGenJets(df)
    #sample_dict["RequestOnlyResolvedGenJets"]=df.Count().GetValue()
    #df = Baseline.RecoLeptonsSelection(df)
    #sample_dict["RecoLeptonsSelection"]=df.Count().GetValue()
    #print("1 RecoLeptonsSelection ", df.Count().GetValue())
    # df = Baseline.RecoJetAcceptance(df)
    #df = Baseline.RecoHttCandidateSelection(df, config["GLOBAL"])
    #sample_dict["RecoHttCandidateSelection"]=df.Count().GetValue()
    #print("1 RecoHttCandidateSelection ", df.Count().GetValue())

    #df = Baseline.RecoJetSelection(df)
    #sample_dict["RecoJetSelection"]=df.Count().GetValue()
    #print("1 RecoJetSelection ", df.Count().GetValue())

    #df = Baseline.ThirdLeptonVeto(df)
    #sample_dict["ThirdLeptonVeto"]=df.Count().GetValue()
    #print("1 ThirdLeptonVeto ", df.Count().GetValue())


    #df = df.Define('genChannel', 'genHttCandidate->channel()')
    #df = df.Define('recoChannel', 'HttCandidate.channel()')

    #df = df.Filter("genChannel == recoChannel", "SameGenRecoChannels")
    #print("1 SameGenRecoChannels ", df.Count().GetValue())
    #print("eTau ", df.Filter("recoChannel==Channel::eTau").Count().GetValue())
    #print("muTau ", df.Filter("recoChannel==Channel::muTau").Count().GetValue())
    #print("tauTau ", df.Filter("recoChannel==Channel::tauTau").Count().GetValue())

    #sample_dict["genChannel == recoChannel"]=df.Count().GetValue()
    #df = df.Filter("GenRecoMatching(*genHttCandidate, HttCandidate, 0.2)", "SameGenxRecoHTT")
    # df = Baseline.RequestOnlyResolvedRecoJets(df)
    #sample_dict["GenRecoMatching"]=df.Count().GetValue()


    #df = Baseline.GenRecoJetMatching(df)
    #sample_dict["GenRecoJetMatching"]=df.Count().GetValue()


    return dfw

def denumCalc(dfw, syst_name, isData, trigger_class, mode, nLegs, deepTauVersion='2p1'):
    dfw.Apply(Baseline.SelectRecoP4, syst_name)
    #print("2 SelectRecoP4 ", dfw.df.Count().GetValue())
    # qua va Select btagShapeWeight
    sample_dict["SelectRecoP4"]=dfw.df.Count().GetValue()
    #print(sample_dict)
    if mode == "HH":
        dfw.Apply(Baseline.RecoLeptonsSelection)
        sample_dict["RecoLeptonsSelection"]=dfw.df.Count().GetValue()
        #print(sample_dict)
        #print("2 RecoLeptonsSelection ", dfw.df.Count().GetValue())

        dfw.Apply(Baseline.RecoHttCandidateSelection, config["GLOBAL"])
        sample_dict["RecoHttCandidateSelection"]=dfw.df.Count().GetValue()
        #print("2 RecoHttCandidateSelection ", dfw.df.Count().GetValue())
        #print(sample_dict)
        dfw.Apply(Baseline.RecoJetSelection)
        sample_dict["RecoJetSelection"]=dfw.df.Count().GetValue()
        #print("2 RecoJetSelection ", dfw.df.Count().GetValue())
        #print(sample_dict)
        dfw.Apply(Baseline.ThirdLeptonVeto)
        sample_dict["ThirdLeptonVeto"]=dfw.df.Count().GetValue()
        #print("2 ThirdLeptonVeto ", dfw.df.Count().GetValue())
    elif mode == 'ttHH':
        dfw.Apply(Baseline.RecottHttCandidateSelection_ttHH)
        dfw.Apply(Baseline.RecoJetSelection_ttHH)

    dfw.Apply(Baseline.DefineHbbCand)
    dfw.DefineAndAppend("Hbb_isValid" , "HbbCandidate.has_value()")
    dfw.Apply(Baseline.ExtraRecoJetSelection)
    #print(f"after ExtraRecoJetSelection: {dfw.df.Count().GetValue()}")

    dfw.Apply(Corrections.jet.getEnergyResolution)
    #print(f"after getEnergyResolution: {dfw.df.Count().GetValue()}")


    dfw.Apply(Corrections.btag.getWPid)
    #print(f"after getWPid: {dfw.df.Count().GetValue()}")

    dfw.Apply(Baseline.ApplyJetSelection)
    #print(f"after ApplyJetSelection: {dfw.df.Count().GetValue()}")
    sample_dict["ApplyJetSelection"]=dfw.df.Count().GetValue()
    #print(sample_dict)

    dfw.Define(f"Tau_recoJetMatchIdx", f"FindMatching(Tau_p4, Jet_p4, 0.5)")
    dfw.Define(f"Muon_recoJetMatchIdx", f"FindMatching(Muon_p4, Jet_p4, 0.5)")
    dfw.Define( f"Electron_recoJetMatchIdx", f"FindMatching(Electron_p4, Jet_p4, 0.5)")
    dfw.DefineAndAppend("channelId","static_cast<int>(HttCandidate.channel())")

    channel_to_select = " || ".join(f"HttCandidate.channel()==Channel::{ch}" for ch in config["GLOBAL"]["channelSelection"])
    dfw.Filter(channel_to_select, "select channels")
    sample_dict["Channels"]=dfw.df.Count().GetValue()

    deepTauYear = '2017' if deepTauVersion == '2p1' else '2018'
    for leg_idx in range(2):
        def LegVar(var_name, var_expr, var_type=None, var_cond=None, check_leg_type=True, default=0):
                cond = var_cond
                if check_leg_type:
                    type_cond = f"HttCandidate.leg_type[{leg_idx}] != Leg::none"
                    cond = f"{type_cond} && ({cond})" if cond else type_cond
                define_expr = f'static_cast<{var_type}>({var_expr})' if var_type else var_expr
                if cond:
                    define_expr = f'{cond} ? ({define_expr}) : {default}'
                dfw.DefineAndAppend( f"tau{leg_idx+1}_{var_name}", define_expr)
        LegVar('charge', f'HttCandidate.leg_charge[{leg_idx}]', var_type='int')

        dfw.Define(f"tau{leg_idx+1}_recoJetMatchIdx", f"""HttCandidate.leg_type[{leg_idx}] != Leg::none
                                                          ? FindMatching(HttCandidate.leg_p4[{leg_idx}], Jet_p4, 0.3)
                                                          : -1""")
        LegVar('iso', f"HttCandidate.leg_rawIso.at({leg_idx})")
        for deepTauScore in deepTauScores:
            LegVar(deepTauScore, f"Tau_{deepTauScore}.at(HttCandidate.leg_index[{leg_idx}])",
                   var_cond=f"HttCandidate.leg_type[{leg_idx}] == Leg::tau", default='-1.f')
    ##tau2_iso = f"Tau_idDeepTau{deepTauYear}{deepTauVersion}VSjet(HttCandidate.leg_index[1])"
    #dfw.df.Define("tau1_charge", "HttCandidate.leg_charge[0]").Display({"tau1_charge"}).Print()#Define("tau2_charge", "HttCandidate.leg_charge[1]").Define(f"tau2_iso", "Tau_idDeepTau2017v2p1VSjet(HttCandidate.leg_index[1])").Display({"tau1_charge","tau2_charge"}).Print()
    #dfw.df.Define("tau2_charge", "HttCandidate.leg_charge[2]").Display({"tau2_charge"}).Print()#Define("tau2_charge", "HttCandidate.leg_charge[1]").Define(f"tau2_iso", "Tau_idDeepTau2017v2p1VSjet(HttCandidate.leg_index[1])").Display({"tau1_charge","tau2_charge"}).Print()
    os_iso_filtering = f"""(tau1_charge * tau2_charge < 0) && tau2_idDeepTau{deepTauYear}v{deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value}"""
    #print(os_iso_filtering)

    dfw.Filter(os_iso_filtering, "os_iso_filtering")
    sample_dict["os_iso_filtering"]=dfw.df.Count().GetValue()

    sample_dict["eTau"]=dfw.df.Filter("HttCandidate.channel()==Channel::eTau").Count().GetValue()
    #print(sample_dict)

    sample_dict["muTau"]=dfw.df.Filter("HttCandidate.channel()==Channel::muTau").Count().GetValue()
    #print(sample_dict)

    sample_dict["tauTau"]=dfw.df.Filter("HttCandidate.channel()==Channel::tauTau").Count().GetValue()
    #print(sample_dict)


    if trigger_class is not None:
        hltBranches = dfw.Apply(trigger_class.ApplyTriggers, nLegs, isData)
        #print(f"after ApplyTriggers: {dfw.df.Count().GetValue()}")
        sample_dict["Triggers"]=dfw.df.Count().GetValue()
        #print(sample_dict)




if __name__ == "__main__":
    import argparse
    import os
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, type=str)
    parser.add_argument('--nEvents', type=int, default=None)
    parser.add_argument('--evtIds', type=str, default='')
    parser.add_argument('--customisations', type=str, default="")
    parser.add_argument('--mode', type=str, default="HH")
    parser.add_argument('--sample',type=str, default="GluGluToBulkGravitonToHHTo2B2Tau_M-1000")
    args = parser.parse_args()

    startTime = time.time()
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "include/GenTools.h"')
    particleFile = f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt"
    ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{particleFile}\");")
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    if len(args.customisations)>0:
        Utilities.ApplyConfigCustomisations(config['GLOBAL'], args.customisations)
    loadTF = args.mode == "HH"
    loadHHBtag = args.mode == "HH"
    nLegs = 4 if args.mode == "ttHH" else 2
    Baseline.Initialize(loadTF, loadHHBtag)
    Corrections.Initialize(config=config['GLOBAL'],isData=False)
    triggerFile = config['GLOBAL'].get('triggerFile')
    #print(triggerFile)
    if triggerFile is not None:
        triggerFile = os.path.join(os.environ['ANALYSIS_PATH'], triggerFile)
        trigger_class = Triggers.Triggers(triggerFile)
    else:
        trigger_class = None
    dict_denumerators_BG = {}
    isData = False

    for sampleName,dict_sample in dict_files_BulkGraviton.items():
        if(sampleName!=args.sample) : continue
        sample_dict = {}
        dfw = createDenumDictEntry('Events', dict_sample['fileName'],sample_dict, config, args.nEvents, args.evtIds)
        mass = dict_sample['mass']
        denumCalc(dfw, 'Central', isData, trigger_class, args.mode, nLegs)
        #print(sample_dict)
        sample_dict['mass'] = mass
        dict_denumerators_BG[sampleName] = sample_dict
        print(dict_denumerators_BG)
    with open('/afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/Studies/Triggers/denumerators_BulkGraviton.json', 'w') as fp:
        json.dump(dict_denumerators_BG, fp)
    dict_denumerators_R = {}
    for sampleName,dict_sample in dict_files_Radion.items():
        if(sampleName!=args.sample) : continue
        sample_dict = {}
        dfw = createDenumDictEntry('Events', dict_sample['fileName'],sample_dict, config, args.nEvents, args.evtIds)
        mass = dict_sample['mass']
        sample_dict['mass'] = mass
        denumCalc(dfw, 'Central', isData, trigger_class, args.mode, nLegs)

        dict_denumerators_R[sampleName] = sample_dict
        print(dict_denumerators_R)

    with open('/afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/Studies/Triggers/denumerators_Radion.json', 'w') as fp:
        json.dump(dict_denumerators_R, fp)

    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))
