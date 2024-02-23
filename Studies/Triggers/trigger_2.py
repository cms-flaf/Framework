
import copy
import datetime
import os
import sys
import ROOT
import shutil
import zlib
import time
import json
import matplotlib.pyplot as plt

if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])


#ROOT.EnableImplicitMT(1)
ROOT.EnableThreadSafety()

import Common.Utilities as Utilities
from Analysis.HistHelper import *
from Analysis.hh_bbtautau import *





def AddEfficiencyToDict(dfWrapped,trg_list, channel, n_initial_channel,eff_key, eff_dict):
    filter_expr = f' {channel} && ('
    filter_expr+= ' || '.join(trg for trg in trg_list)
    filter_expr+= ')'
    n_channel = dfWrapped.df.Filter(filter_expr).Count().GetValue()
    eff_channel = n_channel / n_initial_channel
    print(f"with {eff_key} : n_initial{channel} = {n_initial_channel}, n_{channel} = {n_channel}, eff_{channel} = {round(eff_channel,2)}")
    if eff_key not in eff_dict.keys():
        eff_dict[eff_key] = []
    eff_dict[eff_key].append(round(eff_channel,2))

def makeplot(eff_dict, labels, channel, x_values, sample):
    colors = ['blue', 'green', 'red', 'orange', 'purple', 'pink', 'yellow', 'cyan','black']
    markers = ['o', '^', 's', 'D', 'x', 'v', 'p', '*','o']
    plt.figure()
    for i, (key, values) in enumerate(eff_dict.items()):
        plt.plot(x_values, values, color=colors[i % len(colors)],marker=markers[i % len(markers)], label=labels[i%len(labels)])

    #### Legend + titles + axis adjustment ####
    plt.title(f'efficiencies for channel {channel} and {sample}')
    plt.xlabel('mass')
    plt.ylabel('efficiency')
    plt.ylim(0., 1.)
    plt.legend()

    plt.savefig(f'Studies/Triggers/eff_{channel}_{sample}_wrongDenum.png')

if __name__ == "__main__":

    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--sample', required=False, type=str, default='GluGluToBulkGraviton')
    parser.add_argument('--deepTauVersion', required=False, type=str, default='v2p1')
    args = parser.parse_args()

    startTime = time.time()
    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    ROOT.gInterpreter.Declare(f'#include "include/KinFitNamespace.h"')
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    ROOT.gROOT.ProcessLine('#include "include/AnalysisTools.h"')

    #### useful stuff ####

    json_denumerators_dict = {}
    name_of_jsonFile = 'GluGluToRadion.json' if 'GluGluToRadion' in args.sample else 'GluGluToBulkGraviton.json'
    with open(os.path.join('Studies', 'Triggers', f'{args.sample}.json'), 'r') as f:
        json_denumerators_dict = json.load(f)

    inDir = "/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v1_deepTau2p1_HTT/"
    masses = []

    eff_etau = {}
    eff_mutau = {}
    eff_tautau = {}
    #### OS_ISO requirements ####

    deepTauYear = '2017' if args.deepTauVersion=='v2p1' else '2018'
    os_iso_filtering = f"""(tau1_charge * tau2_charge < 0) && tau2_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value}"""

    #### begin of loop over samples ####
    etau_labels = []
    mutau_labels = []
    tautau_labels = []
    regions = {
    'MET_region':{
        'eTau': 'met_pt > 180 && (( tau1_pt < 33 && tau2_pt < 35 ) || ( tau1_pt < 25 && tau2_pt < 190 ))',
        'muTau':'met_pt > 180 && (( tau1_pt < 25 && tau2_pt < 32 ) || ( tau1_pt < 21 && tau2_pt < 190 ))',
        'tauTau':'met_pt > 180 && (( tau1_pt < 40 && tau2_pt < 190 ) || ( tau1_pt < 190 && tau2_pt < 40 ))'
        }
    }
    regions['singleTau_region']={
            'eTau':f"""( tau1_pt < 25 && tau2_pt >= 190  && abs(tau2_eta) < 2.1) && {regions["MET_region"]["eTau"]} """,
            'muTau':f"""( tau1_pt < 21 && tau2_pt >= 190 && abs(tau2_eta) < 2.1 ) && {regions["MET_region"]["muTau"]} """,
            'tauTau':f"""( (tau2_pt < 40 && tau1_pt >= 190 && abs(tau1_eta) < 2.1) || (tau1_pt < 40 && tau2_pt >= 190 && abs(tau2_eta) < 2.1) ) && {regions["MET_region"]["tauTau"]}"""
            }
    regions['other_trg_region'] = {
            'eTau': f"""! ({regions["MET_region"]["eTau"]} || {regions["singleTau_region"]["eTau"]})""",
            'muTau': f"""! ({regions["MET_region"]["muTau"]} || {regions["singleTau_region"]["muTau"]})""",
            'tauTau': f"""! ({regions["MET_region"]["tauTau"]} || {regions["singleTau_region"]["tauTau"]})""",
            }

    for sample_name,json_dict in json_denumerators_dict.items():
        inFile = os.path.join(inDir,f"{sample_name}", "nanoHTT_0.root")
        if not os.path.exists(inFile) :
            print(f"{inFile} does not exist")
            continue
        masses.append(json_dict['mass'])
        #print(sample_name)
        dfWrapped_central  = DataFrameBuilder(ROOT.RDataFrame('Events', inFile), args.deepTauVersion)
        PrepareDfWrapped(dfWrapped_central)
        dfWrapped_central.df = dfWrapped_central.df.Filter('OS_Iso')

        # denumerator = number of events taken from the dict, passing baseline + channel
        nInitial_eTau = json_dict['eTau']
        nInitial_muTau = json_dict['muTau']
        nInitial_tauTau = json_dict['tauTau']

        for region,reg_dict in regions.items():
            for channel in channels:
                #print(f"{region}_{channel}")
                dfWrapped_central.df = dfWrapped_central.df.Define(f"{region}_{channel}", f"""{reg_dict[channel]}""")
        #### eTau efficiencies ####
        pass_met_eTau = "(HLT_MET && MET_region_eTau && !(singleTau_region_eTau) && ! (other_trg_region_eTau))"
        pass_singleTau_eTau = "(HLT_singleTau && singleTau_region_eTau && !(MET_region_eTau) && ! (other_trg_region_eTau))"
        pass_singleEle_eTau = "(HLT_singleEle && other_trg_region_eTau  && !(MET_region_eTau) && ! (singleTau_region_eTau))"
        pass_eleTau_eTau = "(HLT_etau && other_trg_region_eTau  && !(MET_region_eTau) && ! (singleTau_region_eTau))"


        trg_eTau_list_1 = [pass_eleTau_eTau, pass_singleEle_eTau, pass_singleTau_eTau, pass_met_eTau ]
        etau_labels.append("singleEle,eTau,singleTau,MET")
        AddEfficiencyToDict(dfWrapped_central, trg_eTau_list_1, 'eTau', nInitial_eTau,'eff_eTau_1', eff_etau)


        trg_eTau_list_2 = [pass_eleTau_eTau, pass_singleEle_eTau, pass_singleTau_eTau]
        etau_labels.append("singleEle,eTau,singleTau")
        AddEfficiencyToDict(dfWrapped_central, trg_eTau_list_2, 'eTau', nInitial_eTau,'eff_eTau_2', eff_etau)

        trg_eTau_list_3 = [pass_eleTau_eTau, pass_singleEle_eTau, pass_met_eTau ]
        etau_labels.append("singleEle,eTau,MET")
        AddEfficiencyToDict(dfWrapped_central, trg_eTau_list_3, 'eTau', nInitial_eTau,'eff_eTau_3', eff_etau)

        trg_eTau_list_4 = [pass_eleTau_eTau, pass_singleEle_eTau]
        etau_labels.append("singleEle,eTau")
        AddEfficiencyToDict(dfWrapped_central, trg_eTau_list_4, 'eTau', nInitial_eTau,'eff_eTau_4', eff_etau)



        #### muTau efficiencies ####
        pass_met_muTau = "(HLT_MET && MET_region_muTau && !(singleTau_region_muTau) && ! (other_trg_region_muTau))"
        pass_singleTau_muTau = "(HLT_singleTau && singleTau_region_muTau && !(MET_region_muTau) && ! (other_trg_region_muTau))"
        pass_singleMu_muTau = "(HLT_singleMu && other_trg_region_muTau  && !(MET_region_muTau) && ! (singleTau_region_muTau))"
        pass_singleMu50_muTau = "(HLT_singleMu50 && other_trg_region_muTau  && !(MET_region_muTau) && ! (singleTau_region_muTau))"
        pass_muTau_muTau = "(HLT_mutau && other_trg_region_muTau  && !(MET_region_muTau) && ! (singleTau_region_muTau))"

        trg_muTau_list_1 = [pass_singleMu_muTau, pass_singleMu50_muTau, pass_muTau_muTau, pass_met_muTau,pass_singleTau_muTau]
        mutau_labels.append("all")
        AddEfficiencyToDict(dfWrapped_central, trg_muTau_list_1, 'muTau', nInitial_muTau,'eff_muTau_1', eff_mutau)


        trg_muTau_list_2 = [pass_singleMu_muTau, pass_muTau_muTau, pass_singleTau_muTau,pass_met_muTau]
        mutau_labels.append("singleMu,muTau,singleTau,MET")
        AddEfficiencyToDict(dfWrapped_central, trg_muTau_list_2, 'muTau', nInitial_muTau,'eff_muTau_2', eff_mutau)

        trg_muTau_list_3 = [pass_singleMu_muTau, pass_muTau_muTau,pass_singleTau_muTau]
        mutau_labels.append("singleMu,muTau,singleTau")
        AddEfficiencyToDict(dfWrapped_central, trg_muTau_list_3, 'muTau', nInitial_muTau,'eff_muTau_3', eff_mutau)

        trg_muTau_list_4 = [pass_singleMu_muTau, pass_muTau_muTau,pass_met_muTau]
        mutau_labels.append("singleMu,muTau,MET")
        AddEfficiencyToDict(dfWrapped_central, trg_muTau_list_4, 'muTau', nInitial_muTau,'eff_muTau_4', eff_mutau)

        trg_muTau_list_5 = [pass_singleMu_muTau,pass_singleMu50_muTau, pass_muTau_muTau]
        mutau_labels.append("singleMu,muTau,singleMu50,singleTau")
        AddEfficiencyToDict(dfWrapped_central, trg_muTau_list_5, 'muTau', nInitial_muTau,'eff_muTau_5', eff_mutau)

        trg_muTau_list_6 = [pass_singleMu_muTau,pass_singleMu50_muTau, pass_muTau_muTau, pass_met_muTau]
        mutau_labels.append("singleMu,muTau,singleMu50,singleTau,MET")
        AddEfficiencyToDict(dfWrapped_central, trg_muTau_list_6, 'muTau', nInitial_muTau,'eff_muTau_6', eff_mutau)

        trg_muTau_list_7 = [pass_singleMu_muTau,pass_singleMu50_muTau, pass_muTau_muTau, pass_met_muTau]
        mutau_labels.append("singleMu,muTau,singleMu50,MET")
        AddEfficiencyToDict(dfWrapped_central, trg_muTau_list_7, 'muTau', nInitial_muTau,'eff_muTau_7', eff_mutau)


        trg_muTau_list_8 = [pass_singleMu_muTau, pass_muTau_muTau, pass_singleMu50_muTau]
        mutau_labels.append("singleMu,muTau,singleMu50")
        AddEfficiencyToDict(dfWrapped_central, trg_muTau_list_8, 'muTau', nInitial_muTau,'eff_muTau_8', eff_mutau)

        trg_muTau_list_9 = [pass_singleMu_muTau, pass_muTau_muTau]
        mutau_labels.append("singleMu,muTau")
        AddEfficiencyToDict(dfWrapped_central, trg_muTau_list_9, 'muTau', nInitial_muTau,'eff_muTau_9', eff_mutau)

        #### tauTau efficiencies ####

        pass_met_tauTau = "(HLT_MET && MET_region_tauTau && !(singleTau_region_tauTau) && ! (other_trg_region_tauTau))"
        pass_singleTau_tauTau = "(HLT_singleTau && singleTau_region_tauTau && !(MET_region_tauTau) && ! (other_trg_region_tauTau))"
        pass_diTau_tauTau = "(HLT_ditau && other_trg_region_tauTau  && !(MET_region_tauTau) && ! (singleTau_region_tauTau))"

        trg_tauTau_list_1 = [pass_diTau_tauTau, pass_singleTau_tauTau, pass_met_tauTau]
        tautau_labels.append("diTau,singleTau,MET")
        AddEfficiencyToDict(dfWrapped_central, trg_tauTau_list_1, 'tauTau', nInitial_tauTau,'eff_tauTau_1', eff_tautau)

        trg_tauTau_list_2 = [pass_diTau_tauTau, pass_singleTau_tauTau]
        tautau_labels.append("diTau,singleTau")
        AddEfficiencyToDict(dfWrapped_central, trg_tauTau_list_2, 'tauTau', nInitial_tauTau,'eff_tauTau_2', eff_tautau)

        trg_tauTau_list_3 = [pass_diTau_tauTau, pass_met_tauTau]
        tautau_labels.append("diTau,MET")
        AddEfficiencyToDict(dfWrapped_central, trg_tauTau_list_3, 'tauTau', nInitial_tauTau,'eff_tauTau_3', eff_tautau)

        trg_tauTau_list_4 = [pass_diTau_tauTau]
        tautau_labels.append("diTau")
        AddEfficiencyToDict(dfWrapped_central, trg_tauTau_list_4, 'tauTau', nInitial_tauTau,'eff_tauTau_4', eff_tautau)

    #print(eff_etau)
    x_values = masses
    #print(eff_etau)
    #print(etau_labels)
    #print(len(masses))
    makeplot(eff_etau, etau_labels, 'eTau', x_values, args.sample)
    makeplot(eff_mutau, mutau_labels, 'muTau', x_values, args.sample)
    makeplot(eff_tautau, tautau_labels, 'tauTau', x_values, args.sample)
    plt.show()

    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))


