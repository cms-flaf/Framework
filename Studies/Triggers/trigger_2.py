
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

''' # unused function
def GetTrgWeight(triggers_list):
    weight_expression_1 = "weight_tau1_TrgSF_{}_Central"
    weight_expression_2 = "weight_tau2_TrgSF_{}_Central"
    weight_triggers = []
    for trg_name in triggers_list:
        trg_name_split = trg_name.split('_')
        if trg_name_split[1] =='MET' : continue
        weight_triggers.append(weight_expression_1.format(trg_name_split[1]))
        weight_triggers.append(weight_expression_2.format(trg_name_split[1]))
    weights_to_apply = [ "weight_TauID_Central", "weight_tau1_EleidSF_Central", "weight_tau1_MuidSF_Central", "weight_tau2_EleidSF_Central", "weight_tau2_MuidSF_Central","weight_total", "weight_L1PreFiring_Central","weight_L1PreFiring_ECAL_Central", "weight_bTagShapeSF"]
    weights_to_apply.extend(["weight_Jet_PUJetID_Central_b1", "weight_Jet_PUJetID_Central_b2"])
    weights_to_apply.extend(weight_triggers)
    total_weight = '*'.join(weights_to_apply)
    return total_weight
'''

def AddEfficiencyToDict(trg_list, channel, n_initial_channel,eff_key, eff_dict):
    filter_expr = f' {channel} && ('
    filter_expr+= ' || '.join(trg for trg in trg_list)
    filter_expr+= ')'
    n_channel = dfWrapped_central.df.Filter(filter_expr).Count().GetValue()
    eff_channel = n_channel / n_initial_channel
    print(f"with {filter_expr} : n_{channel} = {n_channel}, eff_{channel} = {round(eff_channel,2)}")
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

    plt.savefig(f'Studies/Triggers/eff_{channel}_{sample}.png')

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

    inDir = "/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v10_deepTau2p1/"
    masses = []

    eff_etau = {}
    eff_mutau = {}
    eff_tautau = {}
    #### OS_ISO requirements ####

    deepTauYear = '2017' if args.deepTauVersion=='v2p1' else '2018'
    os_iso_filtering = f"""(tau1_charge * tau2_charge < 0) && tau2_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value}"""

    #### begin of loop over samples ####
    for sample_name,json_dict in json_denumerators_dict.items():
        inFile = os.path.join(inDir,f"{sample_name}", "nano.root")
        masses.append(json_dict['mass'])
        print(sample_name)
        dfWrapped_central  = DataFrameBuilder(ROOT.RDataFrame('Events', inFile), args.deepTauVersion)
        PrepareDfWrapped(dfWrapped_central)
        dfWrapped_central.df = dfWrapped_central.df.Filter('OS_Iso')

        # denumerator = number of events taken from the dict, passing baseline + channel
        nInitial_eTau = json_dict['eTau']
        nInitial_muTau = json_dict['muTau']
        nInitial_tauTau = json_dict['tauTau']


        #### eTau efficiencies ####

        trg_eTau_list_1 = ["HLT_singleEle", "HLT_etau", "HLT_singleTau", "HLT_MET"]
        AddEfficiencyToDict(trg_eTau_list_1, 'eTau', nInitial_eTau,'eff_eTau_1', eff_etau)

        trg_eTau_list_2 = ["HLT_singleEle", "HLT_etau", "HLT_singleTau"]
        AddEfficiencyToDict(trg_eTau_list_2, 'eTau', nInitial_eTau,'eff_eTau_2', eff_etau)

        trg_eTau_list_3 = ["HLT_singleEle", "HLT_etau"]
        AddEfficiencyToDict(trg_eTau_list_3, 'eTau', nInitial_eTau,'eff_eTau_3', eff_etau)

        trg_eTau_list_4 = ["HLT_singleEle", "HLT_etau", "HLT_MET"]
        AddEfficiencyToDict(trg_eTau_list_4, 'eTau', nInitial_eTau,'eff_eTau_4', eff_etau)

        #### muTau efficiencies ####
        trg_muTau_list_1 = ["HLT_singleMu", "HLT_singleMu50", "HLT_mutau", "HLT_singleTau", "HLT_MET"]
        AddEfficiencyToDict(trg_muTau_list_1, 'muTau', nInitial_muTau,'eff_muTau_1', eff_mutau)
        trg_muTau_list_2 = ["HLT_singleMu", "HLT_mutau", "HLT_singleTau", "HLT_MET"]
        AddEfficiencyToDict(trg_muTau_list_2, 'muTau', nInitial_muTau,'eff_muTau_2', eff_mutau)
        trg_muTau_list_3 = ["HLT_singleMu", "HLT_mutau", "HLT_singleTau"]
        AddEfficiencyToDict(trg_muTau_list_3, 'muTau', nInitial_muTau,'eff_muTau_3', eff_mutau)
        trg_muTau_list_4 = ["HLT_singleMu", "HLT_mutau", "HLT_MET"]
        AddEfficiencyToDict(trg_muTau_list_4, 'muTau', nInitial_muTau,'eff_muTau_4', eff_mutau)
        trg_muTau_list_5 = ["HLT_singleMu", "HLT_mutau"]
        AddEfficiencyToDict(trg_muTau_list_5, 'muTau', nInitial_muTau,'eff_muTau_5', eff_mutau)
        trg_muTau_list_6 = ["HLT_singleMu", "HLT_singleMu50", "HLT_mutau"]
        AddEfficiencyToDict(trg_muTau_list_6, 'muTau', nInitial_muTau,'eff_muTau_6', eff_mutau)
        trg_muTau_list_7 = ["HLT_singleMu", "HLT_singleMu50", "HLT_singleTau", "HLT_mutau"]
        AddEfficiencyToDict(trg_muTau_list_7, 'muTau', nInitial_muTau,'eff_muTau_7', eff_mutau)
        trg_muTau_list_8 = ["HLT_singleMu", "HLT_singleMu50", "HLT_MET", "HLT_mutau"]
        AddEfficiencyToDict(trg_muTau_list_8, 'muTau', nInitial_muTau,'eff_muTau_8', eff_mutau)
        trg_muTau_list_9 = ["HLT_singleMu", "HLT_singleMu50", "HLT_mutau"]
        AddEfficiencyToDict(trg_muTau_list_9, 'muTau', nInitial_muTau,'eff_muTau_9', eff_mutau)

        #### tauTau efficiencies ####

        trg_tauTau_list_1 = ["HLT_ditau", "HLT_singleTau", "HLT_MET"]
        AddEfficiencyToDict(trg_tauTau_list_1, 'tauTau', nInitial_tauTau,'eff_tauTau_1', eff_tautau)

        trg_tauTau_list_2 = ["HLT_ditau", "HLT_singleTau"]
        AddEfficiencyToDict(trg_tauTau_list_2, 'tauTau', nInitial_tauTau,'eff_tauTau_2', eff_tautau)

        trg_tauTau_list_3 = ["HLT_ditau"]
        AddEfficiencyToDict(trg_tauTau_list_3, 'tauTau', nInitial_tauTau,'eff_tauTau_3', eff_tautau)

        trg_tauTau_list_4 = ["HLT_ditau", "HLT_MET"]
        AddEfficiencyToDict(trg_tauTau_list_4, 'tauTau', nInitial_tauTau,'eff_tauTau_4', eff_tautau)

    #print(eff_etau)
    etau_labels = ["singleEle,eTau,singleTau,MET","singleEle,eTau,singleTau","singleEle,eTau","singleEle,eTau,MET"]

    mutau_labels = ["all","singleMu,muTau,singleTau,MET","singleMu,muTau,singleTau","singleMu,muTau,MET","singleMu,muTau","singleMu,muTau,singleMu50,singleTau","singleMu,muTau,singleMu50,MET","singleMu,muTau,signgleMu50"]

    tautau_labels = ["diTau,singleTau,MET", "diTau,singleTau","diTau","diTau,MET"]


    x_values = masses
    print(eff_etau)
    print(etau_labels)
    print(len(masses))
    makeplot(eff_etau, etau_labels, 'eTau', x_values, args.sample)
    makeplot(eff_mutau, mutau_labels, 'muTau', x_values, args.sample)
    makeplot(eff_tautau, tautau_labels, 'tauTau', x_values, args.sample)
    plt.show()

    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))


