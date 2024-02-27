
import copy
import datetime
import os
import sys
import ROOT
import shutil
import zlib
import time
import matplotlib.pyplot as plt

if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])


#ROOT.EnableImplicitMT(1)
ROOT.EnableThreadSafety()

import Common.Utilities as Utilities
from Analysis.HistHelper import *
from Analysis.hh_bbtautau import *

ggR_samples = [ "GluGluToRadionToHHTo2B2Tau_M-250", "GluGluToRadionToHHTo2B2Tau_M-260", "GluGluToRadionToHHTo2B2Tau_M-270", "GluGluToRadionToHHTo2B2Tau_M-280", "GluGluToRadionToHHTo2B2Tau_M-300", "GluGluToRadionToHHTo2B2Tau_M-320", "GluGluToRadionToHHTo2B2Tau_M-350", "GluGluToRadionToHHTo2B2Tau_M-450", "GluGluToRadionToHHTo2B2Tau_M-500", "GluGluToRadionToHHTo2B2Tau_M-550", "GluGluToRadionToHHTo2B2Tau_M-600", "GluGluToRadionToHHTo2B2Tau_M-650", "GluGluToRadionToHHTo2B2Tau_M-700", "GluGluToRadionToHHTo2B2Tau_M-750", "GluGluToRadionToHHTo2B2Tau_M-800", "GluGluToRadionToHHTo2B2Tau_M-850", "GluGluToRadionToHHTo2B2Tau_M-900", "GluGluToRadionToHHTo2B2Tau_M-1000", "GluGluToRadionToHHTo2B2Tau_M-1250", "GluGluToRadionToHHTo2B2Tau_M-1500", "GluGluToRadionToHHTo2B2Tau_M-1750", "GluGluToRadionToHHTo2B2Tau_M-2000", "GluGluToRadionToHHTo2B2Tau_M-2500", "GluGluToRadionToHHTo2B2Tau_M-3000"]

ggBG_samples = [
    'GluGluToBulkGravitonToHHTo2B2Tau_M-250', 'GluGluToBulkGravitonToHHTo2B2Tau_M-260', 'GluGluToBulkGravitonToHHTo2B2Tau_M-270', 'GluGluToBulkGravitonToHHTo2B2Tau_M-280', 'GluGluToBulkGravitonToHHTo2B2Tau_M-300', 'GluGluToBulkGravitonToHHTo2B2Tau_M-320', 'GluGluToBulkGravitonToHHTo2B2Tau_M-350', #'GluGluToBulkGravitonToHHTo2B2Tau_M-400',
    'GluGluToBulkGravitonToHHTo2B2Tau_M-450', 'GluGluToBulkGravitonToHHTo2B2Tau_M-500', 'GluGluToBulkGravitonToHHTo2B2Tau_M-550', 'GluGluToBulkGravitonToHHTo2B2Tau_M-600', 'GluGluToBulkGravitonToHHTo2B2Tau_M-650', 'GluGluToBulkGravitonToHHTo2B2Tau_M-700', 'GluGluToBulkGravitonToHHTo2B2Tau_M-750', 'GluGluToBulkGravitonToHHTo2B2Tau_M-800', 'GluGluToBulkGravitonToHHTo2B2Tau_M-850', 'GluGluToBulkGravitonToHHTo2B2Tau_M-900',
    'GluGluToBulkGravitonToHHTo2B2Tau_M-1000', 'GluGluToBulkGravitonToHHTo2B2Tau_M-1250', 'GluGluToBulkGravitonToHHTo2B2Tau_M-1500', 'GluGluToBulkGravitonToHHTo2B2Tau_M-1750', 'GluGluToBulkGravitonToHHTo2B2Tau_M-2000', 'GluGluToBulkGravitonToHHTo2B2Tau_M-2500','GluGluToBulkGravitonToHHTo2B2Tau_M-3000'
    ]

def AddEfficiencyToDict(df,trg_list, channel, n_initial_channel,eff_key, eff_dict):
    filter_expr = f' {channel} && ('
    filter_expr+= ' || '.join(trg for trg in trg_list)
    filter_expr+= ')'
    n_channel = df.Filter(filter_expr).Count().GetValue()
    eff_channel = n_channel / n_initial_channel
    print(f"with {filter_expr} : n_initial{channel} = {n_initial_channel}, n_{channel} = {n_channel}, eff_{channel} = {round(eff_channel,2)}")
    if eff_key not in eff_dict.keys():
        eff_dict[eff_key] = []
    eff_dict[eff_key].append(round(eff_channel,2))

def makeplot(eff_dict, labels, channel, x_values, sample,wantBigTau,deepTauWP):
    colors = ['blue', 'green', 'red', 'orange', 'purple', 'pink', 'yellow', 'cyan','black']
    markers = ['o', '^', 's', 'D', 'x', 'v', 'p', '*','o']
    plt.figure()
    for i, (key, values) in enumerate(eff_dict.items()):
        print(i, key, values)
        plt.plot(x_values, values, color=colors[i % len(colors)],marker=markers[i % len(markers)], label=labels[i%len(labels)])

    #### Legend + titles + axis adjustment ####
    plt.title(f'efficiencies for channel {channel} and {sample}')
    plt.xlabel('mass')
    plt.ylabel('efficiency')
    plt.ylim(0., 1.)
    plt.legend()
    figName = f'Studies/Triggers/eff_{channel}_{sample}_{deepTauWP}'
    if wantBigTau:
        figName+='_bigTau'
    plt.savefig(f'{figName}.png')


tau_pt_limits_dict = {
    "2016":
    {
        "eTau":[26,26,26],
        "muTau":[25, 20, 25],
        "tauTau":[40, 40, 40],
    },
    "2017":
    {
        "eTau":[33, 25, 35],
        "muTau":[28, 21, 32],
        "tauTau":[40, 40, 40],
    },
    "2018":
    {
        "eTau":[33, 25, 35],
        "muTau":[25, 21, 32],
        "tauTau":[40, 40, 40],
    }
}
def get_regions_dict(channel, singleTau_pt_limits=[190,190], met_th=180, bigtau=False):
    regions = {}
    tau1_eta_sel = {
        "eTau": "abs(tau1_eta) <= 2.5",
        "muTau": "abs(tau1_eta) <= 2.4",
        "tauTau": "abs(tau1_eta)<=2.1"
    }
    tau1_2p1 = "abs(tau1_eta)<=2.1"
    tau2_2p1 = "abs(tau2_eta)<=2.1"
    tau_pt_limits = tau_pt_limits_dict["2018"][channel]
    first_tau_sel = "tau1_pt >= {0} && {1}"
    second_tau_sel = "tau2_pt >= {0} && {1}"

    second_tau_sel_singleTau = second_tau_sel.format(singleTau_pt_limits[1], tau2_2p1)
    first_tau_sel_singleTau = first_tau_sel.format(singleTau_pt_limits[0], tau1_eta_sel[channel])
    first_tau_sel_other = first_tau_sel.format(tau_pt_limits[0], tau1_eta_sel[channel])
    second_tau_sel_other = second_tau_sel.format(tau_pt_limits[1], tau2_2p1)
    single_lepton_validity = first_tau_sel.format(tau_pt_limits[0], tau1_eta_sel[channel])
    cross_lepton_validity_first = first_tau_sel.format(tau_pt_limits[1], tau1_2p1)
    cross_lepton_validity_second = second_tau_sel.format(tau_pt_limits[2], tau2_2p1)
    cross_lepton_validity  = f"{cross_lepton_validity_first} && {cross_lepton_validity_second}"


    if bigtau:
        if channel == "tauTau":
            #second_tau_sel_singleTau = second_tau_sel.format(singleTau_pt_limits[1], tau2_2p1)
            #first_tau_sel_singleTau = first_tau_sel.format(singleTau_pt_limits[0], tau1_eta_sel[channel])
            regions["singleTau_region"] = f"""(({first_tau_sel_singleTau}) || ({second_tau_sel_singleTau}))"""

            #first_tau_sel_other = first_tau_sel.format(tau_pt_limits[0], tau1_eta_sel[channel])
            #second_tau_sel_other = second_tau_sel.format(tau_pt_limits[1], tau2_2p1)
            regions["other_trg_region"] = f"""(({first_tau_sel_other}) &&( {second_tau_sel_other})) && !({regions["singleTau_region"]})"""
        else:
            #second_tau_sel_singleTau = second_tau_sel.format(singleTau_pt_limits[1], tau2_2p1)
            regions["singleTau_region"] = second_tau_sel_singleTau

            #single_lepton_validity = first_tau_sel.format(tau_pt_limits[0], tau1_eta_sel[channel])
            #cross_lepton_validity_first = first_tau_sel.format(tau_pt_limits[1], tau1_2p1)
            #cross_lepton_validity_second = second_tau_sel.format(tau_pt_limits[2], tau2_2p1)
            #cross_lepton_validity  = f"{cross_lepton_validity_first} && {cross_lepton_validity_second}"
            regions['other_trg_region'] = f"""(({single_lepton_validity}) || ({cross_lepton_validity})) && !({regions["singleTau_region"]})"""

    else:
        if channel == "tauTau":
            #first_tau_sel_other = first_tau_sel.format(tau_pt_limits[0], tau1_eta_sel[channel])
            #second_tau_sel_other = second_tau_sel.format(tau_pt_limits[1], tau2_2p1)
            #second_tau_sel_singleTau = second_tau_sel.format(singleTau_pt_limits[1], tau2_2p1)
            #first_tau_sel_singleTau = first_tau_sel.format(singleTau_pt_limits[0], tau1_eta_sel[channel])
            regions["other_trg_region"] = f"""(({first_tau_sel_other}) && ({second_tau_sel_other}))"""
            regions["singleTau_region"] = f"""(({first_tau_sel_singleTau}) || ({second_tau_sel_singleTau}))&& !({regions["other_trg_region"]})"""

        else:
            #single_lepton_validity = first_tau_sel.format(tau_pt_limits[0], tau1_eta_sel[channel])
            #cross_lepton_validity_first = first_tau_sel.format(tau_pt_limits[1], tau1_2p1)
            #cross_lepton_validity_second = second_tau_sel.format(tau_pt_limits[2], tau2_2p1)
            #cross_lepton_validity  = f"{cross_lepton_validity_first} && {cross_lepton_validity_second}"
            #second_tau_sel_singleTau = second_tau_sel.format(singleTau_pt_limits[1], tau2_2p1)
            regions["other_trg_region"] = f"""(({single_lepton_validity}) || ({cross_lepton_validity}))"""
            regions["singleTau_region"] = f"""({second_tau_sel_singleTau}) &&!({regions["other_trg_region"]}) """

    regions["MET_region"] = f"""(met_pt > {met_th} && !( ({regions["other_trg_region"]}) || ( {regions["singleTau_region"]} ) ) )"""
    return regions



if __name__ == "__main__":

    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--sample', required=False, type=str, default='GluGluToBulkGraviton')
    parser.add_argument('--deepTauVersion', required=False, type=str, default='v2p1')
    parser.add_argument('--wantBigTau', required=False, type=bool, default=False)
    parser.add_argument('--deepTauWP', required=False, type=str, default='VLoose')
    args = parser.parse_args()

    startTime = time.time()
    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    ROOT.gInterpreter.Declare(f'#include "include/KinFitNamespace.h"')
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    ROOT.gROOT.ProcessLine('#include "include/AnalysisTools.h"')

    #### useful stuff ####

    inDir = "/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v1_deepTau2p1_HTT/"
    #inDir = "/afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/output/Run2_2018/{}/anaTuples/"
    masses = []

    eff_etau = {}
    eff_mutau = {}
    eff_tautau = {}
    #### OS_ISO requirements ####

    deepTauYear = '2017' if args.deepTauVersion=='v2p1' else '2018'

    os_iso_filtering = {
        'VLoose': f"""(tau1_charge * tau2_charge < 0) && tau2_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.VLoose.value}""",
        'Loose': f"""(tau1_charge * tau2_charge < 0) && tau2_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Loose.value}""",
        'Medium':f"""(tau1_charge * tau2_charge < 0) && tau2_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value}"""
        }

    #### begin of loop over samples ####
    etau_labels = []
    mutau_labels = []
    tautau_labels = []

    sample_list = ggR_samples if args.sample == 'GluGluToRadion' else ggBG_samples

    for sample_name in sample_list:
        mass_string = sample_name.split('-')[-1]
        #if mass_string != '300':continue
        mass_int = int(mass_string)
        masses.append(mass_int)
        #inFile = os.path.join(inDir.format(sample_name), "nanoHTT_0.root")
        inFile = os.path.join(inDir,f"{sample_name}", "nanoHTT_0.root")
        if not os.path.exists(inFile) :
            print(f"{inFile} does not exist")
            continue
        #print(sample_name)
        print(inFile)
        df_initial = ROOT.RDataFrame('Events', inFile)
        dfWrapped_central  = DataFrameBuilder(ROOT.RDataFrame('Events', inFile), args.deepTauVersion)
        PrepareDfWrapped(dfWrapped_central)

        #print(f"nEventds initial : {dfWrapped_central.df.Count().GetValue()}" )
        nInitial_eTau_0 = dfWrapped_central.df.Filter('eTau').Count().GetValue()
        nInitial_muTau_0 = dfWrapped_central.df.Filter('muTau').Count().GetValue()
        nInitial_tauTau_0 = dfWrapped_central.df.Filter('tauTau').Count().GetValue()

        df_eTau = dfWrapped_central.df.Filter('eTau').Filter(os_iso_filtering[args.deepTauWP])
        nInitial_eTau = df_eTau.Count().GetValue()

        df_muTau = dfWrapped_central.df.Filter('muTau').Filter(os_iso_filtering[args.deepTauWP])
        nInitial_muTau = df_muTau.Count().GetValue()

        df_tauTau = dfWrapped_central.df.Filter('tauTau').Filter(os_iso_filtering[args.deepTauWP])
        nInitial_tauTau = df_tauTau.Count().GetValue()

        #df_tauTau = df_tauTau.Filter("OS")
        #print(f"nEvents after OS = {df_tauTau.Count().GetValue()}")
        #df_tauTau = df_tauTau.Filter("Iso")
        #print(f"nEvents after OS, Iso = {df_tauTau.Count().GetValue()}")

        dataframes_channel = {'eTau':df_eTau, 'muTau':df_muTau, 'tauTau': df_tauTau}
        for channel in ['eTau','muTau','tauTau']:
            regions_expr = get_regions_dict(channel, [190,190], 180, args.wantBigTau)
            #print(regions_expr)
            for reg_key,reg_exp in regions_expr.items():
                #print(reg_key)
                #print(reg_exp)
                dataframes_channel[channel] = dataframes_channel[channel].Define(reg_key, reg_exp)
                #print(channel)
                #print(dataframes_channel[channel].GetColumnNames())
        pass_met = "(HLT_MET && MET_region && !(singleTau_region) && ! (other_trg_region))"
        pass_singleTau = "(HLT_singleTau && singleTau_region && !(MET_region) && ! (other_trg_region))"
        pass_other_trg = "({} && other_trg_region  && !(MET_region) && ! (singleTau_region) )  "

        #### eTau efficiencies ####

        pass_eleTau = pass_other_trg.format("HLT_etau")
        #print(pass_eleTau)
        pass_singleEle = pass_other_trg.format("HLT_singleEle")
        trg_eTau_list_1 = [pass_singleEle,pass_eleTau, pass_singleTau, pass_met ]
        etau_labels.append("singleEle,eTau,singleTau,MET")
        AddEfficiencyToDict(dataframes_channel['eTau'], trg_eTau_list_1, 'eTau', nInitial_eTau,'eff_eTau_1', eff_etau)


        trg_eTau_list_2 = [pass_singleEle,pass_eleTau, pass_singleTau ]
        etau_labels.append("singleEle,eTau,singleTau")
        AddEfficiencyToDict(dataframes_channel['eTau'], trg_eTau_list_2, 'eTau', nInitial_eTau,'eff_eTau_2', eff_etau)

        trg_eTau_list_3 = [pass_singleEle,pass_eleTau, pass_met ]
        etau_labels.append("singleEle,eTau,MET")
        AddEfficiencyToDict(dataframes_channel['eTau'], trg_eTau_list_3, 'eTau', nInitial_eTau,'eff_eTau_3', eff_etau)

        trg_eTau_list_4 = [pass_singleEle,pass_eleTau]
        etau_labels.append("singleEle,eTau")
        AddEfficiencyToDict(dataframes_channel['eTau'], trg_eTau_list_4, 'eTau', nInitial_eTau,'eff_eTau_4', eff_etau)


        #### muTau efficiencies ####
        pass_muTau = pass_other_trg.format("HLT_mutau")
        pass_singleMu = pass_other_trg.format("HLT_singleMu")
        pass_singleMu50 = pass_other_trg.format("HLT_singleMu50")

        trg_muTau_list_1 = [pass_singleMu, pass_singleMu50, pass_muTau, pass_met,pass_singleTau]
        mutau_labels.append("all")
        AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_1, 'muTau', nInitial_muTau,'eff_muTau_1', eff_mutau)


        trg_muTau_list_2 = [pass_singleMu, pass_muTau, pass_singleTau,pass_met]
        mutau_labels.append("singleMu,muTau,singleTau,MET")
        AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_2, 'muTau', nInitial_muTau,'eff_muTau_2', eff_mutau)

        trg_muTau_list_3 = [pass_singleMu, pass_muTau, pass_singleTau]
        mutau_labels.append("singleMu,muTau,singleTau")
        AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_3, 'muTau', nInitial_muTau,'eff_muTau_3', eff_mutau)

        trg_muTau_list_4 = [pass_singleMu, pass_muTau, pass_met]
        mutau_labels.append("singleMu,muTau,MET")
        AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_4, 'muTau', nInitial_muTau,'eff_muTau_4', eff_mutau)

        trg_muTau_list_5 = [pass_singleMu, pass_muTau,pass_singleMu50, pass_singleTau]
        mutau_labels.append("singleMu,muTau,singleMu50,singleTau")
        AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_5, 'muTau', nInitial_muTau,'eff_muTau_5', eff_mutau)

        trg_muTau_list_6 = [pass_singleMu, pass_muTau,pass_singleMu50, pass_singleTau, pass_met]
        mutau_labels.append("singleMu,muTau,singleMu50,singleTau,MET")
        AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_6, 'muTau', nInitial_muTau,'eff_muTau_6', eff_mutau)

        trg_muTau_list_7 = [pass_singleMu, pass_muTau,pass_singleMu50, pass_met]

        mutau_labels.append("singleMu,muTau,singleMu50,MET")
        AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_7, 'muTau', nInitial_muTau,'eff_muTau_7', eff_mutau)

        trg_muTau_list_8 = [pass_singleMu, pass_muTau,pass_singleMu50]
        mutau_labels.append("singleMu,muTau,singleMu50")
        AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_8, 'muTau', nInitial_muTau,'eff_muTau_8', eff_mutau)

        trg_muTau_list_9 = [pass_singleMu, pass_muTau]
        mutau_labels.append("singleMu,muTau")
        AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_9, 'muTau', nInitial_muTau,'eff_muTau_9', eff_mutau)


        #### tauTau efficiencies ####
        pass_diTau = pass_other_trg.format("HLT_ditau")
        #print(f""" after only tauTau region {dataframes_channel['tauTau'].Filter("other_trg_region_tauTau").Count().GetValue()}""")
        #print(f""" after only HLT_ditau {dataframes_channel['tauTau'].Filter("HLT_ditau").Count().GetValue()}""")
        #print(f""" after tauTau region && HLT_ditau {dataframes_channel['tauTau'].Filter(pass_diTau_tauTau).Count().GetValue()}""")
        trg_tauTau_list_1 = [pass_diTau, pass_singleTau, pass_met]
        tautau_labels.append("diTau,singleTau,MET")
        AddEfficiencyToDict(dataframes_channel['tauTau'], trg_tauTau_list_1, 'tauTau', nInitial_tauTau,'eff_tauTau_1', eff_tautau)

        trg_tauTau_list_2 = [pass_diTau, pass_singleTau]
        tautau_labels.append("diTau,singleTau")
        AddEfficiencyToDict(dataframes_channel['tauTau'], trg_tauTau_list_2, 'tauTau', nInitial_tauTau,'eff_tauTau_2', eff_tautau)

        trg_tauTau_list_3 = [pass_diTau, pass_met]
        tautau_labels.append("diTau,MET")
        AddEfficiencyToDict(dataframes_channel['tauTau'], trg_tauTau_list_3, 'tauTau', nInitial_tauTau,'eff_tauTau_3', eff_tautau)

        trg_tauTau_list_4 = [pass_diTau]

        tautau_labels.append("diTau")
        AddEfficiencyToDict(dataframes_channel['tauTau'], trg_tauTau_list_4, 'tauTau', nInitial_tauTau,'eff_tauTau_4', eff_tautau)

    #print(eff_etau)
    x_values = masses
    #print(eff_etau)
    #print(etau_labels)
    #print(len(masses))
    makeplot(eff_etau, etau_labels, 'eTau', x_values, args.sample, args.wantBigTau, args.deepTauWP)
    makeplot(eff_mutau, mutau_labels, 'muTau', x_values, args.sample, args.wantBigTau, args.deepTauWP)
    makeplot(eff_tautau, tautau_labels, 'tauTau', x_values, args.sample, args.wantBigTau, args.deepTauWP)
    plt.show()

    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))


