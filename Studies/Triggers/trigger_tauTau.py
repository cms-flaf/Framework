
import copy
import datetime
import os
import sys
import ROOT
import shutil
import zlib
import time

if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])


#ROOT.EnableImplicitMT(1)
ROOT.EnableThreadSafety()

import Common.Utilities as Utilities
from Analysis.HistHelper import *
from Analysis.hh_bbtautau import *
from Studies.Triggers.Trig_utilities import *


if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--sample', required=False, type=str, default='GluGluToBulkGraviton')
    parser.add_argument('--deepTauVersion', required=False, type=str, default='v2p1')
    parser.add_argument('--wantBigTau', required=False, type=bool, default=False)
    parser.add_argument('--wantTightFirstTau', required=False, type=bool, default=False)
    parser.add_argument('--deepTauWP', required=False, type=str, default='medium')


    args = parser.parse_args()

    startTime = time.time()
    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    ROOT.gInterpreter.Declare(f'#include "include/KinFitNamespace.h"')
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    ROOT.gROOT.ProcessLine('#include "include/AnalysisTools.h"')

    #### useful stuff ####

    #inDir = "/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v1_deepTau2p1_HTT/"
    inDir = "/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v2_deepTau2p1_HTT/"
    #inDir = "/afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/output/Run2_2018/{}/anaTuples/"
    masses = []

    eff_tautau_validity = {}
    eff_tautau_application = {}
    eff_tautau_application_bigTau={}
    eff_tautau_application_bigTauOnly={}


    #### OS_ISO requirements ####

    deepTauYear = '2017' if args.deepTauVersion=='v2p1' else '2018'

    os_iso_filtering = {
        'VLoose': f"""(tau1_charge * tau2_charge < 0) && tau2_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.VLoose.value}""",
        'Loose': f"""(tau1_charge * tau2_charge < 0) && tau2_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Loose.value}""",
        'Medium':f"""(tau1_charge * tau2_charge < 0) && tau2_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value}"""
        }

    #### begin of loop over samples ####
    sample_list = ggR_samples if args.sample == 'GluGluToRadion' else ggBG_samples

    for sample_name in sample_list:
        tautau_labels_application = []
        tautau_labels_validity = []
        tautau_labels_application_bigTau = []
        tautau_labels_application_bigTauOnly = []

        tautau_linestyles_application = []
        tautau_linestyles_validity = []
        tautau_linestyles_application_bigTau = []
        tautau_linestyles_application_bigTauOnly = []

        tautau_colors_application = []
        tautau_colors_validity = []
        tautau_colors_application_bigTau = []
        tautau_colors_application_bigTauOnly = []

        mass_string = sample_name.split('-')[-1]
        mass_int = int(mass_string)
        masses.append(mass_int)
        inFile = os.path.join(inDir,f"{sample_name}", "nanoHTT_0.root")
        if not os.path.exists(inFile) :
            print(f"{inFile} does not exist")
            continue
        #print(sample_name)
        print(inFile)
        df_initial = ROOT.RDataFrame('Events', inFile)
        #print(f"nEventds initial : {df_initial.Count().GetValue()}" )
        dfWrapped_central  = DataFrameBuilder(ROOT.RDataFrame('Events', inFile), args.deepTauVersion)
        PrepareDfWrapped(dfWrapped_central)
        df_tauTau = dfWrapped_central.df.Filter('tauTau').Filter("tau1_gen_kind==5 && tau2_gen_kind==5")
        nInitial_tauTau = df_tauTau.Count().GetValue()


        df_tautau_vloose = df_tauTau.Filter(os_iso_filtering['VLoose'])
        df_tautau_loose = df_tauTau.Filter(os_iso_filtering['Loose'])
        df_tautau_medium = df_tauTau.Filter(os_iso_filtering['Medium'])
        if args.wantTightFirstTau:
            df_tautau_vloose.Filter("tau1_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Tight.value}")
            df_tautau_medium.Filter("tau1_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Tight.value}")
            df_tautau_loose.Filter("tau1_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Tight.value}")

        dataframes_channel = {
            #'eTau':df_eTau,
            #'muTau':df_muTau,
            #'tauTau': df_tauTau,
            'tautau_medium': df_tautau_medium,
            'tautau_loose': df_tautau_loose,
            'tautau_vloose': df_tautau_vloose,
        }
        dataframes_channel_bigTau = {
            #'eTau':df_eTau,
            #'muTau':df_muTau,
            #'tauTau': df_tauTau,
            'tautau_medium': df_tautau_medium,
            'tautau_loose': df_tautau_loose,
            'tautau_vloose': df_tautau_vloose,
        }
        for channel in ['tautau_medium','tautau_loose', 'tautau_vloose']:
            regions_expr= get_regions_dict('tauTau', [190,190], 180, False)
            regions_expr_bigTau= get_regions_dict('tauTau', [190,190], 180, True)
            for reg_key,reg_exp in regions_expr.items():
                dataframes_channel[channel] = dataframes_channel[channel].Define(reg_key, reg_exp)
            for reg_key,reg_exp in regions_expr_bigTau.items():
                dataframes_channel_bigTau[channel] = dataframes_channel_bigTau[channel].Define(reg_key, reg_exp)

        pass_MET_application = "(HLT_MET && MET_region && !(singleTau_region) && ! (other_trg_region))"
        pass_singleTau_application = "(HLT_singleTau && singleTau_region && !(MET_region) && ! (other_trg_region))"
        pass_other_trg_application= "({} && other_trg_region  && !(MET_region) && ! (singleTau_region) )  "

        pass_singleTau_validity = f"""HLT_singleTau && ({GetTrgValidityRegion("HLT_singleTau")})"""
        pass_MET_validity = f"""HLT_MET && ({GetTrgValidityRegion("HLT_MET")})"""


        #### tauTau efficiencies ####
        pass_ditau_application = pass_other_trg_application.format("HLT_ditau")
        pass_ditau_validity = f"""HLT_ditau && ({GetTrgValidityRegion("HLT_ditau")})"""

        ##### validity triggers #####
        trg_tauTau_list_validity_fullOR = [pass_ditau_validity,pass_singleTau_validity,pass_MET_validity]
        trg_tauTau_list_validity_diTau_singleTau = [pass_ditau_validity,pass_singleTau_validity]
        trg_tauTau_list_validity_diTau_MET = [pass_ditau_validity,pass_MET_validity]
        trg_tauTau_list_validity_diTau = [pass_ditau_validity] # ratio denumerator

        ##### application triggers #####
        trg_tauTau_list_application_fullOR = [pass_ditau_application,pass_singleTau_application,pass_MET_application]
        trg_tauTau_list_application_diTau_singleTau = [pass_ditau_application,pass_singleTau_application]
        trg_tauTau_list_application_diTau_MET = [pass_ditau_application,pass_MET_application]
        trg_tauTau_list_application_diTau = [pass_ditau_application] # ratio denumerator

        ##### preparing dict for validity region plot #####
        channel=f'tautau_{args.deepTauWP}'

        AddEfficiencyToDict(dataframes_channel[channel], trg_tauTau_list_validity_diTau, 'tauTau', nInitial_tauTau,'eff_validity_diTau', eff_tautau_validity)
        tautau_linestyles_validity.append('solid')
        tautau_labels_validity.append('diTau')
        tautau_colors_validity.append("gold")

        AddEfficiencyToDict(dataframes_channel[channel], trg_tauTau_list_validity_diTau_singleTau, 'tauTau', nInitial_tauTau,'eff_validity_diTauSingleTau', eff_tautau_validity)
        tautau_linestyles_validity.append('solid')
        tautau_labels_validity.append("diTau||singleTau")
        tautau_colors_validity.append("red")

        AddEfficiencyToDict(dataframes_channel[channel], trg_tauTau_list_validity_diTau_MET, 'tauTau', nInitial_tauTau,'eff_validity_diTauMET', eff_tautau_validity)
        tautau_linestyles_validity.append('solid')
        tautau_labels_validity.append("diTau||MET")
        tautau_colors_validity.append("cyan")

        AddEfficiencyToDict(dataframes_channel[channel], trg_tauTau_list_validity_fullOR, 'tauTau', nInitial_tauTau,'eff_validity_fullOR', eff_tautau_validity)
        tautau_linestyles_validity.append('solid')
        tautau_labels_validity.append("all")
        tautau_colors_validity.append("blue")

        ##### preparing dict for application region plot #####
        AddEfficiencyToDict(dataframes_channel[channel], trg_tauTau_list_application_diTau, 'tauTau', nInitial_tauTau,'eff_application_diTau', eff_tautau_application)
        tautau_labels_application.append('diTau')
        tautau_colors_application.append("gold")
        tautau_linestyles_application.append('solid')

        AddEfficiencyToDict(dataframes_channel[channel], trg_tauTau_list_application_diTau_singleTau, 'tauTau', nInitial_tauTau,'eff_application_diTauSingleTau', eff_tautau_application)
        tautau_labels_application.append("diTau||singleTau")
        tautau_colors_application.append("red")
        tautau_linestyles_application.append('solid')

        AddEfficiencyToDict(dataframes_channel[channel], trg_tauTau_list_application_diTau_MET, 'tauTau', nInitial_tauTau,'eff_application_diTauMET', eff_tautau_application)
        tautau_labels_application.append("diTau||MET")
        tautau_colors_application.append("cyan")
        tautau_linestyles_application.append('solid')

        AddEfficiencyToDict(dataframes_channel[channel], trg_tauTau_list_application_fullOR, 'tauTau', nInitial_tauTau,'eff_application_fullOR', eff_tautau_application)
        tautau_labels_application.append("all")
        tautau_colors_application.append("blue")
        tautau_linestyles_application.append('solid')

        AddEfficiencyToDict(dataframes_channel[channel], trg_tauTau_list_validity_fullOR, 'tauTau', nInitial_tauTau,'eff_validity_fullOR', eff_tautau_application)
        tautau_labels_application.append("fullOR")
        tautau_colors_application.append("dodgerblue")
        tautau_linestyles_application.append('dashed')

        ##### preparing dict for application region plot bigTau = False VS bigTau = True #####

        #### bigTau = False ####
        AddEfficiencyToDict(dataframes_channel[channel], trg_tauTau_list_application_diTau, 'tauTau', nInitial_tauTau,'eff_application_diTau', eff_tautau_application_bigTau)
        tautau_labels_application_bigTau.append('diTau')
        tautau_colors_application_bigTau.append("gold")
        tautau_linestyles_application_bigTau.append('solid')

        AddEfficiencyToDict(dataframes_channel[channel], trg_tauTau_list_application_diTau_singleTau, 'tauTau', nInitial_tauTau,'eff_application_diTauSingleTau', eff_tautau_application_bigTau)
        tautau_labels_application_bigTau.append("diTau||singleTau")
        tautau_colors_application_bigTau.append("red")
        tautau_linestyles_application_bigTau.append('solid')

        AddEfficiencyToDict(dataframes_channel[channel], trg_tauTau_list_application_diTau_MET, 'tauTau', nInitial_tauTau,'eff_application_diTauMET', eff_tautau_application_bigTau)
        tautau_labels_application_bigTau.append("diTau||MET")
        tautau_colors_application_bigTau.append("cyan")
        tautau_linestyles_application_bigTau.append('solid')

        AddEfficiencyToDict(dataframes_channel[channel], trg_tauTau_list_application_fullOR, 'tauTau', nInitial_tauTau,'eff_application_fullOR', eff_tautau_application_bigTau)
        tautau_labels_application_bigTau.append("all")
        tautau_colors_application_bigTau.append("blue")
        tautau_linestyles_application_bigTau.append('solid')

        #### bigTau = True ####
        AddEfficiencyToDict(dataframes_channel_bigTau[channel], trg_tauTau_list_application_diTau, 'tauTau', nInitial_tauTau,'eff_application_diTau_bigTau', eff_tautau_application_bigTau)
        tautau_labels_application_bigTau.append('diTau')
        tautau_colors_application_bigTau.append("olivedrab")
        tautau_linestyles_application_bigTau.append('solid')

        AddEfficiencyToDict(dataframes_channel_bigTau[channel], trg_tauTau_list_application_diTau_singleTau, 'tauTau', nInitial_tauTau,'eff_application_diTauSingleTau_bigTau', eff_tautau_application_bigTau)
        tautau_labels_application_bigTau.append("diTau||singleTau")
        tautau_colors_application_bigTau.append("maroon")
        tautau_linestyles_application_bigTau.append('solid')

        AddEfficiencyToDict(dataframes_channel_bigTau[channel], trg_tauTau_list_application_diTau_MET, 'tauTau', nInitial_tauTau,'eff_application_diTauMET_bigTau', eff_tautau_application_bigTau)
        tautau_labels_application_bigTau.append("diTau||MET")
        tautau_colors_application_bigTau.append("paleturquoise")
        tautau_linestyles_application_bigTau.append('solid')

        AddEfficiencyToDict(dataframes_channel_bigTau[channel], trg_tauTau_list_application_fullOR, 'tauTau', nInitial_tauTau,'eff_application_fullOR_bigTau', eff_tautau_application_bigTau)
        tautau_labels_application_bigTau.append("all")
        tautau_colors_application_bigTau.append("midnightblue")
        tautau_linestyles_application_bigTau.append('solid')

        AddEfficiencyToDict(dataframes_channel[channel], trg_tauTau_list_validity_fullOR, 'tauTau', nInitial_tauTau,'eff_validity_fullOR', eff_tautau_application_bigTau)
        tautau_labels_application_bigTau.append("fullOR")
        tautau_colors_application_bigTau.append("dodgerblue")
        tautau_linestyles_application_bigTau.append('dashed')

        ############# bigTau only #############

        AddEfficiencyToDict(dataframes_channel_bigTau[channel], trg_tauTau_list_application_diTau, 'tauTau', nInitial_tauTau,'eff_application_diTau_bigTau', eff_tautau_application_bigTauOnly)
        tautau_labels_application_bigTauOnly.append('diTau')
        tautau_colors_application_bigTauOnly.append("olivedrab")
        tautau_linestyles_application_bigTauOnly.append('solid')

        AddEfficiencyToDict(dataframes_channel_bigTau[channel], trg_tauTau_list_application_diTau_singleTau, 'tauTau', nInitial_tauTau,'eff_application_diTauSingleTau_bigTau', eff_tautau_application_bigTauOnly)
        tautau_labels_application_bigTauOnly.append("diTau||singleTau")
        tautau_colors_application_bigTauOnly.append("maroon")
        tautau_linestyles_application_bigTauOnly.append('solid')

        AddEfficiencyToDict(dataframes_channel_bigTau[channel], trg_tauTau_list_application_diTau_MET, 'tauTau', nInitial_tauTau,'eff_application_diTauMET_bigTau', eff_tautau_application_bigTauOnly)
        tautau_labels_application_bigTauOnly.append("diTau||MET")
        tautau_colors_application_bigTauOnly.append("paleturquoise")
        tautau_linestyles_application_bigTauOnly.append('solid')

        AddEfficiencyToDict(dataframes_channel_bigTau[channel], trg_tauTau_list_application_fullOR, 'tauTau', nInitial_tauTau,'eff_application_fullOR_bigTau', eff_tautau_application_bigTauOnly)
        tautau_labels_application_bigTauOnly.append("all")
        tautau_colors_application_bigTauOnly.append("midnightblue")
        tautau_linestyles_application_bigTauOnly.append('solid')

        AddEfficiencyToDict(dataframes_channel_bigTau[channel], trg_tauTau_list_validity_fullOR, 'tauTau', nInitial_tauTau,'eff_validity_fullOR', eff_tautau_application_bigTauOnly)
        tautau_labels_application_bigTauOnly.append("fullOR")
        tautau_colors_application_bigTauOnly.append("dodgerblue")
        tautau_linestyles_application_bigTauOnly.append('dashed')


    x_values = masses
    ratio_ref = eff_tautau_validity['eff_validity_diTau']

    # arguments: (eff_dict, labels, linestyles, channel, x_values, sample, deepTauWP, deepTauVersion, ratio_ref,suffix='', colors=[])

    makeplot(eff_tautau_validity, tautau_labels_validity, tautau_linestyles_validity, 'tauTau', x_values, args.sample, args.deepTauWP, args.deepTauVersion, ratio_ref,'_validity',tautau_colors_validity)

    makeplot(eff_tautau_application, tautau_labels_application, tautau_linestyles_application, 'tauTau', x_values, args.sample, args.deepTauWP, args.deepTauVersion, ratio_ref,'_application',tautau_colors_application)

    makeplot(eff_tautau_application_bigTau, tautau_labels_application_bigTau, tautau_linestyles_application_bigTau, 'tauTau', x_values, args.sample,args.deepTauWP, args.deepTauVersion, ratio_ref,'_application_bigTau',tautau_colors_application_bigTau)

    makeplot(eff_tautau_application_bigTauOnly, tautau_labels_application_bigTauOnly, tautau_linestyles_application_bigTauOnly, 'tauTau', x_values, args.sample,args.deepTauWP, args.deepTauVersion, ratio_ref,'_application_bigTauOnly',tautau_colors_application_bigTauOnly)

    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))


