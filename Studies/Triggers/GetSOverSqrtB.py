
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
from Studies.Triggers.Trig_utilities import *

ROOT.gInterpreter.Declare("""
    std::unique_ptr<TF1> sigmoid_function;

    void LoadSigmoidFunction(const char* fileName, const char* functionName) {
        TFile file(fileName, "READ");
        sigmoid_function.reset((TF1*)file.Get(functionName)->Clone());
    }

    float getMETsf( const float metnomu_pt)
    {
        if(metnomu_pt>180 && metnomu_pt<300){
            if (sigmoid_function){
                return sigmoid_function->Eval(metnomu_pt);
            }
            else {
                std::cerr << "Sigmoid function is not loaded." << std::endl;
                return 0.0;
            }
        }
    return 1.f;
    }
    """)
def getSumWeights(df, trg_reg_list,weight_expression):
    filter_expr = f' ('
    filter_expr+= ' || '.join(trg for trg in trg_reg_list)
    filter_expr+= ')'
    sum_channel = df.Define("weight_for_sum",weight_expression).Filter(filter_expr).Sum("weight_for_sum").GetValue()
    sum_error = math.sqrt(df.Define("weight_for_sum_error",f'{weight_expression}*{weight_expression}').Filter(filter_expr).Sum("weight_for_sum_error").GetValue())
    return sum_channel,sum_error

def getInFiles(inDir):
    infiles= []
    for rootfile in os.listdir(inDir):
        if rootfile.endswith(".root"):
            infiles.append(os.path.join(inDir, rootfile))
    return infiles

if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--sample', required=False, type=str, default='GluGluToBulkGraviton')
    parser.add_argument('--deepTauVersion', required=False, type=str, default='v2p1')
    parser.add_argument('--wantBigTau', required=False, type=bool, default=False)
    parser.add_argument('--deepTauWP', required=False, type=str, default='medium')


    args = parser.parse_args()

    startTime = time.time()
    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    ROOT.gInterpreter.Declare(f'#include "include/KinFitNamespace.h"')
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    ROOT.gROOT.ProcessLine('#include "include/AnalysisTools.h"')
    header_path_RootExt = os.path.join(os.environ['ANALYSIS_PATH'], "PlotKit/include/RootExt.h")
    ROOT.gInterpreter.Declare(f'#include "{header_path_RootExt}"')

    #### useful stuff ####

    inDir = "/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v2_deepTau2p1_HTT/"

    MET_SF_fileName = os.path.join("/afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/Studies/Triggers/files", "eff_Data_Mu_MC_DY_TT_WJets_mumu_metnomu_et_TRG_METNoMu120_CUTS_mhtnomu_et_L_100_default_mutau.root")
    #MET_SF_file = ROOT.TFile.Open(MET_SF_fileName, "READ")
    ROOT.LoadSigmoidFunction(MET_SF_fileName, "SigmoidFuncSF")


    #### OS_ISO requirements ####

    deepTauYear = '2017' if args.deepTauVersion=='v2p1' else '2018'

    os_iso_filtering = {
        'VLoose': f"""(tau1_charge * tau2_charge < 0) && tau2_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.VLoose.value}""",
        'Loose': f"""(tau1_charge * tau2_charge < 0) && tau2_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Loose.value}""",
        'Medium':f"""(tau1_charge * tau2_charge < 0) && tau2_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value}"""
        }
    #### begin of loop over bckg samples ####
    bckg_samples = ['TTToHadronic','TTTo2L2Nu']
    #bckg_samples = ['TTToSemiLeptonic','TTToHadronic','TTTo2L2Nu']
    signal_samples = ggR_samples if args.sample == 'GluGluToRadion' else ggBG_samples

    b_values = {}
    s_values = {}

    #### begin of loop over signal samples ####

    for sample_name in signal_samples + bckg_samples :
        print(sample_name)
        all_inFiles = getInFiles(os.path.join(inDir,f"{sample_name}"))
        #print(all_inFiles)
        df_initial = ROOT.RDataFrame('Events', Utilities.ListToVector(all_inFiles))
        dfWrapped_central  = DataFrameBuilder(df_initial, args.deepTauVersion)
        PrepareDfWrapped(dfWrapped_central)
        df_tauTau = dfWrapped_central.df.Filter('tauTau')
        if sample_name in signal_samples:
            df_tauTau = df_tauTau.Filter("tau1_gen_kind==5 && tau2_gen_kind==5")
        else:
            df_tauTau = df_tauTau.Filter("tau1_gen_kind!=5 || tau2_gen_kind!=5")
        total_weight_string_denum = 'weight_total*weight_TauID_Central*weight_tau1_EleidSF_Central*weight_tau1_MuidSF_Central*weight_tau2_EleidSF_Central*weight_tau2_MuidSF_Central*weight_L1PreFiring_Central*weight_L1PreFiring_ECAL_Central*weight_L1PreFiring_Muon_Central'
        nInitial_tauTau = df_tauTau.Define('total_weight_string_denum', total_weight_string_denum).Sum('total_weight_string_denum').GetValue()
        nInitial_tauTau = df_tauTau.Count().GetValue()


        df_tautau_vloose = df_tauTau.Filter(os_iso_filtering['VLoose'])
        df_tautau_loose = df_tauTau.Filter(os_iso_filtering['Loose'])
        df_tautau_medium = df_tauTau.Filter(os_iso_filtering['Medium'])
        dataframes_channel = {
            'tautau_medium_medium': df_tautau_medium,
            'tautau_medium_loose': df_tautau_loose,
            'tautau_medium_vloose': df_tautau_vloose,
            'tautau_tight_medium': df_tautau_medium.Filter(f"tau1_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Tight.value}"),
            'tautau_tight_loose': df_tautau_loose.Filter(f"tau1_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Tight.value}"),
            'tautau_tight_vloose': df_tautau_vloose.Filter(f"tau1_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Tight.value}"),
            'tautau_vtight_medium': df_tautau_medium.Filter(f"tau1_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.VTight.value}"),
            'tautau_vtight_loose': df_tautau_loose.Filter(f"tau1_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.VTight.value}"),
            'tautau_vtight_vloose': df_tautau_vloose.Filter(f"tau1_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.VTight.value}"),

        }
        ##### application triggers #####

        pass_MET_application = "(HLT_MET && MET_region && !(singleTau_region) && ! (other_trg_region))"
        pass_singleTau_application = "(HLT_singleTau && singleTau_region && !(MET_region) && ! (other_trg_region))"
        pass_other_trg_application= "({} && other_trg_region  && !(MET_region) && ! (singleTau_region) )  "
        pass_ditau_application = pass_other_trg_application.format("HLT_ditau")
        trg_tauTau_list_application_fullOR = [pass_ditau_application,pass_singleTau_application,pass_MET_application]
        trg_tauTau_list_application_ditau_singleTau = [pass_ditau_application,pass_singleTau_application]
        trg_tauTau_list_application_ditau_MET = [pass_ditau_application,pass_MET_application]
        trg_tauTau_list_application_ditau = [pass_ditau_application] # ratio denumerator

        for channel in dataframes_channel.keys():
            regions_expr= get_regions_dict('tauTau', [190,190], 180, False)
            for reg_key,reg_exp in regions_expr.items():
                dataframes_channel[channel] = dataframes_channel[channel].Define(reg_key, reg_exp)
            dataframes_channel[channel] = dataframes_channel[channel].Define(f"weight_ditau_application",f" {pass_ditau_application}?  weight_tau1_TrgSF_ditau_Central*weight_tau2_TrgSF_ditau_Central : 1.f ")
            dataframes_channel[channel] = dataframes_channel[channel].Define(f"weight_singleTau_application",f"""if({pass_singleTau_application}){{
                if (tau1_pt > 190 && abs(tau1_eta) < 2.1 && tau2_pt > 190 && abs(tau2_eta) < 2.1 ) return tau1_pt>tau2_pt ? weight_tau1_TrgSF_singleTau_Central : weight_tau2_TrgSF_singleTau_Central;
                if (tau1_pt > 190 && abs(tau1_eta) < 2.1) return weight_tau1_TrgSF_singleTau_Central;
                if (tau2_pt > 190 && abs(tau2_eta) < 2.1) return weight_tau2_TrgSF_singleTau_Central;
                throw std::runtime_error("problem with region"); }}
                return 1.f;""")
            dataframes_channel[channel] = dataframes_channel[channel].Define("weight_MET_application", f"{pass_MET_application}? getMETsf(metnomu_pt) : 1.f")

            sum_weights, sum_weights_error = getSumWeights(dataframes_channel[channel], trg_tauTau_list_application_fullOR,f'{total_weight_string_denum}*weight_ditau_application*weight_singleTau_application*weight_MET_application')

            if channel not in b_values.keys():
                b_values[channel] = {}
                b_values[channel]["all"] = 0
            if sample_name in bckg_samples and

            if channel not in s_values.keys():
                s_values[channel] = {}
                s_values[channel]["all"] = 0
            if sample_name in signal_samples and sample_name not in s_values[channel].keys():
                s_values[channel][sample_name] = {}

            if sample_name in bckg_samples:
                if sample_name not in b_values[channel].keys():
                    b_values[channel][sample_name] = {}
                b_values[channel][sample_name]['sum'] = sum_weights
                b_values[channel][sample_name]['error'] = sum_weights_error
                b_values[channel]['all'] += sum_weights
            else:
                if sample_name not in s_values[channel].keys():
                    s_values[channel][sample_name] = {}
                s_values[channel][sample_name]['sum'] = sum_weights
                s_values[channel][sample_name]['error'] = sum_weights_error
                b_values[channel]['all'] += sum_weights

        print(f"b_values = {b_values}")
        print(f"s_values={s_values}")


    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))


