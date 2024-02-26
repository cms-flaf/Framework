
import copy
import datetime
import os
import sys
import ROOT
import shutil
import zlib
import time
import matplotlib.pyplot as plt
from statsmodels.stats.proportion import proportion_confint

if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])


#ROOT.EnableImplicitMT(1)
ROOT.EnableThreadSafety()

import Common.Utilities as Utilities
from Analysis.HistHelper import *
from Analysis.hh_bbtautau import *

ggR_samples = [
               "GluGluToRadionToHHTo2B2Tau_M-250", "GluGluToRadionToHHTo2B2Tau_M-260", "GluGluToRadionToHHTo2B2Tau_M-270", "GluGluToRadionToHHTo2B2Tau_M-280", "GluGluToRadionToHHTo2B2Tau_M-300", "GluGluToRadionToHHTo2B2Tau_M-320", "GluGluToRadionToHHTo2B2Tau_M-350", "GluGluToRadionToHHTo2B2Tau_M-450", "GluGluToRadionToHHTo2B2Tau_M-500", "GluGluToRadionToHHTo2B2Tau_M-550", "GluGluToRadionToHHTo2B2Tau_M-600", "GluGluToRadionToHHTo2B2Tau_M-650", "GluGluToRadionToHHTo2B2Tau_M-700", "GluGluToRadionToHHTo2B2Tau_M-750", "GluGluToRadionToHHTo2B2Tau_M-800", "GluGluToRadionToHHTo2B2Tau_M-850", "GluGluToRadionToHHTo2B2Tau_M-900", "GluGluToRadionToHHTo2B2Tau_M-1000", "GluGluToRadionToHHTo2B2Tau_M-1250", "GluGluToRadionToHHTo2B2Tau_M-1500", "GluGluToRadionToHHTo2B2Tau_M-1750", "GluGluToRadionToHHTo2B2Tau_M-2000", "GluGluToRadionToHHTo2B2Tau_M-2500", "GluGluToRadionToHHTo2B2Tau_M-3000"]

ggBG_samples = [
    'GluGluToBulkGravitonToHHTo2B2Tau_M-1000', 'GluGluToBulkGravitonToHHTo2B2Tau_M-1250', 'GluGluToBulkGravitonToHHTo2B2Tau_M-1500', 'GluGluToBulkGravitonToHHTo2B2Tau_M-1750', 'GluGluToBulkGravitonToHHTo2B2Tau_M-2000', 'GluGluToBulkGravitonToHHTo2B2Tau_M-250', 'GluGluToBulkGravitonToHHTo2B2Tau_M-2500', 'GluGluToBulkGravitonToHHTo2B2Tau_M-260', 'GluGluToBulkGravitonToHHTo2B2Tau_M-270', 'GluGluToBulkGravitonToHHTo2B2Tau_M-280', 'GluGluToBulkGravitonToHHTo2B2Tau_M-300', 'GluGluToBulkGravitonToHHTo2B2Tau_M-3000', 'GluGluToBulkGravitonToHHTo2B2Tau_M-320', 'GluGluToBulkGravitonToHHTo2B2Tau_M-350', 'GluGluToBulkGravitonToHHTo2B2Tau_M-400', 'GluGluToBulkGravitonToHHTo2B2Tau_M-450', 'GluGluToBulkGravitonToHHTo2B2Tau_M-500', 'GluGluToBulkGravitonToHHTo2B2Tau_M-550', 'GluGluToBulkGravitonToHHTo2B2Tau_M-600', 'GluGluToBulkGravitonToHHTo2B2Tau_M-650', 'GluGluToBulkGravitonToHHTo2B2Tau_M-700', 'GluGluToBulkGravitonToHHTo2B2Tau_M-750', 'GluGluToBulkGravitonToHHTo2B2Tau_M-800', 'GluGluToBulkGravitonToHHTo2B2Tau_M-850', 'GluGluToBulkGravitonToHHTo2B2Tau_M-900' ]


def makeEffPlot(fileName,hist_num, hist_denum,x_bins,xlabel):
    efficiencies = []
    x_points = []
    errors_x = []
    errors_y = []
    for i in range(1, len(x_bins)):
        numerator = hist_num.GetBinContent(i)
        denominator = hist_denum.GetBinContent(i)
        efficiency = numerator / denominator if denominator != 0 else 0
        conf_interval = proportion_confint(numerator, denominator, alpha=0.05, method='beta')
        error_low = efficiency - conf_interval[0]
        error_up = conf_interval[1] - efficiency
        efficiencies.append(efficiency)
        errors_x.append((x_bins[i] - x_bins[i-1]) / 2)
        errors_y.append((error_low + error_up) / 2)
        x_points.append((x_bins[i-1] + x_bins[i]) / 2)

    plt.errorbar(x_points, efficiencies, xerr=errors_x, yerr=errors_y, marker='o', linestyle='None')
    plt.xlabel('tau1_pt')
    #plt.xlabel('tau2_pt')
    plt.ylabel('eff')
    plt.title('MediumWPEff')
    plt.xscale('log')
    #plt.ylim(0, 1)
    plt.ylim(0.1, 1)
    plt.yscale('log')
    plt.savefig(fileName)
    #plt.savefig(os.path.join(outFileDir.format(sample_name), 'eff_midWP_tau2.png'))
    #plt.show()
    plt.close()

if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--sample', required=False, type=str, default='GluGluToBulkGraviton')
    parser.add_argument('--deepTauVersion', required=False, type=str, default='v2p1')
    parser.add_argument('--mass', required=False, type=str, default='750')
    args = parser.parse_args()

    startTime = time.time()
    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    ROOT.gInterpreter.Declare(f'#include "include/AnalysisTools.h"')

    #### useful stuff ####

    inDir = "/eos/cms/store/group/phys_higgs/HLepRare/HTT_skim_v1/Run2_2018/{}/"
    outFileDir = "/afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/output/Run2_2018/{}/histograms/"
    masses = []

    #### OS_ISO requirements ####

    deepTauYear = '2017' if args.deepTauVersion=='v2p1' else '2018'
    sample_list = ggR_samples if args.sample == 'GluGluToRadion' else ggBG_samples
    x_bins = [20, 30, 40,50, 60,70, 80,90, 100,120, 140,160,180,200, 250, 300,400,800]
    x_bins_vec = Utilities.ListToVector(x_bins, "double")

    for sample_name in sample_list:
        mass_string = sample_name.split('-')[-1]
        if mass_string != args.mass:continue
        print(mass_string)
        mass_int = int(mass_string)
        masses.append(mass_int)
        inFile = os.path.join(inDir.format(sample_name), "nanoHTT_0.root")
        if not os.path.exists(inFile) :
            print(f"{inFile} does not exist")
            continue

        if not os.path.exists(outFileDir.format(sample_name)):
            os.makedirs(outFileDir.format(sample_name), exist_ok=True)


        outFile = os.path.join(outFileDir.format(sample_name), 'efficiency_hists.root')
        output_file = ROOT.TFile(outFile, "RECREATE")

        df_initial = ROOT.RDataFrame('Events', inFile)

        #########   denumerator   ##########

        WP_requirements_denum = f"""
            Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.VVVLoose.value} &&
            Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSmu >= {Utilities.WorkingPointsTauVSmu.Tight.value} &&
            Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSe >= {Utilities.WorkingPointsTauVSe.VVLoose.value}
            """
        df_denum = df_initial.Define(f"Tau_idx", f"CreateIndexes(Tau_pt.size())").Define("denum_req",f"Tau_pt>20 && abs(Tau_eta)<2.3 && Tau_genPartFlav == 5 && {WP_requirements_denum}").Filter("Tau_idx[denum_req].size()>0")

        model_denum_Taupt_greaterZero = ROOT.RDF.TH1DModel("Tau_pt_denum", "Tau_pt_denum", x_bins_vec.size()-1, x_bins_vec.data())
        hist_denum_Taupt_greaterZero = df_denum.Histo1D(model_denum_Taupt_greaterZero, 'Tau_pt')

        df_denum = df_denum.Filter("Tau_idx[denum_req].size()==2")
        model_denum_Taupt_exactlyTwo = ROOT.RDF.TH1DModel("Tau_pt_denum", "Tau_pt_denum", x_bins_vec.size()-1, x_bins_vec.data())
        hist_denum_Taupt_exactlyTwo = df_denum.Histo1D(model_denum_Taupt_exactlyTwo, 'Tau_pt')

        df_denum = df_denum.Filter("Tau_idx[denum_req].size()==2").Define("tau1_idx_denum",f"""Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet[Tau_idx[1]] > Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet[Tau_idx[0]] ? Tau_idx[1] : Tau_idx[0]""").Define("tau2_idx_denum","""tau1_idx_denum == Tau_idx[0] ? Tau_idx[1] : Tau_idx[0]""").Define("tau1_pt_denum", "Tau_pt[tau1_idx_denum]").Define("tau2_pt_denum", "Tau_pt[tau2_idx_denum]")

        #### tau1_pt
        model_denum_tau1_pt = ROOT.RDF.TH1DModel("tau1_pt_denum", "tau1_pt_denum", x_bins_vec.size()-1, x_bins_vec.data())
        hist_denum_tau1_pt = df_denum.Histo1D(model_denum_tau1_pt, 'tau1_pt_denum')

        #### tau2_pt
        model_denum_tau2_pt = ROOT.RDF.TH1DModel("tau2_pt_denum", "tau2_pt_denum", x_bins_vec.size()-1, x_bins_vec.data())
        hist_denum_tau2_pt = df_denum.Histo1D(model_denum_tau2_pt, 'tau2_pt_denum')


        hist_denum_Taupt_greaterZero.Write()
        hist_denum_Taupt_exactlyTwo.Write()
        hist_denum_tau1_pt.Write()
        hist_denum_tau2_pt.Write()


        #########   numerator   ##########
        WP_requirements_num = f"Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value}"
        df_num = df_denum.Define("num_req",WP_requirements_num).Filter("Tau_idx[num_req].size()>0")

        model_num_Taupt_greaterZero = ROOT.RDF.TH1DModel("Tau_pt_num", "Tau_pt_num", x_bins_vec.size()-1, x_bins_vec.data())
        hist_num_Taupt_greaterZero = df_num.Histo1D(model_num_Taupt_greaterZero, 'Tau_pt')

        df_num = df_num.Filter("Tau_idx[num_req].size()==2")
        model_num_Taupt_exactlyTwo = ROOT.RDF.TH1DModel("Tau_pt_num", "Tau_pt_num", x_bins_vec.size()-1, x_bins_vec.data())
        hist_num_Taupt_exactlyTwo = df_num.Histo1D(model_num_Taupt_exactlyTwo, 'Tau_pt')

        df_num = df_num.Filter("Tau_idx[num_req].size()==2").Define("tau1_idx_num",f"""Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet[Tau_idx[1]] > Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet[Tau_idx[0]] ? Tau_idx[1] : Tau_idx[0]""").Define("tau2_idx_num","""tau1_idx_num == Tau_idx[0] ? Tau_idx[1] : Tau_idx[0]""").Define("tau1_pt_num", "Tau_pt[tau1_idx_num]").Define("tau2_pt_num", "Tau_pt[tau2_idx_num]")

        #### tau1_pt
        model_num_tau1_pt = ROOT.RDF.TH1DModel("tau1_pt_num", "tau1_pt_num", x_bins_vec.size()-1, x_bins_vec.data())
        hist_num_tau1_pt = df_num.Histo1D(model_num_tau1_pt, 'tau1_pt_num')

        #### tau2_pt
        model_num_tau2_pt = ROOT.RDF.TH1DModel("tau2_pt_num", "tau2_pt_num", x_bins_vec.size()-1, x_bins_vec.data())
        hist_num_tau2_pt = df_num.Histo1D(model_num_tau2_pt, 'tau2_pt_num')

        hist_num_Taupt_greaterZero.Write()
        hist_num_Taupt_exactlyTwo.Write()
        hist_num_tau1_pt.Write()
        hist_num_tau2_pt.Write()
        output_file.Close()
        makeEffPlot(os.path.join(outFileDir.format(sample_name), f'eff_midWP_greaterZeroTaus.png'),hist_num_Taupt_greaterZero, hist_denum_Taupt_greaterZero,x_bins,'Tau_pt')
        makeEffPlot(os.path.join(outFileDir.format(sample_name), f'eff_midWP_exactlyTwoTaus.png'),hist_num_Taupt_exactlyTwo, hist_denum_Taupt_exactlyTwo,x_bins,'Tau_pt')
        makeEffPlot(os.path.join(outFileDir.format(sample_name), f'eff_midWP_tau1_pt.png'),hist_num_tau1_pt, hist_denum_tau1_pt,x_bins,'tau1_pt')
        makeEffPlot(os.path.join(outFileDir.format(sample_name), f'eff_midWP_tau2_pt.png'),hist_num_tau2_pt, hist_denum_tau2_pt,x_bins,'tau2_pt')


    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))


