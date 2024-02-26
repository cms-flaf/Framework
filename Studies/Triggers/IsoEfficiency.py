
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

    #### useful stuff ####

    inDir = "/eos/cms/store/group/phys_higgs/HLepRare/HTT_skim_v1/Run2_2018/{}/"
    outFileDir = "/afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/output/Run2_2018/{}/histograms/"
    masses = []

    #### OS_ISO requirements ####

    deepTauYear = '2017' if args.deepTauVersion=='v2p1' else '2018'
    sample_list = ggR_samples if args.sample == 'GluGluToRadion' else ggBG_samples
    x_bins = [20, 30, 40,50, 60,70, 80,90, 100,120, 140,160,180,200, 250, 300,400,500,1000]
    x_bins_vec = Utilities.ListToVector(x_bins, "double")

    for sample_name in sample_list:
        mass_string = sample_name.split('-')[-1]
        if mass_string != args.mass:continue
        mass_int = int(mass_string)
        masses.append(mass_int)
        inFile = os.path.join(inDir.format(sample_name), "nanoHTT_0.root")
        if not os.path.exists(inFile) :
            print(f"{inFile} does not exist")
            continue

        if not os.path.exists(outFileDir.format(sample_name)):
            os.mkdir(outFileDir.format(sample_name))

        outFile = os.path.join(outFileDir.format(sample_name), 'efficiency_hists.root')
        output_file = ROOT.TFile(outFile, "RECREATE")

        df_initial = ROOT.RDataFrame('Events', inFile)

        WP_requirements_denum = f"Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.VVVLoose.value} && Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSmu >= {Utilities.WorkingPointsTauVSmu.VLoose.value} && Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSe >= {Utilities.WorkingPointsTauVSe.VVVLoose.value} "
        df_denum = df_initial.Define("denum_req",f"Tau_pt>20 && abs(Tau_eta)<2.3 && Tau_genPartFlav == 5 && {WP_requirements_denum}").Filter("Tau_pt[denum_req].size()>0")
        model_denum = ROOT.RDF.TH1DModel("Tau_pt_denum", "Tau_pt_denum", x_bins_vec.size()-1, x_bins_vec.data())
        hist_denum = df_denum.Histo1D(model_denum, 'Tau_pt')
        hist_denum.Write()

        WP_requirements_num = f"Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value}  "
        df_num = df_denum.Define("num_req",WP_requirements_num).Filter("Tau_pt[num_req].size()>0")
        model_num = ROOT.RDF.TH1DModel("Tau_pt_num", "Tau_pt_num", x_bins_vec.size()-1, x_bins_vec.data())
        hist_num = df_num.Histo1D(model_num, 'Tau_pt')
        hist_num.Write()
        output_file.Close()

        efficiencies = []
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

        plt.errorbar(x_bins[:-1], efficiencies, xerr=errors_x, yerr=errors_y, marker='o', linestyle='None')
        plt.xlabel('tau_pt')
        plt.ylabel('eff')
        plt.title('MediumWPEff')
        plt.xscale('log')
        plt.ylim(0.1, 1)
        plt.yscale('log')
        plt.savefig(os.path.join(outFileDir.format(sample_name), 'eff_midWP.png'))
        plt.show()

    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))


