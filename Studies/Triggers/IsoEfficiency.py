
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
        if numerator > denominator:
            print(f"bin = {i}")
            print(f"numerator = {numerator}")
            print(f"denominator = {denominator}")
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
    #plt.xscale('log')
    #plt.ylim(0, 1)
    #plt.ylim(0.1, 1)
    #plt.yscale('log')
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
    ROOT.gInterpreter.Declare(f'#include "include/GenLepton.h"')
    #### useful stuff ####

    inDir = "/eos/cms/store/group/phys_higgs/HLepRare/HTT_skim_v1/Run2_2018/{}/"
    outFileDir = "/afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/output/Run2_2018/{}/histograms/"
    masses = []

    #### OS_ISO requirements ####

    deepTauYear = '2017' if args.deepTauVersion=='v2p1' else '2018'
    sample_list = ggR_samples if args.sample == 'GluGluToRadion' else ggBG_samples
    x_bins = [20, 30, 40,50, 60,70, 80,90, 100,120]#, 140,160,180,200, 250, 300,400,800]
    x_bins_vec = Utilities.ListToVector(x_bins, "double")

    for sample_name in sample_list:
        mass_string = sample_name.split('-')[-1]
        if mass_string != args.mass:continue
        print(f"mass = {mass_string}")
        mass_int = int(mass_string)
        masses.append(mass_int)
        inFile = os.path.join(inDir.format(sample_name), "nanoHTT_0.root")
        if not os.path.exists(inFile) :
            print(f"{inFile} does not exist")
            continue
        print(f"inFile = {inFile}")
        if not os.path.exists(outFileDir.format(sample_name)):
            os.makedirs(outFileDir.format(sample_name), exist_ok=True)


        outFile = os.path.join(outFileDir.format(sample_name), 'efficiency_hists.root')
        output_file = ROOT.TFile(outFile, "RECREATE")

        df_initial = ROOT.RDataFrame('Events', inFile)
        #print(f"""initially = {df_initial.Count().GetValue()}""")
        #########   denumerator   ##########

        df_initial = df_initial.Define("genLeptons", """reco_tau::gen_truth::GenLepton::fromNanoAOD(GenPart_pt, GenPart_eta,
                                        GenPart_phi, GenPart_mass, GenPart_genPartIdxMother, GenPart_pdgId,
                                        GenPart_statusFlags, event)""")
        #print(f"""more than 2 genLep {df_initial.Filter("genLeptons.size()>2").Count().GetValue()}""")
        #print(f"""exactly 2 genLep {df_initial.Filter("genLeptons.size()==2").Count().GetValue()}""")

        WP_requirements_denum = f"""
            Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.VVVLoose.value} &&
            Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSmu >= {Utilities.WorkingPointsTauVSmu.Tight.value} &&
            Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSe >= {Utilities.WorkingPointsTauVSe.VVLoose.value}
            """
        df_denum = df_initial.Filter("genLeptons.size()==2")
        denum_requirements = f"Tau_pt>20 && abs(Tau_eta)<2.3 && Tau_genPartFlav == 5 && {WP_requirements_denum} && (Tau_decayMode!=5 && Tau_decayMode!=6) && abs(Tau_dz) < 0.2 "
        df_denum = df_denum.Define("denum_req",denum_requirements).Define("Tau_pt_denum","Tau_pt[denum_req]")
        df_denum = df_denum.Define("Tau_denum_idxUnordered", "CreateIndexes(Tau_pt_denum.size())")
        df_denum = df_denum.Define("Tau_denum_idxOrdered", f"ReorderObjects(Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet, Tau_denum_idxUnordered)")
        df_denum = df_denum.Define("Tau_denum_idxOrdered_Pt", f"ReorderObjects(Tau_pt, Tau_denum_idxUnordered)")

        model_denum_Taupt = ROOT.RDF.TH1DModel("Tau_pt_denum", "Tau_pt_denum", x_bins_vec.size()-1, x_bins_vec.data())
        hist_denum_Taupt = df_denum.Histo1D(model_denum_Taupt, 'Tau_pt_denum')

        df_denum = df_denum.Define("tau1_pt_denum", "Tau_denum_idxOrdered.size() > 0 ? Tau_pt_denum[Tau_denum_idxOrdered[0]]: -1 ")
        df_denum = df_denum.Define("tau2_pt_denum", "Tau_denum_idxOrdered.size() > 1 ? Tau_pt_denum[Tau_denum_idxOrdered[1]] : -1 ")

        df_denum = df_denum.Define("tau1_pt_pt_denum", "Tau_denum_idxOrdered_Pt.size() > 0 ? Tau_pt_denum[Tau_denum_idxOrdered_Pt[0]] : -1")
        df_denum = df_denum.Define("tau2_pt_pt_denum", "Tau_denum_idxOrdered_Pt.size() > 1 ? Tau_pt_denum[Tau_denum_idxOrdered_Pt[1]] : -1 ")

        #### tau1_pt
        model_denum_tau1_pt = ROOT.RDF.TH1DModel("tau1_pt_denum", "tau1_pt_denum", x_bins_vec.size()-1, x_bins_vec.data())
        hist_denum_tau1_pt = df_denum.Filter('tau1_pt_denum >= 0').Histo1D(model_denum_tau1_pt, 'tau1_pt_denum')

        model_denum_tau1_pt_pt = ROOT.RDF.TH1DModel("tau1_pt_pt_denum", "tau1_pt_pt_denum", x_bins_vec.size()-1, x_bins_vec.data())
        hist_denum_tau1_pt_pt = df_denum.Filter("tau1_pt_pt_denum >= 0").Histo1D(model_denum_tau1_pt_pt, 'tau1_pt_pt_denum')

        #### tau2_pt
        model_denum_tau2_pt = ROOT.RDF.TH1DModel("tau2_pt_denum", "tau2_pt_denum", x_bins_vec.size()-1, x_bins_vec.data())
        hist_denum_tau2_pt = df_denum.Filter("tau2_pt_denum >= 0").Histo1D(model_denum_tau2_pt, 'tau2_pt_denum')

        model_denum_tau2_pt_pt = ROOT.RDF.TH1DModel("tau2_pt_pt_denum", "tau2_pt_pt_denum", x_bins_vec.size()-1, x_bins_vec.data())
        hist_denum_tau2_pt_pt = df_denum.Filter("tau2_pt_pt_denum >= 0").Histo1D(model_denum_tau2_pt_pt, 'tau2_pt_pt_denum')


        hist_denum_Taupt.Write()
        hist_denum_tau1_pt.Write()
        hist_denum_tau2_pt.Write()
        hist_denum_tau1_pt_pt.Write()
        hist_denum_tau2_pt_pt.Write()


        #########   numerator   ##########
        WP_requirements_num = f"Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value}"
        num_requirements = f"denum_req && {WP_requirements_num} "

        df_num = df_denum.Define("num_req",num_requirements).Define("Tau_pt_num","Tau_pt[num_req]")
        df_num = df_num.Define("Tau_num_idxUnordered", "CreateIndexes(Tau_pt_num.size())")
        df_num = df_num.Define("Tau_num_idxOrdered", f"ReorderObjects(Tau_idDeepTau{deepTauYear}{args.deepTauVersion}VSjet, Tau_num_idxUnordered)")
        df_num = df_num.Define("Tau_num_idxOrdered_Pt", f"ReorderObjects(Tau_pt, Tau_num_idxUnordered)")

        model_num_Taupt = ROOT.RDF.TH1DModel("Tau_pt_num", "Tau_pt_num", x_bins_vec.size()-1, x_bins_vec.data())
        hist_num_Taupt = df_num.Histo1D(model_num_Taupt, 'Tau_pt_num')

        df_num = df_num.Define("tau1_pt_num", "Tau_num_idxOrdered.size() > 0 ? Tau_pt_num[Tau_num_idxOrdered[0]]: -1 ")
        df_num = df_num.Define("tau2_pt_num", "Tau_num_idxOrdered.size() > 1 ? Tau_pt_num[Tau_num_idxOrdered[1]] : -1 ")

        df_num = df_num.Define("tau1_pt_pt_num", "Tau_num_idxOrdered_Pt.size() > 0 ? Tau_pt_num[Tau_num_idxOrdered_Pt[0]] : -1")
        df_num = df_num.Define("tau2_pt_pt_num", "Tau_num_idxOrdered_Pt.size() > 1 ? Tau_pt_num[Tau_num_idxOrdered_Pt[1]] : -1 ")

        #### tau1_pt
        model_num_tau1_pt = ROOT.RDF.TH1DModel("tau1_pt_num", "tau1_pt_num", x_bins_vec.size()-1, x_bins_vec.data())
        hist_num_tau1_pt = df_num.Filter('tau1_pt_num >= 0').Histo1D(model_num_tau1_pt, 'tau1_pt_num')

        model_num_tau1_pt_pt = ROOT.RDF.TH1DModel("tau1_pt_pt_num", "tau1_pt_pt_num", x_bins_vec.size()-1, x_bins_vec.data())
        hist_num_tau1_pt_pt = df_num.Filter("tau1_pt_pt_num >= 0").Histo1D(model_num_tau1_pt_pt, 'tau1_pt_pt_num')

        #### tau2_pt
        model_num_tau2_pt = ROOT.RDF.TH1DModel("tau2_pt_num", "tau2_pt_num", x_bins_vec.size()-1, x_bins_vec.data())
        hist_num_tau2_pt = df_num.Filter("tau2_pt_num >= 0").Histo1D(model_num_tau2_pt, 'tau2_pt_num')

        model_num_tau2_pt_pt = ROOT.RDF.TH1DModel("tau2_pt_pt_num", "tau2_pt_pt_num", x_bins_vec.size()-1, x_bins_vec.data())
        hist_num_tau2_pt_pt = df_num.Filter("tau2_pt_pt_num >= 0").Histo1D(model_num_tau2_pt_pt, 'tau2_pt_pt_num')


        hist_num_Taupt.Write()
        hist_num_tau1_pt.Write()
        hist_num_tau2_pt.Write()
        hist_num_tau1_pt_pt.Write()
        hist_num_tau2_pt_pt.Write()

        makeEffPlot(os.path.join(outFileDir.format(sample_name), f'eff_midWP_tauPt_all.png'),hist_num_Taupt, hist_denum_Taupt,x_bins,'Tau_pt')
        print(os.path.join(outFileDir.format(sample_name), f'eff_midWP_tauPt_all.png'))
        makeEffPlot(os.path.join(outFileDir.format(sample_name), f'eff_midWP_tauPt_1.png'),hist_num_tau1_pt, hist_denum_tau1_pt,x_bins,'tau1_pt')
        print(os.path.join(outFileDir.format(sample_name), f'eff_midWP_tauPt_1.png'))
        makeEffPlot(os.path.join(outFileDir.format(sample_name), f'eff_midWP_tauPt_2.png'),hist_num_tau2_pt, hist_denum_tau2_pt,x_bins,'tau2_pt')
        print(os.path.join(outFileDir.format(sample_name), f'eff_midWP_tauPt_2.png'))

        makeEffPlot(os.path.join(outFileDir.format(sample_name), f'eff_midWP_tauPt_1_ptOrdered.png'),hist_num_tau1_pt_pt, hist_denum_tau1_pt_pt,x_bins,'tau1_pt')
        print(os.path.join(outFileDir.format(sample_name), f'eff_midWP_tauPt_1_ptOrdered.png'))
        makeEffPlot(os.path.join(outFileDir.format(sample_name), f'eff_midWP_tauPt_2_ptOrdered.png'),hist_num_tau2_pt_pt, hist_denum_tau2_pt_pt,x_bins,'tau2_pt')
        print(os.path.join(outFileDir.format(sample_name), f'eff_midWP_tauPt_2_ptOrdered.png'))

    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))


