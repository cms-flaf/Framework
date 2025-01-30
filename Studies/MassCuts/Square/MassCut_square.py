import ROOT
import os
import sys
import math
import yaml
import numpy as np

import argparse


if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *
from Analysis.hh_bbtautau import *
from Studies.MassCuts.Square.GetSquareIntervals import *
from Studies.MassCuts.Square.SquarePlot import *


def FilterForbJets(cat,dfWrapper_s,dfWrapper_b):
    if cat == 'boosted':
        dfWrapper_s.df = dfWrapper_s.df.Define("FatJet_atLeast1BHadron",
        "SelectedFatJet_nBHadrons>0").Filter("SelectedFatJet_p4[FatJet_atLeast1BHadron].size()>0")
        dfWrapper_b.df = dfWrapper_b.df.Define("FatJet_atLeast1BHadron",
        "SelectedFatJet_nBHadrons>0").Filter("SelectedFatJet_p4[FatJet_atLeast1BHadron].size()>0")
    else:
        dfWrapper_s.df = dfWrapper_s.df.Filter("b1_hadronFlavour==5 && b2_hadronFlavour==5 ")
        dfWrapper_b.df = dfWrapper_b.df.Filter("b1_hadronFlavour==5 && b2_hadronFlavour==5 ")
    return dfWrapper_s,dfWrapper_b

def GetModel2D(x_bins, y_bins):#hist_cfg, var1, var2):
    #x_bins = hist_cfg[var1]['x_bins']
    #y_bins = hist_cfg[var2]['x_bins']
    if type(x_bins)==list:
        x_bins_vec = Utilities.ListToVector(x_bins, "double")
        if type(y_bins)==list:
            y_bins_vec = Utilities.ListToVector(y_bins, "double")
            model = ROOT.RDF.TH2DModel("", "", x_bins_vec.size()-1, x_bins_vec.data(), y_bins_vec.size()-1, y_bins_vec.data())
        else:
            n_y_bins, y_bin_range = y_bins.split('|')
            y_start,y_stop = y_bin_range.split(':')
            model = ROOT.RDF.TH2DModel("", "", x_bins_vec.size()-1, x_bins_vec.data(), int(n_y_bins), float(y_start), float(y_stop))
    else:
        n_x_bins, x_bin_range = x_bins.split('|')
        x_start,x_stop = x_bin_range.split(':')
        if type(y_bins)==list:
            y_bins_vec = Utilities.ListToVector(y_bins, "double")
            model = ROOT.RDF.TH2DModel("", "",int(n_x_bins), float(x_start), float(x_stop), y_bins_vec.size()-1, y_bins_vec.data())
        else:
            n_y_bins, y_bin_range = y_bins.split('|')
            y_start,y_stop = y_bin_range.split(':')
            model = ROOT.RDF.TH2DModel("", "",int(n_x_bins), float(x_start), float(x_stop), int(n_y_bins), float(y_start), float(y_stop))
    return model

def getDataFramesFromFile(infile):
    my_file = open(infile, "r")
    data = my_file.read()
    data_into_list = data.split("\n")
    inFiles = Utilities.ListToVector(data_into_list)
    df_initial = ROOT.RDataFrame("Events", inFiles)
    # print(df_initial.Count().GetValue())
    return df_initial

def buildDfWrapped(df_initial,global_cfg_dict,year):
    dfWrapped = PrepareDfForHistograms(DataFrameBuilderForHistograms(df_initial,global_cfg_dict, f"Run2_{year}"))
    particleNet_mass = 'particleNet_mass' if 'SelectedFatJet_particleNet_mass_boosted' in dfWrapped.df.GetColumnNames() else 'particleNetLegacy_mass'
    return dfWrapped

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', required=False, type=str, default='2018')
    parser.add_argument('--cat', required=False, type=str, default='res2b_cat3')
    parser.add_argument('--channels', required=False, type=str, default='tauTau')
    parser.add_argument('--wantPlots', required=False, type=bool, default=False)
    args = parser.parse_args()
    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    ROOT.gInterpreter.Declare(f'#include "include/KinFitNamespace.h"')
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    ROOT.gROOT.ProcessLine('#include "include/AnalysisTools.h"')
    ROOT.gInterpreter.Declare(f'#include "include/pnetSF.h"')

    signalFiles = f"/afs/cern.ch/work/v/vdamante/FLAF/Studies/MassCuts/Inputs/SignalSamples_{args.year}.txt"

    TTFiles = f"/afs/cern.ch/work/v/vdamante/FLAF/Studies/MassCuts/Inputs/TTSamples_{args.year}.txt"
    df_sig = getDataFramesFromFile(signalFiles)
    df_bckg = getDataFramesFromFile(TTFiles)

    global_cfg_file = '/afs/cern.ch/work/v/vdamante/FLAF/config/HH_bbtautau/global.yaml'
    global_cfg_dict = {}
    with open(global_cfg_file, 'r') as f:
        global_cfg_dict = yaml.safe_load(f)
    global_cfg_dict['channels_to_consider']=args.channels.split(',')

    hist_cfg_file = '/afs/cern.ch/work/v/vdamante/FLAF/config/plot/histograms.yaml'
    hist_cfg_dict = {}
    with open(hist_cfg_file, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)

    dfWrapped_sig = buildDfWrapped(df_sig,global_cfg_dict,args.year)
    dfWrapped_bckg =  buildDfWrapped(df_bckg,global_cfg_dict,args.year)
    quantile_sig = 0.68
    quantile_bckg = 0.9
    wantSequential=False
    tt_mass = "tautau_m_vis"
    for cat in  args.cat.split(','):
        bb_mass = "bb_m_vis" if cat != 'boosted_cat3' else "bb_m_vis_softdrop"
        y_bins = hist_cfg_dict[bb_mass]['x_rebin']['other']
        x_bins = hist_cfg_dict['tautau_m_vis']['x_rebin']['other']
        for channel in global_cfg_dict['channels_to_consider']:
            print(channel,cat)

            dfWrapped_sig.df = dfWrapped_sig.df.Filter(f"OS_Iso && {channel} && {cat}")
            dfWrapped_bckg.df = dfWrapped_bckg.df.Filter(f"OS_Iso && {channel} && {cat}")
            dfWrapped_sig,dfWrapped_bckg = FilterForbJets(cat,dfWrapped_sig,dfWrapped_bckg)

            df_sig_old = dfWrapped_sig.df
            df_sig_new = dfWrapped_sig.df
            df_sig_new = df_sig_new.Range(1000)
            df_bckg_old = dfWrapped_bckg.df
            df_bckg_new = dfWrapped_bckg.df
            df_bckg_new = df_bckg_new.Range(1000)
            print()
            n_in_sig = df_sig_old.Count().GetValue()
            n_in_bckg = df_bckg_old.Count().GetValue()
            print(f"initially n sig = {n_in_sig} and nbckg = {n_in_bckg}")

            masses_sig = GetMassesQuantilesJoint(df_sig_new, tt_mass, bb_mass, quantile_sig)
            # masses_bckg = GetMassesQuantilesJoint(df_bckg_new, tt_mass, bb_mass, 1-quantile_bckg)
            # masses_sig, masses_bckg = GetSquareSignalAndBckgMassCut(dfWrapped_sig,dfWrapped_bckg,global_cfg_dict,channel, cat,bb_mass, tt_mass,quantile_sig,quantile_bckg,wantSequential)
            min_bb_sig, max_bb_sig, min_tt_sig, max_tt_sig = masses_sig
            # min_bb_bckg, max_bb_bckg, min_tt_bckg, max_tt_bckg = masses_bckg
            mbb_max = max_bb_sig #min(max_bb_sig, max_bb_bckg)
            mtt_max = max_tt_sig #min(max_tt_sig, max_tt_bckg)
            mbb_min = min_bb_sig #max(min_bb_sig, min_bb_bckg)
            mtt_min = min_tt_sig #max(min_tt_sig, min_tt_bckg)
            print()
            print(f"min_bb_sig, max_bb_sig, min_tt_sig, max_tt_sig = {min_bb_sig, max_bb_sig, min_tt_sig, max_tt_sig }")
            # print(f"min_bb_bckg, max_bb_bckg, min_tt_bckg, max_tt_bckg = {min_bb_bckg, max_bb_bckg, min_tt_bckg, max_tt_bckg }")


            print()
            n_after_sig = df_sig_old.Filter(f"{tt_mass}< {mtt_max} && {tt_mass} > {mtt_min}").Filter(f"{bb_mass}< {mbb_max} && {bb_mass} > {mbb_min}").Count().GetValue()
            print(f"percentage sig = {n_after_sig}/{n_in_sig} = {n_after_sig/n_in_sig} ")
            print()
            n_after_bckg = df_bckg_old.Filter(f"{tt_mass}< {mtt_max} && {tt_mass} > {mtt_min}").Filter(f"{bb_mass}< {mbb_max} && {bb_mass} > {mbb_min}").Count().GetValue()
            print(f"percentage bckg  = {n_after_bckg}/{n_in_bckg} = {n_after_bckg/n_in_bckg} ")
            print()
            ssqrtb_in = n_in_sig/math.sqrt(n_in_bckg)
            print(f"s/sqrt(b) = {n_in_sig}/{math.sqrt(n_in_bckg)} = {ssqrtb_in}")

            # ssqrtb_after = n_after_sig/math.sqrt(n_after_bckg)
            # print(f"s/sqrt(b) = {n_after_sig}/{math.sqrt(n_after_bckg)} = {ssqrtb_after}")
            # mbb_max = min(max_bb_mass_sig, max_bb_mass_bckg)
            # mtt_max = min(max_tt_mass_sig, max_tt_mass_bckg)
            # mbb_min = max(min_bb_mass_sig, min_bb_mass_bckg)
            # mtt_min = max(min_tt_mass_sig, min_tt_mass_bckg)


            # mbb_max = max_bb_mass_bckg
            # mtt_max = max_tt_mass_bckg
            # mbb_min = min_bb_mass_bckg
            # mtt_min = min_tt_mass_bckg
            # mbb_max = 180
            # mbb_min = 50
            # mtt_min = 30
            # mtt_max = 130
            # mbb_max = max_bb_mass_sig
            # mtt_max = max_tt_mass_sig
            # mbb_min = min_bb_mass_sig
            # mtt_min = min_tt_mass_sig
            # print(f"mbb_max = {mbb_max}")
            # print(f"mbb_min = {mbb_min}")
            # print(f"mtt_max = {mtt_max}")
            # print(f"mtt_min = {mtt_min}")



            if args.wantPlots:
                hist_sig=dfWrapped_sig.df.Histo2D(GetModel2D(x_bins, y_bins),bb_mass, tt_mass).GetValue()
                hist_bckg=dfWrapped_bckg.df.Histo2D(GetModel2D(x_bins, y_bins),bb_mass, tt_mass).GetValue()
                outFile_prefix = "/afs/cern.ch/work/v/vdamante/FLAF/Studies/MassCuts/Square/plots/"
                rectangle_coordinates = mbb_max, mbb_min, mtt_max, mtt_min
                plot_2D_histogram(hist_sig, f"signal hist {cat} {channel}","m_{bb}", "m_{#tau#tau}", None, f"{outFile_prefix}{cat}_{channel}_sig", f"Run2_{args.year}", rectangle_coordinates)
                plot_2D_histogram(hist_bckg, f"signal hist {cat} {channel}","m_{bb}", "m_{#tau#tau}", None, f"{outFile_prefix}{cat}_{channel}_bckg", f"Run2_{args.year}", rectangle_coordinates)


