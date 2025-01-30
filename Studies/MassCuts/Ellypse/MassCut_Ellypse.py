import ROOT
import os
import sys
import yaml
import numpy as np

import argparse


if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *
from Analysis.hh_bbtautau import *
from Studies.MassCuts.Ellypse.GetEllypticalIntervals import *
from Studies.MassCuts.Ellypse.plotWithEllypse import *

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
    df_TT = getDataFramesFromFile(TTFiles)

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
    dfWrapped_TT =  buildDfWrapped(df_TT,global_cfg_dict,args.year)
    y_bins = hist_cfg_dict['bb_m_vis']['x_rebin']['other']
    x_bins = hist_cfg_dict['tautau_m_vis']['x_rebin']['other']

    for cat in  args.cat.split(','):
        for channel in global_cfg_dict['channels_to_consider']:
            print(channel,cat)
            # M_c1, M_c2, a, b = GetEllypticalMassCut(dfWrapped_sig,dfWrapped_TT,global_cfg_dict,channel,cat,quantile_sig=1,quantile_ttbar=0.99)
            Mc_bb, Mc_tt, a_bb, a_tt = GetEllypticalMassCut(dfWrapped_sig,dfWrapped_TT,global_cfg_dict,channel,cat,quantile_sig=0.68,quantile_ttbar=0.99)

            # ellypse_expr = f"(tautau_m_vis - {M_c1})^{2}/({a}^2) + (bb_m_vis - {M_c2})^{2}/({b}^2) =< 1"
            # dfWrapped_sig = dfWrapped_sig.Filter(ellypse_expr).Hist2D()
            # dfWrapped_TT = dfWrapped_TT.Filter(ellypse_expr)
            dfWrapped_sig.df = dfWrapped_sig.df.Filter(f"OS_Iso && {channel} && {cat}")
            dfWrapped_TT.df = dfWrapped_TT.df.Filter(f"OS_Iso && {channel} && {cat}")
            dfWrapped_sig,dfWrapped_TT = FilterForbJets(cat,dfWrapped_sig,dfWrapped_TT)
            hist_sig=dfWrapped_sig.df.Histo2D(GetModel2D(x_bins, y_bins),"tautau_m_vis", "bb_m_vis").GetValue()
            hist_TT=dfWrapped_TT.df.Histo2D(GetModel2D(x_bins, y_bins),"tautau_m_vis", "bb_m_vis").GetValue()
            # bins,labels,cat_name,channelname,title,spin,outFileName = getLabels(cat,year,channel,mass,resonance)
            outFile_prefix = "/afs/cern.ch/work/v/vdamante/FLAF/Studies/MassCuts/Ellypse/plots/"
            # ellypse_par = M_c1, M_c2, a, b
            ellypse_par = Mc_bb, Mc_tt, a_bb, a_tt

            print(ellypse_par)
            plot_2D_histogram(hist_sig, f"signal hist {cat} {channel}", xlabel="m_{bb}", ylabel="m_{#tau#tau}", bin_labels=None, filename=f"{outFile_prefix}{cat}_{channel}_sig", year=args.year, ellipse_parameters=ellypse_par)
            plot_2D_histogram(hist_TT, f"TT hist {cat} {channel}", xlabel="m_{bb}", ylabel="m_{#tau#tau}", bin_labels=None, filename=f"{outFile_prefix}{cat}_{channel}_TT", year=args.year, ellipse_parameters=ellypse_par)


