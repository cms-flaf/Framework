import ROOT
import os
import sys
import math
import yaml
import numpy as np

import argparse
import csv

def write_stuff_on_csv(csv_filename,csv_header,row_to_write):
    # csv_filename = "output.csv"

    # Intestazione del CSV
    # csv_header = ["mass", "cat", "ssqrt(b) in", "mtt", "mbb", "%s (lin)", "%b (lin)", "ssqrt(b) lin",
                # "A", "B", "C", "D", "%s (ell)", "%b (ell)", "ssqrt(b) ell"]

    # Apri il file CSV in modalità scrittura
    with open(csv_filename, mode="a", newline="") as file:
        writer = csv.writer(file)
        write_header = not os.path.exists(csv_filename) or os.stat(csv_filename).st_size == 0
        if write_header:
            writer.writerow(csv_header)  # Scrive l'intestazione
        # Scrivi i risultati nel file CSV
        writer.writerow(row_to_write)
        # writer.writerow([args.mass, cat, ssqrtb_in, f"{mtt_min}, {mtt_max}", f"{mbb_min}, {mbb_max}",
        #                 percentage_sig_lin, percentage_bckg_lin, ssqrtb_lin,
        #                 new_par_A, new_par_B, new_par_C, new_par_D,
        #                 percentage_sig_ell, percentage_bckg_ell, ssqrtb_ell])

    print(f"Dati salvati in {csv_filename}")


if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *
from Analysis.hh_bbtautau import *
from Studies.MassCuts.Square.GetSquareIntervals import *
from Studies.MassCuts.Square.SquarePlot import *

import Studies.MassCuts.Ellypse.plotWithEllypse as Ellypse


def createCacheQuantities(dfWrapped_cache, cache_map_name):
    df_cache = dfWrapped_cache.df
    map_creator_cache = ROOT.analysis.CacheCreator(*dfWrapped_cache.colTypes)()
    df_cache = map_creator_cache.processCache(ROOT.RDF.AsRNode(df_cache), Utilities.ListToVector(dfWrapped_cache.colNames), cache_map_name)
    return df_cache


def AddCacheColumnsInDf(dfWrapped_central, dfWrapped_cache,cache_map_name='cache_map_placeholder'):
    col_names_cache =  dfWrapped_cache.colNames
    col_types_cache =  dfWrapped_cache.colTypes
    dfWrapped_cache.df = createCacheQuantities(dfWrapped_cache, cache_map_name)
    if dfWrapped_cache.df.Filter(f"{cache_map_name} > 0").Count().GetValue() <= 0 : raise RuntimeError("no events passed map placeolder")
    dfWrapped_central.AddCacheColumns(col_names_cache,col_types_cache)


def FilterForbJets(cat,dfWrapper_s):
    if cat == 'boosted':
        dfWrapper_s.df = dfWrapper_s.df.Define("FatJet_atLeast1BHadron",
        "SelectedFatJet_nBHadrons>0").Filter("SelectedFatJet_p4[FatJet_atLeast1BHadron].size()>0")
    else:
        dfWrapper_s.df = dfWrapper_s.df.Filter("b1_hadronFlavour==5 && b2_hadronFlavour==5 ")
    return dfWrapper_s

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

def getDataFramesFromFile(infile, res=None, mass=None,printout=False):
    my_file = open(infile, "r")
    data = my_file.read()
    data_into_list = data.split("\n")
    new_data = []
    if printout:
        print(data_into_list)
    if res and mass:
        for line in data_into_list:
            if res in line and mass in line:
                new_data.append(line)
                if printout:print(line)
    else: new_data = data_into_list
    # if res and mass: print(new_data)
    inFiles = Utilities.ListToVector(new_data)
    df_initial = ROOT.RDataFrame("Events", inFiles)
    # print(df_initial.Count().GetValue())
    return df_initial

def buildDfWrapped(df_initial,global_cfg_dict,year,df_initial_cache=None):
    dfBuilder_init = DataFrameBuilderForHistograms(df_initial,global_cfg_dict, f"Run2_{year}",deepTauVersion="v2p5")
    dfBuilder_cache_init = DataFrameBuilderForHistograms(df_initial_cache,global_cfg_dict, f"Run2_{year}",deepTauVersion="v2p5")
    # print(dfBuilder_init.df.Count().GetValue())
    # print(dfBuilder_cache_init.df.Count().GetValue())
    if df_initial_cache:
        AddCacheColumnsInDf(dfBuilder_init, dfBuilder_cache_init, "cache_map")
    dfWrapped = PrepareDfForHistograms(dfBuilder_init)
    particleNet_mass = 'particleNet_mass' if 'SelectedFatJet_particleNet_mass_boosted' in dfWrapped.df.GetColumnNames() else 'particleNetLegacy_mass'
    return dfWrapped

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', required=False, type=str, default='2018')
    parser.add_argument('--cat', required=False, type=str, default='res2b_cat3')
    parser.add_argument('--channels', required=False, type=str, default='tauTau')
    parser.add_argument('--res', required=False, type=str, default=None)
    parser.add_argument('--mass', required=False, type=str, default=None)
    parser.add_argument('--wantPlots', required=False, type=bool, default=False)
    parser.add_argument('--wantPrint', required=False, type=bool, default=False)
    parser.add_argument('--checkBckg', required=False, type=bool, default=False)
    parser.add_argument('--outFileCsv', required=False, type=str, default="output.csv")

    args = parser.parse_args()
    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    ROOT.gInterpreter.Declare(f'#include "include/KinFitNamespace.h"')
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    ROOT.gROOT.ProcessLine('#include "include/AnalysisTools.h"')
    ROOT.gInterpreter.Declare(f'#include "include/pnetSF.h"')

    signalFiles = f"/afs/cern.ch/work/v/vdamante/FLAF/Studies/MassCuts/Inputs/SignalSamples_{args.year}.txt"
    signalCaches = f"/afs/cern.ch/work/v/vdamante/FLAF/Studies/MassCuts/Inputs/SignalCaches_{args.year}.txt"

    TTFiles = f"/afs/cern.ch/work/v/vdamante/FLAF/Studies/MassCuts/Inputs/TTSamples_{args.year}.txt"
    TTCaches = f"/afs/cern.ch/work/v/vdamante/FLAF/Studies/MassCuts/Inputs/TTCaches_{args.year}.txt"


    global_cfg_file = '/afs/cern.ch/work/v/vdamante/FLAF/config/HH_bbtautau/global.yaml'
    global_cfg_dict = {}
    with open(global_cfg_file, 'r') as f:
        global_cfg_dict = yaml.safe_load(f)
    global_cfg_dict['channels_to_consider']=args.channels.split(',')

    hist_cfg_file = '/afs/cern.ch/work/v/vdamante/FLAF/config/plot/histograms.yaml'
    hist_cfg_dict = {}
    with open(hist_cfg_file, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)

    df_sig = getDataFramesFromFile(signalFiles,args.res, args.mass)
    df_sig_cache = getDataFramesFromFile(signalCaches,args.res, args.mass)
    dfWrapped_sig = buildDfWrapped(df_sig,global_cfg_dict,args.year,df_sig_cache)
    quantile_sig = 0.68
    if args.checkBckg:
        df_bckg = getDataFramesFromFile(TTFiles)
        df_bckg_cache = getDataFramesFromFile(TTCaches)
        dfWrapped_bckg = buildDfWrapped(df_bckg,global_cfg_dict,args.year,df_bckg_cache)
        # quantile_bckg = 0.9
    wantSequential=False
    tt_mass = "SVfit_m"
    # tt_mass = "tautau_m_vis"
    # print(dfWrapped_sig.df.Filter("SVfit_valid>=0").Count().GetValue())
    # dfWrapped_sig.df.Filter("SVfit_valid>=0").Display({"SVfit_m","SVfit_valid","SVfit_pt"}).Print()
    for cat in  args.cat.split(','):
        print(cat)
        bb_mass = "bb_m_vis" if cat != 'boosted_cat3' else "bb_m_vis_softdrop"
        print(bb_mass)
        y_bins = hist_cfg_dict[bb_mass]['x_rebin']['other']
        x_bins = hist_cfg_dict[tt_mass]['x_rebin']['other']
        # print(x_bins)
        for channel in global_cfg_dict['channels_to_consider']:
            # print(channel,cat)
            dfWrapped_sig.df = dfWrapped_sig.df.Filter(f"SVfit_valid >0 && OS_Iso && {channel} && {cat}")
            dfWrapped_sig = FilterForbJets(cat,dfWrapped_sig)

            df_sig_old = dfWrapped_sig.df
            df_sig_new = dfWrapped_sig.df
            # df_sig_new = df_sig_new.Range(100000)
            if args.checkBckg:
                dfWrapped_bckg.df = dfWrapped_bckg.df.Filter(f"SVfit_valid>0 && OS_Iso && {channel} && {cat}")
                dfWrapped_bckg = FilterForbJets(cat,dfWrapped_bckg)
                df_bckg_old = dfWrapped_bckg.df
                df_bckg_new = dfWrapped_bckg.df
            # df_bckg_new = df_bckg_new.Range(100000)
            if args.wantPrint:
                # INITIALLY
                n_in_sig = df_sig_old.Count().GetValue()
                n_in_bckg = 0
                ssqrtb_in = 0
                if args.checkBckg:
                    n_in_bckg = df_bckg_old.Count().GetValue()
                    print(f"inizialmente {n_in_sig} eventi di segnale e {n_in_bckg} eventi di fondo")
                    ssqrtb_in =0 if n_in_bckg == 0 else  n_in_sig/math.sqrt(n_in_bckg)

                # SQUARE CUT
                min_tt_sig, max_tt_sig, min_bb_sig, max_bb_sig = GetMassesQuantilesJoint(df_sig_new, tt_mass, bb_mass, quantile_sig)
                mbb_max = max_bb_sig #min(max_bb_sig, max_bb_bckg)
                mtt_max = max_tt_sig #min(max_tt_sig, max_tt_bckg)
                mbb_min = min_bb_sig #max(min_bb_sig, min_bb_bckg)
                mtt_min = min_tt_sig #max(min_tt_sig, min_tt_bckg)
                n_after_sig_lin = df_sig_old.Filter(f"{tt_mass}< {mtt_max} && {tt_mass} > {mtt_min}").Filter(f"{bb_mass}< {mbb_max} && {bb_mass} > {mbb_min}").Count().GetValue()
                percentage_sig_lin =  0 if n_in_sig==0 else n_after_sig_lin/n_in_sig
                n_after_bckg_lin = 0
                percentage_bckg_lin = 0
                ssqrtb_lin = 0
                if args.checkBckg:
                    n_after_bckg_lin = df_bckg_old.Filter(f"{tt_mass}< {mtt_max} && {tt_mass} > {mtt_min}").Filter(f"{bb_mass}< {mbb_max} && {bb_mass} > {mbb_min}").Count().GetValue()
                    print(f"dopo taglio lineare {n_after_sig_lin} eventi di segnale e {n_after_bckg_lin} eventi di fondo")
                    percentage_bckg_lin =  n_after_bckg_lin/n_in_bckg if n_in_bckg!=0 else 0
                    ssqrtb_lin = 0 if n_after_bckg_lin ==0 else n_after_sig_lin/math.sqrt(n_after_bckg_lin)

                # ELLIPTIC CUT
                A, B_final, C, D_final = GetEllipticCut(df_sig_new, tt_mass, bb_mass, quantile_sig)
                new_par_A = (math.ceil(A*100)/100)  # -> 2.36
                new_par_B = (math.ceil(B_final*100)/100)  # -> 2.36
                new_par_C = (math.ceil(C*100)/100)  # -> 2.36
                new_par_D = (math.ceil(D_final*100)/100)  # -> 2.36
                # new_pars = (math.floor(v*100)/100)  # -> 2.35
                # print(f"(({tt_mass} - {new_par_A})*({tt_mass} - {new_par_A}) / ({new_par_B}*{new_par_B}) + ({bb_mass} - {new_par_C})*({bb_mass} - {new_par_C}) / ({new_par_D}*{new_par_D})) < 1")

                n_after_sig_ell =  df_sig_old.Filter(f"(({tt_mass} - {new_par_A})*({tt_mass} - {new_par_A}) / ({new_par_B}*{new_par_B}) + ({bb_mass} - {new_par_C})*({bb_mass} - {new_par_C}) / ({new_par_D}*{new_par_D})) < 1").Count().GetValue()
                percentage_sig_ell =  n_after_sig_ell/n_in_sig if n_in_bckg!=0 else 0
                n_after_bckg_ell = 0
                percentage_bckg_ell = 0
                ssqrtb_ell = 0
                if args.checkBckg:
                    n_after_bckg_ell = df_bckg_old.Filter(f"(({tt_mass} - {new_par_A})*({tt_mass} - {new_par_A}) / ({new_par_B}*{new_par_B}) + ({bb_mass} - {new_par_C})*({bb_mass} - {new_par_C}) / ({new_par_D}*{new_par_D})) < 1").Count().GetValue()
                    print(f"dopo taglio ellittico {n_after_sig_ell} eventi di segnale e {n_after_bckg_ell} eventi di fondo")
                    percentage_bckg_ell =  n_after_bckg_ell/n_in_bckg if n_in_bckg!=0 else 0
                    ssqrtb_ell = n_after_sig_ell/math.sqrt(n_after_bckg_ell) if n_after_bckg_ell!=0 else 0

                print(f"| {args.mass} | {cat} | {ssqrtb_in} | {mtt_min}, {mtt_max} | {mbb_min}, {mbb_max} | {percentage_sig_lin} | {percentage_bckg_lin} |  {ssqrtb_lin}| {new_par_A} | {new_par_B} | {new_par_C} | {new_par_D} | {percentage_sig_ell} | {percentage_bckg_ell} |  {ssqrtb_ell} |" )
                csv_header = ["mass","cat","ssqrt(b) in","mtt min ", "mtt max", "mbb min","mbb max","%s (lin)","%b (lin)"," ssqrt(b) lin","A","B","C","D","%s (ell)","%b (ell)"," ssqrt(b) ell"]
                row_to_write = [args.mass, cat, ssqrtb_in, mtt_min, mtt_max, mbb_min, mbb_max, percentage_sig_lin, percentage_bckg_lin,  ssqrtb_lin, new_par_A, new_par_B, new_par_C, new_par_D, percentage_sig_ell, percentage_bckg_ell,  ssqrtb_ell]
                write_stuff_on_csv(args.outFileCsv,csv_header,row_to_write)
                # csv_header = ["mass", "cat", "ssqrt(b) in", "mtt", "mbb", "%s (lin)", "%b (lin)", "ssqrt(b) lin",
                # "A", "B", "C", "D", "%s (ell)", "%b (ell)", "ssqrt(b) ell"]

        # writer.writerow([args.mass, cat, ssqrtb_in, f"{mtt_min}, {mtt_max}", f"{mbb_min}, {mbb_max}",
        #                 percentage_sig_lin, percentage_bckg_lin, ssqrtb_lin,
        #                 new_par_A, new_par_B, new_par_C, new_par_D,
        #                 percentage_sig_ell, percentage_bckg_ell, ssqrtb_ell])

            # mbb_min = 30
            # mbb_max = 140
            # mtt_min = 55
            # mtt_max = 150

            if args.wantPlots:
                mbb_min = 50
                mbb_max = 150
                mtt_min = 50
                mtt_max = 160
                new_par_A = 121
                new_par_B = 26
                new_par_C = 115
                new_par_D = 36

                if args.res and args.mass:
                    hist_sig=df_sig_old.Histo2D(GetModel2D(x_bins, y_bins),bb_mass, tt_mass).GetValue()
                hist_bckg=df_bckg_old.Histo2D(GetModel2D(x_bins, y_bins),bb_mass, tt_mass).GetValue()
                outFile_prefix = f"/afs/cern.ch/work/v/vdamante/FLAF/Studies/MassCuts/Square/MassCut2DPlots/Run2_{args.year}/{cat}/"
                if args.res and args.mass:
                    outFile_prefix+=f"{args.res}/{args.mass}/"
                else:
                    outFile_prefix+=f"bckg/"
                os.makedirs(outFile_prefix,exist_ok=True)
                rectangle_coordinates = mbb_max, mbb_min, mtt_max, mtt_min
                ellypse_par=  new_par_C, new_par_D,new_par_A, new_par_B
                if args.res and args.mass:
                    finalFileName = f"{outFile_prefix}{channel}_{args.res}_M-{args.mass}"
                    plot_2D_histogram(hist_sig, f"signal hist {cat} {channel}","$m_{bb}$", "$m_{\\tau\\tau}$", None, finalFileName, f"Run2_{args.year}", rectangle_coordinates)
                    Ellypse.plot_2D_histogram(hist_sig, f"signal hist {cat} {channel}","$m_{bb}$", "$m_{\\tau\\tau}$", None, f"{finalFileName}_ellypse_Square", f"{args.year}", ellypse_par,rectangle_coordinates)
                    print(f"{finalFileName}.png")
                    print(f"{finalFileName}_ellypse_Square.png")
                finalFileName = f"{outFile_prefix}{channel}_background"
                plot_2D_histogram(hist_bckg, f" {cat} {channel}","$m_{bb}$", "$m_{\\tau\\tau}$", None, finalFileName, f"Run2_{args.year}", rectangle_coordinates)
                Ellypse.plot_2D_histogram(hist_bckg, f"signal hist {cat} {channel}","$m_{bb}$", "$m_{\\tau\\tau}$", None, f"{finalFileName}_ellypse_Square", f"{args.year}", ellypse_par,rectangle_coordinates)
                print(f"{finalFileName}.png")
                print(f"{finalFileName}_ellypse_Square.png")