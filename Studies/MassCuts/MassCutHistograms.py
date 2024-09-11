import ROOT
import os
import sys
import yaml
import numpy as np

ROOT.gStyle.SetOptStat(0)

ROOT.gStyle.SetPalette(109)
#ROOT.TColor.InvertPalette()

# kCubehelix=58, # 5/10 ma inverted è poco colorblind proven
# kCMYK=73, # 6/10 ma inverted è o cess
# kWaterMelon=108, # 3/10 ma inverted è meglio
# kCividis=113, # 7/10 # invertito è 2/10
# kTemperatureMap=104, # 6.5 /10 # invertito è 2/10
# kColorPrintableOnGrey=62, # 5 /10
# kDeepSea=51, # 8 /10 (inverted è top)
# kBlueYellow= 54, # 7 /10
# kCool=109, # 8 /10 (inverted anche è top)
# kBlueGreenYellow=71, # 6/10
# kBird=57, # 8/10
# kRainBow=55, # /10
# kThermometer=105, # 6 /10
# kViridis=112, # /10

if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

from Studies.MassCuts.DrawPlots import create_2D_histogram,plot_2D_histogram

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




def Plot2DMassRes1b(df, hist_cfg_dict, global_cfg_dict,channel='tauTau', year='2018'):
    x_bins = hist_cfg_dict['bb_m_vis']['x_rebin']['other']
    y_bins = hist_cfg_dict['tautau_m_vis']['x_rebin']['other']
    hist_denum = df.Filter(f"OS_Iso && {channel} && !(res2b_cat3) && !(boosted_baseline_cat3) && nSelBtag == 1").Histo2D(GetModel2D(x_bins, y_bins),"bb_m_vis","tautau_m_vis").GetValue()
    hist_num = df.Filter(f"OS_Iso && {channel} && res1b_cat3").Histo2D(GetModel2D(x_bins, y_bins),"bb_m_vis","tautau_m_vis").GetValue()
    #hist_num.Divide(hist_denum)
    for xbin in range(1,hist_denum.GetNbinsX()+1):
        if int(hist_denum.GetXaxis().GetBinCenter(xbin))== global_cfg_dict['mass_cut_limits']['bb_m_vis']['other'][0]:
            x1 = hist_denum.GetXaxis().GetBinLowEdge(xbin)
        if int(hist_denum.GetXaxis().GetBinLowEdge(xbin))== global_cfg_dict['mass_cut_limits']['bb_m_vis']['other'][0]:
            x1 = hist_denum.GetXaxis().GetBinLowEdge(xbin)
        if int(hist_denum.GetXaxis().GetBinCenter(xbin))==global_cfg_dict['mass_cut_limits']['bb_m_vis']['other'][1]:
            x2 = hist_denum.GetXaxis().GetBinLowEdge(xbin)
        if int(hist_denum.GetXaxis().GetBinUpEdge(xbin))==global_cfg_dict['mass_cut_limits']['bb_m_vis']['other'][1]:
            x2 = hist_denum.GetXaxis().GetBinUpEdge(xbin)
            #print(hist_denum.GetXaxis().GetBinCenter(xbin))
            #print( global_cfg_dict['mass_cut_limits']['bb_m_vis']['other'][0])
            #print(hist_denum.GetXaxis().GetBinLowEdge(xbin))
            #print( global_cfg_dict['mass_cut_limits']['bb_m_vis']['other'][1])

    for ybin in range(0,hist_denum.GetNbinsY()+1):
        if hist_denum.GetYaxis().GetBinCenter(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][0]:
            y1 = hist_denum.GetYaxis().GetBinLowEdge(ybin)
        if hist_denum.GetYaxis().GetBinLowEdge(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][0]:
            y1 = hist_denum.GetYaxis().GetBinLowEdge(ybin)
        if hist_denum.GetYaxis().GetBinCenter(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][1]:
            y2 = hist_denum.GetYaxis().GetBinLowEdge(ybin)
        if hist_denum.GetYaxis().GetBinUpEdge(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][1]:
            y2 = hist_denum.GetYaxis().GetBinUpEdge(ybin)

    plot_2D_histogram(hist_denum, f"{channel} Res1b", "$m_{bb}$", "$m_{\\tau\\tau}$", [], f"Studies/MassCuts/output/den_2D_res1b_{channel}_{year}.png", f"Run2_{year}", (x1, y1, x2-x1, y2-y1))
    #create_2D_hist_denum(hist_denum, c1, "prova.png", global_cfg_dict, f"Run2_{year}")
    #c1.SaveAs(f"Studies/MassCuts/output/den_2D_res2b_{channel}.png")


def Plot2DMassRes2b(df, hist_cfg_dict, global_cfg_dict,channel='tauTau', year='2018'):
    x_bins = hist_cfg_dict['bb_m_vis']['x_rebin']['other']
    y_bins = hist_cfg_dict['tautau_m_vis']['x_rebin']['other']
    hist_denum = df.Filter(f"OS_Iso && {channel} && res2b_inclusive").Histo2D(GetModel2D(x_bins, y_bins),"bb_m_vis","tautau_m_vis").GetValue()
    hist_num = df.Filter(f"OS_Iso && {channel} && res2b_cat3").Histo2D(GetModel2D(x_bins, y_bins),"bb_m_vis","tautau_m_vis").GetValue()
    #hist_num.Divide(hist_denum)
    for xbin in range(1,hist_denum.GetNbinsX()+1):
        if int(hist_denum.GetXaxis().GetBinCenter(xbin))== global_cfg_dict['mass_cut_limits']['bb_m_vis']['other'][0]:
            x1 = hist_denum.GetXaxis().GetBinLowEdge(xbin)
        if int(hist_denum.GetXaxis().GetBinLowEdge(xbin))== global_cfg_dict['mass_cut_limits']['bb_m_vis']['other'][0]:
            x1 = hist_denum.GetXaxis().GetBinLowEdge(xbin)
        if int(hist_denum.GetXaxis().GetBinCenter(xbin))==global_cfg_dict['mass_cut_limits']['bb_m_vis']['other'][1]:
            x2 = hist_denum.GetXaxis().GetBinLowEdge(xbin)
        if int(hist_denum.GetXaxis().GetBinUpEdge(xbin))==global_cfg_dict['mass_cut_limits']['bb_m_vis']['other'][1]:
            x2 = hist_denum.GetXaxis().GetBinUpEdge(xbin)
            #print(hist_denum.GetXaxis().GetBinCenter(xbin))
            #print( global_cfg_dict['mass_cut_limits']['bb_m_vis']['other'][0])
            #print(hist_denum.GetXaxis().GetBinLowEdge(xbin))
            #print( global_cfg_dict['mass_cut_limits']['bb_m_vis']['other'][1])

    for ybin in range(0,hist_denum.GetNbinsY()+1):
        if hist_denum.GetYaxis().GetBinCenter(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][0]:
            y1 = hist_denum.GetYaxis().GetBinLowEdge(ybin)
        if hist_denum.GetYaxis().GetBinLowEdge(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][0]:
            y1 = hist_denum.GetYaxis().GetBinLowEdge(ybin)
        if hist_denum.GetYaxis().GetBinCenter(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][1]:
            y2 = hist_denum.GetYaxis().GetBinLowEdge(ybin)
        if hist_denum.GetYaxis().GetBinUpEdge(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][1]:
            y2 = hist_denum.GetYaxis().GetBinUpEdge(ybin)

    plot_2D_histogram(hist_denum, f"{channel} Res2b", "$m_{bb}$", "$m_{\\tau\\tau}$", [], f"Studies/MassCuts/output/den_2D_res2b_{channel}_{year}.png", f"Run2_{year}", (x1, y1, x2-x1, y2-y1))
    #create_2D_hist_denum(hist_denum, c1, "prova.png", global_cfg_dict, f"Run2_{year}")
    #c1.SaveAs(f"Studies/MassCuts/output/den_2D_res2b_{channel}.png")

def Plot2DMassboosted(df, hist_cfg_dict, global_cfg_dict,channel='tauTau', year='2018', pNetWP=0.):
    x_bins = hist_cfg_dict['bb_m_vis']['x_rebin']['boosted_cat3']
    y_bins = hist_cfg_dict['tautau_m_vis']['x_rebin']['boosted_cat3']
    hist_denum = df.Filter(f"OS_Iso && {channel} && !(res2b_cat3) && SelectedFatJet_p4[fatJet_sel && SelectedFatJet_particleNet_MD_JetTagger>={pNetWP}].size()>0").Histo2D(GetModel2D(x_bins, y_bins),"bb_m_vis_softdrop","tautau_m_vis").GetValue()
    hist_num = df.Filter(f"OS_Iso && {channel} && boosted_cat3").Histo2D(GetModel2D(x_bins, y_bins),"bb_m_vis","tautau_m_vis").GetValue()
    #hist_num.Divide(hist_denum)
    #hist_num.Divide(hist_denum)

    # Converti i numeri dei bin in coordinate reali
    for xbin in range(0,hist_denum.GetNbinsX()+1):
        if hist_denum.GetXaxis().GetBinCenter(xbin)==global_cfg_dict['mass_cut_limits']['bb_m_vis']['boosted'][0]:
            x1 = hist_denum.GetXaxis().GetBinLowEdge(xbin)
        if hist_denum.GetXaxis().GetBinLowEdge(xbin)==global_cfg_dict['mass_cut_limits']['bb_m_vis']['boosted'][0]:
            x1 = hist_denum.GetXaxis().GetBinLowEdge(xbin)
        if hist_denum.GetXaxis().GetBinCenter(xbin)==global_cfg_dict['mass_cut_limits']['bb_m_vis']['boosted'][1]:
            x2 = hist_denum.GetXaxis().GetBinLowEdge(xbin)
        if hist_denum.GetXaxis().GetBinUpEdge(xbin)==global_cfg_dict['mass_cut_limits']['bb_m_vis']['boosted'][1]:
            x2 = hist_denum.GetXaxis().GetBinUpEdge(xbin)

    for ybin in range(0,hist_denum.GetNbinsY()+1):
        if hist_denum.GetYaxis().GetBinCenter(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][0]:
            y1 = hist_denum.GetYaxis().GetBinLowEdge(ybin)
        if hist_denum.GetYaxis().GetBinLowEdge(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][0]:
            y1 = hist_denum.GetYaxis().GetBinLowEdge(ybin)
        if hist_denum.GetYaxis().GetBinCenter(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][1]:
            y2 = hist_denum.GetYaxis().GetBinLowEdge(ybin)
        if hist_denum.GetYaxis().GetBinUpEdge(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][1]:
            y2 = hist_denum.GetYaxis().GetBinUpEdge(ybin)

    plot_2D_histogram(hist_denum, f"{channel} Boosted", "$m_{bb}$", "$m_{\\tau\\tau}$", [], f"Studies/MassCuts/output/den_2D_boosted_{channel}_{year}.png", f"Run2_{year}", (x1, y1, x2-x1, y2-y1))


if __name__ == "__main__":
    import argparse
    import yaml
    import Common.Utilities as Utilities
    from Analysis.HistHelper import *
    from Analysis.hh_bbtautau import *
    import GetIntervals
    import GetIntervalsSimultaneously
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', required=False, type=str, default='2018')
    parser.add_argument('--cat', required=False, type=str, default='res2b')
    args = parser.parse_args()

    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    ROOT.gInterpreter.Declare(f'#include "include/KinFitNamespace.h"')
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    ROOT.gROOT.ProcessLine('#include "include/AnalysisTools.h"')

    inFiles = Utilities.ListToVector([
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1000/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1250/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1500/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1750/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-2000/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-250/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-2500/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-260/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-270/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-280/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-300/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-3000/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-320/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-350/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-400/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-450/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-500/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-550/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-600/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-650/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-700/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-750/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-800/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-850/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-900/nanoHTT_0.root" , f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1000/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1250/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1500/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1750/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-2000/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-250/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-2500/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-260/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-270/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-280/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-300/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-3000/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-320/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-350/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-400/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-450/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-500/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-550/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-600/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-650/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-700/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-750/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-800/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-850/nanoHTT_0.root",
    f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaTuples/v10_deepTau2p1_HTT/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-900/nanoHTT_0.root"
    ])


    print("********************************************************************************")
    print(f"************************************* {args.year} *************************************")
    print("********************************************************************************")
    df_initial = ROOT.RDataFrame("Events", inFiles)

    global_cfg_file = '/afs/cern.ch/work/v/vdamante/FLAF/config/HH_bbtautau/global.yaml'
    global_cfg_dict = {}
    with open(global_cfg_file, 'r') as f:
        global_cfg_dict = yaml.safe_load(f)

    hist_cfg_file = '/afs/cern.ch/work/v/vdamante/FLAF/config/plot/histograms.yaml'
    hist_cfg_dict = {}
    with open(hist_cfg_file, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)

    dfWrapped = PrepareDfForHistograms(DataFrameBuilderForHistograms(df_initial,global_cfg_dict, f"Run2_{args.year}"))
    pNetWP = dfWrapped.pNetWP

    for channel in ['eTau', 'muTau','tauTau']:
        Plot2DMassRes1b(dfWrapped.df,hist_cfg_dict,global_cfg_dict, channel, args.year)
        Plot2DMassRes2b(dfWrapped.df,hist_cfg_dict,global_cfg_dict, channel, args.year)
        Plot2DMassboosted(dfWrapped.df,hist_cfg_dict,global_cfg_dict, channel, args.year, pNetWP)
