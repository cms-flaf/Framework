import ROOT
import os
import sys
import yaml
import numpy as np

ROOT.gStyle.SetOptStat(0)

#ROOT.gStyle.SetPalette(109)
ROOT.gStyle.SetPalette(51)
ROOT.TColor.InvertPalette()

if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])
import Common.Utilities as Utilities
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

def createCacheQuantities(dfWrapped_cache, cache_map_name):
    df_cache = dfWrapped_cache.df
    map_creator_cache = ROOT.analysis.CacheCreator(*dfWrapped_cache.colTypes)()
    df_cache = map_creator_cache.processCache(ROOT.RDF.AsRNode(df_cache), Utilities.ListToVector(dfWrapped_cache.colNames), cache_map_name)
    return df_cache
def createCentralQuantities(df_central, central_col_types, central_columns):
    map_creator = ROOT.analysis.MapCreator(*central_col_types)()
    df_central = map_creator.processCentral(ROOT.RDF.AsRNode(df_central), Utilities.ListToVector(central_columns), 1)
    #df_central = map_creator.getEventIdxFromShifted(ROOT.RDF.AsRNode(df_central))
    return df_central
def AddCacheColumnsInDf(dfWrapped_central, dfWrapped_cache,cache_map_name='cache_map_placeholder'):
    col_names_cache =  dfWrapped_cache.colNames
    col_tpyes_cache =  dfWrapped_cache.colTypes
    #print(col_names_cache)
    #if "kinFit_result" in col_names_cache:
    #    col_names_cache.remove("kinFit_result")
    dfWrapped_cache.df = createCacheQuantities(dfWrapped_cache, cache_map_name)
    if dfWrapped_cache.df.Filter(f"{cache_map_name} > 0").Count().GetValue() <= 0 : raise RuntimeError("no events passed map placeolder")
    dfWrapped_central.AddCacheColumns(col_names_cache,col_tpyes_cache)


def GetModel1D(x_bins):#hist_cfg, var1, var2):
    #x_bins = hist_cfg[var1]['x_bins']
    #y_bins = hist_cfg[var2]['x_bins']
    if type(x_bins)==list:
        x_bins_vec = Utilities.ListToVector(x_bins, "double")
        model = ROOT.RDF.TH1DModel("", "", x_bins_vec.size()-1, x_bins_vec.data())
    else:
        n_x_bins, x_bin_range = x_bins.split('|')
        x_start,x_stop = x_bin_range.split(':')
        model = ROOT.RDF.TH1DModel("", "",int(n_x_bins), float(x_start), float(x_stop))
    return model

def PlotMass(df, hist_cfg_dict, global_cfg_dict,filter_str,cat,channel='tauTau', year='2018',mass='1250',resonance='', return_hists=False):
    saveFile = not return_hists
    labels = ["$m^{vis}_{HH}$","$m^{vis+MET}_{HH}$","$m_{HH}^{kinFit}$"] if cat != 'boosted_cat3_masswindow' else  ["$m^{vis}_{HH}$","$m^{vis+MET}_{HH}$"]
    bins,labels,cat_name,channelname,title,spin,outFileName = getLabels(cat,"all",channel,mass,res_str,labels)
    hist_list = []
    total_weight_expression = GetWeight(channel,cat,global_cfg_dict['boosted_categories']) #if sample_type!='data' else "1"
    btag_weight = GetBTagWeight(global_cfg_dict,cat,applyBtag=False)
    total_weight_expression = "*".join([total_weight_expression,btag_weight])

    hist_bbttmass = df.Filter(filter_str).Define("final_weight", total_weight_expression).Histo1D(GetModel1D(bins),  "bbtautau_mass", "final_weight").GetValue()
    hist_list.append(hist_bbttmass)
    hist_bbttmass_met = df.Filter(filter_str).Define("final_weight", total_weight_expression).Histo1D(GetModel1D(bins),  "bbtautau_mass_met", "final_weight").GetValue()
    hist_list.append(hist_bbttmass_met)
    hist_kinFit_m = df.Filter(filter_str).Define("final_weight", total_weight_expression).Histo1D(GetModel1D(bins), "kinFit_m", "final_weight").GetValue()
    if cat != 'boosted_cat3_masswindow':
        hist_list.append(hist_kinFit_m)
    if saveFile:
        plot_1D_histogram(hist_list,labels, bins,title, [], outFileName, f"Run2_{year}", mass, spin)
        print(outFileName+".pdf")
    if return_hists:
        return hist_list

if __name__ == "__main__":
    import argparse
    import yaml

    from Analysis.HistHelper import *
    from Analysis.hh_bbtautau import *
    from Studies.MassCuts.utils import *
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', required=False, type=str, default='2018')
    parser.add_argument('--cat', required=False, type=str, default='')
    parser.add_argument('--mass', required=False, type=str, default='1250')
    parser.add_argument('--channels', required=False, type=str, default = '')
    parser.add_argument('--res', required=False, type=str, default='radion')
    args = parser.parse_args()

    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    ROOT.gInterpreter.Declare(f'#include "include/KinFitNamespace.h"')
    ROOT.gInterpreter.Declare(f'#include "include/HistHelper.h"')
    ROOT.gInterpreter.Declare(f'#include "include/Utilities.h"')
    ROOT.gInterpreter.Declare(f'#include "include/pnetSF.h"')
    ROOT.gROOT.ProcessLine('#include "include/AnalysisTools.h"')


    years_list = args.year.split(",")
    if args.year == 'all':
        years_list = ["2016_HIPM","2016","2017","2018"]
    masses_list = args.mass.split(',')
    if args.mass == 'all':
        masses_list = [1000, 1250, 1500, 1750, 2000, 2500, 250, 260, 270, 280, 3000, 300, 320, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900]
    channels = ['eTau', 'muTau','tauTau'] if args.channels == '' else args.channels.split(',')
    cats = ['res1b_cat3_masswindow', 'res2b_cat3_masswindow','baseline_masswindow', 'inclusive_masswindow','boosted_cat3_masswindow'] if args.cat == '' else args.cat.split(',')
    inFiles = []
    inCacheFiles = []
    hists={}
    mass = args.mass
    for year in years_list:
        if args.res == 'radion':
            inFiles.append(f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{year}/GluGluToBulkGravitonToHHTo2B2Tau_M-{mass}/nanoHTT_0.root")
        elif args.res == 'graviton':
            inFiles.append(f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{year}/GluGluToRadionToHHTo2B2Tau_M-{mass}/nanoHTT_0.root")
        else:
            inFiles.append(f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{year}/GluGluToBulkGravitonToHHTo2B2Tau_M-{mass}/nanoHTT_0.root")
            inFiles.append(f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{year}/GluGluToRadionToHHTo2B2Tau_M-{mass}/nanoHTT_0.root")


        if args.res == 'radion':
            inCacheFiles.append(f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaCacheTuples/Run2_{year}/GluGluToBulkGravitonToHHTo2B2Tau_M-{mass}/v13_deepTau2p1_HTT/SC/nanoHTT_0.root")
        elif args.res == 'graviton':
            inCacheFiles.append(f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaCacheTuples/Run2_{year}/GluGluToRadionToHHTo2B2Tau_M-{mass}/v13_deepTau2p1_HTT/SC/nanoHTT_0.root")
        else:
            inCacheFiles.append(f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaCacheTuples/Run2_{year}/GluGluToBulkGravitonToHHTo2B2Tau_M-{mass}/v13_deepTau2p1_HTT/SC/nanoHTT_0.root")
            inCacheFiles.append(f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaCacheTuples/Run2_{year}/GluGluToRadionToHHTo2B2Tau_M-{mass}/v13_deepTau2p1_HTT/SC/nanoHTT_0.root")

        res_str = ''
        if args.res == 'graviton':
            res_str = 'grav'
        elif args.res == 'radion':
            res_str = 'rad'
        global_cfg_file = '/afs/cern.ch/work/v/vdamante/FLAF/config/HH_bbtautau/global.yaml'
        global_cfg_dict = {}
        with open(global_cfg_file, 'r') as f:
            global_cfg_dict = yaml.safe_load(f)
        global_cfg_dict['channels_to_consider'] = channels
        hist_cfg_file = '/afs/cern.ch/work/v/vdamante/FLAF/config/plot/histograms.yaml'
        hist_cfg_dict = {}
        with open(hist_cfg_file, 'r') as f:
            hist_cfg_dict = yaml.safe_load(f)
        df_initial = ROOT.RDataFrame("Events", Utilities.ListToVector(inFiles))
        df_cache_initial = ROOT.RDataFrame("Events", Utilities.ListToVector(inCacheFiles))

        key_filter_dict = createKeyFilterDict(global_cfg_dict, f"Run2_{year}")
        dfWrapped_central = DataFrameBuilderForHistograms(df_initial,global_cfg_dict, period=f"Run2_{year}", region="SR",isData=False)

        dfWrapped_cache_central = DataFrameBuilderForHistograms(df_cache_initial,global_cfg_dict, period=f"Run2_{year}", region="SR",isData=False)

        AddCacheColumnsInDf(dfWrapped_central, dfWrapped_cache_central, "cache_map_Central")
        dfWrapped = PrepareDfForHistograms(dfWrapped_central)
        pNetWP = dfWrapped.pNetWP
        for channel in channels:
            for cat in cats:
                filter_str = key_filter_dict[(channel, 'OS_Iso', cat)]
                if (channel,cat) not in hists.keys():
                    hists[(channel,cat)] = {}
                hists[(channel,cat)][year] = PlotMass(dfWrapped.df,hist_cfg_dict,global_cfg_dict, filter_str,cat,channel, args.year, mass,res_str, return_hists=True)

    for channel in channels:
        for cat in cats:
            labels = ["$m^{vis}_{HH}$","$m^{vis+MET}_{HH}$","$m_{HH}^{kinFit}$"] if cat !='boosted_cat3_masswindow' else ["$m^{vis}_{HH}$","$m^{vis+MET}_{HH}$"]
            hist_cat_list = hists[(channel,cat)][years_list[0]]
            for year_idx in (1, len(years_list)-1):
                hist_cat_list_2 = hists[(channel,cat)][years_list[year_idx]]
                for hist1,hist2 in zip(hist_cat_list,hist_cat_list_2):
                    hist1.Add(hist2)
            bins,labels,cat_name,channelname,title,spin,outFileName = getLabels(cat,"all",channel,mass,res_str,labels)

            plot_1D_histogram(hist_cat_list, labels, bins,title, [], outFileName, "Run2_all", mass, spin)
            print(outFileName+".pdf")

