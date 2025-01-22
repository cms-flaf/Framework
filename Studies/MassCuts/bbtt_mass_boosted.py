import ROOT
import os
import sys
import yaml
import numpy as np

ROOT.gStyle.SetOptStat(0)

#ROOT.gStyle.SetPalette(109)
ROOT.gStyle.SetPalette(51)
ROOT.TColor.InvertPalette()

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

from Studies.MassCuts.DrawPlots import plot_1D_histogram

# def createCacheQuantities(dfWrapped_cache, cache_map_name):
#     df_cache = dfWrapped_cache.df
#     map_creator_cache = ROOT.analysis.CacheCreator(*dfWrapped_cache.colTypes)()
#     df_cache = map_creator_cache.processCache(ROOT.RDF.AsRNode(df_cache), Utilities.ListToVector(dfWrapped_cache.colNames), cache_map_name)
#     return df_cache

def GetCorrectBinning():

    # Valori dei centri (già ordinati)
    centri = [250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000, 1050, 1100, 1150, 1200, 1250, 1300, 1350, 1400, 1450, 1500, 1550, 1600, 1650, 1700, 1750, 1800, 1850, 1900, 1950, 2000, 2050, 2100, 2150, 2200, 2250, 2300, 2350, 2400, 2450, 2500, 2550, 2600, 2650, 2700, 2750, 2800, 2850, 2900, 2950, 3000]

    # Definisci gli intervalli
    bins = [centri[i] for i in range(len(centri) - 1)] + [centri[-1] + 1]  # Aggiungi un valore massimo per il bin finale
    # Calcola i limiti dei bin
    limits = [centri[0] - (centri[1] - centri[0]) / 2]  # Limite inferiore del primo bin

    for i in range(len(centri) - 1):
        limits.append((centri[i] + centri[i + 1]) / 2)

    limits.append(centri[-1] + (centri[-1] - centri[-2]) / 2)  # Limite superiore dell'ultimo bin

    # Converte in un array NumPy
    #limits = np.array(limits)
    return limits



# def AddCacheColumnsInDf(dfWrapped_central, dfWrapped_cache,cache_map_name='cache_map_placeholder'):
#     col_names_cache =  dfWrapped_cache.colNames
#     col_tpyes_cache =  dfWrapped_cache.colTypes
#     #print(col_names_cache)
#     #if "kinFit_result" in col_names_cache:
#     #    col_names_cache.remove("kinFit_result")
#     dfWrapped_cache.df = createCacheQuantities(dfWrapped_cache, cache_map_name)
#     if dfWrapped_cache.df.Filter(f"{cache_map_name} > 0").Count().GetValue() <= 0 : raise RuntimeError("no events passed map placeolder")
#     dfWrapped_central.AddCacheColumns(col_names_cache,col_tpyes_cache)

def createCentralQuantities(df_central, central_col_types, central_columns):
    map_creator = ROOT.analysis.MapCreator(*central_col_types)()
    df_central = map_creator.processCentral(ROOT.RDF.AsRNode(df_central), Utilities.ListToVector(central_columns), 1)
    #df_central = map_creator.getEventIdxFromShifted(ROOT.RDF.AsRNode(df_central))
    return df_central


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

def getLabels(cat,year,channel,mass,resonance):
    bins = GetCorrectBinning()
    labels = ["$m^{vis}_{HH}$","$m^{vis+MET}_{HH}$"] #,"$m_{HH}^{kinFit}$"
    cat_name = cat.split('_')[0]
    # if cat_name == 'baseline':
    #     cat_name == cat
    outDir = f"output/Masses_histograms/Run2_{year}/{cat_name}"
    if not os.path.isdir(outDir):
        os.makedirs(outDir)
    outFileName = f"{outDir}/{channel}_M-{mass}"
    if resonance != 'both' :
        outFileName = (f"{outDir}/{channel}_{resonance}_M-{mass}")
    # catname = cat.split("_")[0]
    spin = 0 if resonance == 'rad' else 2
    channelnames = {
        "eTau":"bbe$\\tau$",
        "muTau":"bb$\\mu\\tau$",
        "tauTau":"bb$\\tau\\tau$",
    }
    channelname = channelnames[channel]
    title = f"{channelname} {cat_name}"
    return bins,labels,cat_name,channelname,title,spin,outFileName

def PlotMass(df, hist_cfg_dict, global_cfg_dict,filter_str,cat,channel='tauTau', year='2018',mass='1250',resonance='', return_hists=False):
    saveFile = not return_hists
    bins,labels,cat_name,channelname,title,spin,outFileName = getLabels(cat,"all",channel,mass,res_str)
    hist_list = []
    total_weight_expression = GetWeight(channel,cat,global_cfg_dict['boosted_categories']) #if sample_type!='data' else "1"
    btag_weight = GetBTagWeight(global_cfg_dict,cat,applyBtag=False)
    total_weight_expression = "*".join([total_weight_expression,btag_weight])

    hist_bbttmass = df.Filter(filter_str).Define("final_weight", total_weight_expression).Histo1D(GetModel1D(bins),  "bbtautau_mass", "final_weight").GetValue()
    hist_list.append(hist_bbttmass)
    hist_bbttmass_met = df.Filter(filter_str).Define("final_weight", total_weight_expression).Histo1D(GetModel1D(bins),  "bbtautau_mass_met", "final_weight").GetValue()
    hist_list.append(hist_bbttmass_met)
    # hist_kinFit_m = df.Filter(filter_str).Define("final_weight", total_weight_expression).Histo1D(GetModel1D(bins), "kinFit_m", "final_weight").GetValue()
    # hist_list.append(hist_kinFit_m)
    if saveFile:
        plot_1D_histogram(hist_list,labels, bins,title, [], outFileName, f"Run2_{year}", mass, spin)
        print(outFileName+".pdf")
    if return_hists:
        return hist_list



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
    inFiles = Utilities.ListToVector([
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1000/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1250/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1500/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1750/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-2000/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-250/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-2500/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-260/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-270/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-280/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-300/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-3000/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-320/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-350/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-400/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-450/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-500/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-550/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-600/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-650/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-700/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-750/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-800/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-850/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-900/nanoHTT_0.root" , f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1000/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1250/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1500/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1750/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-2000/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-250/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-2500/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-260/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-270/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-280/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-300/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-3000/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-320/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-350/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-400/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-450/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-500/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-550/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-600/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-650/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-700/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-750/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-800/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-850/nanoHTT_0.root",
        f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-900/nanoHTT_0.root"
    ])


    years_list = args.year.split(",")
    if args.year == 'all':
        years_list = ["2016_HIPM","2016","2017","2018"]
    masses_list = args.mass.split(',')
    if args.mass == 'all':
        masses_list = [1000, 1250, 1500, 1750, 2000, 2500, 250, 260, 270, 280, 3000, 300, 320, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900]
    channels = ['eTau', 'muTau','tauTau'] if args.channels == '' else args.channels.split(',')
    cats = ['boosted_cat3_masswindow'] if args.cat == '' else args.cat.split(',') # 'boosted_baseline_masswindow'
    inFiles = []
    # inCacheFiles = []
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


        # if args.res == 'radion':
        #     inCacheFiles.append(f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaCacheTuples/Run2_{year}/GluGluToBulkGravitonToHHTo2B2Tau_M-{mass}/v13_deepTau2p1_HTT/SC/nanoHTT_0.root")
        # elif args.res == 'graviton':
        #     inCacheFiles.append(f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaCacheTuples/Run2_{year}/GluGluToRadionToHHTo2B2Tau_M-{mass}/v13_deepTau2p1_HTT/SC/nanoHTT_0.root")
        # else:
        #     inCacheFiles.append(f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaCacheTuples/Run2_{year}/GluGluToBulkGravitonToHHTo2B2Tau_M-{mass}/v13_deepTau2p1_HTT/SC/nanoHTT_0.root")
        #     inCacheFiles.append(f"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/anaCacheTuples/Run2_{year}/GluGluToRadionToHHTo2B2Tau_M-{mass}/v13_deepTau2p1_HTT/SC/nanoHTT_0.root")

        res_str = ''
        if args.res == 'graviton':
            res_str = 'grav'
        elif args.res == 'radion':
            res_str = 'rad'
        #print(inFiles)
        global_cfg_file = '/afs/cern.ch/work/v/vdamante/FLAF/config/HH_bbtautau/global.yaml'
        global_cfg_dict = {}
        with open(global_cfg_file, 'r') as f:
            global_cfg_dict = yaml.safe_load(f)
        global_cfg_dict['channels_to_consider'] = channels
        hist_cfg_file = '/afs/cern.ch/work/v/vdamante/FLAF/config/plot/histograms.yaml'
        hist_cfg_dict = {}
        with open(hist_cfg_file, 'r') as f:
            hist_cfg_dict = yaml.safe_load(f)
        # print("********************************************************************************")
        # print(f"************************************* {year} *************************************")
        # print("********************************************************************************")
        df_initial = ROOT.RDataFrame("Events", Utilities.ListToVector(inFiles))
        # df_cache_initial = ROOT.RDataFrame("Events", Utilities.ListToVector(inCacheFiles))

        key_filter_dict = createKeyFilterDict(global_cfg_dict, f"Run2_{year}")
        dfWrapped_central = DataFrameBuilderForHistograms(df_initial,global_cfg_dict, period=f"Run2_{year}", region="SR",isData=False)

        # dfWrapped_cache_central = DataFrameBuilderForHistograms(df_cache_initial,global_cfg_dict, period=f"Run2_{year}", region="SR",isData=False)

        # AddCacheColumnsInDf(dfWrapped_central, dfWrapped_cache_central, "cache_map_Central")
        dfWrapped = PrepareDfForHistograms(dfWrapped_central)
        #print(dfWrapped.df.GetColumnNames())
        pNetWP = dfWrapped.pNetWP

        # hists[year] = {}
        # print(key_filter_dict)
        for channel in channels:
            # print(channel)
            for cat in cats:
                # print(cat)
                filter_str = key_filter_dict[(channel, 'OS_Iso', cat)]
                #print(filter_str)
                if (channel,cat) not in hists.keys():
                    hists[(channel,cat)] = {}
                hists[(channel,cat)][year] = PlotMass(dfWrapped.df,hist_cfg_dict,global_cfg_dict, filter_str,cat,channel, args.year, mass,res_str, return_hists=True)

    # print(hists)
    for channel in channels:
        # print(channel)
        for cat in cats:
            hist_cat_list = hists[(channel,cat)][years_list[0]]
            for year_idx in (1, len(years_list)-1):
                hist_cat_list_2 = hists[(channel,cat)][years_list[year_idx]]
                for hist1,hist2 in zip(hist_cat_list,hist_cat_list_2):
                    hist1.Add(hist2)
            bins,labels,cat_name,channelname,title,spin,outFileName = getLabels(cat,"all",channel,mass,res_str)

            plot_1D_histogram(hist_cat_list, labels, bins,title, [], outFileName, "Run2_all", mass, spin)
            print(outFileName+".pdf")
            # plot_1D_histogram(hist_cat_list,labels, bbtautau_mass_bins,f"{channelnames[channel]} {cat_name}", [], outFileName, f"Run2_{year}", mass, spin)
