import copy
import datetime
import os
import sys
import ROOT

if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
import PlotKit.Plotter as Plotter
import Common.skimAnatuple as skimAnatuple
page_cfg = "config/plot/cms_stacked.yaml"
page_cfg_custom = "config/plot/2018.yaml"
hist_cfg = "config/plot/histograms.yaml"
inputs_cfg = "config/plot/inputs.yaml"

files= {
    "DY":["DY"],
    "Other":["EWK", "ST", "TTT", "TTTT", "TTVH", "TTV", "TTVV", "TTTV", "VH", "VV", "VVV", "H", "HHnonRes", "TTHH", "ttH"],
    "GluGluToBulkGraviton":["GluGluToBulkGraviton"],
    "GluGluToRadion":["GluGluToRadion"],
    "QCD":["QCD"],
    "TT":["TT"],
    "VBFToBulkGraviton":["VBFToBulkGraviton"],
    "VBFToRadion":["VBFToRadion"],
    "W":["W"],
    #"data":["data"],
}

def MakeStackedPlot(df_dict, var):
    hists = {}
    plotter = Plotter.Plotter(page_cfg=page_cfg, page_cfg_custom=page_cfg_custom, hist_cfg=hist_cfg, inputs_cfg=inputs_cfg)
    x_bins = Utilities.ListToVector(plotter.hist_cfg[var]['x_bins'], "double")
    model = ROOT.RDF.TH1DModel("", "", x_bins.size()-1, x_bins.data())
    for sample in df_dict.keys():
        hists[sample] = df.Histo1D(model,var).GetValue()
    plotter.plot(var, hists, 'output/prova_hist.pdf')



if __name__ == "__main__":
    import argparse
    import os
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--period', required=False, type=str, default = 'Run2_2018')
    parser.add_argument('--version', required=False, type=str, default = 'v2_deepTau_v2p1')
    parser.add_argument('--vars', required=False, type=str, default = 'tau1_pt')

    args = parser.parse_args()

    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    abs_path = os.environ['CENTRAL_STORAGE']
    anaTuplePath= "/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v2_deepTau_v2p1/" # os.path.join(os.environ['CENTRAL_STORAGE'], 'anaTuples', args.period, args.version)


    dataframes = {}

    for sample in files.keys():
        rootFiles = [anaTuplePath+f + ".root" for f in files[sample]]
        df = ROOT.RDataFrame("Events", rootFiles)
        dataframes[sample] = skimAnatuple.skimAnatuple(df)

        # apply changes to dF
    for var in args.vars.split(','):
        if(var == 'tau_inv_mass'):
            df = skimAnatuple.findInvMass(df)
        print(var)
        MakeStackedPlot(dataframes, var)

