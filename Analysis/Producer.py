import ROOT
import Common.Utilities as Utilities
import sys
import os
import math
from TauIDSFs_modifier import *


weights_to_apply = ["weight_Central"]#,"weight_tau1_TrgSF_ditau_Central","weight_tau2_TrgSF_ditau_Central"]#, "weight_tauID_Central",]
files= {
    "DY":["DY"],
    "Other":["EWK", "ST", "TTT", "TTTT", "TTVH", "TTV", "TTVV", "TTTV", "VH", "VV", "VVV", "H", "ttH"],
    #"GluGluToBulkGraviton":["GluGluToBulkGraviton"],
    "SM_HH": ["HHnonRes"],
    "TT":["TT"],
    "GluGluToRadion":["GluGluToRadion"],
    #"VBFToBulkGraviton":["VBFToBulkGraviton"],
    # "VBFToRadion":["VBFToRadion"],
    "W":["W"],
    "data":["data_unique"],
}
signals = ["GluGluToBulkGraviton", "GluGluToRadion", "VBFToBulkGraviton", "VBFToRadion"]
deepTauYears = {'v2p1':'2017','v2p5':'2018'}
regions = ["A", "B", "C", "D"]
class AnaSkimmer:
    def defineP4(self, name):
        self.df = self.df.Define(f"{name}_p4", f"ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>({name}_pt,{name}_eta,{name}_phi,{name}_mass)")

    def defineAllP4(self):
        for idx in [0,1]:
            self.defineP4(f"tau{idx+1}")
            self.defineP4(f"b{idx+1}")
            self.defineP4(f"tau{idx+1}_seedingJet")

    def defineRegions(self):
        tau2_iso_var = f"tau2_idDeepTau{self.deepTauYear()}{self.deepTauVersion}VSjet"
        self.df = self.df.Define("region_A", f"tau1_charge*tau2_charge < 0 && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.Medium.value}")
        self.df = self.df.Define("region_B", f"tau1_charge*tau2_charge > 0 && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.Medium.value}")
        self.df = self.df.Define("region_C", f"tau1_charge*tau2_charge < 0 && {tau2_iso_var} < {Utilities.WorkingPointsTauVSjet.Medium.value} && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.VVVLoose.value}")
        self.df = self.df.Define("region_D", f"tau1_charge*tau2_charge > 0 && {tau2_iso_var} < {Utilities.WorkingPointsTauVSjet.Medium.value} && {tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.VVVLoose.value}")


    def createInvMass(self):
        self.df = self.df.Define("tautau_m_vis", "(tau1_p4+tau2_p4).M()")
        self.df = self.df.Define("bb_m_vis", "(b1_p4+b2_p4).M()")
        self.df = self.df.Define("bbtautau_mass", "(b1_p4+b2_p4+tau1_p4+tau2_p4).M()")
        self.df = self.df.Define("dR_tautau", 'ROOT::Math::VectorUtil::DeltaR(tau1_p4, tau2_p4)')

    def deepTauYear(self):
        return deepTauYears[self.deepTauVersion]

    def __init__(self, df, deepTauVersion):
        self.df = df
        self.deepTauVersion = deepTauVersion
        self.defineAllP4()
        self.createInvMass()
        self.defineRegions()


    def skimAnatuple(self):
        self.df = self.df.Filter('HLT_ditau')

def defineWeights(df_dict, use_new_weights=False, deepTauVersion='v2p1'):
    for sample in df_dict.keys():
        weight_names=[]
        if sample != "data":
            if(use_new_weights):
                df_dict[sample] = GetNewSFs_DM(df_dict[sample],weights_to_apply, deepTauVersion)
            '''
            else:
                if "weight_tauID_Central" not in weights_to_apply:
                    weights_to_apply.append("weight_tauID_Central")
            '''
        for weight in weights_to_apply:
            weight_names.append(weight if sample!="data" else "1")
            if weight == 'weight_Central' and weight in df_dict[sample].GetColumnNames():
                weight_names.append("1000")
                if(sample=="TT"):
                    weight_names.append('791. / 687.')
        weight_str = " * ".join(weight_names)
        df_dict[sample]=df_dict[sample].Define("weight",weight_str)



def RenormalizeHistogram(histogram, norm, include_overflows=True):
    integral = histogram.Integral(0, histogram.GetNbinsX()+1) if include_overflows else histogram.Integral()
    histogram.Scale(norm / integral)


def FixNegativeContributions(histogram):
    correction_factor = 0.

    ss_debug = ""
    ss_negative = ""

    original_Integral = histogram.Integral(0, histogram.GetNbinsX()+1)
    ss_debug += "\nSubtracted hist for '{}'.\n".format(histogram.GetName())
    ss_debug += "Integral after bkg subtraction: {}.\n".format(original_Integral)
    if original_Integral < 0:
        print(debug_info)
        print("Integral after bkg subtraction is negative for histogram '{}'".format(histogram.GetName()))
        return False

    for n in range(1, histogram.GetNbinsX()+1):
        if histogram.GetBinContent(n) >= 0:
            continue
        prefix = "WARNING" if histogram.GetBinContent(n) + histogram.GetBinError(n) >= 0 else "ERROR"

        ss_negative += "{}: {} Bin {}, content = {}, error = {}, bin limits=[{},{}].\n".format(
            prefix, histogram.GetName(), n, histogram.GetBinContent(n), histogram.GetBinError(n),
            histogram.GetBinLowEdge(n), histogram.GetBinLowEdge(n+1))

        error = correction_factor - histogram.GetBinContent(n)
        new_error = math.sqrt(math.pow(error, 2) + math.pow(histogram.GetBinError(n), 2))
        histogram.SetBinContent(n, correction_factor)
        histogram.SetBinError(n, new_error)

    RenormalizeHistogram(histogram, original_Integral, True)
    return True, ss_debug, ss_negative

def GetValues(collectio):
    for key, value in collection.items():
        if isinstance(value, dict):
            GetValues(value)
        else:
            collection[key] = value.GetValue()
    return collection

def Estimate_QCD(histograms, sums):
    hist_data = histograms['data']
    sum_data = sums['data']
    hist_data_B = hist_data['region_B']
    n_C = sum_data['region_C']
    n_D = sum_data['region_D']
    for sample in histograms.keys():
        if sample=='data' or sample in signals:
            continue
        # find kappa value
        n_C -= sums[sample]['region_C']
        n_D -= sums[sample]['region_D']
        hist_data_B.Add(histograms[sample]['region_B'], -1)
    kappa = n_C/n_D
    if n_C <= 0 or n_D <= 0:
        raise  RuntimeError(f"transfer factor <=0 ! {kappa}")
    hist_data_B.Scale(kappa)
    fix_negative_contributions,debug_info,negative_bins_info = FixNegativeContributions(hist_data_B)
    if not fix_negative_contributions:
        print(debug_info)
        print(negative_bins_info)
        raise RuntimeError("Unable to estimate QCD")
    return hist_data_B

def createHistograms(df_dict, var):
    hists = {}
    x_bins = plotter.hist_cfg[var]['x_bins']
    if type(plotter.hist_cfg[var]['x_bins'])==list:
        x_bins_vec = Utilities.ListToVector(x_bins, "double")
        model = ROOT.RDF.TH1DModel("", "", x_bins_vec.size()-1, x_bins_vec.data())
    else:
        n_bins, bin_range = x_bins.split('|')
        start,stop = bin_range.split(':')
        model = ROOT.RDF.TH1DModel("", "",int(n_bins), float(start), float(stop))
    for sample in df_dict.keys():
        hists[sample] = {}
        for region in regions:
            hists[sample][f"region_{region}"] = df_dict[sample].Filter(f"region_{region}").Histo1D(model,var, "weight")
    return hists

def createSums(df_dict):
    sums = {}
    for sample in df_dict.keys():
        sums[sample] = {}
        for region in regions:
            sums[sample][f"region_{region}"] = df_dict[sample].Filter(f"region_{region}").Sum("weight")
    return sums

if __name__ == "__main__":
    import argparse
    import PlotKit.Plotter as Plotter
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--period', required=False, type=str, default = 'Run2_2018')
    parser.add_argument('--version', required=False, type=str, default = 'v2_deepTau_v2p1')
    parser.add_argument('--vars', required=False, type=str, default = 'tau1_pt')
    parser.add_argument('--mass', required=False, type=int, default=500)
    parser.add_argument('--new-weights', required=False, type=bool, default=False)
    args = parser.parse_args()
    print(f"using new weights {args.new_weights}")
    abs_path = os.environ['CENTRAL_STORAGE']
    anaTuplePath= f"/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/{args.version}/"
    page_cfg = "config/plot/cms_stacked.yaml"
    page_cfg_custom = "config/plot/2018.yaml"
    hist_cfg = "config/plot/histograms.yaml"
    inputs_cfg = "config/plot/inputs.yaml"
    with open(inputs_cfg, 'r') as f:
        inputs_cfg_dict = yaml.safe_load(f)

    plotter = Plotter.Plotter(page_cfg=page_cfg, page_cfg_custom=page_cfg_custom, hist_cfg=hist_cfg, inputs_cfg=inputs_cfg_dict)

    dataframes = {}
    for sample in files.keys():
        rootFiles = [anaTuplePath+f + ".root" for f in files[sample]]
        df = ROOT.RDataFrame("Events", rootFiles)
        anaskimmer = AnaSkimmer(df, args.version.split('_')[-1])
        if(sample in signals):
            for input in inputs_cfg_dict:
                name = input['name']
                if(name == sample):
                    input['title']+= f"mass {args.mass}"
            anaskimmer.df = anaskimmer.df.Filter(f"X_mass=={args.mass}")
        anaskimmer.skimAnatuple()
        dataframes[sample] = anaskimmer.df
    defineWeights(dataframes, args.new_weights,args.version.split('_')[-1])

    all_histograms = {}
    vars = args.vars.split(',')
    all_sums = createSums(dataframes)

    for var in vars:
        hists = createHistograms(dataframes, var)
        all_histograms[var] = hists

    hists_to_plot = {}
    all_histograms=GetValues(all_histograms)
    all_sums=GetValues(all_sums)
    for var in vars:
        hists_to_plot[var] = {}
        for sample in all_histograms[var].keys():
            for region in regions:
                hists_to_plot[var][sample] = all_histograms[var][sample]['region_A']
        hists_to_plot[var]['QCD'] = Estimate_QCD(all_histograms[var], all_sums)
        custom1= {'cat_text':'inclusive'}
        plotter.plot(var, hists_to_plot[var], f"output/plots/{var}_XMass{args.mass}_{args.version}.pdf")#, custom=custom1)
        for sample in  hists_to_plot[var].keys():
            print(f"{sample}, {hists_to_plot[var][sample].Integral()}")