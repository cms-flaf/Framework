import ROOT
import Common.Utilities as Utilities
import sys
import os


weights_to_apply = ["weight_Central","weight_tau1_TrgSF_ditau_Central","weight_tau2_TrgSF_ditau_Central", "weight_tauID_Central",]
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

class AnaSkimmer:
    def defineP4(self, name):
        self.df = self.df.Define(f"{name}_p4", f"ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>({name}_pt,{name}_eta,{name}_phi,{name}_mass)")

    def defineAllP4(self):
        for idx in [0,1]:
            self.defineP4(f"tau{idx+1}")
            self.defineP4(f"b{idx+1}")
            self.defineP4(f"tau{idx+1}_seedingJet")

    def createInvMass(self):
        self.df = self.df.Define("tautau_m_vis", "(tau1_p4+tau2_p4).M()")
        self.df = self.df.Define("bb_m_vis", "(b1_p4+b2_p4).M()")
        self.df = self.df.Define("bbtautau_mass", "(b1_p4+b2_p4+tau1_p4+tau2_p4).M()")
        self.df = self.df.Define("dR_tautau", 'ROOT::Math::VectorUtil::DeltaR(tau1_p4, tau2_p4)')

    def __init__(self, df):
        self.df = df
        self.deepTauVersion = 'v2p1'
        self.deepTauYear = deepTauYears[self.deepTauVersion]
        self.defineAllP4()
        self.createInvMass()


    def skimAnatuple(self):
        for tau_idx in [1,2]:
            self.df = self.df.Filter(f'tau{tau_idx}_idDeepTau{self.deepTauYear}{self.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value}')
        self.df = self.df.Filter('tau1_charge * tau2_charge <0')
        self.df = self.df.Filter('HLT_ditau')


def defineWeights(df_dict):
    for sample in df_dict.keys():
        weight_names=[]
        for weight in weights_to_apply:
            weight_names.append(weight if sample!="data" else "1")
            if weight == 'weight_Central' and weight in df_dict[sample].GetColumnNames():
                weight_names.append("1000")
                if(sample=="TT"):
                    weight_names.append('791. / 687.')
        weight_str = " * ".join(weight_names)
        df_dict[sample]=df_dict[sample].Define("weight",weight_str)

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
        hists[sample] = df_dict[sample].Histo1D(model,var, "weight")
    return hists



if __name__ == "__main__":
    import argparse
    import PlotKit.Plotter as Plotter
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--period', required=False, type=str, default = 'Run2_2018')
    parser.add_argument('--version', required=False, type=str, default = 'v2_deepTau_v2p1')
    parser.add_argument('--vars', required=False, type=str, default = 'tau1_pt')
    parser.add_argument('--mass', required=False, type=int, default=500)
    args = parser.parse_args()

    abs_path = os.environ['CENTRAL_STORAGE']
    anaTuplePath= "/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v2_deepTau_v2p1/" # os.path.join(os.environ['CENTRAL_STORAGE'], 'anaTuples', args.period, args.version)

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
        anaskimmer = AnaSkimmer(df)
        anaskimmer.deepTauVersion = args.version.split('_')[-1]
        if(sample in signals):
            for input in inputs_cfg_dict:
                name = input['name']
                if(name == sample):
                    input['title']+= f"mass {args.mass}"
            anaskimmer.df = anaskimmer.df.Filter(f"X_mass=={args.mass}")
        anaskimmer.skimAnatuple()
        dataframes[sample] = anaskimmer.df

    defineWeights(dataframes)
    all_histograms = {}
    vars = args.vars.split(',')
    for var in vars:
        hists = createHistograms(dataframes, var)
        all_histograms[var] = hists

    for var in vars:
        custom1= {'cat_text':'inclusive'}
        print(var)
        for sample in hists.keys():
            hist = all_histograms[var][sample]
            all_histograms[var][sample] = hist.GetValue()
        plotter.plot(var, all_histograms[var], f"output/plots/{var}_XMass{args.mass}.pdf")#, custom=custom1)

