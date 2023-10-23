
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

from Analysis.HistHelper import *
from Common.Utilities import *

deepTauYears = {'v2p1':'2017','v2p5':'2018'}
QCDregions = ['OS_Iso', 'SS_Iso', 'OS_AntiIso', 'SS_AntiIso']
categories = ['res2b', 'res1b', 'inclusive', 'btag_shape']
#categories = ['res2b', 'res1b', 'inclusive', 'boosted']
channels = {'eTau':13, 'muTau':23, 'tauTau':33}
triggers = {'eTau':'HLT_singleEle', 'muTau':'HLT_singleMu', 'tauTau':"HLT_ditau"}
btag_wps = {'res2b':'Medium', 'res1b':'Medium', 'boosted':"Loose", 'inclusive':'','btag_shape':''}

filters = {
        'channels':[('eTau','eTau && HLT_singleEle'), ('muTau','muTau && HLT_singleMu'),('tauTau','tauTau && HLT_ditau')],
        'QCD_regions':[('OS_Iso','OS_Iso'),('SS_Iso','SS_Iso'),('OS_AntiIso','OS_AntiIso'),('SS_AntiIso','SS_AntiIso')] ,
        'categories': [('res2b', 'res2b'), ('res1b', 'res1b'), ('inclusive', 'return true;'),('btag_shape', 'return true;')],
        }

def createKeyFilterDict():
    reg_dict = {}
    filter_str = ""
    for ch in channels:
        for reg in QCDregions:
            for cat in categories:
                filter_str = f"b1_pt>0 && b2_pt>0 && {ch} && {triggers[ch]} && {reg}"
                #if cat != 'inclusive':
                    #filter_str+=f" && {cat}"
                key = (ch, reg, cat)
                reg_dict[key] = filter_str
                #print(filter_str)
    return reg_dict

def QCD_Estimation(histograms, all_samples_list, channel, category, uncName, scale):
    #print(channel, category)
    #print(histograms.keys())
    key_B_data = ((channel, 'OS_AntiIso', category), ('Central', 'Central'))
    key_B = ((channel, 'OS_AntiIso', category), (uncName, scale))
    key_C_data = ((channel, 'SS_Iso', category), ('Central', 'Central'))
    key_C = ((channel, 'SS_Iso', category), (uncName, scale))
    key_D_data = ((channel, 'SS_AntiIso', category), ('Central', 'Central'))
    key_D = ((channel, 'SS_AntiIso', category), (uncName, scale))
    hist_data = histograms['data']
    #print(hist_data.keys())
    hist_data_B = hist_data[key_B_data].Clone()
    #if channel != 'tauTau' and category != 'inclusive': return hist_data_B
    hist_data_C = hist_data[key_C_data].Clone()
    hist_data_D = hist_data[key_D_data].Clone()
    n_data_C = hist_data_C.Integral(0, hist_data_C.GetNbinsX()+1)
    n_data_D = hist_data_D.Integral(0, hist_data_D.GetNbinsX()+1)
    #print(f"Yield for data {key_C_data} is {n_data_C}")
    #print(f"Yield for data {key_D_data} is {n_data_D}")
    for sample in all_samples_list:
        if sample=='data' :
            #print(f"sample {sample} is not considered")
            continue
        #print(sample)
        # find kappa value
        hist_sample = histograms[sample]
        #print(histograms[sample].keys())
        hist_sample_B = hist_sample[key_B].Clone()
        hist_sample_C = hist_sample[key_C].Clone()
        hist_sample_D = hist_sample[key_D].Clone()
        n_sample_C = hist_sample_C.Integral(0, hist_sample_C.GetNbinsX()+1)
        n_data_C-=n_sample_C
        n_sample_D = hist_sample_D.Integral(0, hist_sample_D.GetNbinsX()+1)
        n_data_D-=n_sample_D
        #print(f"Yield for data {key_C_data} after removing {sample} with yield {n_sample_C} is {n_data_C}")
        #print(f"Yield for data {key_D_data} after removing {sample} with yield {n_sample_D} is {n_data_D}")

        hist_data_B.Add(hist_sample_B, -1)
    #if n_data_C <= 0 or n_data_D <= 0:
        #print(f"n_data_C = {n_data_C}")
        #print(f"n_data_D = {n_data_D}")
    kappa = n_data_C/n_data_D
    if kappa<0:
        print(f"transfer factor <0")
        return ROOT.TH1D()
        #raise  RuntimeError(f"transfer factor <=0 ! {kappa}")
    hist_data_B.Scale(kappa)
    fix_negative_contributions,debug_info,negative_bins_info = FixNegativeContributions(hist_data_B)
    if not fix_negative_contributions:
        #return hist_data_B
        print(debug_info)
        print(negative_bins_info)
        print("Unable to estimate QCD")

        return ROOT.TH1D()
        #raise RuntimeError("Unable to estimate QCD")
    return hist_data_B


def CompareYields(histograms, all_samples_list, channel, category, uncName, scale):
    #print(channel, category)
    #print(histograms.keys())key_B_data = ((channel, 'OS_AntiIso', category), ('Central', 'Central'))
    key_A_data = ((channel, 'OS_Iso', category), ('Central', 'Central'))
    key_A = ((channel, 'OS_Iso', category), (uncName, scale))
    key_B_data = ((channel, 'OS_AntiIso', category), ('Central', 'Central'))
    key_B = ((channel, 'OS_AntiIso', category), (uncName, scale))
    key_C_data = ((channel, 'SS_Iso', category), ('Central', 'Central'))
    key_C = ((channel, 'SS_Iso', category), (uncName, scale))
    key_D_data = ((channel, 'SS_AntiIso', category), ('Central', 'Central'))
    key_D = ((channel, 'SS_AntiIso', category), (uncName, scale))
    hist_data = histograms['data']
    #print(hist_data.keys())
    hist_data_A = hist_data[key_A_data]
    hist_data_B = hist_data[key_B_data]
    #if channel != 'tauTau' and category != 'inclusive': return hist_data_B
    hist_data_C = hist_data[key_C_data]
    hist_data_D = hist_data[key_D_data]
    n_data_A = hist_data_A.Integral(0, hist_data_A.GetNbinsX()+1)
    n_data_B = hist_data_B.Integral(0, hist_data_B.GetNbinsX()+1)
    n_data_C = hist_data_C.Integral(0, hist_data_C.GetNbinsX()+1)
    n_data_D = hist_data_D.Integral(0, hist_data_D.GetNbinsX()+1)
    print(f"data || {key_A_data} || {n_data_A}")
    print(f"data || {key_B_data} || {n_data_B}")
    print(f"data || {key_C_data} || {n_data_C}")
    print(f"data || {key_D_data} || {n_data_D}")
    for sample in all_samples_list:
        #print(sample)
        # find kappa value
        hist_sample = histograms[sample]
        #print(histograms[sample].keys())
        hist_sample_A = hist_sample[key_A]
        hist_sample_B = hist_sample[key_B]
        hist_sample_C = hist_sample[key_C]
        hist_sample_D = hist_sample[key_D]
        n_sample_A = hist_sample_A.Integral(0, hist_sample_A.GetNbinsX()+1)
        n_sample_B = hist_sample_B.Integral(0, hist_sample_B.GetNbinsX()+1)
        n_sample_C = hist_sample_C.Integral(0, hist_sample_C.GetNbinsX()+1)
        n_sample_D = hist_sample_D.Integral(0, hist_sample_D.GetNbinsX()+1)

        print(f"{sample} || {key_A} || {n_sample_A}")
        print(f"{sample} || {key_B} || {n_sample_B}")
        print(f"{sample} || {key_C} || {n_sample_C}")
        print(f"{sample} || {key_D} || {n_sample_D}")

def AddQCDInHistDict(all_histograms, channels, categories, sample_type, uncName, all_samples_list, scales):
    if 'QCD' not in all_histograms.keys():
            all_histograms['QCD'] = {}
    for channel in channels:
        for cat in categories:
            if cat == 'btag_shape': continue
            #key =( (channel, 'OS_Iso', cat), ('Central', 'Central'))
            #all_histograms['QCD'][key] = QCD_Estimation(all_histograms, all_samples_list, channel, cat, 'Central', 'Central')
            for scale in scales:
                if uncName=='Central' : scale = 'Central'
                key =( (channel, 'OS_Iso', cat), (uncName, scale))
                all_histograms['QCD'][key] = QCD_Estimation(all_histograms, all_samples_list, channel, cat, uncName, scale)

def ApplyBTagWeight(cat,applyBtag=True, finalWeight_name = 'final_weight'):
    btag_weight = "1"
    btagshape_weight = "1"
    if applyBtag:
        if btag_wps[cat]!='' : btag_weight = f"weight_bTagSF_{btag_wps[cat]}_Central"
    else:
        if cat !='inclusive': btagshape_weight = "weight_bTagShapeSF"
    return f'{finalWeight_name}*{btag_weight}*{btagshape_weight}'


def GetWeight(channel):
    trg_weights_dict = {
        'eTau':["weight_tau1_TrgSF_singleEle_Central","weight_tau2_TrgSF_singleEle_Central"],
        'muTau':["weight_tau1_TrgSF_singleMu_Central","weight_tau2_TrgSF_singleMu_Central"],
        'tauTau':["weight_tau1_TrgSF_ditau_Central","weight_tau2_TrgSF_ditau_Central"]
        }
    weights_to_apply = [ "weight_Jet_PUJetID_Central_b1", "weight_Jet_PUJetID_Central_b2", "weight_TauID_Central", "weight_tau1_EleidSF_Central", "weight_tau1_MuidSF_Central", "weight_tau2_EleidSF_Central", "weight_tau2_MuidSF_Central","weight_total"]
    weights_to_apply.extend(trg_weights_dict[channel])
    total_weight = '*'.join(weights_to_apply)
    return total_weight


class DataFrameBuilder(DataFrameBuilderBase):

    def defineSelectionRegions(self):
        self.df = self.df.Define("nSelBtag", f"int(b1_idbtagDeepFlavB >= {self.bTagWP}) + int(b2_idbtagDeepFlavB >= {self.bTagWP})")
        self.df = self.df.Define("res1b", f"nSelBtag == 1")
        self.df = self.df.Define("res2b", f"nSelBtag == 2")
        self.df = self.df.Define("inclusive", f"return true;")
        self.df = self.df.Define("btag_shape", f"return true;")

    def defineChannels(self):
        self.df = self.df.Define("eTau", f"channelId==13")
        self.df = self.df.Define("muTau", f"channelId==23")
        self.df = self.df.Define("tauTau", f"channelId==33")

    def defineQCDRegions(self):
        tau2_iso_var = f"tau2_idDeepTau{self.deepTauYear()}{self.deepTauVersion}VSjet"
        self.df = self.df.Define("OS", "tau1_charge*tau2_charge < 0")
        self.df = self.df.Define("Iso", f"{tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.Medium.value}")
        self.df = self.df.Define("AntiIso", f"{tau2_iso_var} >= {Utilities.WorkingPointsTauVSjet.VVVLoose.value} && !Iso")
        self.df = self.df.Define("OS_Iso", f"OS && Iso")
        self.df = self.df.Define("SS_Iso", f"!OS && Iso")
        self.df = self.df.Define("OS_AntiIso", f"OS && AntiIso")
        self.df = self.df.Define("SS_AntiIso", f"!OS && AntiIso")

    def deepTauYear(self):
        return deepTauYears[self.deepTauVersion]

    def selectTrigger(self, trigger):
        self.df = self.df.Filter(trigger)

    def addCut (self, cut=""):
        if cut!="":
            self.df = self.df.Filter(cut)

    def __init__(self, df, deepTauVersion='v2p1', bTagWP = 2):
        super(DataFrameBuilder, self).__init__(df)
        self.deepTauVersion = deepTauVersion
        self.bTagWP = bTagWP

def PrepareDfWrapped(dfWrapped):
    dfWrapped.df = defineAllP4(dfWrapped.df)
    dfWrapped.df = createInvMass(dfWrapped.df)
    dfWrapped.defineQCDRegions()
    dfWrapped.defineSelectionRegions()
    dfWrapped.defineChannels()
    return dfWrapped