
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

from Analysis.HistHelper import *
from Common.Utilities import *

deepTauYears = {'v2p1':'2017','v2p5':'2018'}
QCDregions = ['OS_Iso', 'SS_Iso', 'OS_AntiIso', 'SS_AntiIso']
categories = ['res2b', 'res1b', 'inclusive']
#categories = ['res2b', 'res1b', 'inclusive', 'boosted']
channels = {'eTau':13, 'muTau':23, 'tauTau':33}
triggers = {'eTau':'HLT_singleEle', 'muTau':'HLT_singleMu', 'tauTau':"HLT_ditau"}
btag_wps = {'res2b':'Medium', 'res1b':'Medium', 'boosted':"Loose", 'inclusive':''}

filters = {
        'channels':[('eTau','eTau && HLT_singleEle'), ('muTau','muTau && HLT_singleMu'),('tauTau','tauTau && HLT_ditau')],
        'QCD_regions':[('OS_Iso','OS_Iso'),('SS_Iso','SS_Iso'),('OS_AntiIso','OS_AntiIso'),('SS_AntiIso','SS_AntiIso')] ,
        'categories': [('res2b', 'res2b'), ('res1b', 'res1b'), ('inclusive', 'return true;')],
        }

def createKeyFilterDict():
    reg_dict = {}
    filter_str = ""
    for ch in channels:
        for reg in QCDregions:
            for cat in categories:
                filter_str = f"b1_pt>0 && b2_pt>0 && {ch} && {triggers[ch]} && {reg}"
                if cat != 'inclusive':
                    filter_str+=f" && {cat}"
                key = (ch, reg, cat)
                reg_dict[key] = filter_str
                #print(filter_str)
    return reg_dict


def AddQCDInHistDict(all_histograms, channels, categories, sample_type, uncNameTypes, all_samples_list, scales, onlyCentral):
    if 'QCD' not in all_histograms.keys():
            all_histograms['QCD'] = {}
    for channel in channels:
        print(f"adding QCD for channel {channel}")
        for cat in categories:
            print(f".. and category {cat}")
            key =( (channel, 'OS_Iso', cat), ('Central', 'Central'))
            CompareYields(all_histograms, all_samples_list, channel, cat, 'Central', 'Central')
            print(f".. and uncNameType Central")
            all_histograms['QCD'][key] = QCD_Estimation(all_histograms, all_samples_list, channel, cat, 'Central', 'Central')
            for uncNameType in uncNameTypes:
                print(f".. and uncNameType {uncNameType}")
                for scale in scales:
                    print(f" .. and uncScale {scale}")
                    if onlyCentral and uncNameType!='Central' and scale!='Central' : continue
                    key =( (channel, 'OS_Iso', cat), (uncNameType, scale))
                    CompareYields(all_histograms, all_samples_list, channel, cat, uncNameType, scale)
                    all_histograms['QCD'][key] = QCD_Estimation(all_histograms, all_samples_list, channel, cat, uncNameType, scale)


def GetWeight(channel,cat):
    btag_weight = "1"
    if btag_wps[cat]!='':
        btag_weight = f"weight_bTagSF_{btag_wps[cat]}_Central"
    trg_weights_dict = {
        'eTau':["weight_tau1_TrgSF_singleEle_Central","weight_tau2_TrgSF_singleEle_Central"],
        'muTau':["weight_tau1_TrgSF_singleMu_Central","weight_tau2_TrgSF_singleMu_Central"],
        'tauTau':["weight_tau1_TrgSF_ditau_Central","weight_tau2_TrgSF_ditau_Central"]
        }
    #weights_to_apply = [ "weight_Jet_PUJetID_Central_b1", "weight_Jet_PUJetID_Central_b2", "weight_TauID_Central", btag_weight, "weight_tau1_EleidSF_Central", "weight_tau1_MuidSF_Central", "weight_tau2_EleidSF_Central", "weight_tau2_MuidSF_Central","weight_total"]
    weights_to_apply = ["weight_TauID_Central", btag_weight, "weight_tau1_EleidSF_Central", "weight_tau1_MuidSF_Central", "weight_tau2_EleidSF_Central", "weight_tau2_MuidSF_Central","weight_total"]
    weights_to_apply.extend(trg_weights_dict[channel])
    total_weight = '*'.join(weights_to_apply)
    return total_weight


class DataFrameBuilder(DataFrameBuilderBase):

    def defineSelectionRegions(self):
        self.df = self.df.Define("nSelBtag", f"int(b1_idbtagDeepFlavB >= {self.bTagWP}) + int(b2_idbtagDeepFlavB >= {self.bTagWP})")
        self.df = self.df.Define("res1b", f"nSelBtag == 1")
        self.df = self.df.Define("res2b", f"nSelBtag == 2")
        self.df = self.df.Define("inclusive", f"return true;")

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