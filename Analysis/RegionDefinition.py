
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

from Analysis.HistHelper import *
from Common.Utilities import *

deepTauYears = {'v2p1':'2017','v2p5':'2018'}
QCDregions = ['OS_Iso', 'SS_Iso', 'OS_AntiIso', 'SS_AntiIso']
categories = ['res2b', 'res1b', 'inclusive']#, 'boosted']
#categories = ['res2b', 'res1b', 'inclusive', 'boosted']
channels = {'eTau':13, 'muTau':23, 'tauTau':33}
triggers = {'eTau':'HLT_singleEle', 'muTau':'HLT_singleMu', 'tauTau':"HLT_ditau"}

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
                filter_str = f"{ch} && {triggers[ch]} && {reg}"
                if cat != 'inclusive':
                    filter_str+=f" && {cat}"
                key = (ch, reg, cat)
                #print(key, filter_str)
                reg_dict[key] = filter_str
                #filter_str = ""
    #print(reg_dict)
    return reg_dict

class DataFrameBuilder(DataFrameBuilderBase):

    def defineSelectionRegions(self):
        self.df = self.df.Define("nSelBtag", f"int(b1_idbtagDeepFlavB >= {self.bTagWP}) + int(b2_idbtagDeepFlavB >= {self.bTagWP})")
        self.df = self.df.Define("res1b", f"nSelBtag == 1")
        self.df = self.df.Define("res2b", f"nSelBtag == 2")
        self.df = self.df.Define("inclusive", f"return true;")

    def defineChannels(self):
        self.df = self.df.Define("eTau", f"channelId==13")
        self.df = self.df.Define("muTau", f"channelId==23")
        self.df = self.df.Define("tauTau", f"channelId==23")

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