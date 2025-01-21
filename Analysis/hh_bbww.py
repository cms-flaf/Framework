import ROOT
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

from Analysis.HistHelper import *
from Common.Utilities import *


def createKeyFilterDict(global_cfg_dict, year):
    reg_dict = {}
    filter_str = ""
    channels_to_consider = global_cfg_dict['channels_to_consider']
    qcd_regions_to_consider = global_cfg_dict['QCDRegions']
    categories_to_consider = global_cfg_dict["categories"]
    #triggers = global_cfg_dict['hist_triggers']
    #mass_cut_limits = global_cfg_dict['mass_cut_limits']
    for ch in channels_to_consider:
        for reg in qcd_regions_to_consider:
            for cat in categories_to_consider:
                #print(ch, reg, cat, filter_str)
                #print()
                #filter_base = f" ({ch} && {triggers[ch]} && {reg} && {cat})"

                #Need to get the channel ID from config dict
                filter_base = f" ((channelId == {global_cfg_dict['channelDefinition'][ch]}) && {reg} && {cat})"
                filter_str = f"(" + filter_base
                #print(ch, reg, cat, filter_str)
                #print()
                #for mass_name,mass_limits in mass_cut_limits.items():
                #    filter_str+=f"&& ({mass_name} >= {mass_limits[0]})"
                #print(filter_str)
                #if cat != 'boosted' and cat!= 'baseline':
                #    filter_str += "&& (b1_pt>0 && b2_pt>0)"
                filter_str += ")"
                #print(filter_str)
                key = (ch, reg, cat)
                reg_dict[key] = filter_str
                #print(ch, reg, cat, filter_str)
                #print()

    return reg_dict

def GetBTagWeight(global_cfg_dict,cat,applyBtag=False):
    #This does not just apply btag, but it replaces the existing weight with a new weight! So dumb!!!
    btag_weight = "1"
    btagshape_weight = "1"
    if applyBtag:
        if global_cfg_dict['btag_wps'][cat]!='' : btag_weight = f"weight_bTagSF_{btag_wps[cat]}_Central"
    else:
        if cat !='btag_shape' and cat !='boosted': btagshape_weight = "weight_bTagShape_Central"
    return f'1.'
    #return f'{btag_weight}*{btagshape_weight}'


def GetWeight(channel, cat, boosted_categories):
    weights_to_apply = ["weight_MC_Lumi_pu"]
    total_weight = '*'.join(weights_to_apply)
    for lep_index in [1,2]:
        total_weight = f"{total_weight} * {GetLepWeight(lep_index)}"
    total_weight = f"{total_weight} * {GetTriggerWeight()}"
    return total_weight

def GetLepWeight(lep_index):
    weight_Ele = f"(lep{lep_index}_type == static_cast<int>(Leg::e) ? 1.0 : 1.0)"

    #Medium pT Muon SF
    weight_Mu = f"(lep{lep_index}_type == static_cast<int>(Leg::mu) ? weight_lep{lep_index}_MuonID_SF_TightID_TrkCentral * weight_lep{lep_index}_MuonID_SF_LoosePFIsoCentral : 1.0)"

    #High pT Muon SF
    #weight_Mu = f"(lep{lep_index}_type == static_cast<int>(Leg::mu) ? weight_lep{lep_index}_HighPt_MuonID_SF_HighPtIDCentral * weight_lep{lep_index}_HighPt_MuonID_SF_RecoCentral * weight_lep{lep_index}_HighPt_MuonID_SF_TightIDCentral : 1.0)"

    #No Muon SF
    #weight_Mu = f"(lep{lep_index}_type == static_cast<int>(Leg::mu) ? 1.0 : 1.0)"

    return f"{weight_Mu} * {weight_Ele}"


def GetTriggerWeight():
    weight_MuTrg = f"(lep1_type == static_cast<int>(Leg::mu) ? weight_lep1_TrgSF_singleIsoMu_Central : 1.0)"

    return f"{weight_MuTrg}"

def AddQCDInHistDict(var, all_histograms, channels, categories, uncName, all_samples_list, scales, unc_to_not_consider_boosted, wantNegativeContributions=False):
    return



class DataFrameBuilderForHistograms(DataFrameBuilderBase):

    def defineCategories(self):
        self.bTagWP = 0.43 #Temp for now, everything is a 'b jet'
        self.df = self.df.Define("nSelBtag", f"int(bjet1_btagPNetB >= {self.bTagWP}) + int(bjet2_btagPNetB >= {self.bTagWP})")

        #self.df = self.df.Define("nSelBtag", f"int(centralJet_btagPNetB >= {self.bTagWP}) + int(centralJet_btagPNetB >= {self.bTagWP})")
        #self.df = self.df.Define("boosted", "nSelectedFatJet > 0")
        self.df = self.df.Define("boosted", "SelectedFatJet_pt.size() > 0")
        self.df = self.df.Define("resolved", "!boosted && centralJet_pt.size() >= 2")
        self.df = self.df.Define("res1b", f"!boosted && resolved && nSelBtag == 1")
        self.df = self.df.Define("res2b", f"!boosted && resolved && nSelBtag == 2")
        self.df = self.df.Define("inclusive", f"!boosted && resolved")
        self.df = self.df.Define("baseline",f"return true;")

    def defineChannels(self):
        #self.df = self.df.Define("channelId", f"(lep1_type*10) + lep2_type") #Muhammad moved this to anaTupleDef like a jerk
        for channel in self.config['channelSelection']:
            ch_value = self.config['channelDefinition'][channel]
            #self.df = self.df.Define(f"{channel}", f"channelId=={ch_value}")

    def defineLeptonPreselection(self):
        #Later we will defined some lepton selections

        self.df = self.df.Define("passed_singleIsoMu", "HLT_singleIsoMu && (lep1_type == 2 && lep1_HasMatching_singleIsoMu)")
        self.df = self.df.Filter(f"passed_singleIsoMu")

    def defineJetSelections(self):
        self.df = self.df.Define("jet1_isvalid", "centralJet_pt.size() > 0")
        self.df = self.df.Define("jet2_isvalid", "centralJet_pt.size() > 1")
        self.df = self.df.Define("fatjet_isvalid", "SelectedFatJet_pt.size() > 0")

        self.df = self.df.Define("bjet1_btagPNetB", "jet1_isvalid ? centralJet_btagPNetB[0] : -1.0")
        self.df = self.df.Define("bjet2_btagPNetB", "jet2_isvalid ? centralJet_btagPNetB[1] : -1.0")
        self.df = self.df.Define("bsubjet1_btagDeepB", "fatjet_isvalid ? SelectedFatJet_SubJet1_btagDeepB[0] : -1.0")
        self.df = self.df.Define("bsubjet2_btagDeepB", "fatjet_isvalid ? SelectedFatJet_SubJet2_btagDeepB[0] : -1.0")


    def defineQCDRegions(self):
        self.df = self.df.Define("OS", "(lep2_type < 1) || (lep1_charge*lep2_charge < 0)")
        self.df = self.df.Define("SS", "!OS")

        self.df = self.df.Define("Iso", f"( (lep1_type == 1 && lep1_Electron_mvaIso_WP80) || (lep1_type == 2 && lep1_Muon_pfIsoId >= {MuonPfIsoID_WP.Loose.value}) ) && (lep2_type < 1 || ( (lep2_type == 1 && lep2_Electron_mvaIso_WP80) || (lep2_type == 2 && lep2_Muon_pfIsoId >= {MuonPfIsoID_WP.Loose.value}) ))") #Ask if this is supposed to be lep*_Muon_pfIsoId
        self.df = self.df.Define("AntiIso", f"!Iso") #This is probably not correct, but required for QCD_Estimation.py

        self.df = self.df.Define("OS_Iso", f"OS && Iso") 
        self.df = self.df.Define("SS_Iso", f"SS && Iso")
        self.df = self.df.Define("OS_AntiIso", f"OS && AntiIso")
        self.df = self.df.Define("SS_AntiIso", f"SS && AntiIso")

    def defineControlRegions(self):
        #Define Single Muon Control Region (W Region) -- Require Muon + High MT (>50)
        #Define Double Muon Control Region (Z Region) -- Require lep1 lep2 are opposite sign muons, and combined mass is within 10GeV of 91

        self.df = self.df.Define("Z_Region", f"(lep1_type == 2 && lep2_type == 2) && (abs(diLep_p4.mass() - 91) < 10)")
        self.df = self.df.Define("diLep_mass", f"(lep1_type == 2 && lep2_type == 2) ? diLep_p4.mass() : 0.0")

        self.df = self.df.Define("Lep1Lep2Jet1Jet2_mass", f"(lep1_type == 2 && lep2_type == 2) ? Lep1Lep2Jet1Jet2_p4.mass() : 0.0")
        self.df = self.df.Define("Lep1Jet1Jet2_mass", f"(lep1_type == 2) ? Lep1Jet1Jet2_p4.mass() : 0.0")

    def calculateMT(self):
        self.df = self.df.Define("MT_lep1", f"(lep1_type > 0) ? Calculate_MT(lep1_p4, PuppiMET_p4) : 0.0")
        self.df = self.df.Define("MT_lep2", f"(lep2_type > 0) ? Calculate_MT(lep1_p4, PuppiMET_p4) : 0.0")
        self.df = self.df.Define("MT_tot", f"(lep1_type > 0 && lep2_type > 0) ? Calculate_TotalMT(lep1_p4, lep2_p4, DeepMETResolutionTune_p4) : 0.0")

    def selectTrigger(self, trigger):
        self.df = self.df.Filter(trigger)

    def addCut (self, cut=""):
        if cut!="":
            self.df = self.df.Filter(cut)



    def defineTriggers(self):
        for ch in self.config['channelSelection']:
            for trg in self.config['triggers'][ch]:
                trg_name = 'HLT_'+trg
                if trg_name not in self.df.GetColumnNames():
                    print(f"{trg_name} not present in colNames")
                    self.df = self.df.Define(trg_name, "1")


        singleTau_th_dict = self.config['singleTau_th']
        #singleMu_th_dict = self.config['singleMu_th']
        #singleEle_th_dict = self.config['singleEle_th']
        for trg_name,trg_dict in self.config['application_regions'].items():
            for key in trg_dict.keys():
                region_name = trg_dict['region_name']
                region_cut = trg_dict['region_cut'].format(tau_th=singleTau_th_dict[self.period])
                if region_name not in self.df.GetColumnNames():
                    self.df = self.df.Define(region_name, region_cut)

    def __init__(self, df, config, period, **kwargs):
        super(DataFrameBuilderForHistograms, self).__init__(df, **kwargs)
        self.config = config
        self.period = period



def defineAllP4(df):
    df = df.Define(f"SelectedFatJet_idx", f"CreateIndexes(SelectedFatJet_pt.size())")
    df = df.Define(f"SelectedFatJet_p4", f"GetP4(SelectedFatJet_pt, SelectedFatJet_eta, SelectedFatJet_phi, SelectedFatJet_mass, SelectedFatJet_idx)")
    for idx in [0,1]:
        df = Utilities.defineP4(df, f"lep{idx+1}")
    df = df.Define(f"centralJet_p4", f"GetP4(centralJet_pt, centralJet_eta, centralJet_phi, centralJet_mass)")
    for met_var in ['DeepMETResolutionTune', 'DeepMETResponseTune', 'PuppiMET']:
        df = df.Define(f"{met_var}_p4", f"ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>({met_var}_pt,0.,{met_var}_phi,0.)")

    df = df.Define(f"diLep_p4", f"(lep1_p4+lep2_p4)")
    df = df.Define(f"Lep1Lep2Jet1Jet2_p4", f"(centralJet_pt.size() >= 2) ? (lep1_p4+lep2_p4+centralJet_p4[0]+centralJet_p4[1]) : LorentzVectorM()")
    df = df.Define(f"Lep1Jet1Jet2_p4", f"(centralJet_pt.size() >= 2) ? (lep1_p4+centralJet_p4[0]+centralJet_p4[1]) : LorentzVectorM()")
    return df



def PrepareDfForHistograms(dfForHistograms):
    dfForHistograms.df = defineAllP4(dfForHistograms.df)
    #dfForHistograms.defineChannels()
    dfForHistograms.defineLeptonPreselection()
    dfForHistograms.defineJetSelections()
    dfForHistograms.defineQCDRegions()
    dfForHistograms.defineControlRegions()
    #dfForHistograms.defineBoostedVariables()
    dfForHistograms.defineCategories()
    #dfForHistograms.defineTriggers()
    #dfForHistograms.redefineWeights()
    #dfForHistograms.df = createInvMass(dfForHistograms.df)
    dfForHistograms.calculateMT()
    return dfForHistograms
