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
        self.df = self.df.Define("resolved", "centralJet_pt.size() >= 2")
        self.df = self.df.Define("res1b", f"resolved && nSelBtag == 1")
        self.df = self.df.Define("res2b", f"resolved && nSelBtag >= 2")
        self.df = self.df.Define("boosted", "resolved && !res2b && SelectedFatJet_pt.size() > 0") #Work in progress

        self.df = self.df.Define("inclusive", f"centralJet_pt.size() >= 2")
        self.df = self.df.Define("baseline",f"return true;")

    def defineChannels(self):
        #self.df = self.df.Define("channelId", f"(lep1_type*10) + lep2_type") #Muhammad moved this to anaTupleDef like a jerk
        for channel in self.config['channelSelection']:
            ch_value = self.config['channelDefinition'][channel]
            #self.df = self.df.Define(f"{channel}", f"channelId=={ch_value}")

    def defineLeptonPreselection(self):
        #Later we will defined some lepton selections

        self.df = self.df.Define("passed_singleIsoMu", "HLT_singleIsoMu && (lep1_type == 2 && lep1_HasMatching_singleIsoMu)")

        muMu_df = self.df.Filter("(channelId == 22)")
        eMu_df = self.df.Filter("(channelId == 12)")
        muE_df = self.df.Filter("(channelId == 21)")
        eE_df = self.df.Filter("(channelId == 11)")
        print(f"Checking nEvents muMu {muMu_df.Count().GetValue()}")
        print(f"Checking nEvents eMu {eMu_df.Count().GetValue()}")
        print(f"Checking nEvents muE {muE_df.Count().GetValue()}")
        print(f"Checking nEvents eE {eE_df.Count().GetValue()}")

        self.df = self.df.Define("passed_singleEleWpTight", "HLT_singleEleWpTight && (lep1_type == 1 && lep1_HasMatching_singleEleWpTight)")
        # self.df = self.df.Define("passed_singleEleWpTight", "HLT_singleEleWpTight && (lep1_type == 1)")
        print(f"Checking nEvents before defineLeptonPreselection {self.df.Count().GetValue()}")
        self.df = self.df.Filter(f"passed_singleIsoMu || passed_singleEleWpTight")
        print(f"Checking nEvents after defineLeptonPreselection {self.df.Count().GetValue()}")


        muMu_df = self.df.Filter("(channelId == 22)")
        eMu_df = self.df.Filter("(channelId == 12)")
        muE_df = self.df.Filter("(channelId == 21)")
        eE_df = self.df.Filter("(channelId == 11)")
        print(f"Checking nEvents muMu {muMu_df.Count().GetValue()}")
        print(f"Checking nEvents eMu {eMu_df.Count().GetValue()}")
        print(f"Checking nEvents muE {muE_df.Count().GetValue()}")
        print(f"Checking nEvents eE {eE_df.Count().GetValue()}")

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
    for met_var in ['DeepMETResolutionTune', 'DeepMETResponseTune', 'PuppiMET', 'met']:
        df = df.Define(f"{met_var}_p4", f"ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>({met_var}_pt,0.,{met_var}_phi,0.)")


    df = df.Define('HT', f"Sum(centralJet_pt)")

    df = df.Define('dR_dilep', f"ROOT::Math::VectorUtil::DeltaR(lep1_p4, lep2_p4)")
    df = df.Define('dR_dibjet', f"ROOT::Math::VectorUtil::DeltaR(centralJet_p4[0], centralJet_p4[1])")
    df = df.Define('dR_dilep_dibjet', f"ROOT::Math::VectorUtil::DeltaR((lep1_p4+lep2_p4), (centralJet_p4[0]+centralJet_p4[1]))")
    df = df.Define('dR_dilep_dijet', f"(centralJet_pt.size() >= 4) ? ROOT::Math::VectorUtil::DeltaR((lep1_p4+lep2_p4), (centralJet_p4[2]+centralJet_p4[3])) : -100.")
    df = df.Define('dPhi_lep1_lep2', f"ROOT::Math::VectorUtil::DeltaPhi(lep1_p4,lep2_p4)")
    df = df.Define('dPhi_jet1_jet2', f"ROOT::Math::VectorUtil::DeltaPhi(centralJet_p4[0],centralJet_p4[1])")
    df = df.Define('dPhi_MET_dilep', f"ROOT::Math::VectorUtil::DeltaPhi(met_p4,(lep1_p4+lep2_p4))")
    df = df.Define('dPhi_MET_dibjet', f"ROOT::Math::VectorUtil::DeltaPhi(met_p4,(centralJet_p4[0]+centralJet_p4[1]))")
    df = df.Define('min_dR_lep0_jets', f"MinDeltaR(lep1_p4, centralJet_p4)")
    df = df.Define('min_dR_lep1_jets', f"MinDeltaR(lep2_p4, centralJet_p4)")

    df = df.Define('MT', f"(lep1_type > 0 && lep2_type > 0) ? Calculate_TotalMT(lep1_p4, lep2_p4, met_p4) : -100.")
    df = df.Define('MT2', f'(lep1_type > 0 && lep2_type > 0) ? float(analysis::Calculate_MT2(lep1_p4, lep2_p4, centralJet_p4[0], centralJet_p4[1], met_p4)) : -100.')
    df = df.Define('MT2_bbWW', f'(lep1_type > 0 && lep2_type > 0) ? float(analysis::Calculate_MT2_bbWW(lep1_p4, lep2_p4, centralJet_p4[0], centralJet_p4[1], met_p4)) : -100.')

    #Functional form of MT2 claculation
    df = df.Define('MT2_ll', f'(lep1_type > 0 && lep2_type > 0) ? float(analysis::Calculate_MT2_func(lep1_p4, lep2_p4, centralJet_p4[0] + centralJet_p4[1] + met_p4, centralJet_p4[0].mass(), centralJet_p4[1].mass())) : -100.')
    df = df.Define('MT2_bb', f'(lep1_type > 0 && lep2_type > 0) ? float(analysis::Calculate_MT2_func(centralJet_p4[0], centralJet_p4[1], lep1_p4 + lep2_p4 + met_p4, 80.4, 80.4)) : -100.')
    df = df.Define('MT2_blbl', f'(lep1_type > 0 && lep2_type > 0) ? float(analysis::Calculate_MT2_func(lep1_p4 + centralJet_p4[1], lep2_p4 + centralJet_p4[1], met_p4, 0.0, 0.0)) : -100.')

    df = df.Define('CosTheta_bb', f'(centralJet_pt.size() > 1) ? analysis::CosDTheta(centralJet_p4[0], centralJet_p4[1]) : -1.0')

    df = df.Define(f"ll_mass","(lep1_type > 0 && lep2_type > 0) ? (lep1_p4+lep2_p4).mass() : -1.0") 

    df = df.Define(f"bb_mass","centralJet_pt.size() > 1 ? (centralJet_p4[0]+centralJet_p4[1]).mass() : -1.0") 

    df = df.Define(f"diLep_p4", f"(lep1_p4+lep2_p4)")
    df = df.Define(f"Lep1Lep2Jet1Jet2_p4", f"(centralJet_pt.size() >= 2) ? (lep1_p4+lep2_p4+centralJet_p4[0]+centralJet_p4[1]) : LorentzVectorM()")
    df = df.Define(f"Lep1Jet1Jet2_p4", f"(centralJet_pt.size() >= 2) ? (lep1_p4+centralJet_p4[0]+centralJet_p4[1]) : LorentzVectorM()")
    return df




# We want to remove this DNN For Training -- Strategy will be to first create a base 'anaTuple' level for training
# Then close the file, open again and apply the 'Cache' level variables -- This removes _entry and removes the define/redefine issue

# Safe Define/Redefine is needed for the DNN Training file maker -- Since we fill by _entry we need something smart for the filling
def DFSafeDefine(df, branch_name, cut, expression, init_value):
    if branch_name in df.GetColumnNames():
        df = df.Redefine(branch_name, f"{cut} ? {expression} : {branch_name}")
    else:
        df = df.Define(branch_name, f"{cut} ? {expression} : {init_value}")
    return df

def AddDNNVariablesForTraining(df):
    df = df.Define(f"SelectedFatJet_idx", f"CreateIndexes(SelectedFatJet_pt.size())")
    df = df.Define(f"SelectedFatJet_p4", f"GetP4(SelectedFatJet_pt, SelectedFatJet_eta, SelectedFatJet_phi, SelectedFatJet_mass, SelectedFatJet_idx)")
    for idx in [0,1]:
        df = Utilities.defineP4(df, f"lep{idx+1}")
    df = df.Define(f"centralJet_p4", f"GetP4(centralJet_pt, centralJet_eta, centralJet_phi, centralJet_mass)")
    for met_var in ['DeepMETResolutionTune', 'DeepMETResponseTune', 'PuppiMET', 'met']:
        df = df.Define(f"{met_var}_p4", f"ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>({met_var}_pt,0.,{met_var}_phi,0.)")

    df = DFSafeDefine(df, "HT", "_entry", "Sum(centralJet_pt)", "-100.")

    df = DFSafeDefine(df, "dR_dilep", "_entry", "ROOT::Math::VectorUtil::DeltaR(lep1_p4, lep2_p4)", "-100.")
    df = DFSafeDefine(df, "dR_dibjet", "_entry", "ROOT::Math::VectorUtil::DeltaR(centralJet_p4[0], centralJet_p4[1])", "-100.")
    df = DFSafeDefine(df, "dR_dilep_dibjet", "_entry", "ROOT::Math::VectorUtil::DeltaR((lep1_p4+lep2_p4), (centralJet_p4[0]+centralJet_p4[1]))", "-100.")
    df = DFSafeDefine(df, "dR_dilep_dijet", "_entry && (centralJet_pt.size() >= 4)", "ROOT::Math::VectorUtil::DeltaR((lep1_p4+lep2_p4), (centralJet_p4[2]+centralJet_p4[3]))", "-100.")
    df = DFSafeDefine(df, "dPhi_lep1_lep2", "_entry", "ROOT::Math::VectorUtil::DeltaPhi(lep1_p4,lep2_p4)", "-100.")
    df = DFSafeDefine(df, "dPhi_jet1_jet2", "_entry", "ROOT::Math::VectorUtil::DeltaPhi(centralJet_p4[0],centralJet_p4[1])", "-100.")
    df = DFSafeDefine(df, "dPhi_MET_dilep", "_entry", "ROOT::Math::VectorUtil::DeltaPhi(met_p4,(lep1_p4+lep2_p4))", "-100.")
    df = DFSafeDefine(df, "dPhi_MET_dibjet", "_entry", "ROOT::Math::VectorUtil::DeltaPhi(met_p4,(centralJet_p4[0]+centralJet_p4[1]))", "-100.")
    df = DFSafeDefine(df, "min_dR_lep0_jets", "_entry", "MinDeltaR(lep1_p4, centralJet_p4)", "-100.")
    df = DFSafeDefine(df, "min_dR_lep1_jets", "_entry", "MinDeltaR(lep2_p4, centralJet_p4)", "-100.")

    df = DFSafeDefine(df, "MT", "_entry && (lep1_type > 0 && lep2_type > 0)", "Calculate_TotalMT(lep1_p4, lep2_p4, met_p4)", "-100.")
    df = DFSafeDefine(df, "MT2", "_entry && (lep1_type > 0 && lep2_type > 0)", "float(analysis::Calculate_MT2(lep1_p4, lep2_p4, centralJet_p4[0], centralJet_p4[1], met_p4))", "-100.")
    df = DFSafeDefine(df, "MT2_bbWW", "_entry && (lep1_type > 0 && lep2_type > 0)", "float(analysis::Calculate_MT2_bbWW(lep1_p4, lep2_p4, centralJet_p4[0], centralJet_p4[1], met_p4))", "-100.")

    df = DFSafeDefine(df, "MT2_ll", "_entry && (lep1_type > 0 && lep2_type > 0)", "float(analysis::Calculate_MT2_func(lep1_p4, lep2_p4, centralJet_p4[0] + centralJet_p4[1] + met_p4, centralJet_p4[0].mass(), centralJet_p4[1].mass()))", "-100.")
    df = DFSafeDefine(df, "MT2_bb", "_entry && (lep1_type > 0 && lep2_type > 0)", "float(analysis::Calculate_MT2_func(centralJet_p4[0], centralJet_p4[1], lep1_p4 + lep2_p4 + met_p4, 80.4, 80.4))", "-100.")
    df = DFSafeDefine(df, "MT2_blbl", "_entry && (lep1_type > 0 && lep2_type > 0)", "float(analysis::Calculate_MT2_func(lep1_p4 + centralJet_p4[1], lep2_p4 + centralJet_p4[1], met_p4, 0.0, 0.0))", "-100.")

    df = DFSafeDefine(df, "CosTheta_bb", "_entry && (centralJet_pt.size() > 1)", "analysis::CosDTheta(centralJet_p4[0], centralJet_p4[1])", "-100.")

    df = DFSafeDefine(df, "ll_mass", "_entry && (lep1_type > 0 && lep2_type > 0)", "(lep1_p4+lep2_p4).mass()", "-100.")
    df = DFSafeDefine(df, "bb_mass", "_entry && (centralJet_pt.size() > 1)", "(centralJet_p4[0]+centralJet_p4[1]).mass()", "-100.")

    df = DFSafeDefine(df, "diLep_p4", "_entry", "(lep1_p4+lep2_p4)", "LorentzVectorM()")
    df = DFSafeDefine(df, "Lep1Lep2Jet1Jet2_p4", "_entry && (centralJet_pt.size() >= 2)", "(lep1_p4+lep2_p4+centralJet_p4[0]+centralJet_p4[1])", "LorentzVectorM()")
    df = DFSafeDefine(df, "Lep1Jet1Jet2_p4", "_entry && (centralJet_pt.size() >= 2)", "(lep1_p4+centralJet_p4[0]+centralJet_p4[1])", "LorentzVectorM()")

    return df


#In application, there is no _entry var
def AddDNNVariablesForApplication(df):
    df = df.Define(f"SelectedFatJet_idx", f"CreateIndexes(SelectedFatJet_pt.size())")
    df = df.Define(f"SelectedFatJet_p4", f"GetP4(SelectedFatJet_pt, SelectedFatJet_eta, SelectedFatJet_phi, SelectedFatJet_mass, SelectedFatJet_idx)")
    for idx in [0,1]:
        df = Utilities.defineP4(df, f"lep{idx+1}")
    df = df.Define(f"centralJet_p4", f"GetP4(centralJet_pt, centralJet_eta, centralJet_phi, centralJet_mass)")
    for met_var in ['DeepMETResolutionTune', 'DeepMETResponseTune', 'PuppiMET', 'met']:
        df = df.Define(f"{met_var}_p4", f"ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>({met_var}_pt,0.,{met_var}_phi,0.)")

    df = DFSafeDefine(df, "HT", "true", "Sum(centralJet_pt)", "-100.")

    df = DFSafeDefine(df, "dR_dilep", "true", "ROOT::Math::VectorUtil::DeltaR(lep1_p4, lep2_p4)", "-100.")
    df = DFSafeDefine(df, "dR_dibjet", "true", "ROOT::Math::VectorUtil::DeltaR(centralJet_p4[0], centralJet_p4[1])", "-100.")
    df = DFSafeDefine(df, "dR_dilep_dibjet", "true", "ROOT::Math::VectorUtil::DeltaR((lep1_p4+lep2_p4), (centralJet_p4[0]+centralJet_p4[1]))", "-100.")
    df = DFSafeDefine(df, "dR_dilep_dijet", "(centralJet_pt.size() >= 4)", "ROOT::Math::VectorUtil::DeltaR((lep1_p4+lep2_p4), (centralJet_p4[2]+centralJet_p4[3]))", "-100.")
    df = DFSafeDefine(df, "dPhi_lep1_lep2", "true", "ROOT::Math::VectorUtil::DeltaPhi(lep1_p4,lep2_p4)", "-100.")
    df = DFSafeDefine(df, "dPhi_jet1_jet2", "true", "ROOT::Math::VectorUtil::DeltaPhi(centralJet_p4[0],centralJet_p4[1])", "-100.")
    df = DFSafeDefine(df, "dPhi_MET_dilep", "true", "ROOT::Math::VectorUtil::DeltaPhi(met_p4,(lep1_p4+lep2_p4))", "-100.")
    df = DFSafeDefine(df, "dPhi_MET_dibjet", "true", "ROOT::Math::VectorUtil::DeltaPhi(met_p4,(centralJet_p4[0]+centralJet_p4[1]))", "-100.")
    df = DFSafeDefine(df, "min_dR_lep0_jets", "true", "MinDeltaR(lep1_p4, centralJet_p4)", "-100.")
    df = DFSafeDefine(df, "min_dR_lep1_jets", "true", "MinDeltaR(lep2_p4, centralJet_p4)", "-100.")

    df = DFSafeDefine(df, "MT", "(lep1_type > 0 && lep2_type > 0)", "Calculate_TotalMT(lep1_p4, lep2_p4, met_p4)", "-100.")
    df = DFSafeDefine(df, "MT2", "(lep1_type > 0 && lep2_type > 0)", "float(analysis::Calculate_MT2(lep1_p4, lep2_p4, centralJet_p4[0], centralJet_p4[1], met_p4))", "-100.")
    df = DFSafeDefine(df, "MT2_bbWW", "(lep1_type > 0 && lep2_type > 0)", "float(analysis::Calculate_MT2_bbWW(lep1_p4, lep2_p4, centralJet_p4[0], centralJet_p4[1], met_p4))", "-100.")

    df = DFSafeDefine(df, "MT2_ll", "(lep1_type > 0 && lep2_type > 0)", "float(analysis::Calculate_MT2_func(lep1_p4, lep2_p4, centralJet_p4[0] + centralJet_p4[1] + met_p4, centralJet_p4[0].mass(), centralJet_p4[1].mass()))", "-100.")
    df = DFSafeDefine(df, "MT2_bb", "(lep1_type > 0 && lep2_type > 0)", "float(analysis::Calculate_MT2_func(centralJet_p4[0], centralJet_p4[1], lep1_p4 + lep2_p4 + met_p4, 80.4, 80.4))", "-100.")
    df = DFSafeDefine(df, "MT2_blbl", "(lep1_type > 0 && lep2_type > 0)", "float(analysis::Calculate_MT2_func(lep1_p4 + centralJet_p4[1], lep2_p4 + centralJet_p4[1], met_p4, 0.0, 0.0))", "-100.")

    df = DFSafeDefine(df, "CosTheta_bb", "(centralJet_pt.size() > 1)", "analysis::Calculate_CosDTheta(centralJet_p4[0], centralJet_p4[1])", "-100.")

    df = DFSafeDefine(df, "ll_mass", "(lep1_type > 0 && lep2_type > 0)", "(lep1_p4+lep2_p4).mass()", "-100.")
    df = DFSafeDefine(df, "bb_mass", "(centralJet_pt.size() > 1)", "(centralJet_p4[0]+centralJet_p4[1]).mass()", "-100.")

    df = DFSafeDefine(df, "diLep_p4", "true", "(lep1_p4+lep2_p4)", "LorentzVectorM()")
    df = DFSafeDefine(df, "Lep1Lep2Jet1Jet2_p4", "(centralJet_pt.size() >= 2)", "(lep1_p4+lep2_p4+centralJet_p4[0]+centralJet_p4[1])", "LorentzVectorM()")
    df = DFSafeDefine(df, "Lep1Jet1Jet2_p4", "(centralJet_pt.size() >= 2)", "(lep1_p4+centralJet_p4[0]+centralJet_p4[1])", "LorentzVectorM()")

    return df



def RedefineAllP4_DNNBatchMaker(df):
    df = df.Define(f"SelectedFatJet_idx", f"CreateIndexes(SelectedFatJet_pt.size())")
    df = df.Define(f"SelectedFatJet_p4", f"GetP4(SelectedFatJet_pt, SelectedFatJet_eta, SelectedFatJet_phi, SelectedFatJet_mass, SelectedFatJet_idx)")
    for idx in [0,1]:
        df = Utilities.defineP4(df, f"lep{idx+1}")
    df = df.Define(f"centralJet_p4", f"GetP4(centralJet_pt, centralJet_eta, centralJet_phi, centralJet_mass)")
    for met_var in ['DeepMETResolutionTune', 'DeepMETResponseTune', 'PuppiMET', 'met']:
        df = df.Define(f"{met_var}_p4", f"ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>({met_var}_pt,0.,{met_var}_phi,0.)")


    df = df.Redefine('HT', f"_entry ? Sum(centralJet_pt) : HT")

    df = df.Redefine('dR_dilep', f"_entry ? ROOT::Math::VectorUtil::DeltaR(lep1_p4, lep2_p4) : dR_dilep")
    df = df.Redefine('dR_dibjet', f"_entry ? ROOT::Math::VectorUtil::DeltaR(centralJet_p4[0], centralJet_p4[1]) : dR_dibjet")
    df = df.Redefine('dR_dilep_dibjet', f"_entry ? ROOT::Math::VectorUtil::DeltaR((lep1_p4+lep2_p4), (centralJet_p4[0]+centralJet_p4[1])) : dR_dilep_dijet")
    df = df.Redefine('dR_dilep_dijet', f"_entry && (centralJet_pt.size() >= 4) ? ROOT::Math::VectorUtil::DeltaR((lep1_p4+lep2_p4), (centralJet_p4[2]+centralJet_p4[3])) : dR_dilep_dibjet")
    df = df.Redefine('dPhi_lep1_lep2', f"_entry ? ROOT::Math::VectorUtil::DeltaPhi(lep1_p4,lep2_p4) : dPhi_lep1_lep2")
    df = df.Redefine('dPhi_jet1_jet2', f"_entry ? ROOT::Math::VectorUtil::DeltaPhi(centralJet_p4[0],centralJet_p4[1]) : dPhi_jet1_jet2")
    df = df.Redefine('dPhi_MET_dilep', f"_entry ? ROOT::Math::VectorUtil::DeltaPhi(met_p4,(lep1_p4+lep2_p4)) : dPhi_MET_dilep")
    df = df.Redefine('dPhi_MET_dibjet', f"_entry ? ROOT::Math::VectorUtil::DeltaPhi(met_p4,(centralJet_p4[0]+centralJet_p4[1])) : dPhi_MET_dibjet")
    df = df.Redefine('min_dR_lep0_jets', f"_entry ? MinDeltaR(lep1_p4, centralJet_p4) : min_dR_lep0_jets")
    df = df.Redefine('min_dR_lep1_jets', f"_entry ? MinDeltaR(lep2_p4, centralJet_p4) : min_dR_lep1_jets")

    df = df.Redefine('MT', f"_entry && (lep1_type > 0 && lep2_type > 0) ? Calculate_TotalMT(lep1_p4, lep2_p4, met_p4) : MT")
    df = df.Redefine('MT2', f'_entry && (lep1_type > 0 && lep2_type > 0) ? float(analysis::Calculate_MT2(lep1_p4, lep2_p4, centralJet_p4[0], centralJet_p4[1], met_p4)) : MT2')
    df = df.Redefine('MT2_bbWW', f'_entry && (lep1_type > 0 && lep2_type > 0) ? float(analysis::Calculate_MT2_bbWW(lep1_p4, lep2_p4, centralJet_p4[0], centralJet_p4[1], met_p4)) : MT2_bbWW')

    #Functional form of MT2 claculation
    df = df.Redefine('MT2_ll', f'_entry && (lep1_type > 0 && lep2_type > 0) ? float(analysis::Calculate_MT2_func(lep1_p4, lep2_p4, centralJet_p4[0] + centralJet_p4[1] + met_p4, centralJet_p4[0].mass(), centralJet_p4[1].mass())) : MT2_ll')
    df = df.Redefine('MT2_bb', f'_entry && (lep1_type > 0 && lep2_type > 0) ? float(analysis::Calculate_MT2_func(centralJet_p4[0], centralJet_p4[1], lep1_p4 + lep2_p4 + met_p4, 80.4, 80.4)) : MT2_bb')
    df = df.Redefine('MT2_blbl', f'_entry && (lep1_type > 0 && lep2_type > 0) ? float(analysis::Calculate_MT2_func(lep1_p4 + centralJet_p4[1], lep2_p4 + centralJet_p4[1], met_p4, 0.0, 0.0)) : MT2_blbl')

    df = df.Redefine('CosTheta_bb', f'_entry && (centralJet_pt.size() > 1) ? analysis::Calculate_CosDTheta(centralJet_p4[0], centralJet_p4[1]) : CosTheta_bb')

    df = df.Redefine(f"ll_mass","_entry && (lep1_type > 0 && lep2_type > 0) ? (lep1_p4+lep2_p4).mass() : ll_mass") 

    df = df.Redefine(f"bb_mass","centralJet_pt.size() > 1 ? (centralJet_p4[0]+centralJet_p4[1]).mass() : bb_mass") 

    return df


def PrepareDfForDNN(dfForHistograms):
    dfForHistograms.df = AddDNNVariablesForApplication(dfForHistograms.df)
    #dfForHistograms.defineBoostedVariables()
    return dfForHistograms


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
