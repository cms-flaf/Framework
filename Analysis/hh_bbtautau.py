import ROOT
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

from Analysis.HistHelper import *

from Common.Utilities import *
# import Analysis.GetTauTauWeights as CrossTauTauWeights
import Analysis.GetCrossWeights as CrossWeights
uncReNames = {
    "bTagShape_lf":"CMS_btag_LF",
    "bTagShape_hf":"CMS_btag_HF",
    "bTagShape_lfstats1":"CMS_btag_lfstats1_{}",
    "bTagShape_lfstats2":"CMS_btag_lfstats2_{}",
    "bTagShape_hfstats1":"CMS_btag_hfstats1_{}",
    "bTagShape_hfstats2":"CMS_btag_hfstats2_{}",
    "bTagShape_cferr1":"CMS_btag_cferr1",
    "bTagShape_cferr2":"CMS_btag_cferr2",
    "pNet_SF":"CMS_pnet_{}", # correlated??
    "PileUp_Lumi_MC":"CMS_pu_lumi_MC_{}",
    "PUJetID":"CMS_eff_j_PUJET_id_{}",
    # "PileUp_Lumi_MC":"CMS_pileup_{}",
    "L1Prefiring":"CMS_l1_prefiring_{}",
    "L1Prefiring_ECAL":"CMS_l1_ecal_prefiring_{}",
    "L1Prefiring_Muon_Stat":"CMS_l1_muon_stat_prefiring_{}",
    "L1Prefiring_Muon_Syst":"CMS_l1_muon_syst_prefiring_{}",
    "TauID_total":"CMS_eff_t_id_total_{}", # da capire
    "TauID_stat1_DM0":"CMS_eff_t_id_stat1_DM0_{}",
    "TauID_stat1_DM10":"CMS_eff_t_id_stat1_DM10_{}",
    "TauID_stat1_DM11":"CMS_eff_t_id_stat1_DM11_{}",
    "TauID_stat1_DM1":"CMS_eff_t_id_stat1_DM1_{}",
    "TauID_stat2_DM0":"CMS_eff_t_id_stat2_DM0_{}",
    "TauID_stat2_DM10":"CMS_eff_t_id_stat2_DM10_{}",
    "TauID_stat2_DM11":"CMS_eff_t_id_stat2_DM11_{}",
    "TauID_stat2_DM1":"CMS_eff_t_id_stat2_DM1_{}",
    "TauID_syst_alleras":"CMS_eff_t_id_syst_alleras",
    "TauID_syst_year":"CMS_eff_t_id_syst_{}",
    "TauID_syst_year_DM0":"CMS_eff_t_id_syst_{}_DM0",
    "TauID_syst_year_DM10":"CMS_eff_t_id_syst_{}_DM10",
    "TauID_syst_year_DM11":"CMS_eff_t_id_syst_{}_DM11",
    "TauID_syst_year_DM1":"CMS_eff_t_id_syst_{}_DM1",
    "TauID_genuineElectron_barrel":"CMS_eff_t_id_etauFR_barrel_{}",
    "TauID_genuineElectron_endcaps":"CMS_eff_t_id_etauFR_endcaps_{}",
    "TauID_genuineMuon_eta0p4to0p8":"CMS_eff_t_id_mutauFR_eta0p4to0p8_{}",
    "TauID_genuineMuon_eta0p8to1p2":"CMS_eff_t_id_mutauFR_eta0p8to1p2_{}",
    "TauID_genuineMuon_eta1p2to1p7":"CMS_eff_t_id_mutauFR_eta1p2to1p7_{}",
    "TauID_genuineMuon_etaGt1p7":"CMS_eff_t_id_mutauFR_etaGt1p7_{}",
    "TauID_genuineMuon_etaLt0p4":"CMS_eff_t_id_mutauFR_etaLt0p4_{}",
    "TauID_SF_stat_highpT_bin1":"CMS_eff_t_id_stat_highpT_bin1_{}",
    "TauID_SF_stat_highpT_bin2":"CMS_eff_t_id_stat_highpT_bin2_{}",
    "TauID_SF_syst_highpT_bin1":"CMS_eff_t_id_syst_highpT_bin1",
    "TauID_SF_syst_highpT_bin2":"CMS_eff_t_id_syst_highpT_bin2",
    "TauID_SF_syst_highpT_extrap":"CMS_eff_t_id_syst_highpT_extrap",
    "EleID_Iso":"CMS_eff_e_{}",
    "EleIS_noIso":"CMS_eff_e_noiso_{}",
    "MuID_Reco":"CMS_eff_m_id_reco_{}",
    "MuID_Reco_highPt":"CMS_eff_m_id_reco_highpt_{}",
    "MuID_TightID":"CMS_eff_m_id_{}",
    "MuID_TightID_highPt":"CMS_eff_m_id_highpt_{}",
    "MuID_TightRelIso":"CMS_eff_m_id_iso_{}",
    "HLT_MET":"CMS_bbtt_trig_MET_{}",
    "HLT_ditau_DM0":"CMS_bbtt_trig_diTau_DM0_{}",
    "HLT_ditau_DM1":"CMS_bbtt_trig_diTau_DM1_{}",
    "HLT_ditau_3Prong":"CMS_bbtt_trig_diTau_3Prong_{}",
    "HLT_singleTau":"CMS_bbtt_trig_singleTau_{}",
    "trigSF_ele":"CMS_bbtt_trig_ele_{}", # SL+xL electron trigger SF for eTau channel
    "trigSF_SL_ele": "CMS_bbtt_trig_SL_ele_{}",
    "trigSF_cross_ele": "CMS_bbtt_trig_cross_ele_{}",
    "trigSF_mu":"CMS_bbtt_trig_mu_{}", # SL+xL muon trigger SF for muTau channel
    "trigSF_SL_mu": "CMS_bbtt_trig_SL_mu_{}",
    "trigSF_cross_mu": "CMS_bbtt_trig_cross_mu_{}",
    "trigSF_tau":"CMS_bbtt_trig_tau_{}", # for all channels and already integrated the DM dependence
    "TauES_DM0": "CMS_scale_t_DM0_{}",
    "TauES_DM1": "CMS_scale_t_DM1_{}",
    "TauES_3prong": "CMS_scale_t_3prong_{}",
    "EleFakingTauES_DM0": "CMS_scale_t_eFake_DM0_{}",
    "EleFakingTauES_DM1": "CMS_scale_t_eFake_DM1_{}",
    "MuFakingTauES": "CMS_scale_t_muFake_{}",
    "JER": "CMS_res_j_{}",
    "JES_Absolute": "CMS_scale_j_Abs",
    "JES_Absolute_2018": "CMS_scale_j_Abs_2018",
    "JES_Absolute_2017": "CMS_scale_j_Abs_2017",
    "JES_Absolute_2016preVFP": "CMS_scale_j_Abs_2016_HIPM" ,
    "JES_Absolute_2016postVFP": "CMS_scale_j_Abs_2016" ,
    "JES_BBEC1": "CMS_scale_j_BBEC1",
    "JES_BBEC1_2018": "CMS_scale_j_BBEC1_2018",
    "JES_BBEC1_2017": "CMS_scale_j_BBEC1_2017",
    "JES_BBEC1_2016preVFP": "CMS_scale_j_BBEC1_2016_HIPM" ,
    "JES_BBEC1_2016postVFP": "CMS_scale_j_BBEC1_2016" ,
    "JES_EC2": "CMS_scale_j_EC2",
    "JES_EC2_2018": "CMS_scale_j_EC2_2018",
    "JES_EC2_2017": "CMS_scale_j_EC2_2017",
    "JES_EC2_2016preVFP": "CMS_scale_j_EC2_2016_HIPM" ,
    "JES_EC2_2016postVFP": "CMS_scale_j_EC2_2016" ,
    "JES_FlavorQCD": "CMS_scale_j_FlavQCD",
    "JES_HF": "CMS_scale_j_HF",
    "JES_HF_2018": "CMS_scale_j_HF_2018",
    "JES_HF_2017": "CMS_scale_j_HF_2017",
    "JES_HF_2016preVFP": "CMS_scale_j_HF_2016_HIPM" ,
    "JES_HF_2016postVFP": "CMS_scale_j_HF_2016" ,
    "JES_RelativeBal": "CMS_scale_j_RelBal",
    "JES_RelativeSample_2018": "CMS_scale_j_RelSample_2018",
    "JES_RelativeSample_2017": "CMS_scale_j_RelSample_2017",
    "JES_RelativeSample_2016preVFP": "CMS_scale_j_RelSample_2016_HIPM" ,
    "JES_RelativeSample_2016postVFP": "CMS_scale_j_RelSample_2016" ,
    "QCDScale":"CMS_scale_qcd_{}",
    "QCDNorm":"CMS_norm_qcd_{}",

}

processes= [
    "GluGluToRadionToHHTo2B2Tau_M-250","GluGluToRadionToHHTo2B2Tau_M-260","GluGluToRadionToHHTo2B2Tau_M-270","GluGluToRadionToHHTo2B2Tau_M-280","GluGluToRadionToHHTo2B2Tau_M-300","GluGluToRadionToHHTo2B2Tau_M-320","GluGluToRadionToHHTo2B2Tau_M-350","GluGluToRadionToHHTo2B2Tau_M-400","GluGluToRadionToHHTo2B2Tau_M-450","GluGluToRadionToHHTo2B2Tau_M-500","GluGluToRadionToHHTo2B2Tau_M-550","GluGluToRadionToHHTo2B2Tau_M-600","GluGluToRadionToHHTo2B2Tau_M-650","GluGluToRadionToHHTo2B2Tau_M-700","GluGluToRadionToHHTo2B2Tau_M-750","GluGluToRadionToHHTo2B2Tau_M-800","GluGluToRadionToHHTo2B2Tau_M-850","GluGluToRadionToHHTo2B2Tau_M-900","GluGluToRadionToHHTo2B2Tau_M-1000","GluGluToRadionToHHTo2B2Tau_M-1250","GluGluToRadionToHHTo2B2Tau_M-1500","GluGluToRadionToHHTo2B2Tau_M-1750","GluGluToRadionToHHTo2B2Tau_M-2000","GluGluToRadionToHHTo2B2Tau_M-2500","GluGluToRadionToHHTo2B2Tau_M-3000","GluGluToBulkGravitonToHHTo2B2Tau_M-250","GluGluToBulkGravitonToHHTo2B2Tau_M-260","GluGluToBulkGravitonToHHTo2B2Tau_M-270","GluGluToBulkGravitonToHHTo2B2Tau_M-280","GluGluToBulkGravitonToHHTo2B2Tau_M-300","GluGluToBulkGravitonToHHTo2B2Tau_M-320","GluGluToBulkGravitonToHHTo2B2Tau_M-350","GluGluToBulkGravitonToHHTo2B2Tau_M-400","GluGluToBulkGravitonToHHTo2B2Tau_M-450","GluGluToBulkGravitonToHHTo2B2Tau_M-500","GluGluToBulkGravitonToHHTo2B2Tau_M-550","GluGluToBulkGravitonToHHTo2B2Tau_M-600","GluGluToBulkGravitonToHHTo2B2Tau_M-650","GluGluToBulkGravitonToHHTo2B2Tau_M-700","GluGluToBulkGravitonToHHTo2B2Tau_M-750","GluGluToBulkGravitonToHHTo2B2Tau_M-800","GluGluToBulkGravitonToHHTo2B2Tau_M-850","GluGluToBulkGravitonToHHTo2B2Tau_M-900","GluGluToBulkGravitonToHHTo2B2Tau_M-1000","GluGluToBulkGravitonToHHTo2B2Tau_M-1250","GluGluToBulkGravitonToHHTo2B2Tau_M-1500","GluGluToBulkGravitonToHHTo2B2Tau_M-1750","GluGluToBulkGravitonToHHTo2B2Tau_M-2000","GluGluToBulkGravitonToHHTo2B2Tau_M-2500","GluGluToBulkGravitonToHHTo2B2Tau_M-3000","data","QCD","W","TT","DY","EWK_ZTo2L","EWK_ZTo2Nu","EWK_WplusToLNu","EWK_WminusToLNu","ggHToZZTo2L2Q","GluGluHToTauTau_M125","VBFHToTauTau_M125","GluGluHToWWTo2L2Nu_M125","VBFHToWWTo2L2Nu_M125","WplusHToTauTau_M125","WminusHToTauTau_M125","ZHToTauTau_M125","ZH_Hbb_Zll","ZH_Hbb_Zqq","HWplusJ_HToWW_M125","HWminusJ_HToWW_M125","HZJ_HToWW_M125","WW","WZ","ZZ","WWW_4F","WWZ_4F","WZZ","ZZZ","ST_t-channel_antitop_4f_InclusiveDecays","ST_t-channel_top_4f_InclusiveDecays","ST_tW_antitop_5f_InclusiveDecays","ST_tW_top_5f_InclusiveDecays","TTWW","TTWH","TTZH","TTZZ","TTWZ","TTTT","TTTW","TTTJ","TTGG","TTGJets","TT4b","ttHTobb_M125","ttHToTauTau_M125"
]

WorkingPointsParticleNet = {
        "Run2_2018":{
            "Loose":0.9172,
            "Medium":0.9734,
            "Tight":0.988
        },
        "Run2_2017":{
            "Loose":0.9105,
            "Medium":0.9714,
            "Tight":0.987
        },
        "Run2_2016":{
            "Loose":0.9137,
            "Medium":0.9735,
            "Tight":0.9883
        },
        "Run2_2016_HIPM":{
            "Loose":0.9088,
            "Medium":0.9737,
            "Tight":0.9883
        },
    }
WorkingPointsDeepFlav = {
        "Run2_2018":{
            "Loose":0.049,
            "Medium":0.2783,
            "Tight":0.71
        },
        "Run2_2017":{
            "Loose":0.0532,
            "Medium":0.304,
            "Tight":0.7476
        },
        "Run2_2016_HIPM":{
            "Loose":0.0508,
            "Medium":0.2598,
            "Tight":0.6502
        },
        "Run2_2016":{
            "Loose":0.048,
            "Medium":0.2489,
            "Tight":0.6377
        },
    }


def createKeyFilterDict(global_cfg_dict, year):
    reg_dict = {}
    filter_str = ""
    channels_to_consider = global_cfg_dict['channels_to_consider']
    qcd_regions_to_consider = global_cfg_dict['QCDRegions']
    categories_to_consider = global_cfg_dict["categories"] + global_cfg_dict["boosted_categories"]
    boosted_categories = global_cfg_dict["boosted_categories"]
    triggers_dict = global_cfg_dict['hist_triggers']
    mass_cut_limits = global_cfg_dict['mass_cut_limits']
    for ch in channels_to_consider:
        triggers = triggers_dict[ch]['default']
        if year in triggers_dict[ch].keys():
            triggers = triggers_dict[ch][year]
        for reg in qcd_regions_to_consider:
            for cat in categories_to_consider:
                filter_base = f" ({ch} && {triggers} && {reg} && {cat})"
                filter_str = f"(" + filter_base
                if cat not in boosted_categories and not (cat.startswith("baseline")):
                    filter_str += "&& (b1_pt>0 && b2_pt>0)"
                filter_str += ")"
                key = (ch, reg, cat)
                reg_dict[key] = filter_str

    return reg_dict


def GetBTagWeight(global_cfg_dict,cat,applyBtag=False):
    btag_weight = "1"
    btagshape_weight = "1"
    if applyBtag:
        if global_cfg_dict['btag_wps'][cat]!='' : btag_weight = f"weight_bTagSF_{btag_wps[cat]}_Central"
    else:
        if cat not in global_cfg_dict['boosted_categories'] and not cat.startswith("baseline"):
            btagshape_weight = "weight_bTagShape_Central"
    return f'{btag_weight}*{btagshape_weight}'


def GetWeight(channel, cat, boosted_categories,wantPrefiring=True):
    weights_to_apply = ["weight_MC_Lumi_pu"]#,"weight_L1PreFiring_ECAL_Central", "weight_L1PreFiring_Muon_Central"]
    trg_weights_dict = {
        'eTau':["weight_trigSF_eTau", "weight_trigSF_singleTau", "weight_trigSF_MET"],
        'muTau':["weight_trigSF_muTau", "weight_trigSF_singleTau", "weight_trigSF_MET"],
        'tauTau':["weight_trigSF_diTau", "weight_trigSF_singleTau", "weight_trigSF_MET"],
        'eE':["weight_trigSF_singleEle"],
        'muMu':["weight_trigSF_singleMu"],
        'eMu':["weight_trigSF_eMu"]
    }
    ID_weights_dict = {
        'eTau': ["weight_tau1_EleSF_wp80iso_EleIDCentral", "weight_tau2_TauID_SF_Medium_Central"], # theorically
        'muTau': ["weight_tau1_HighPt_MuonID_SF_RecoCentral", "weight_tau1_HighPt_MuonID_SF_TightIDCentral", "weight_tau1_MuonID_SF_RecoCentral", "weight_tau1_MuonID_SF_TightID_TrkCentral", "weight_tau1_MuonID_SF_TightRelIsoCentral","weight_tau2_TauID_SF_Medium_Central"],
        'tauTau': ["weight_tau1_TauID_SF_Medium_Central", "weight_tau2_TauID_SF_Medium_Central"],
        'muMu': ["weight_tau1_HighPt_MuonID_SF_RecoCentral", "weight_tau1_HighPt_MuonID_SF_TightIDCentral", "weight_tau1_MuonID_SF_RecoCentral", "weight_tau1_MuonID_SF_TightID_TrkCentral", "weight_tau1_MuonID_SF_TightRelIsoCentral", "weight_tau2_HighPt_MuonID_SF_RecoCentral", "weight_tau2_HighPt_MuonID_SF_TightIDCentral", "weight_tau2_MuonID_SF_RecoCentral", "weight_tau2_MuonID_SF_TightID_TrkCentral", "weight_tau2_MuonID_SF_TightRelIsoCentral"],
        'eMu': ["weight_tau1_EleSF_wp80iso_EleIDCentral","weight_tau2_HighPt_MuonID_SF_RecoCentral", "weight_tau2_HighPt_MuonID_SF_TightIDCentral", "weight_tau2_MuonID_SF_RecoCentral", "weight_tau2_MuonID_SF_TightID_TrkCentral", "weight_tau2_MuonID_SF_TightRelIsoCentral"],
        #'eMu': ["weight_tau1_MuonID_SF_RecoCentral","weight_tau1_HighPt_MuonID_SF_RecoCentral","weight_tau1_MuonID_SF_TightID_TrkCentral","weight_tau1_MuonID_SF_TightRelIsoCentral","weight_tau2_EleSF_wp80iso_EleIDCentral"]
        'eE':["weight_tau1_EleSF_wp80iso_EleIDCentral","weight_tau2_EleSF_wp80noiso_EleIDCentral"]
        }
    if wantPrefiring:
        weights_to_apply.extend(["weight_L1PreFiring_Central"])
    weights_to_apply.extend(ID_weights_dict[channel])
    weights_to_apply.extend(trg_weights_dict[channel])
    if cat not in boosted_categories:
         weights_to_apply.extend(["weight_Jet_PUJetID_Central_b1_2", "weight_Jet_PUJetID_Central_b2_2"])
    else:
        weights_to_apply.extend(["weight_pNet_Central"])
    total_weight = '*'.join(weights_to_apply)
    return total_weight

class DataFrameBuilderForHistograms(DataFrameBuilderBase):

    def defineBoostedVariables(self): # needs p4 def
        FatJetObservables = self.config['FatJetObservables']
        particleNet_MD_JetTagger = "SelectedFatJet_particleNetMD_Xbb/(SelectedFatJet_particleNetMD_QCD + SelectedFatJet_particleNetMD_Xbb)"
        if "SelectedFatJet_particleNetMD_Xbb" not in self.df.GetColumnNames() and "SelectedFatJet_particleNetLegacy_Xbb" in self.df.GetColumnNames():
            particleNet_MD_JetTagger = "SelectedFatJet_particleNetLegacy_Xbb/ (SelectedFatJet_particleNetLegacy_Xbb + SelectedFatJet_particleNetLegacy_QCD)"
        particleNet_HbbvsQCD = 'SelectedFatJet_particleNet_HbbvsQCD' if 'SelectedFatJet_particleNet_HbbvsQCD' in self.df.GetColumnNames() else 'SelectedFatJet_particleNetWithMass_HbbvsQCD'
        self.df = self.df.Define("SelectedFatJet_particleNet_MD_JetTagger", particleNet_MD_JetTagger)
        self.df = self.df.Define("fatJet_presel", f"SelectedFatJet_pt>250")
        self.df = self.df.Define("fatJet_sel"," RemoveOverlaps(SelectedFatJet_p4, fatJet_presel, {tau1_p4, tau2_p4}, 0.8)")

        self.df = self.df.Define("SelectedFatJet_size_boosted","SelectedFatJet_p4[fatJet_sel].size()")
        # def the correct discriminator
        self.df = self.df.Define(f"SelectedFatJet_particleNet_MD_JetTagger_boosted_vec",f"SelectedFatJet_particleNet_MD_JetTagger[fatJet_sel]")
        self.df = self.df.Define("SelectedFatJet_idxUnordered", "CreateIndexes(SelectedFatJet_p4[fatJet_sel].size())")
        self.df = self.df.Define("SelectedFatJet_idxOrdered", f"ReorderObjects(SelectedFatJet_particleNet_MD_JetTagger_boosted_vec, SelectedFatJet_idxUnordered)")
        for fatJetVar in FatJetObservables:
            if f'SelectedFatJet_{fatJetVar}' in self.df.GetColumnNames():
                if f'SelectedFatJet_{fatJetVar}_boosted_vec' not in self.df.GetColumnNames():
                    self.df = self.df.Define(f'SelectedFatJet_{fatJetVar}_boosted_vec',f""" SelectedFatJet_{fatJetVar}[fatJet_sel];""")
                self.df = self.df.Define(f'SelectedFatJet_{fatJetVar}_boosted',f"""
                                    SelectedFatJet_{fatJetVar}_boosted_vec[SelectedFatJet_idxOrdered[0]];
                                   """)

    def defineTriggers(self):
        for ch in self.config['channelSelection']:
            for trg in self.config['triggers'][ch]:
                trg_name = 'HLT_'+trg
                if trg_name not in self.df.GetColumnNames():
                    print(f"{trg_name} not present in colNames")
                    self.df = self.df.Define(trg_name, "1")

    def definePNetSFs(self):
        self.df= self.df.Define("weight_pNet_Central", f"""getSFPNet(SelectedFatJet_p4_boosted.Pt(), "{self.period}", "Central", "{self.pNetWPstring}",{self.whichType})""")
        self.df= self.df.Define("weight_pNet_Up", f"""getSFPNet(SelectedFatJet_p4_boosted.Pt(), "{self.period}", "Up", "{self.pNetWPstring}",{self.whichType})""")
        self.df= self.df.Define("weight_pNet_Up_rel", f"""weight_pNet_Up/weight_pNet_Central""")
        self.df= self.df.Define("weight_pNet_Down", f"""getSFPNet(SelectedFatJet_p4_boosted.Pt(), "{self.period}", "Down", "{self.pNetWPstring}",{self.whichType})""")
        self.df= self.df.Define("weight_pNet_Down_rel", f"""weight_pNet_Down/weight_pNet_Central""")

    def defineApplicationRegions(self):
        for ch in self.config['channels_to_consider']:
            for trg in self.config['triggers'][ch]:
                if f"HLT_{trg}" not in self.df.GetColumnNames():
                    print(f"{trg} not present in colNames")
                    self.df = self.df.Define(trg, "1")
        singleTau_th_dict = self.config['singleTau_th']
        singleMu_th_dict = self.config['singleMu_th']
        singleEle_th_dict = self.config['singleEle_th']
        legacy_region_definition= "( ( eTau && (SingleEle_region  || CrossEleTau_region) ) || ( muTau && (SingleMu_region  || CrossMuTau_region) ) || ( tauTau && ( diTau_region ) ) || ( eE && (SingleEle_region)) || (eMu && ( SingleEle_region || SingleMu_region ) ) || (muMu && (SingleMu_region)) )"
        #legacy_region_definition= "( ( eTau && (SingleEle_region ) ) || ( muTau && (SingleMu_region ) ) || ( tauTau && ( diTau_region ) ) || ( eE && (SingleEle_region)) || (eMu && ( SingleEle_region || SingleMu_region ) ) || (muMu && (SingleMu_region)) )" # if not including xtrgs
        for reg_name, reg_exp in self.config['application_regions'].items():
            self.df = self.df.Define(reg_name, reg_exp.format(tau_th=singleTau_th_dict[self.period], ele_th=singleEle_th_dict[self.period], mu_th=singleMu_th_dict[self.period]))
        self.df = self.df.Define("Legacy_region", legacy_region_definition)

    def defineCRs(self): # needs inv mass def
        SR_mass_limits_bb_boosted = self.config['mass_cut_limits']['bb_m_vis']['boosted']
        SR_mass_limits_bb = self.config['mass_cut_limits']['bb_m_vis']['other']
        SR_mass_limits_tt = self.config['mass_cut_limits']['tautau_m_vis']
        ellypse_limts_A = self.config['ellypse_limits']['A']
        ellypse_limts_B = self.config['ellypse_limits']['B']
        ellypse_limts_C = self.config['ellypse_limits']['C']
        ellypse_limts_D = self.config['ellypse_limits']['D']

        self.df = self.df.Define("SR_tt", f"return (tautau_m_vis > {SR_mass_limits_tt[0]} && tautau_m_vis  < {SR_mass_limits_tt[1]});")
        self.df = self.df.Define("SR_bb", f"(bb_m_vis > {SR_mass_limits_bb[0]} && bb_m_vis < {SR_mass_limits_bb[1]});")
        self.df = self.df.Define("SR_bb_boosted", f"(bb_m_vis_softdrop > {SR_mass_limits_bb_boosted[0]} && bb_m_vis_softdrop < {SR_mass_limits_bb_boosted[1]});")
        self.df = self.df.Define("SR", f" SR_tt &&  SR_bb")
        self.df = self.df.Define("SR_boosted", f" SR_tt &&  SR_bb_boosted")
        self.df = self.df.Define("DYCR", "if(muMu || eE) {return (tautau_m_vis < 100 && tautau_m_vis > 80);} return true;")
        self.df = self.df.Define("DYCR_boosted", "DYCR")
        self.df = self.df.Define("SR_ellyptical", f"(((SVfit_m-{ellypse_limts_A})*(SVfit_m-{ellypse_limts_A})/({ellypse_limts_B}*{ellypse_limts_A})) + ((bb_m_vis-{ellypse_limts_C})*(bb_m_vis-{ellypse_limts_C})/({ellypse_limts_D}*{ellypse_limts_D}))) < 1 ")
        self.df = self.df.Define("SR_ellyptical_boosted", f"(((SVfit_m-{ellypse_limts_A})*(SVfit_m-{ellypse_limts_A})/({ellypse_limts_B}*{ellypse_limts_A})) + ((bb_m_vis_softdrop-{ellypse_limts_C})*(bb_m_vis_softdrop-{ellypse_limts_C})/({ellypse_limts_D}*{ellypse_limts_D}))) < 1 ")
        # self.df = self.df.Define("SR_ellyptical_boosted_tt", "SVfit_m < 152 && SVfit_m > 80 ")
        # self.df = self.df.Define("SR_ellyptical_boosted_bb", "bb_m_vis_softdrop < 160 && bb_m_vis_softdrop > 90 ")
        # self.df = self.df.Define("SR_ellyptical_boosted", "SR_ellyptical_boosted_tt && SR_ellyptical_boosted_bb")

        TTCR_mass_limits_eTau = self.config['TTCR_mass_limits']['eTau']
        TTCR_mass_limits_muTau = self.config['TTCR_mass_limits']['muTau']
        TTCR_mass_limits_tauTau = self.config['TTCR_mass_limits']['tauTau']
        TTCR_mass_limits_muMu = self.config['TTCR_mass_limits']['muMu']
        TTCR_mass_limits_eE = self.config['TTCR_mass_limits']['eE']
        self.df = self.df.Define("TTCR", f"""
                                if(eTau || muTau || tauTau) {{ return !(SR_ellyptical);
                                }};
                                if(muMu) {{return (tautau_m_vis < {TTCR_mass_limits_muMu[0]} || tautau_m_vis > {TTCR_mass_limits_muMu[1]});
                                 }};
                                if(eE) {{return (tautau_m_vis < {TTCR_mass_limits_eE[0]} || tautau_m_vis > {TTCR_mass_limits_eE[1]});
                                 }};
                                 return true;""")
        self.df = self.df.Define("TTCR_boosted", "TTCR")

    def redefinePUJetIDWeights(self):
        for weight in ["weight_Jet_PUJetID_Central_b1","weight_Jet_PUJetID_Central_b2","weight_Jet_PUJetID_effUp_rel_b1","weight_Jet_PUJetID_effUp_rel_b2","weight_Jet_PUJetID_effDown_rel_b1","weight_Jet_PUJetID_effDown_rel_b2"]:
            if weight not in self.df.GetColumnNames(): continue
            self.df = self.df.Define(f"{weight}_2", f"""
                                         if({weight}!=-100)
                                            return static_cast<float>({weight}) ;
                                         return 1.f;""")


    def defineCategories(self): # needs lot of stuff --> at the end
        self.df = self.df.Define("nSelBtag", f"int(b1_btagDeepFlavB >{self.bTagWP}) + int(b2_btagDeepFlavB >{self.bTagWP})")
        for category_to_def in self.config['category_definition'].keys():
            category_name = category_to_def
            self.df = self.df.Define(category_to_def, self.config['category_definition'][category_to_def].format(pNetWP=self.pNetWP, region=self.region))

    def defineChannels(self):
        for channel in self.config['all_channels']:
            ch_value = self.config['channelDefinition'][channel]
            self.df = self.df.Define(f"{channel}", f"channelId=={ch_value}")

    def defineL1PrefiringRelativeWeights(self):
        if "weight_L1PreFiringDown_rel" not in self.df.GetColumnNames():
            self.df = self.df.Define("weight_L1PreFiringDown_rel","weight_L1PreFiring_Down/weight_L1PreFiring_Central")
        if "weight_L1PreFiringUp_rel" not in self.df.GetColumnNames():
            self.df = self.df.Define("weight_L1PreFiringUp_rel","weight_L1PreFiringUp/weight_L1PreFiring_Central")
        if "weight_L1PreFiring_ECALDown_rel" not in self.df.GetColumnNames():
            self.df = self.df.Define("weight_L1PreFiring_ECALDown_rel","weight_L1PreFiring_ECALDown/weight_L1PreFiring_ECAL_Central")
        if "weight_L1PreFiring_Muon_StatUp_rel" not in self.df.GetColumnNames():
            self.df = self.df.Define("weight_L1PreFiring_Muon_StatUp_rel","weight_L1PreFiring_Muon_StatUp/weight_L1PreFiring_Muon_Central")
        if "weight_L1PreFiring_Muon_StatDown_rel" not in self.df.GetColumnNames():
            self.df = self.df.Define("weight_L1PreFiring_Muon_StatDown_rel","weight_L1PreFiring_Muon_StatDown/weight_L1PreFiring_Muon_Central")
        if "weight_L1PreFiring_Muon_SystUp_rel" not in self.df.GetColumnNames():
            self.df = self.df.Define("weight_L1PreFiring_Muon_SystUp_rel","weight_L1PreFiring_Muon_SystUp/weight_L1PreFiring_Muon_Central")
        if "weight_L1PreFiring_Muon_SystDown_rel" not in self.df.GetColumnNames():
            self.df = self.df.Define("weight_L1PreFiring_Muon_SystDown_rel","weight_L1PreFiring_Muon_SystDown/weight_L1PreFiring_Muon_Central")


    def defineLeptonPreselection(self): # needs channel def
        if self.period == 'Run2_2016' or self.period == 'Run2_2016_HIPM':
            self.df = self.df.Define("eleEta2016", "if(eE) {return (abs(tau1_eta) < 2 && abs(tau2_eta)<2); } if(eTau||eMu) {return (abs(tau1_eta) < 2); } return true;")
        else:
            self.df = self.df.Define("eleEta2016", "return true;")
        self.df = self.df.Define("muon1_tightId", "if(muTau || muMu) {return (tau1_Muon_tightId && tau1_Muon_pfRelIso04_all < 0.15); } return true;")
        self.df = self.df.Define("muon2_tightId", "if(muMu || eMu) {return (tau2_Muon_tightId && tau2_Muon_pfRelIso04_all < 0.3);} return true;")
        self.df = self.df.Define("firstele_mvaIso", "if(eMu || eE){return tau1_Electron_mvaIso_WP80==1 && tau1_Electron_pfRelIso03_all < 0.15 ; } return true; ")
        self.df = self.df.Define("tau1_iso_medium", f"if(tauTau) return (tau1_idDeepTau{self.deepTauYear()}{self.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value}); return true;")
        if f"tau1_gen_kind" not in self.df.GetColumnNames():
            self.df=self.df.Define("tau1_gen_kind", "if(isData) return 5; return 0;")
        if f"tau2_gen_kind" not in self.df.GetColumnNames():
            self.df=self.df.Define("tau2_gen_kind", "if(isData) return 5; return 0;")
        self.df = self.df.Define("tau_true", f"""(tau1_gen_kind==5 && tau2_gen_kind==5)""")
        self.df = self.df.Define(f"lepton_preselection", "eleEta2016 && tau1_iso_medium && muon1_tightId && muon2_tightId && firstele_mvaIso")

    def defineQCDRegions(self):
        self.df = self.df.Define("OS", "tau1_charge*tau2_charge < 0")
        self.df = self.df.Define("SS", "!OS")

        self.df = self.df.Define("Iso", f"(((tauTau || eTau || muTau) && (tau2_idDeepTau{self.deepTauYear()}{self.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value} )) || ((muMu||eMu) && (tau2_Muon_pfRelIso04_all < 0.15)) || (eE && tau2_Electron_pfRelIso03_all < 0.15 && tau2_Electron_mvaNoIso_WP80))")

        self.df = self.df.Define("AntiIso", f"(((tauTau || eTau || muTau) && (tau2_idDeepTau{self.deepTauYear()}{self.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.VVVLoose.value} && tau2_idDeepTau{self.deepTauYear()}{self.deepTauVersion}VSjet < {Utilities.WorkingPointsTauVSjet.Medium.value})) || ((muMu||eMu) && (tau2_Muon_pfRelIso04_all >= 0.15 && tau2_Muon_pfRelIso04_all < 0.3) ) || (eE && (tau2_Electron_pfRelIso03_all < 0.3 && tau2_Electron_pfRelIso03_all >= 0.15 && tau2_Electron_mvaNoIso_WP80 )))")

        self.df = self.df.Define("OS_Iso", f"lepton_preselection && OS && Iso")
        self.df = self.df.Define("SS_Iso", f"lepton_preselection && SS && Iso")
        self.df = self.df.Define("OS_AntiIso", f"lepton_preselection && OS && AntiIso")
        self.df = self.df.Define("SS_AntiIso", f"lepton_preselection && SS && AntiIso")

    def deepTauYear(self):
        return self.config['deepTauYears'][self.deepTauVersion]

    def addNewCols(self):
        self.colNames = []
        self.colTypes = []
        colNames = [str(c) for c in self.df.GetColumnNames()] #if 'kinFit_result' not in str(c)]
        cols_to_remove = []
        for colName in colNames:
            col_name_split = colName.split("_")
            if "p4" in col_name_split or "vec" in col_name_split:
                cols_to_remove.append(colName)
        for col_to_remove in cols_to_remove:
            colNames.remove(col_to_remove)
        entryIndexIdx = colNames.index("entryIndex")
        runIdx = colNames.index("run")
        eventIdx = colNames.index("event")
        lumiIdx = colNames.index("luminosityBlock")
        colNames[entryIndexIdx], colNames[0] = colNames[0], colNames[entryIndexIdx]
        colNames[runIdx], colNames[1] = colNames[1], colNames[runIdx]
        colNames[eventIdx], colNames[2] = colNames[2], colNames[eventIdx]
        colNames[lumiIdx], colNames[3] = colNames[3], colNames[lumiIdx]
        self.colNames = colNames
        self.colTypes = [str(self.df.GetColumnType(c)) for c in self.colNames]
        for colName,colType in zip(self.colNames,self.colTypes):
            print(colName,colType)

    def __init__(self, df, config, period, deepTauVersion='v2p1', bTagWPString = "Medium", pNetWPstring="Loose", region="SR",isData=False, isCentral=False, wantTriggerSFErrors=False, whichType=3, wantScales=True):
        super(DataFrameBuilderForHistograms, self).__init__(df)
        self.deepTauVersion = deepTauVersion
        self.config = config
        self.bTagWPString = bTagWPString
        self.pNetWPstring = pNetWPstring
        self.pNetWP = WorkingPointsParticleNet[period][pNetWPstring]
        self.bTagWP = WorkingPointsDeepFlav[period][bTagWPString]
        self.period = period
        self.region = region
        self.isData = isData
        self.whichType = whichType
        self.isCentral = isCentral
        self.wantTriggerSFErrors = wantTriggerSFErrors
        self.wantScales = isCentral and wantScales

def PrepareDfForHistograms(dfForHistograms):
    dfForHistograms.df = defineAllP4(dfForHistograms.df)
    dfForHistograms.defineTriggers()
    dfForHistograms.defineBoostedVariables()
    dfForHistograms.redefinePUJetIDWeights()
    dfForHistograms.df = createInvMass(dfForHistograms.df)
    dfForHistograms.defineChannels()
    dfForHistograms.defineLeptonPreselection()
    dfForHistograms.defineApplicationRegions()
    if not dfForHistograms.isData:
        dfForHistograms.definePNetSFs()
        wantErrors = dfForHistograms.wantTriggerSFErrors and dfForHistograms.isCentral
        # if wantErrors:
        #     print(dfForHistograms.df.Describe())
        if dfForHistograms.deepTauVersion=='v2p5':
            CrossWeights.AddTauTauTriggerWeightsAndErrors(dfForHistograms,wantErrors)
        else:
            CrossWeights.AddTriggerWeightsAndErrors(dfForHistograms,wantErrors)
    dfForHistograms.defineCRs()
    dfForHistograms.defineCategories()
    dfForHistograms.defineQCDRegions()
    return dfForHistograms



def defineAllP4(df):
    df = df.Define(f"SelectedFatJet_idx", f"CreateIndexes(SelectedFatJet_pt.size())")
    df = df.Define(f"SelectedFatJet_p4", f"GetP4(SelectedFatJet_pt, SelectedFatJet_eta, SelectedFatJet_phi, SelectedFatJet_mass, SelectedFatJet_idx)")
    for idx in [0,1]:
        df = Utilities.defineP4(df, f"tau{idx+1}")
        df = Utilities.defineP4(df, f"b{idx+1}")
    for met_var in ['met','metnomu']:
        df = df.Define(f"{met_var}_p4", f"ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>({met_var}_pt,0.,{met_var}_phi,0.)")
        for leg_idx in [0,1]:
            df = df.Define(f"deltaPhi_{met_var}_tau{leg_idx+1}",f"ROOT::Math::VectorUtil::DeltaPhi({met_var}_p4,tau{leg_idx+1}_p4)")
            df = df.Define(f"deltaPhi_{met_var}_b{leg_idx+1}",f"ROOT::Math::VectorUtil::DeltaPhi({met_var}_p4,b{leg_idx+1}_p4)")
    df = df.Define(f"met_nano_p4", f"ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>(met_pt_nano,0.,met_phi_nano,0.)")
    df = df.Define(f"pt_ll", "(tau1_p4+tau2_p4).Pt()")
    df = df.Define(f"pt_bb", "(b1_p4+b2_p4).Pt()")
    return df

