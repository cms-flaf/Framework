import ROOT
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

from Analysis.HistHelper import *
from Common.Utilities import *


def createKeyFilterDict(global_cfg_dict):
    reg_dict = {}
    filter_str = ""
    channels_to_consider = global_cfg_dict['channels_to_consider']
    qcd_regions_to_consider = global_cfg_dict['QCDRegions']
    categories_to_consider = global_cfg_dict["categories"] + global_cfg_dict["boosted_categories"]
    boosted_categories = global_cfg_dict["boosted_categories"]
    triggers = global_cfg_dict['hist_triggers']
    mass_cut_limits = global_cfg_dict['mass_cut_limits']
    for ch in channels_to_consider:
        for reg in qcd_regions_to_consider:
            for cat in categories_to_consider:
                #print(ch, reg, cat, filter_str)
                #print()
                filter_base = f" ({ch} && {triggers[ch]} && {reg} && {cat})"
                filter_str = f"(" + filter_base
                #print(ch, reg, cat, filter_str)
                #print()
                #print(filter_str)
                if cat not in boosted_categories and not (cat.startswith("baseline")):
                    filter_str += "&& (b1_pt>0 && b2_pt>0)"
                filter_str += ")"
                #print(filter_str)
                key = (ch, reg, cat)
                reg_dict[key] = filter_str
                #print(ch, reg, cat, filter_str)
                #print()

    return reg_dict

def QCD_Estimation(histograms, all_samples_list, channel, category, uncName, scale, wantNegativeContributions):
    key_B = ((channel, 'OS_AntiIso', category), (uncName, scale))
    key_C = ((channel, 'SS_Iso', category), (uncName, scale))
    key_D = ((channel, 'SS_AntiIso', category), (uncName, scale))
    hist_data = histograms['data']
    hist_data_B = hist_data[key_B].Clone()
    hist_data_C = hist_data[key_C].Clone()
    hist_data_D = hist_data[key_D].Clone()
    n_data_B = hist_data_B.Integral(0, hist_data_B.GetNbinsX()+1)
    n_data_C = hist_data_C.Integral(0, hist_data_C.GetNbinsX()+1)
    n_data_D = hist_data_D.Integral(0, hist_data_D.GetNbinsX()+1)
    print(f"Initially Yield for data in OS AntiIso region is {key_B} is {n_data_B}")
    print(f"Initially Yield for data in SS Iso region is{key_C} is {n_data_C}")
    print(f"Initially Yield for data in SS AntiIso region is{key_D} is {n_data_D}")
    for sample in all_samples_list:
        if sample=='data' or 'GluGluToBulkGraviton' in sample or 'GluGluToRadion' in sample or 'VBFToBulkGraviton' in sample or 'VBFToRadion' in sample or sample=='QCD':
            ##print(f"sample {sample} is not considered")
            continue
        hist_sample = histograms[sample]
        hist_sample_B = hist_sample[key_B].Clone()
        hist_sample_C = hist_sample[key_C].Clone()
        hist_sample_D = hist_sample[key_D].Clone()
        n_sample_B= hist_sample_B.Integral(0, hist_sample_B.GetNbinsX()+1)
        n_data_B-=n_sample_B
        n_sample_C = hist_sample_C.Integral(0, hist_sample_C.GetNbinsX()+1)
        n_data_C-=n_sample_C
        n_sample_D = hist_sample_D.Integral(0, hist_sample_D.GetNbinsX()+1)
        n_data_D-=n_sample_D
        if n_data_B < 0:
            print(f"Yield for data in OS AntiIso region {key_B} after removing {sample} with yield {n_sample_B} is {n_data_B}")
        if n_data_C < 0:
            print(f"Yield for data in SS Iso region {key_C} after removing {sample} with yield {n_sample_C} is {n_data_C}")
        if n_data_D < 0:
            print(f"Yield for data in SS AntiIso region {key_D} after removing {sample} with yield {n_sample_D} is {n_data_D}")
        hist_data_B.Add(hist_sample_B, -1)
        hist_data_C.Add(hist_sample_C, -1)
    if n_data_C <= 0 or n_data_D <= 0:
        print(f"n_data_C = {n_data_C}")
        print(f"n_data_D = {n_data_D}")

    qcd_norm = n_data_B * n_data_C / n_data_D if n_data_D != 0 else 0
    if qcd_norm<0:
        print(f"transfer factor <0, {category}, {channel}, {uncName}, {scale}")
        return ROOT.TH1D("","",hist_data_B.GetNbinsX(), hist_data_B.GetXaxis().GetBinLowEdge(1), hist_data_B.GetXaxis().GetBinUpEdge(hist_data_B.GetNbinsX())),ROOT.TH1D("","",hist_data_B.GetNbinsX(), hist_data_B.GetXaxis().GetBinLowEdge(1), hist_data_B.GetXaxis().GetBinUpEdge(hist_data_B.GetNbinsX())),ROOT.TH1D("","",hist_data_B.GetNbinsX(), hist_data_B.GetXaxis().GetBinLowEdge(1), hist_data_B.GetXaxis().GetBinUpEdge(hist_data_B.GetNbinsX()))
        #raise  RuntimeError(f"transfer factor <=0 ! {qcd_norm}")
    #hist_data_B.Scale(kappa)
    if n_data_B != 0:
        hist_data_B.Scale(1/n_data_B)
    if n_data_C != 0:
        hist_data_C.Scale(1/n_data_C)

    hist_qcd_Up = hist_data_B.Clone()
    hist_qcd_Up.Scale(qcd_norm)
    hist_qcd_Down = hist_data_C.Clone()
    hist_qcd_Down.Scale(qcd_norm)
    hist_qcd_Central = hist_data_B.Clone()
    hist_qcd_Central.Add(hist_data_C)
    hist_qcd_Central.Scale(1./2.)
    hist_qcd_Central.Scale(qcd_norm)
    if wantNegativeContributions:
        fix_negative_contributions,debug_info,negative_bins_info = FixNegativeContributions(hist_qcd_Central)
        if not fix_negative_contributions:
            #return hist_data_B
            print(debug_info)
            print(negative_bins_info)
            print("Unable to estimate QCD")
            final_hist = ROOT.TH1D("","",hist_qcd_Central.GetNbinsX(), hist_qcd_Central.GetXaxis().GetBinLowEdge(1), hist_qcd_Central.GetXaxis().GetBinUpEdge(hist_qcd_Central.GetNbinsX())),ROOT.TH1D("","",hist_qcd_Central.GetNbinsX(), hist_qcd_Central.GetXaxis().GetBinLowEdge(1), hist_qcd_Central.GetXaxis().GetBinUpEdge(hist_qcd_Central.GetNbinsX()))
            return final_hist,final_hist,final_hist
            #raise RuntimeError("Unable to estimate QCD")
    #if uncName == 'Central':
    #    return hist_qcd_Central,hist_qcd_Up,hist_qcd_Down
    return hist_qcd_Central,hist_qcd_Up,hist_qcd_Down


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

def AddQCDInHistDict(var, all_histograms, channels, categories, uncName, all_samples_list, scales, wantNegativeContributions=False):
    if 'QCD' not in all_histograms.keys():
            all_histograms['QCD'] = {}
    for channel in channels:
        for cat in categories:
            for scale in scales + ['Central']:
                if uncName=='Central' and scale != 'Central': continue
                if uncName!='Central' and scale == 'Central': continue
                key =( (channel, 'OS_Iso', cat), (uncName, scale))
                hist_qcd_Central,hist_qcd_Up,hist_qcd_Down = QCD_Estimation(all_histograms, all_samples_list, channel, cat, uncName, scale,wantNegativeContributions)
                all_histograms['QCD'][key] = hist_qcd_Central
            if uncName=='QCDScale':
                keyQCD_up =( (channel, 'OS_Iso', cat), ('QCDScale', 'Up'))
                keyQCD_down =( (channel, 'OS_Iso', cat), ('QCDScale', 'Down'))
                all_histograms['QCD'][keyQCD_up] = hist_qcd_Up
                all_histograms['QCD'][keyQCD_down] = hist_qcd_Down

def ApplyBTagWeight(global_cfg_dict,cat,applyBtag=False, finalWeight_name = 'final_weight_0'):
    btag_weight = "1"
    btagshape_weight = "1"
    if applyBtag:
        if global_cfg_dict['btag_wps'][cat]!='' : btag_weight = f"weight_bTagSF_{btag_wps[cat]}_Central"
    else:
        if cat not in global_cfg_dict['boosted_categories'] or cat.startswith("baseline"):
            btagshape_weight = "weight_bTagShape_Central"
    return f'{finalWeight_name}*{btag_weight}*{btagshape_weight}'



def GetWeight(channel, cat, boosted_categories):
    weights_to_apply = ["weight_MC_Lumi_pu", "weight_L1PreFiring_Central","weight_L1PreFiring_ECAL_Central","weight_L1PreFiring_Muon_Central"]
    trg_weights_dict = {
        #'eTau':["weight_tau1_TrgSF_singleEle_Central_application","weight_tau2_TrgSF_singleEle_Central_application", "weight_tau1_TrgSF_etau_Central_application", "weight_tau2_TrgSF_etau_Central_application", "weight_tau1_TrgSF_singleTau_Central_application","weight_tau2_TrgSF_singleTau_Central_application", "weight_TrgSF_MET_Central_application"]
        'eTau':["weight_tau1_TrgSF_singleEle_Central_application","weight_tau2_TrgSF_singleEle_Central_application", "weight_tau1_TrgSF_singleTau_Central_application","weight_tau2_TrgSF_singleTau_Central_application", "weight_TrgSF_MET_Central_application"],
        #'muTau':["weight_tau1_TrgSF_singleMu_Central_application","weight_tau2_TrgSF_singleMu_Central_application", "weight_tau1_TrgSF_mutau_Central_application", "weight_tau2_TrgSF_mutau_Central_application", "weight_tau1_TrgSF_singleTau_Central_application","weight_tau2_TrgSF_singleTau_Central_application", "weight_TrgSF_MET_Central_application"]

        'muTau':["weight_tau1_TrgSF_singleMu_Central_application","weight_tau2_TrgSF_singleMu_Central_application", "weight_tau1_TrgSF_singleTau_Central_application","weight_tau2_TrgSF_singleTau_Central_application", "weight_TrgSF_MET_Central_application"],

        'muMu':["weight_tau1_TrgSF_singleMu_Central_application","weight_tau2_TrgSF_singleMu_Central_application"],

        'eE':["weight_tau1_TrgSF_singleEle_Central_application","weight_tau2_TrgSF_singleEle_Central_application"],

        'eMu':["weight_tau1_TrgSF_singleEle_Central_application","weight_tau2_TrgSF_singleMu_Central_application"], # 1st leg ele, 2nd leg mu
        #'eMu':["weight_tau2_TrgSF_singleEle_Central_application","weight_tau1_TrgSF_singleMu_Central_application"],  # 1st leg muon, 2nd leg ele

        'tauTau':["weight_tau1_TrgSF_ditau_Central_application","weight_tau2_TrgSF_ditau_Central_application","weight_tau1_TrgSF_singleTau_Central_application","weight_tau2_TrgSF_singleTau_Central_application", "weight_TrgSF_MET_Central_application"]
        }
    ID_weights_dict = {
        'eTau': ["weight_tau1_EleSF_wp80iso_EleIDCentral", "weight_tau2_TauID_SF_Medium_Central"], # theorically
        'muTau': ["weight_tau1_MuonID_SF_RecoCentral","weight_tau1_HighPt_MuonID_SF_RecoCentral","weight_tau1_MuonID_SF_TightID_TrkCentral","weight_tau1_MuonID_SF_TightRelIsoCentral","weight_tau2_TauID_SF_Medium_Central"],
        'tauTau': ["weight_tau1_TauID_SF_Medium_Central", "weight_tau2_TauID_SF_Medium_Central"],
        'muMu': ["weight_tau1_MuonID_SF_RecoCentral","weight_tau1_HighPt_MuonID_SF_RecoCentral","weight_tau1_MuonID_SF_TightID_TrkCentral","weight_tau1_MuonID_SF_TightRelIsoCentral", "weight_tau2_MuonID_SF_RecoCentral","weight_tau2_HighPt_MuonID_SF_RecoCentral","weight_tau2_MuonID_SF_TightID_TrkCentral","weight_tau2_MuonID_SF_TightRelIsoCentral"],
        'eMu': ["weight_tau1_EleSF_wp80iso_EleIDCentral","weight_tau2_MuonID_SF_RecoCentral","weight_tau2_HighPt_MuonID_SF_RecoCentral","weight_tau2_MuonID_SF_TightID_TrkCentral","weight_tau2_MuonID_SF_TightRelIsoCentral"],
        #'eMu': ["weight_tau1_MuonID_SF_RecoCentral","weight_tau1_HighPt_MuonID_SF_RecoCentral","weight_tau1_MuonID_SF_TightID_TrkCentral","weight_tau1_MuonID_SF_TightRelIsoCentral","weight_tau2_EleSF_wp80iso_EleIDCentral"]
        'eE':["weight_tau1_EleSF_wp80iso_EleIDCentral","weight_tau2_EleSF_wp80noiso_EleIDCentral"]
        }
    # weight_tau1_EleSF_wp80iso_EleIDCentral
    # weight_tau1_EleSF_wp80noiso_EleIDCentral
    #tau_weights =["TauID_SF_Medium_Central", "HighPt_MuonID_SF_HighPtIDCentral", "HighPt_MuonID_SF_HighPtIdRelTkIsoCentral","MuonID_SF_RecoCentral", "HighPt_MuonID_SF_RecoCentral"]
    #tau_weights =["EleSF_EleIDCentral", "TauID_SF_Medium_Central", "HighPt_MuonID_SF_HighPtIDCentral", "HighPt_MuonID_SF_HighPtIdRelTkIsoCentral","MuonID_SF_RecoCentral", "HighPt_MuonID_SF_RecoCentral"]
    #for tau_suffix in tau_weights:
    #    for tau_idx in [1,2]:
    #        weights_to_apply.append(f"weight_tau{tau_idx}_{tau_suffix}")
    weights_to_apply.extend(ID_weights_dict[channel])
    weights_to_apply.extend(trg_weights_dict[channel])
    if cat not in boosted_categories:
         weights_to_apply.extend(["weight_Jet_PUJetID_Central_b1_2", "weight_Jet_PUJetID_Central_b2_2"])
    total_weight = '*'.join(weights_to_apply)
    return total_weight
class DataFrameBuilderForHistograms(DataFrameBuilderBase):

    def defineBoostedVariables(self):
        FatJetObservables = self.config['FatJetObservables']
        #print(f"fatJetOBservables are {FatJetObservables}")
        # for next iteration:
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
                #print(fatJetVar)

    def defineTriggers(self):
        for ch in self.config['channelSelection']:
            for trg in self.config['triggers'][ch].split(' || '):
                if trg not in self.df.GetColumnNames():
                    print(f"{trg} not present in colNames")
                    self.df = self.df.Define(trg, "1")

        singleTau_th_dict = self.config['singleTau_th']
        singleMu_th_dict = self.config['singleMu_th']
        singleEle_th_dict = self.config['singleEle_th']
        legacy_region_definition= "( ( channelId == 13 && (SingleEle_region  || CrossEleTau_region) ) || ( channelId == 23 && (SingleMu_region  || CrossMuTau_region) ) || ( channelId == 33 && ( diTau_region ) ) || (channelId==11 && (SingleEle_region)) || (channelId==12 && ( SingleEle_region || SingleMu_region ) ) || (channelId==22 && (SingleMu_region)) )"
        for trg_name,trg_dict in self.config['application_regions'].items():
            if trg_name == 'HLT_MET':
                self.df = self.df.Define("Legacy_region", legacy_region_definition)
            for key in trg_dict.keys():
                region_name = trg_dict['region_name']
                region_cut = trg_dict['region_cut'].format(tau_th=singleTau_th_dict[self.period], ele_th=singleEle_th_dict[self.period], mu_th=singleMu_th_dict[self.period])
                if region_name not in self.df.GetColumnNames():
                    self.df = self.df.Define(region_name, region_cut)

    def defineCRs(self):
        SR_mass_limits_bb_boosted = self.config['mass_cut_limits']['bb_m_vis']['boosted']
        SR_mass_limits_bb = self.config['mass_cut_limits']['bb_m_vis']['other']
        SR_mass_limits_tt = self.config['mass_cut_limits']['tautau_m_vis']
        self.df = self.df.Define("SR_tt", f"return (tautau_m_vis > {SR_mass_limits_tt[0]} && tautau_m_vis  < {SR_mass_limits_tt[1]});")
        self.df = self.df.Define("SR_bb", f"(bb_m_vis > {SR_mass_limits_bb[0]} && bb_m_vis < {SR_mass_limits_bb[1]});")
        self.df = self.df.Define("SR_bb_boosted", f"(bb_m_vis_softdrop > {SR_mass_limits_bb_boosted[0]} && bb_m_vis_softdrop < {SR_mass_limits_bb_boosted[1]});")
        self.df = self.df.Define("SR", f" SR_tt &&  SR_bb")
        self.df = self.df.Define("SR_boosted", f" SR_tt &&  SR_bb_boosted")


        self.df = self.df.Define("DYCR", "if(muMu || eE) {return (tautau_m_vis < 92 && tautau_m_vis > 89);} return true;")
        self.df = self.df.Define("DYCR_boosted", "DYCR")


        TTCR_mass_limits_eTau = self.config['TTCR_mass_limits']['eTau']
        TTCR_mass_limits_muTau = self.config['TTCR_mass_limits']['muTau']
        TTCR_mass_limits_tauTau = self.config['TTCR_mass_limits']['tauTau']
        TTCR_mass_limits_muMu = self.config['TTCR_mass_limits']['muMu']
        TTCR_mass_limits_eE = self.config['TTCR_mass_limits']['eE']
        self.df = self.df.Define("TTCR", f"""
                                if(eTau) {{return (tautau_m_vis < {TTCR_mass_limits_eTau[0]} || tautau_m_vis > {TTCR_mass_limits_eTau[1]});
                                }};
                                 if(muTau) {{return (tautau_m_vis < {TTCR_mass_limits_muTau[0]} || tautau_m_vis > {TTCR_mass_limits_muTau[1]});
                                 }};
                                 if(tauTau) {{return (tautau_m_vis < {TTCR_mass_limits_tauTau[0]} || tautau_m_vis > {TTCR_mass_limits_tauTau[1]});
                                 }};
                                 if(muMu) {{return (tautau_m_vis < {TTCR_mass_limits_muMu[0]} || tautau_m_vis > {TTCR_mass_limits_muMu[1]});
                                 }};
                                 if(eE) {{return (tautau_m_vis < {TTCR_mass_limits_eE[0]} || tautau_m_vis > {TTCR_mass_limits_eE[1]});
                                 }};
                                 return true;""")
        self.df = self.df.Define("TTCR_boosted", "TTCR")

    def redefineWeights(self):
        weights_to_redefine = ["weight_tau1_TrgSF_ditau_Central","weight_tau2_TrgSF_ditau_Central","weight_tau1_TrgSF_singleTau_Central","weight_tau2_TrgSF_singleTau_Central","weight_TrgSF_MET_Central","weight_tau1_TrgSF_singleEle_Central", "weight_tau2_TrgSF_singleEle_Central", "weight_tau1_TrgSF_singleMu_Central", "weight_tau2_TrgSF_singleMu_Central"]
        regions= ["Legacy_region","Legacy_region","SingleTau_region","SingleTau_region","MET_region","SingleEle_region","SingleEle_region","SingleMu_region","SingleMu_region"]
        for weight,region in zip(weights_to_redefine, regions):
            if weight in self.df.GetColumnNames():
                self.df = self.df.Define(f"{weight}_application", f"""
                                         if({region})
                                            return static_cast<float>({weight}) ;
                                         return 1.f;""")
            else:
                self.df = self.df.Define(f"{weight}_application", f""" 1.f;""")
        for weight in ["weight_Jet_PUJetID_Central_b1","weight_Jet_PUJetID_Central_b2"]:
            if weight not in self.df.GetColumnNames(): continue
            self.df = self.df.Define(f"{weight}_2", f"""
                                         if({weight}!=-100)
                                            return static_cast<float>({weight}) ;
                                         return 1.f;""")

    def defineCategories(self):
        self.df = self.df.Define("nSelBtag", f"int(b1_btagDeepFlavB > 0.2783 + b2_btagDeepFlavB>0.2783)")
        for category_to_def in self.config['category_definition'].keys():
            category_name = category_to_def
            #print(self.config['category_definition'][category_to_def].format(pNetWP=self.pNetWP, region=self.region))
            self.df = self.df.Define(category_to_def, self.config['category_definition'][category_to_def].format(pNetWP=self.pNetWP, region=self.region))

    def defineChannels(self):
        for channel in self.config['channelSelection']:
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

    def defineLeptonPreselection(self):
        self.df = self.df.Define("muon1_tightId", "if(muTau || muMu) {return (tau1_Muon_tightId && tau1_Muon_pfRelIso04_all < 0.15); } return true;")
        self.df = self.df.Define("muon2_tightId", "if(muMu || eMu) {return (tau2_Muon_tightId && tau2_Muon_pfRelIso04_all < 0.3);} return true;")
        self.df = self.df.Define("tau1_iso_medium", f"if(tauTau) return (tau1_idDeepTau{self.deepTauYear()}{self.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value}); return true;")
        if f"tau1_gen_kind" not in self.df.GetColumnNames():
            self.df=self.df.Define("tau1_gen_kind", "if(isData) return 5; return 0;")
        if f"tau2_gen_kind" not in self.df.GetColumnNames():
            self.df=self.df.Define("tau2_gen_kind", "if(isData) return 5; return 0;")
        self.df = self.df.Define("tau_true", f"""(tau1_gen_kind==5 && tau2_gen_kind==5)""")
        self.df = self.df.Define(f"lepton_preselection", "tau1_iso_medium && muon1_tightId && muon2_tightId")
        self.df = self.df.Filter(f"lepton_preselection")

    def defineQCDRegions(self):
        self.df = self.df.Define("OS", "tau1_charge*tau2_charge < 0")
        self.df = self.df.Define("Iso", f"((tauTau || eTau || muTau) && (tau2_idDeepTau{self.deepTauYear()}{self.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value} )) || ((muMu||eMu) && (tau2_Muon_pfRelIso04_all < 0.15)) || (eE && tau2_Electron_pfRelIso03_all < 0.15 )")
        self.df = self.df.Define("AntiIso", f"((tauTau || eTau || muTau) && (tau2_idDeepTau{self.deepTauYear()}{self.deepTauVersion}VSjet >= {Utilities.WorkingPointsTauVSjet.VVVLoose.value} && tau2_idDeepTau{self.deepTauYear()}{self.deepTauVersion}VSjet < {Utilities.WorkingPointsTauVSjet.Medium.value})) || ((muMu||eMu) && (tau2_Muon_pfRelIso04_all >= 0.15 && tau2_Muon_pfRelIso04_all < 0.3) ) || (eE && (tau2_Electron_pfRelIso03_all >= 0.15 && tau2_Electron_mvaNoIso_WP80 ))")
        self.df = self.df.Define("OS_Iso", f"OS && Iso")
        self.df = self.df.Define("SS_Iso", f"!OS && Iso")
        self.df = self.df.Define("OS_AntiIso", f"OS && AntiIso")
        self.df = self.df.Define("SS_AntiIso", f"!OS && AntiIso")
    def deepTauYear(self):
        return self.config['deepTauYears'][self.deepTauVersion]

    def selectTrigger(self, trigger):
        self.df = self.df.Filter(trigger)

    def addCut (self, cut=""):
        if cut!="":
            self.df = self.df.Filter(cut)

    def __init__(self, df, config, period, deepTauVersion='v2p1', bTagWP = 2, pNetWPstring="Loose", region="SR"):
        super(DataFrameBuilderForHistograms, self).__init__(df)
        self.deepTauVersion = deepTauVersion
        self.config = config
        self.bTagWP = bTagWP
        self.pNetWP = WorkingPointsParticleNet[period][pNetWPstring]
        self.period = period
        self.region = region

def PrepareDfForHistograms(dfForHistograms):
    dfForHistograms.df = defineAllP4(dfForHistograms.df)
    dfForHistograms.defineChannels()
    dfForHistograms.defineLeptonPreselection()
    dfForHistograms.defineBoostedVariables()
    dfForHistograms.df = createInvMass(dfForHistograms.df)
    dfForHistograms.defineCRs()
    dfForHistograms.defineCategories()
    dfForHistograms.defineQCDRegions()
    dfForHistograms.defineTriggers()
    dfForHistograms.redefineWeights()
    return dfForHistograms