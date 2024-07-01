import AnaProd.HH_bbWW.baseline as AnaBaseline
import Common.BaselineSelection as CommonBaseline
from Corrections.Corrections import Corrections

loadTF = False
loadHHBtag = False
lepton_legs = [ "tau1", "tau2" ]

deepTauScores= ["rawDeepTau2017v2p1VSe","rawDeepTau2017v2p1VSmu",
            "rawDeepTau2017v2p1VSjet", "rawDeepTau2018v2p5VSe", "rawDeepTau2018v2p5VSmu",
            "rawDeepTau2018v2p5VSjet",
            "idDeepTau2017v2p1VSe", "idDeepTau2017v2p1VSjet", "idDeepTau2017v2p1VSmu",
            "idDeepTau2018v2p5VSe","idDeepTau2018v2p5VSjet","idDeepTau2018v2p5VSmu",
            "decayMode"]
Muon_observables = ["Muon_tkRelIso", "Muon_pfRelIso04_all"]
Electron_observables = ["Electron_mvaNoIso_WP90", "Electron_mvaIso_WP90", "Electron_pfRelIso03_all"]
JetObservables = ["particleNetAK4_B", "particleNetAK4_CvsB",
                "particleNetAK4_CvsL","particleNetAK4_QvsG","particleNetAK4_puIdDisc",
                "btagDeepFlavB","btagDeepFlavCvB","btagDeepFlavCvL", "bRegCorr", "bRegRes", "idbtagDeepFlavB",
                "btagPNetB", "btagPNetCvL", "btagPNetCvB", "btagPNetQvG", "btagPNetTauVJet", "PNetRegPtRawCorr", "PNetRegPtRawCorrNeutrino", "PNetRegPtRawRes"] # 2016]
JetObservablesMC = ["hadronFlavour","partonFlavour"]
FatJetObservables = ["area", "btagCSVV2", "btagDDBvLV2", "btagDeepB", "btagHbb", "deepTagMD_HbbvsQCD",
                     "deepTagMD_ZHbbvsQCD", "deepTagMD_ZbbvsQCD", "deepTagMD_bbvsLight", "deepTag_H",
                     "jetId", "msoftdrop", "nBHadrons", "nCHadrons", "nConstituents","rawFactor",
                      "particleNetMD_QCD", "particleNetMD_Xbb", "particleNet_HbbvsQCD", "particleNet_mass", # 2018
                     "particleNet_QCD","particleNet_XbbVsQCD", # 2016
                     "particleNetLegacy_QCD", "particleNetLegacy_Xbb", "particleNetLegacy_mass", # 2016
                     "particleNetWithMass_QCD", "particleNetWithMass_HbbvsQCD", "particleNet_massCorr" # 2016
                     ]

# in this PR https://github.com/cms-sw/cmssw/commit/17457a557bd75ab479dfb78013edf9e551ecd6b7, particleNet MD have been removed therefore we will switch to take


# # New ParticleNet trainings are not available in MiniAOD until Run3 13X
'''
particleNetWithMass_QCD YES
particleNetWithMass_TvsQCD NO
particleNetWithMass_WvsQCD NO
particleNetWithMass_ZvsQCD NO
particleNetWithMass_H4qvsQCD NO
particleNetWithMass_HbbvsQCD YES
particleNetWithMass_HccvsQCD NO
particleNet_QCD YES
particleNet_QCD2HF NO
particleNet_QCD1HF NO
particleNet_QCD0HF NO
particleNet_massCorr YES
particleNet_XbbVsQCD YES
particleNet_XccVsQCD NO
particleNet_XqqVsQCD NO
particleNet_XggVsQCD NO
particleNet_XttVsQCD NO
particleNet_XtmVsQCD NO
particleNet_XteVsQCD NO
'''
# Restore taggers that were decommisionned for Run-3
'''
particleNetLegacy_mass YES
particleNetLegacy_Xbb YES
particleNetLegacy_Xcc NO
particleNetLegacy_Xqq NO
particleNetLegacy_QCD YES
'''

# ParticleNet legacy jet tagger is already in 106Xv2 MINIAOD,
# add PartlceNet legacy mass regression and new combined tagger + mass regression

# for more info: ---> https://github.com/cms-sw/cmssw/blob/master/PhysicsTools/NanoAOD/python/jetsAK8_cff.py

FatJetObservablesMC = ["hadronFlavour","partonFlavour"]

SubJetObservables = ["btagDeepB", "eta", "mass", "phi", "pt", "rawFactor"]
SubJetObservablesMC = ["hadronFlavour","partonFlavour"]

defaultColToSave = ["entryIndex","luminosityBlock", "run","event", "sample_type", "sample_name", "period", "X_mass", "X_spin", "isData","PuppiMET_pt", "PuppiMET_phi", "nJet","DeepMETResolutionTune_pt", "DeepMETResolutionTune_phi","DeepMETResponseTune_pt", "DeepMETResponseTune_phi","PV_npvs"]

def getDefaultColumnsToSave(isData):
    colToSave = defaultColToSave.copy()
    if not isData:
        colToSave.extend(['Pileup_nTrueInt'])
    return colToSave


# rewrite this function
# add reco p4, create W (ntuple will contain only W)
def addAllVariables(dfw, syst_name, isData, trigger_class, lepton_legs, isSignal, global_params):
    print(f"Adding variables for {syst_name}")
    dfw.Apply(CommonBaseline.SelectRecoP4, syst_name, global_params["nano_version"])
    dfw.Apply(AnaBaseline.RecoHWWCandidateSelection)
    dfw.Apply(AnaBaseline.RecoHWWJetSelection)
    # dfw.Apply(AnaBaseline.RecoHttCandidateSelection, global_params)
    # dfw.Apply(AnaBaseline.RecoJetSelection)
    # dfw.Apply(AnaBaseline.ThirdLeptonVeto)
    # dfw.Apply(AnaBaseline.DefineHbbCand)
    # dfw.DefineAndAppend("Hbb_isValid" , "HbbCandidate.has_value()")
    # dfw.Apply(AnaBaseline.ExtraRecoJetSelection)
    # dfw.Apply(Corrections.getGlobal().jet.getEnergyResolution)
    # dfw.Apply(Corrections.getGlobal().btag.getWPid)
    # jet_obs = []
    # jet_obs.extend(JetObservables)
    # dfw.Apply(AnaBaseline.ApplyJetSelection)
    # if not isData:
    #     dfw.Define(f"Jet_genJet_idx", f" FindMatching(Jet_p4,GenJet_p4,0.3)")
    #     jet_obs.extend(JetObservablesMC)

    dfw.DefineAndAppend("lep1_pt", "lep1_p4.pt()")
    dfw.DefineAndAppend("lep1_eta", "lep1_p4.eta()")
    dfw.DefineAndAppend("lep1_phi", "lep1_p4.phi()")
    dfw.DefineAndAppend("lep1_mass", "lep1_p4.mass()")

    dfw.DefineAndAppend("lep2_pt", "lep2_p4.pt()")
    dfw.DefineAndAppend("lep2_eta", "lep2_p4.eta()")
    dfw.DefineAndAppend("lep2_phi", "lep2_p4.phi()")
    dfw.DefineAndAppend("lep2_mass", "lep2_p4.mass()")

    dfw.colToSave.extend(["lep1_type", "lep2_type"])

    kinematic_variables = ["pt", "eta", "phi", "mass"]
    for i in range(10):
        for var in kinematic_variables:
            name = f"Jet_{i}_{var}"
            dfw.DefineAndAppend(name, f"""  if (n_Jet_Sel > {i})
                                                return Jet_p4[Jet_sel][{i}].{var}();
                                            return 0.0; """)

    # dfw.Define("H_to_WW", """GetGenHWWCandidate(event, genLeptons, GenPart_pdgId, GenPart_daughters, GenPart_statusFlags, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, true)""")
    # dfw.DefineAndAppend(f"genHWW_E", f"H_to_WW.cand_p4.E()")
    # dfw.DefineAndAppend(f"genHWW_px", f"H_to_WW.cand_p4.px()")
    # dfw.DefineAndAppend(f"genHWW_py", f"H_to_WW.cand_p4.py()")
    # dfw.DefineAndAppend(f"genHWW_pz", f"H_to_WW.cand_p4.pz()")

    # dfw.DefineAndAppend(f"genW1_E", f"H_to_WW.legs[0].cand_p4.E()")
    # dfw.DefineAndAppend(f"genW1_px", f"H_to_WW.legs[0].cand_p4.px()")
    # dfw.DefineAndAppend(f"genW1_py", f"H_to_WW.legs[0].cand_p4.py()")
    # dfw.DefineAndAppend(f"genW1_pz", f"H_to_WW.legs[0].cand_p4.pz()")

    # dfw.DefineAndAppend(f"genW2_E", f"H_to_WW.legs[1].cand_p4.E()")
    # dfw.DefineAndAppend(f"genW2_px", f"H_to_WW.legs[1].cand_p4.px()")
    # dfw.DefineAndAppend(f"genW2_py", f"H_to_WW.legs[1].cand_p4.py()")
    # dfw.DefineAndAppend(f"genW2_pz", f"H_to_WW.legs[1].cand_p4.pz()")

    # dfw.DefineAndAppend(f"genq1_E", f"H_to_WW.legs[0].leg_p4[0].E()")
    # dfw.DefineAndAppend(f"genq1_px", f"H_to_WW.legs[0].leg_p4[0].px()")
    # dfw.DefineAndAppend(f"genq1_py", f"H_to_WW.legs[0].leg_p4[0].py()")
    # dfw.DefineAndAppend(f"genq1_pz", f"H_to_WW.legs[0].leg_p4[0].pz()")

    # dfw.DefineAndAppend(f"genq2_E", f"H_to_WW.legs[0].leg_p4[1].E()")
    # dfw.DefineAndAppend(f"genq2_px", f"H_to_WW.legs[0].leg_p4[1].px()")
    # dfw.DefineAndAppend(f"genq2_py", f"H_to_WW.legs[0].leg_p4[1].py()")
    # dfw.DefineAndAppend(f"genq2_pz", f"H_to_WW.legs[0].leg_p4[1].pz()")

    # dfw.DefineAndAppend(f"genlep_E", f"H_to_WW.legs[1].leg_p4[0].E()")
    # dfw.DefineAndAppend(f"genlep_px", f"H_to_WW.legs[1].leg_p4[0].px()")
    # dfw.DefineAndAppend(f"genlep_py", f"H_to_WW.legs[1].leg_p4[0].py()")
    # dfw.DefineAndAppend(f"genlep_pz", f"H_to_WW.legs[1].leg_p4[0].pz()")

    # dfw.DefineAndAppend(f"gennu_E", f"H_to_WW.legs[1].leg_p4[1].E()")
    # dfw.DefineAndAppend(f"gennu_px", f"H_to_WW.legs[1].leg_p4[1].px()")
    # dfw.DefineAndAppend(f"gennu_py", f"H_to_WW.legs[1].leg_p4[1].py()")
    # dfw.DefineAndAppend(f"gennu_pz", f"H_to_WW.legs[1].leg_p4[1].pz()")

    dfw.Define("H_to_VV", """GetGenHVVCandidate(event, genLeptons, GenPart_pdgId, GenPart_daughters, GenPart_statusFlags, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, true)""")
    dfw.DefineAndAppend(f"genHVV_E", f"H_to_VV.cand_p4.E()")
    dfw.DefineAndAppend(f"genHVV_px", f"H_to_VV.cand_p4.px()")
    dfw.DefineAndAppend(f"genHVV_py", f"H_to_VV.cand_p4.py()")
    dfw.DefineAndAppend(f"genHVV_pz", f"H_to_VV.cand_p4.pz()")

    dfw.DefineAndAppend(f"genV1_E", f"H_to_VV.legs[0].cand_p4.E()")
    dfw.DefineAndAppend(f"genV1_px", f"H_to_VV.legs[0].cand_p4.px()")
    dfw.DefineAndAppend(f"genV1_py", f"H_to_VV.legs[0].cand_p4.py()")
    dfw.DefineAndAppend(f"genV1_pz", f"H_to_VV.legs[0].cand_p4.pz()")

    dfw.DefineAndAppend(f"genV2_E", f"H_to_VV.legs[1].cand_p4.E()")
    dfw.DefineAndAppend(f"genV2_px", f"H_to_VV.legs[1].cand_p4.px()")
    dfw.DefineAndAppend(f"genV2_py", f"H_to_VV.legs[1].cand_p4.py()")
    dfw.DefineAndAppend(f"genV2_pz", f"H_to_VV.legs[1].cand_p4.pz()")

    dfw.DefineAndAppend(f"genV1prod1_E", f"H_to_VV.legs[0].leg_p4[0].E()")
    dfw.DefineAndAppend(f"genV1prod1_px", f"H_to_VV.legs[0].leg_p4[0].px()")
    dfw.DefineAndAppend(f"genV1prod1_py", f"H_to_VV.legs[0].leg_p4[0].py()")
    dfw.DefineAndAppend(f"genV1prod1_pz", f"H_to_VV.legs[0].leg_p4[0].pz()")

    dfw.DefineAndAppend(f"genV1prod2_E", f"H_to_VV.legs[0].leg_p4[1].E()")
    dfw.DefineAndAppend(f"genV1prod2_px", f"H_to_VV.legs[0].leg_p4[1].px()")
    dfw.DefineAndAppend(f"genV1prod2_py", f"H_to_VV.legs[0].leg_p4[1].py()")
    dfw.DefineAndAppend(f"genV1prod2_pz", f"H_to_VV.legs[0].leg_p4[1].pz()")

    dfw.DefineAndAppend(f"genV2prod1_E", f"H_to_VV.legs[1].leg_p4[0].E()")
    dfw.DefineAndAppend(f"genV2prod1_px", f"H_to_VV.legs[1].leg_p4[0].px()")
    dfw.DefineAndAppend(f"genV2prod1_py", f"H_to_VV.legs[1].leg_p4[0].py()")
    dfw.DefineAndAppend(f"genV2prod1_pz", f"H_to_VV.legs[1].leg_p4[0].pz()")

    dfw.DefineAndAppend(f"genV2prod2_E", f"H_to_VV.legs[1].leg_p4[1].E()")
    dfw.DefineAndAppend(f"genV2prod2_px", f"H_to_VV.legs[1].leg_p4[1].px()")
    dfw.DefineAndAppend(f"genV2prod2_py", f"H_to_VV.legs[1].leg_p4[1].py()")
    dfw.DefineAndAppend(f"genV2prod2_pz", f"H_to_VV.legs[1].leg_p4[1].pz()")

    dfw.Define("H_to_bb", """GetGenHBBCandidate(event, GenPart_pdgId, GenPart_daughters, GenPart_statusFlags, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, true)""")

    dfw.DefineAndAppend(f"genHbb_E", f"H_to_bb.cand_p4.E()")
    dfw.DefineAndAppend(f"genHbb_px", f"H_to_bb.cand_p4.px()")
    dfw.DefineAndAppend(f"genHbb_py", f"H_to_bb.cand_p4.py()")
    dfw.DefineAndAppend(f"genHbb_pz", f"H_to_bb.cand_p4.pz()")

    dfw.DefineAndAppend(f"genb1_E", f"H_to_bb.leg_p4[0].E()")
    dfw.DefineAndAppend(f"genb1_px", f"H_to_bb.leg_p4[0].px()")
    dfw.DefineAndAppend(f"genb1_py", f"H_to_bb.leg_p4[0].py()")
    dfw.DefineAndAppend(f"genb1_pz", f"H_to_bb.leg_p4[0].pz()")

    dfw.DefineAndAppend(f"genb2_E", f"H_to_bb.leg_p4[1].E()")
    dfw.DefineAndAppend(f"genb2_px", f"H_to_bb.leg_p4[1].px()")
    dfw.DefineAndAppend(f"genb2_py", f"H_to_bb.leg_p4[1].py()")
    dfw.DefineAndAppend(f"genb2_pz", f"H_to_bb.leg_p4[1].pz()")

    # dfw.Define("genHbbIdx", """GetGenHBBIndex(event, GenPart_pdgId, GenPart_daughters, GenPart_statusFlags)""")
    # dfw.DefineAndAppend(f"gen_Hbb_pt", f"GenPart_pt[genHbbIdx]")

    # dfw.DefineAndAppend(f"nBJets", f"Jet_p4[Jet_bCand].size()")
    # if global_params["storeExtraJets"]:
    #     dfw.DefineAndAppend(f"ExtraJet_pt", f"v_ops::pt(Jet_p4[ExtraJet_B1])")
    #     dfw.DefineAndAppend(f"ExtraJet_eta", f"v_ops::eta(Jet_p4[ExtraJet_B1])")
    #     dfw.DefineAndAppend(f"ExtraJet_phi", f"v_ops::phi(Jet_p4[ExtraJet_B1])")
    #     dfw.DefineAndAppend(f"ExtraJet_mass", f"v_ops::mass(Jet_p4[ExtraJet_B1])")
    #     dfw.DefineAndAppend(f"ExtraJet_ptRes", f"Jet_ptRes[ExtraJet_B1]")
    #     for jetVar in jet_obs:
    #         if(f"Jet_{jetVar}" not in dfw.df.GetColumnNames()): continue
    #         dfw.DefineAndAppend(f"ExtraJet_{jetVar}", f"Jet_{jetVar}[ExtraJet_B1]")
    # else:
    #     dfw.DefineAndAppend(f"nExtraJets", f"Jet_p4[ExtraJet_B1].size()")

    # met_name = 'MET' if global_params["nano_version"] == 'v12' else 'PFMET'
    # dfw.DefineAndAppend("met_pt", f"static_cast<float>({met_name}_p4.pt())")
    # dfw.DefineAndAppend("met_phi", f"static_cast<float>({met_name}_p4.phi())")
    # dfw.DefineAndAppend("metnomu_pt", f"static_cast<float>(GetMetNoMu(HttCandidate, {met_name}_p4).pt())")
    # dfw.DefineAndAppend("metnomu_phi", f"static_cast<float>(GetMetNoMu(HttCandidate, {met_name}_p4).phi())")
    # for var in ["covXX", "covXY", "covYY"]:
    #     dfw.DefineAndAppend(f"met_{var}", f"static_cast<float>({met_name}_{var})")

    # if trigger_class is not None:
    #     hltBranches = dfw.Apply(trigger_class.ApplyTriggers, lepton_legs, isData, isSignal)
    #     dfw.colToSave.extend(hltBranches)
    # dfw.Define(f"Tau_recoJetMatchIdx", f"FindMatching(Tau_p4, Jet_p4, 0.5)")
    # dfw.Define(f"Muon_recoJetMatchIdx", f"FindMatching(Muon_p4, Jet_p4, 0.5)")
    # dfw.Define( f"Electron_recoJetMatchIdx", f"FindMatching(Electron_p4, Jet_p4, 0.5)")
    # dfw.DefineAndAppend("channelId","static_cast<int>(HttCandidate.channel())")
    # channel_to_select = " || ".join(f"HttCandidate.channel()==Channel::{ch}" for ch in global_params["channelSelection"])
    # dfw.Filter(channel_to_select, "select channels")
    # fatjet_obs = []
    # fatjet_obs.extend(FatJetObservables)
    # if not isData:
    #     dfw.Define(f"FatJet_genJet_idx", f" FindMatching(FatJet_p4[FatJet_bbCand],GenJetAK8_p4,0.3)")
    #     fatjet_obs.extend(JetObservablesMC)
    #     if isSignal:
    #         dfw.DefineAndAppend("genchannelId","static_cast<int>(genHttCandidate->channel())")
    # dfw.DefineAndAppend(f"SelectedFatJet_pt", f"v_ops::pt(FatJet_p4[FatJet_bbCand])")
    # dfw.DefineAndAppend(f"SelectedFatJet_eta", f"v_ops::eta(FatJet_p4[FatJet_bbCand])")
    # dfw.DefineAndAppend(f"SelectedFatJet_phi", f"v_ops::phi(FatJet_p4[FatJet_bbCand])")
    # dfw.DefineAndAppend(f"SelectedFatJet_mass", f"v_ops::mass(FatJet_p4[FatJet_bbCand])")

    # for fatjetVar in fatjet_obs:
    #     if(f"FatJet_{fatjetVar}" not in dfw.df.GetColumnNames()): continue
    #     dfw.DefineAndAppend(f"SelectedFatJet_{fatjetVar}", f"FatJet_{fatjetVar}[FatJet_bbCand]")
    # subjet_obs = []
    # subjet_obs.extend(SubJetObservables)
    # if not isData:
    #     dfw.Define(f"SubJet1_genJet_idx", f" FindMatching(SubJet_p4[FatJet_subJetIdx1],SubGenJetAK8_p4,0.3)")
    #     dfw.Define(f"SubJet2_genJet_idx", f" FindMatching(SubJet_p4[FatJet_subJetIdx2],SubGenJetAK8_p4,0.3)")
    #     fatjet_obs.extend(SubJetObservablesMC)
    # for subJetIdx in [1,2]:
    #     dfw.Define(f"SelectedFatJet_subJetIdx{subJetIdx}", f"FatJet_subJetIdx{subJetIdx}[FatJet_bbCand]")
    #     dfw.Define(f"FatJet_SubJet{subJetIdx}_isValid", f" FatJet_subJetIdx{subJetIdx} >=0 && FatJet_subJetIdx{subJetIdx} < nSubJet")
    #     dfw.DefineAndAppend(f"SelectedFatJet_SubJet{subJetIdx}_isValid", f"FatJet_SubJet{subJetIdx}_isValid[FatJet_bbCand]")
    #     for subJetVar in subjet_obs:
    #         dfw.DefineAndAppend(f"SelectedFatJet_SubJet{subJetIdx}_{subJetVar}", f"""
    #                             RVecF subjet_var(SelectedFatJet_pt.size(), 0.f);
    #                             for(size_t fj_idx = 0; fj_idx<SelectedFatJet_pt.size(); fj_idx++) {{
    #                                 auto sj_idx = SelectedFatJet_subJetIdx{subJetIdx}.at(fj_idx);
    #                                 if(sj_idx >= 0 && sj_idx < SubJet_{subJetVar}.size()){{
    #                                     subjet_var[fj_idx] = SubJet_{subJetVar}.at(sj_idx);
    #                                 }}
    #                             }}
    #                             return subjet_var;
    #                             """)

    # n_legs = 2
    # for leg_idx in range(n_legs):
    #     def LegVar(var_name, var_expr, var_type=None, var_cond=None, check_leg_type=True, default=0):
    #         cond = var_cond
    #         if check_leg_type:
    #             type_cond = f"HttCandidate.leg_type[{leg_idx}] != Leg::none"
    #             cond = f"{type_cond} && ({cond})" if cond else type_cond
    #         define_expr = f'static_cast<{var_type}>({var_expr})' if var_type else var_expr
    #         if cond:
    #             define_expr = f'{cond} ? ({define_expr}) : {default}'
    #         dfw.DefineAndAppend( f"tau{leg_idx+1}_{var_name}", define_expr)

    #     LegVar('legType', f"HttCandidate.leg_type[{leg_idx}]", var_type='int', check_leg_type=False)
    #     for var in [ 'pt', 'eta', 'phi', 'mass' ]:
    #         LegVar(var, f'HttCandidate.leg_p4[{leg_idx}].{var}()', var_type='float', default='-1.f')
    #     LegVar('charge', f'HttCandidate.leg_charge[{leg_idx}]', var_type='int')

    #     dfw.Define(f"tau{leg_idx+1}_recoJetMatchIdx", f"""HttCandidate.leg_type[{leg_idx}] != Leg::none
    #                                                       ? FindMatching(HttCandidate.leg_p4[{leg_idx}], Jet_p4, 0.3)
    #                                                       : -1""")
    #     LegVar('iso', f"HttCandidate.leg_rawIso.at({leg_idx})")
    #     for deepTauScore in deepTauScores:
    #         LegVar(deepTauScore, f"Tau_{deepTauScore}.at(HttCandidate.leg_index[{leg_idx}])",
    #                var_cond=f"HttCandidate.leg_type[{leg_idx}] == Leg::tau", default='-1.f')
    #     for muon_obs in Muon_observables:
    #         LegVar(muon_obs, f"{muon_obs}.at(HttCandidate.leg_index[{leg_idx}])",
    #                var_cond=f"HttCandidate.leg_type[{leg_idx}] == Leg::mu", default='-1')
    #     for ele_obs in Electron_observables:
    #         LegVar(ele_obs, f"{ele_obs}.at(HttCandidate.leg_index[{leg_idx}])",
    #                var_cond=f"HttCandidate.leg_type[{leg_idx}] == Leg::e", default='-1')
    #     if not isData:
    #         dfw.Define(f"tau{leg_idx+1}_genMatchIdx",
    #                    f"HttCandidate.leg_type[{leg_idx}] != Leg::none ? HttCandidate.leg_genMatchIdx[{leg_idx}] : -1")
    #         LegVar('gen_kind', f'genLeptons.at(tau{leg_idx+1}_genMatchIdx).kind()',
    #                var_type='int', var_cond=f"tau{leg_idx+1}_genMatchIdx>=0",
    #                default='static_cast<int>(GenLeptonMatch::NoMatch)')
    #         for var in [ 'pt', 'eta', 'phi', 'mass' ]:
    #             LegVar(f'gen_vis_{var}', f'genLeptons.at(tau{leg_idx+1}_genMatchIdx).visibleP4().{var}()',
    #                    var_type='float', var_cond=f"tau{leg_idx+1}_genMatchIdx>=0", default='-1.f')
    #         LegVar('gen_nChHad', f'genLeptons.at(tau{leg_idx+1}_genMatchIdx).nChargedHadrons()',
    #                var_type='int', var_cond=f"tau{leg_idx+1}_genMatchIdx>=0", default='-1')
    #         LegVar('gen_nNeutHad', f'genLeptons.at(tau{leg_idx+1}_genMatchIdx).nNeutralHadrons()',
    #                var_type='int', var_cond=f"tau{leg_idx+1}_genMatchIdx>=0", default='-1')
    #         LegVar('gen_charge', f'genLeptons.at(tau{leg_idx+1}_genMatchIdx).charge()',
    #                var_type='int', var_cond=f"tau{leg_idx+1}_genMatchIdx>=0", default='-10')
    #         LegVar('seedingJet_partonFlavour', f'Jet_partonFlavour.at(tau{leg_idx+1}_recoJetMatchIdx)',
    #                var_type='int', var_cond=f"tau{leg_idx+1}_recoJetMatchIdx>=0", default='-10')
    #         LegVar('seedingJet_hadronFlavour', f'Jet_hadronFlavour.at(tau{leg_idx+1}_recoJetMatchIdx)',
    #                var_type='int', var_cond=f"tau{leg_idx+1}_recoJetMatchIdx>=0", default='-10')

    #     for var in [ 'pt', 'eta', 'phi', 'mass' ]:
    #         LegVar(f'seedingJet_{var}', f"Jet_p4.at(tau{leg_idx+1}_recoJetMatchIdx).{var}()",
    #                var_type='float', var_cond=f"tau{leg_idx+1}_recoJetMatchIdx>=0", default='-1.f')

    #     dfw.Define(f"b{leg_idx+1}_idx", f"Hbb_isValid ? HbbCandidate->leg_index[{leg_idx}] : -100")
    #     dfw.DefineAndAppend(f"b{leg_idx+1}_pt", f"Hbb_isValid ? static_cast<float>(HbbCandidate->leg_p4[{leg_idx}].Pt()) : -100.f")
    #     dfw.DefineAndAppend(f"b{leg_idx+1}_pt_raw", f"Hbb_isValid ? static_cast<float>(Jet_pt.at(HbbCandidate->leg_index[{leg_idx}])) : - 100.f")
    #     dfw.DefineAndAppend(f"b{leg_idx+1}_eta", f"Hbb_isValid ? static_cast<float>(HbbCandidate->leg_p4[{leg_idx}].Eta()) : -100.f")
    #     dfw.DefineAndAppend(f"b{leg_idx+1}_phi", f"Hbb_isValid ? static_cast<float>(HbbCandidate->leg_p4[{leg_idx}].Phi()) : -100.f")
    #     dfw.DefineAndAppend(f"b{leg_idx+1}_mass", f"Hbb_isValid ? static_cast<float>(HbbCandidate->leg_p4[{leg_idx}].M()) : -100.f")
    #     if not isData:
    #         dfw.Define(f"b{leg_idx+1}_genJet_idx", f" Hbb_isValid ?  Jet_genJet_idx.at(HbbCandidate->leg_index[{leg_idx}]) : -100")
    #         for var in [ 'pt', 'eta', 'phi', 'mass' ]:
    #             dfw.DefineAndAppend(f"b{leg_idx+1}_genJet_{var}", f"Hbb_isValid && b{leg_idx+1}_genJet_idx>=0 ? static_cast<float>(GenJet_p4.at(b{leg_idx+1}_genJet_idx).{var}()) : -100.f")
    #     for jetVar in jet_obs:
    #         if(f"Jet_{jetVar}" not in dfw.df.GetColumnNames()): continue
    #         dfw.DefineAndAppend(f"b{leg_idx+1}_{jetVar}", f"Hbb_isValid ? Jet_{jetVar}.at(HbbCandidate->leg_index[{leg_idx}]) : -100")
