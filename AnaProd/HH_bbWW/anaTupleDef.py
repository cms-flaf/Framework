import AnaProd.HH_bbWW.baseline as AnaBaseline
import Common.BaselineSelection as CommonBaseline
from Corrections.Corrections import Corrections

loadTF = False
loadHHBtag = False
lepton_legs = [ "lep1", "lep2" ]


Muon_int_observables = ["Muon_tightId","Muon_highPtId","Muon_pfIsoId"]
Muon_float_observables = ["Muon_tkRelIso", "Muon_pfRelIso04_all","Muon_miniPFRelIso_all"]
Muon_observables = Muon_int_observables + Muon_float_observables
Electron_int_observables = ["Electron_mvaNoIso_WP80", "Electron_mvaIso_WP80"]
Electron_float_observables = ["Electron_pfRelIso03_all","Electron_miniPFRelIso_all","Electron_mvaIso","Electron_mvaNoIso"]
Electron_observables = Electron_int_observables + Electron_float_observables
JetObservables = ["PNetRegPtRawCorr", "PNetRegPtRawCorrNeutrino", "PNetRegPtRawRes",
                  "btagDeepFlavB", "btagDeepFlavCvB", "btagDeepFlavCvL", "btagDeepFlavQG",
                  "btagPNetB", "btagPNetCvB", "btagPNetCvL", "btagPNetCvNotB", "btagPNetQvG"] # 2024

JetObservablesMC = ["hadronFlavour", "partonFlavour"]

FatJetObservables = ["area", "btagCSVV2", "btagDDBvLV2", "btagDeepB", "btagHbb", "deepTagMD_HbbvsQCD",
                     "deepTagMD_ZHbbvsQCD", "deepTagMD_ZbbvsQCD", "deepTagMD_bbvsLight", "deepTag_H",
                     "jetId", "msoftdrop", "nBHadrons", "nCHadrons", "nConstituents","rawFactor",
                      "particleNetMD_QCD", "particleNetMD_Xbb", "particleNet_HbbvsQCD", "particleNet_mass", # 2018
                     "particleNet_QCD","particleNet_XbbVsQCD", # 2016
                     "particleNetLegacy_QCD", "particleNetLegacy_Xbb", "particleNetLegacy_mass", # 2016
                     "particleNetWithMass_QCD", "particleNetWithMass_HbbvsQCD", "particleNet_massCorr" # 2016
                     ,"tau1","tau2","tau3","tau4"]

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


def addAllVariables(dfw, syst_name, isData, trigger_class, lepton_legs, isSignal, global_params, channels):
    print(f"Adding variables for {syst_name}")
    # dfw.Apply(CommonBaseline.SelectRecoP4, syst_name, global_params["nano_version"])
    dfw.Apply(AnaBaseline.RecoHWWCandidateSelection)
    dfw.Apply(AnaBaseline.RecoHWWJetSelection)

    PtEtaPhiM = ["pt", "eta", "phi", "mass"]
    # save reco lepton from HWWcandidate
    dfw.DefineAndAppend(f"nPreselMu", f"Muon_pt[Muon_presel].size()")
    dfw.DefineAndAppend(f"nPreselEle", f"Electron_pt[Electron_presel].size()")
    dfw.DefineAndAppend(f"nTightMu", f"Muon_pt[Muon_sel].size()")
    dfw.DefineAndAppend(f"nTightEle", f"Electron_pt[Electron_sel].size()")
    n_legs = 2
    for leg_idx in range(n_legs):
        def LegVar(var_name, var_expr, var_type=None, var_cond=None, default=0):
            cond = f"HwwCandidate.leg_type.size() > {leg_idx}"
            if var_cond:
                cond = f'{cond} && ({var_cond})'
            define_expr = f'static_cast<{var_type}>({var_expr})' if var_type else var_expr
            full_define_expr = f'{cond} ? ({define_expr}) : {default}'
            dfw.DefineAndAppend( f"lep{leg_idx+1}_{var_name}", full_define_expr)
        for var in PtEtaPhiM:
            LegVar(var, f"HwwCandidate.leg_p4.at({leg_idx}).{var}()", var_type='float', default='0.f')
        LegVar('type', f"HwwCandidate.leg_type.at({leg_idx})", var_type='int', default='static_cast<int>(Leg::none)')
        LegVar('charge', f"HwwCandidate.leg_charge.at({leg_idx})", var_type='int', default='0')
        LegVar('iso', f"HwwCandidate.leg_rawIso.at({leg_idx})", var_type='float', default='0')
        for muon_obs in Muon_observables:
            LegVar(muon_obs, f"{muon_obs}.at(HwwCandidate.leg_index.at({leg_idx}))",
                   var_cond=f"HwwCandidate.leg_type.at({leg_idx}) == Leg::mu", default='-1')
        for ele_obs in Electron_observables:
            LegVar(ele_obs, f"{ele_obs}.at(HwwCandidate.leg_index.at({leg_idx}))",
                   var_cond=f"HwwCandidate.leg_type.at({leg_idx}) == Leg::e", default='-1')
        #Save the lep* p4 and index directly to avoid using HwwCandidate in SF LUTs
        dfw.Define( f"lep{leg_idx+1}_p4", f"HwwCandidate.leg_type.size() > {leg_idx} ? HwwCandidate.leg_p4.at({leg_idx}) : LorentzVectorM()")
        dfw.Define( f"lep{leg_idx+1}_index", f"HwwCandidate.leg_type.size() > {leg_idx} ? HwwCandidate.leg_index.at({leg_idx}) : -1")

    #save all pre selcted muons

    for var in PtEtaPhiM:
        dfw.DefineAndAppend(f"SelectedMuon_{var}", f"v_ops::{var}(Muon_p4[Muon_presel])")
    for muon_obs in Muon_float_observables:
        dfw.DefineAndAppend(f"Selected{muon_obs}" , f"RVecF({muon_obs}[Muon_presel])")
    for muon_obs in Muon_int_observables:
        dfw.DefineAndAppend(f"Selected{muon_obs}" , f"RVecI({muon_obs}[Muon_presel])")
    for var in PtEtaPhiM:
        dfw.DefineAndAppend(f"SelectedElectron_{var}", f"v_ops::{var}(Electron_p4[Electron_presel])")
    for ele_obs in Electron_float_observables:
        dfw.DefineAndAppend(f"Selected{ele_obs}" , f"RVecF({ele_obs}[Electron_presel])")
    for ele_obs in Electron_int_observables:
        dfw.DefineAndAppend(f"Selected{ele_obs}" , f"RVecI({ele_obs}[Electron_presel])")
    #save information for fatjets
    fatjet_obs = []
    fatjet_obs.extend(FatJetObservables)
    if not isData:
        dfw.Define(f"FatJet_genJet_idx", f" FindMatching(FatJet_p4[FatJet_sel],GenJetAK8_p4,0.3)")
        fatjet_obs.extend(JetObservablesMC)
    dfw.DefineAndAppend(f"SelectedFatJet_pt", f"v_ops::pt(FatJet_p4[FatJet_sel])")
    dfw.DefineAndAppend(f"SelectedFatJet_eta", f"v_ops::eta(FatJet_p4[FatJet_sel])")
    dfw.DefineAndAppend(f"SelectedFatJet_phi", f"v_ops::phi(FatJet_p4[FatJet_sel])")
    dfw.DefineAndAppend(f"SelectedFatJet_mass", f"v_ops::mass(FatJet_p4[FatJet_sel])")
    for fatjetVar in fatjet_obs:
        if(f"FatJet_{fatjetVar}" not in dfw.df.GetColumnNames()): continue
        dfw.DefineAndAppend(f"SelectedFatJet_{fatjetVar}", f"FatJet_{fatjetVar}[FatJet_sel]")
    subjet_obs = []
    subjet_obs.extend(SubJetObservables)
    if not isData:
        dfw.Define(f"SubJet1_genJet_idx", f" FindMatching(SubJet_p4[FatJet_subJetIdx1],SubGenJetAK8_p4,0.3)")
        dfw.Define(f"SubJet2_genJet_idx", f" FindMatching(SubJet_p4[FatJet_subJetIdx2],SubGenJetAK8_p4,0.3)")
        fatjet_obs.extend(SubJetObservablesMC)
    for subJetIdx in [1,2]:
        dfw.Define(f"SelectedFatJet_subJetIdx{subJetIdx}", f"FatJet_subJetIdx{subJetIdx}[FatJet_sel]")
        dfw.Define(f"FatJet_SubJet{subJetIdx}_isValid", f" FatJet_subJetIdx{subJetIdx} >=0 && FatJet_subJetIdx{subJetIdx} < nSubJet")
        dfw.DefineAndAppend(f"SelectedFatJet_SubJet{subJetIdx}_isValid", f"FatJet_SubJet{subJetIdx}_isValid[FatJet_sel]")
        for subJetVar in subjet_obs:
            dfw.DefineAndAppend(f"SelectedFatJet_SubJet{subJetIdx}_{subJetVar}", f"""
                                RVecF subjet_var(SelectedFatJet_pt.size(), 0.f);
                                for(size_t fj_idx = 0; fj_idx<SelectedFatJet_pt.size(); fj_idx++) {{
                                    auto sj_idx = SelectedFatJet_subJetIdx{subJetIdx}.at(fj_idx);
                                    if(sj_idx >= 0 && sj_idx < SubJet_{subJetVar}.size()){{
                                        subjet_var[fj_idx] = SubJet_{subJetVar}.at(sj_idx);
                                    }}
                                }}
                                return subjet_var;
                                """)
    pf_str = global_params["met_type"]
    dfw.DefineAndAppend(f"met_pt_nano", f"static_cast<float>({pf_str}_p4_nano.pt())")
    dfw.DefineAndAppend(f"met_phi_nano", f"static_cast<float>({pf_str}_p4_nano.phi())")
    dfw.DefineAndAppend("met_pt", f"static_cast<float>({pf_str}_p4.pt())")
    dfw.DefineAndAppend("met_phi", f"static_cast<float>({pf_str}_p4.phi())")

    if trigger_class is not None:
        channel = f'H{global_params["analysis_config_area"][-2:].lower()}'
        hltBranches = dfw.Apply(trigger_class.ApplyTriggers, lepton_legs, channel, isData, isSignal )
        dfw.colToSave.extend(hltBranches)
    dfw.DefineAndAppend("channelId","static_cast<int>(HwwCandidate.channel())")
    channel_to_select = " || ".join(f"HwwCandidate.channel()==Channel::{ch}" for ch in channels)#global_params["channelSelection"])
    dfw.Filter(channel_to_select, "select channels")
    # save all selected reco jets
    dfw.Define("centralJet_idx", "CreateIndexes(Jet_btagPNetB[Jet_sel].size())")
    dfw.Define("centralJet_idxSorted", "ReorderObjects(Jet_btagPNetB[Jet_sel], centralJet_idx)")
    for var in PtEtaPhiM:
        name = f"centralJet_{var}"
        dfw.DefineAndAppend(name, f"Take(v_ops::{var}(Jet_p4[Jet_sel]), centralJet_idxSorted)")

    # save gen jets matched to selected reco jets
    if not isData:
        dfw.Define("centralJet_matchedGenJetIdx", f"Take(Jet_genJetIdx[Jet_sel], centralJet_idxSorted)")
        for var in PtEtaPhiM:
            name = f"centralJet_matchedGenJet_{var}"
            dfw.DefineAndAppend(name, f"""RVecF res;
                                        for (auto idx: centralJet_matchedGenJetIdx)
                                        {{
                                            res.push_back(idx == -1 ? 0.0 : GenJet_p4[idx].{var}());
                                        }}
                                        return res;""")

        for var in JetObservablesMC:
            name = f"centralJet_matchedGenJet_{var}"
            dfw.DefineAndAppend(name, f"""RVecF res;
                                        for (auto idx: centralJet_matchedGenJetIdx)
                                        {{
                                            res.push_back(idx == -1 ? 0.0 : GenJet_{var}[idx]);
                                        }}
                                        return res;""")

        dfw.Define("IsTrueBjet", "GenJet_hadronFlavour == 5")
        dfw.Define("GenJet_TrueBjetTag", "FindTwoJetsClosestToMPV(125.0, GenJet_p4, IsTrueBjet)")
        dfw.DefineAndAppend("centralJet_TrueBjetTag",
                                        """RVecI res;
                                        for (auto idx: centralJet_matchedGenJetIdx)
                                        {
                                            res.push_back(idx == -1 ? 0 : static_cast<int>(GenJet_TrueBjetTag[idx]));
                                        }
                                        return res;""")
    reco_jet_obs = []
    reco_jet_obs.extend(JetObservables)
    if not isData:
        reco_jet_obs.extend(JetObservablesMC)
    for jet_obs in reco_jet_obs:
        name = f"centralJet_{jet_obs}"
        dfw.DefineAndAppend(name, f"Take(Jet_{jet_obs}[Jet_sel], centralJet_idxSorted)")
    if isSignal:
        # save gen H->VV
        dfw.Define("H_to_VV", """GetGenHVVCandidate(event, genLeptons, GenPart_pdgId, GenPart_daughters, GenPart_statusFlags, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, GenJet_p4, true)""")
        for var in PtEtaPhiM:
            dfw.DefineAndAppend(f"genHVV_{var}", f"static_cast<float>(H_to_VV.cand_p4.{var}())")

        # save gen level vector bosons from H->VV
        for boson in [1, 2]:
            name = f"genV{boson}"
            dfw.DefineAndAppend(f"{name}_pdgId", f"GenPart_pdgId[ H_to_VV.legs[{boson - 1}].index ]")
            for var in PtEtaPhiM:
                dfw.DefineAndAppend(f"{name}_{var}", f"static_cast<float>(H_to_VV.legs[{boson - 1}].cand_p4.{var}())")

        # save gen level products of vector boson decays (prod - index of product (quark, leptons or neutrinos))
        for boson in [1, 2]:
            for prod in [1, 2]:
                name = f"genV{boson}prod{prod}"
                for var in PtEtaPhiM:
                    dfw.DefineAndAppend(f"{name}_{var}", f"static_cast<float>(H_to_VV.legs[{boson - 1}].leg_p4[{prod - 1}].{var}())")
                    dfw.DefineAndAppend(f"{name}_vis_{var}", f"static_cast<float>(H_to_VV.legs[{boson - 1}].leg_vis_p4[{prod - 1}].{var}())")
                dfw.DefineAndAppend(f"{name}_legType", f"static_cast<int>(H_to_VV.legs[{boson - 1}].leg_kind[{prod - 1}])")
                dfw.DefineAndAppend(f"{name}_pdgId", f"GenPart_pdgId[ H_to_VV.legs.at({boson - 1}).leg_index.at({prod - 1}) ]")

        # save gen level H->bb
        dfw.Define("H_to_bb", """GetGenHBBCandidate(event, GenPart_pdgId, GenPart_daughters, GenPart_statusFlags, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, GenJet_p4, true)""")
        for var in PtEtaPhiM:
            dfw.DefineAndAppend(f"genHbb_{var}", f"static_cast<float>(H_to_bb.cand_p4.{var}())")

        # save gen level b quarks
        for b_quark in [1, 2]:
            name = f"genb{b_quark}"
            for var in PtEtaPhiM:
                dfw.DefineAndAppend(f"{name}_{var}", f"static_cast<float>(H_to_bb.leg_p4[{b_quark - 1}].{var}())")
                dfw.DefineAndAppend(f"{name}_vis_{var}", f"static_cast<float>(H_to_bb.leg_vis_p4[{b_quark - 1}].{var}())")

    if not isData:
        # save gen leptons matched to reco leptons
        for lep in [1, 2]:
            name = f"lep{lep}_gen"
            # MatchGenLepton returns index of in genLetpons collection if match exists
            dfw.Define(f"{name}_idx", f" HwwCandidate.leg_type.size() >= {lep} ? MatchGenLepton(HwwCandidate.leg_p4.at({lep - 1}), genLeptons, 0.4) : -1")
            dfw.Define(f"{name}_p4", f"return {name}_idx == -1 ? LorentzVectorM() : LorentzVectorM(genLeptons.at({name}_idx).visibleP4());")
            dfw.Define(f"{name}_mother_p4", f"return {name}_idx == -1 ? LorentzVectorM() : (*genLeptons.at({name}_idx).mothers().begin())->p4;")

            dfw.DefineAndAppend(f"{name}_kind", f"return {name}_idx == -1 ? -1 : static_cast<int>(genLeptons.at({name}_idx).kind());")
            dfw.DefineAndAppend(f"{name}_motherPdgId", f"return {name}_idx == -1 ? -1 : (*genLeptons.at({name}_idx).mothers().begin())->pdgId")
            for var in PtEtaPhiM:
                dfw.DefineAndAppend(f"{name}_mother_{var}", f"static_cast<float>({name}_mother_p4.{var}())")
                dfw.DefineAndAppend(f"{name}_{var}", f"static_cast<float>({name}_p4.{var}())")
