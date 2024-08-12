import AnaProd.HH_bbWW.baseline as AnaBaseline
import Common.BaselineSelection as CommonBaseline
from Corrections.Corrections import Corrections

loadTF = False
loadHHBtag = False
lepton_legs = [ "tau1", "tau2" ]


Muon_observables = ["Muon_tkRelIso", "Muon_pfRelIso04_all"]

Electron_observables = ["Electron_mvaNoIso_WP90", "Electron_mvaIso_WP90", "Electron_pfRelIso03_all"]

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


def addAllVariables(dfw, syst_name, isData, trigger_class, lepton_legs, isSignal, global_params):
    print(f"Adding variables for {syst_name}")
    dfw.Apply(CommonBaseline.SelectRecoP4, syst_name, global_params["nano_version"])
    dfw.Apply(AnaBaseline.RecoHWWCandidateSelection)
    dfw.Apply(AnaBaseline.RecoHWWJetSelection)

    PtEtaPhiM = ["pt", "eta", "phi", "mass"]

    # save reco lepton from W decays
    for lep in [1, 2]:
        for var in PtEtaPhiM:
            name = f"lep{lep}_{var}"
            dfw.DefineAndAppend(name, f"lep{lep}_p4.{var}()")
    dfw.colToSave.extend(["lep1_type", "lep2_type"])

    # save all selected reco jets
    dfw.Define("centralJet_idx", "CreateIndexes(Jet_p4[Jet_sel].size())")
    dfw.Define("centralJet_idxSorted", "ReorderObjects(v_ops::pt(Jet_p4[Jet_sel]), centralJet_idx)")
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
            dfw.DefineAndAppend(f"genHVV_{var}", f"H_to_VV.cand_p4.{var}()")

        # save gen level vector bosons from H->VV
        for boson in [1, 2]:
            name = f"genV{boson}"
            dfw.DefineAndAppend(f"{name}_pdgId", f"GenPart_pdgId[ H_to_VV.legs[{boson - 1}].index ]")
            for var in PtEtaPhiM:
                dfw.DefineAndAppend(f"{name}_{var}", f"H_to_VV.legs[{boson - 1}].cand_p4.{var}()")

        # save gen level products of vector boson decays (prod - index of product (quark, leptons or neutrinos))
        for boson in [1, 2]:
            for prod in [1, 2]:
                name = f"genV{boson}prod{prod}"
                for var in PtEtaPhiM:
                    dfw.DefineAndAppend(f"{name}_{var}", f"H_to_VV.legs[{boson - 1}].leg_p4[{prod - 1}].{var}()")
                    dfw.DefineAndAppend(f"{name}_vis_{var}", f"H_to_VV.legs[{boson - 1}].leg_vis_p4[{prod - 1}].{var}()")
                dfw.DefineAndAppend(f"{name}_legType", f"static_cast<int>(H_to_VV.legs[{boson - 1}].leg_kind[{prod - 1}])")
                dfw.DefineAndAppend(f"{name}_pdgId", f"GenPart_pdgId[ H_to_VV.legs.at({boson - 1}).leg_index.at({prod - 1}) ]")

        # save gen level H->bb
        dfw.Define("H_to_bb", """GetGenHBBCandidate(event, GenPart_pdgId, GenPart_daughters, GenPart_statusFlags, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, GenJet_p4, true)""")
        for var in PtEtaPhiM:
            dfw.DefineAndAppend(f"genHbb_{var}", f"H_to_bb.cand_p4.{var}()")

        # save gen level b quarks
        for b_quark in [1, 2]:
            name = f"genb{b_quark}"
            for var in PtEtaPhiM:
                dfw.DefineAndAppend(f"{name}_{var}", f"H_to_bb.leg_p4[{b_quark - 1}].{var}()")
                dfw.DefineAndAppend(f"{name}_vis_{var}", f"H_to_bb.leg_vis_p4[{b_quark - 1}].{var}()")

    if not isData:
        # save gen leptons matched to reco leptons
        for lep in [1, 2]:
            name = f"lep{lep}_genLep"
            # MatchGenLepton returns index of in genLetpons collection if match exists
            dfw.Define(f"{name}_idx", f"MatchGenLepton(lep{lep}_p4, genLeptons, 0.4)")
            dfw.Define(f"{name}_p4", f"return {name}_idx == -1 ? LorentzVectorM() : LorentzVectorM(genLeptons.at({name}_idx).visibleP4());")
            dfw.Define(f"{name}_mother_p4", f"return {name}_idx == -1 ? LorentzVectorM() : (*genLeptons.at({name}_idx).mothers().begin())->p4;")

            dfw.DefineAndAppend(f"{name}_kind", f"return {name}_idx == -1 ? -1 : static_cast<int>(genLeptons.at({name}_idx).kind());")
            dfw.DefineAndAppend(f"{name}_motherPdgId", f"return {name}_idx == -1 ? -1 : (*genLeptons.at({name}_idx).mothers().begin())->pdgId")
            for var in PtEtaPhiM:
                dfw.DefineAndAppend(f"{name}_mother_{var}", f"{name}_mother_p4.{var}()")
                dfw.DefineAndAppend(f"{name}_{var}", f"{name}_p4.{var}()")