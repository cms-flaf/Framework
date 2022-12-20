import FWCore.ParameterSet.Config as cms

from PhysicsTools.NanoAOD.common_cff import Var
from PhysicsTools.PatAlgos.tools.jetTools import updateJetCollection
from RecoBTag.ONNXRuntime.pfParticleNetAK4_cff import _pfParticleNetAK4JetTagsAll
from PhysicsTools.NanoAOD.custom_jme_cff import AddParticleNetAK4Scores

def nanoAOD_addDeepInfoAK4CHS(process, addDeepBTag, addDeepFlavour, addParticleNet):
  _btagDiscriminators=[]
  if addDeepBTag:
    print("Updating process to run DeepCSV btag")
    _btagDiscriminators += ['pfDeepCSVJetTags:probb','pfDeepCSVJetTags:probbb','pfDeepCSVJetTags:probc']
  if addDeepFlavour:
    print("Updating process to run DeepFlavour btag")
    _btagDiscriminators += ['pfDeepFlavourJetTags:probb','pfDeepFlavourJetTags:probbb','pfDeepFlavourJetTags:problepb','pfDeepFlavourJetTags:probc']
  if addParticleNet:
    print("Updating process to run ParticleNet btag")
    _btagDiscriminators += _pfParticleNetAK4JetTagsAll
  if len(_btagDiscriminators)==0: return process
  print("Will recalculate the following discriminators: "+", ".join(_btagDiscriminators))
  updateJetCollection(
    process,
    jetSource = cms.InputTag('slimmedJets'),
    jetCorrections = ('AK4PFchs', cms.vstring(['L1FastJet', 'L2Relative', 'L3Absolute','L2L3Residual']), 'None'),
    btagDiscriminators = _btagDiscriminators,
    postfix = 'WithDeepInfo',
  )
  process.load("Configuration.StandardSequences.MagneticField_cff")
  process.jetCorrFactorsNano.src="selectedUpdatedPatJetsWithDeepInfo"
  process.updatedJets.jetSource="selectedUpdatedPatJetsWithDeepInfo"
  return process

def customise(process, skip_ak4_scores=False):
  process.MessageLogger.cerr.FwkReport.reportEvery = 100
  process.finalGenParticles.select = cms.vstring(
    "drop *",
    "keep++ abs(pdgId) == 15", # keep full decay chain for taus
    "+keep abs(pdgId) == 11 || abs(pdgId) == 13 || abs(pdgId) == 15", #keep leptons, with at most one mother back in the history
    "+keep+ abs(pdgId) == 6 || abs(pdgId) == 23 || abs(pdgId) == 24 || abs(pdgId) == 25 || abs(pdgId) == 35 || abs(pdgId) == 39  || abs(pdgId) == 9990012 || abs(pdgId) == 9900012",   # keep VIP particles
    "drop abs(pdgId)= 2212 && abs(pz) > 1000", #drop LHC protons accidentally added by previous keeps
  )

  if not skip_ak4_scores:
    process = nanoAOD_addDeepInfoAK4CHS(process, False, False, True)
    process = AddParticleNetAK4Scores(process, 'jetTable')

  process.boostedTauTable.variables.dxy = Var("leadChargedHadrCand().dxy()", float,
    doc="d_{xy} of lead track with respect to PV, in cm (with sign)", precision=10)
  process.boostedTauTable.variables.dz = Var("leadChargedHadrCand().dz()", float,
    doc="d_{z} of lead track with respect to PV, in cm (with sign)", precision=14)
  return process


def customise_pnet(process):
    """
    Blunt copy of https://github.com/cms-tau-pog/NanoProd/pull/10.
    """
    process = customise(process, skip_ak4_scores=True)

    addAK8 = False

    pnetDiscriminatorsAK4 = []

    process.pfParticleNetAK4LastJetTagInfos = cms.EDProducer("ParticleNetFeatureEvaluator",
        muons = cms.InputTag("slimmedMuons"),
        electrons = cms.InputTag("slimmedElectrons"),
        photons = cms.InputTag("slimmedPhotons"),
        taus = cms.InputTag("slimmedTaus"),
        vertices = cms.InputTag("offlineSlimmedPrimaryVertices"),
        secondary_vertices = cms.InputTag("slimmedSecondaryVertices"),
        pf_candidates = cms.InputTag("packedPFCandidates"),
        jets = cms.InputTag("updatedJetsWithUserData"),
        losttracks = cms.InputTag("lostTracks"),
        jet_radius = cms.double(0.4),
        min_jet_pt = cms.double(20), # Default value
        max_jet_eta = cms.double(2.5), # Default value
        min_pt_for_pfcandidates = cms.double(0.1), # Default value
        min_pt_for_track_properties = cms.double(-1),
        min_pt_for_losttrack = cms.double(1.0), # Default value
        max_dr_for_losttrack = cms.double(0.2), # Default value
        min_pt_for_taus = cms.double(18), # Default value
        max_eta_for_taus = cms.double(2.5),
        dump_feature_tree = cms.bool(False)
    )

    from RecoBTag.ONNXRuntime.boostedJetONNXJetTagsProducer_cfi import boostedJetONNXJetTagsProducer
    process.pfParticleNetAK4LastJetTags = boostedJetONNXJetTagsProducer.clone();
    process.pfParticleNetAK4LastJetTags.src = cms.InputTag("pfParticleNetAK4LastJetTagInfos");
    process.pfParticleNetAK4LastJetTags.flav_names = cms.vstring('probmu','probele','probtaup1h0p','probtaup1h1p','probtaup1h2p','probtaup3h0p','probtaup3h1p','probtaum1h0p','probtaum1h1p','probtaum1h2p','probtaum3h0p','probtaum3h1p','probb','probc','probuds','probg','ptcorr','ptreshigh','ptreslow');
    process.pfParticleNetAK4LastJetTags.preprocess_json = cms.string('RecoBTag/Combined/data/ParticleNetAK4/CHS/PNETUL/ClassRegQuantileNoJECLost/preprocess.json');
    process.pfParticleNetAK4LastJetTags.model_path = cms.FileInPath('RecoBTag/Combined/data/ParticleNetAK4/CHS/PNETUL/ClassRegQuantileNoJECLost/particle-net.onnx');
    process.pfParticleNetAK4LastJetTags.debugMode = cms.untracked.bool(False)

    pnetDiscriminatorsAK4.extend([
        "pfParticleNetAK4LastJetTags:probmu",
        "pfParticleNetAK4LastJetTags:probele",
        "pfParticleNetAK4LastJetTags:probtaup1h0p",
        "pfParticleNetAK4LastJetTags:probtaup1h1p",
        "pfParticleNetAK4LastJetTags:probtaup1h2p",
        "pfParticleNetAK4LastJetTags:probtaup3h0p",
        "pfParticleNetAK4LastJetTags:probtaup3h1p",
        "pfParticleNetAK4LastJetTags:probtaum1h0p",
        "pfParticleNetAK4LastJetTags:probtaum1h1p",
        "pfParticleNetAK4LastJetTags:probtaum1h2p",
        "pfParticleNetAK4LastJetTags:probtaum3h0p",
        "pfParticleNetAK4LastJetTags:probtaum3h1p",
        "pfParticleNetAK4LastJetTags:probb",
        "pfParticleNetAK4LastJetTags:probc",
        "pfParticleNetAK4LastJetTags:probuds",
        "pfParticleNetAK4LastJetTags:probg",
        "pfParticleNetAK4LastJetTags:ptcorr",
        "pfParticleNetAK4LastJetTags:ptreslow",
        "pfParticleNetAK4LastJetTags:ptreshigh",
    ])

    from PhysicsTools.PatAlgos.producersLayer1.jetUpdater_cfi import updatedPatJets
    process.slimmedJetsUpdatedPNET = updatedPatJets.clone(
        jetSource = "updatedJetsWithUserData",
        addJetCorrFactors = False,
        discriminatorSources = pnetDiscriminatorsAK4
    )

    from RecoJets.JetProducers.PileupJetID_cfi import _chsalgos_106X_UL18, pileupJetId
    process.pileupJetIdUpdatedPNET = pileupJetId.clone(
        jets = cms.InputTag("updatedJetsWithUserData"),
        inputIsCorrected = True,
        applyJec = False,
        vertexes = cms.InputTag("offlineSlimmedPrimaryVertices"),
        algos = cms.VPSet(_chsalgos_106X_UL18),
    )

    process.slimmedJetsUpdatedPNET.userData.userInts.src += ['pileupJetIdUpdatedPNET:fullId'];

    from PhysicsTools.NanoAOD.common_cff import Var
    process.finalJets.src = cms.InputTag("slimmedJetsUpdatedPNET")
    process.jetTable.variables.PNET_taup1h0p = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probtaup1h0p')",float,precision=10)
    process.jetTable.variables.PNET_taup1h1p = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probtaup1h1p')",float,precision=10)
    process.jetTable.variables.PNET_taup1h2p = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probtaup1h2p')",float,precision=10)
    process.jetTable.variables.PNET_taup3h0p = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probtaup3h0p')",float,precision=10)
    process.jetTable.variables.PNET_taup3h1p = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probtaup3h1p')",float,precision=10)
    process.jetTable.variables.PNET_taum1h0p = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probtaum1h0p')",float,precision=10)
    process.jetTable.variables.PNET_taum1h1p = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probtaum1h1p')",float,precision=10)
    process.jetTable.variables.PNET_taum1h2p = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probtaum1h2p')",float,precision=10)
    process.jetTable.variables.PNET_taum3h0p = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probtaum3h0p')",float,precision=10)
    process.jetTable.variables.PNET_taum3h1p = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probtaum3h1p')",float,precision=10)
    process.jetTable.variables.PNET_mu = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probmu')",float,precision=10)
    process.jetTable.variables.PNET_ele = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probele')",float,precision=10)
    process.jetTable.variables.PNET_b = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probb')",float,precision=10)
    process.jetTable.variables.PNET_c = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probc')",float,precision=10)
    process.jetTable.variables.PNET_uds = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probuds')",float,precision=10)
    process.jetTable.variables.PNET_g = Var("bDiscriminator('pfParticleNetAK4LastJetTags:probg')",float,precision=10)
    process.jetTable.variables.PNET_ptcorr = Var("bDiscriminator('pfParticleNetAK4LastJetTags:ptcorr')",float,precision=10)
    process.jetTable.variables.PNET_ptreslow = Var("bDiscriminator('pfParticleNetAK4LastJetTags:ptreslow')",float,precision=10)
    process.jetTable.variables.PNET_ptreshigh = Var("bDiscriminator('pfParticleNetAK4LastJetTags:ptreshigh')",float,precision=10)

    process.edTask = cms.Task()
    process.edTask.add(getattr(process,"slimmedJetsUpdatedPNET"))
    process.edTask.add(getattr(process,"pileupJetIdUpdatedPNET"))
    process.edTask.add(getattr(process,"pfParticleNetAK4LastJetTags"))
    process.edTask.add(getattr(process,"pfParticleNetAK4LastJetTagInfos"))


    if addAK8:
      pnetDiscriminatorsAK8 = []

      process.pfParticleNetAK8LastJetTagInfos = cms.EDProducer("ParticleNetFeatureEvaluator",
          vertices = cms.InputTag("offlineSlimmedPrimaryVertices"),
          secondary_vertices = cms.InputTag("slimmedSecondaryVertices"),
          pf_candidates = cms.InputTag("packedPFCandidates"),
          jets = cms.InputTag("updatedJetsAK8WithUserData"),
          muons = cms.InputTag("slimmedMuons"),
          electrons = cms.InputTag("slimmedElectrons"),
          taus = cms.InputTag("slimmedTaus"),
          jet_radius = cms.double(0.8),
          min_jet_pt = cms.double(200), # Default value
          max_jet_eta = cms.double(2.5), # Default value
          min_pt_for_pfcandidates = cms.double(0.1), # Default value
          use_puppiP4 = cms.bool(False),
          min_puppi_wgt = cms.double(-1),
      )

      process.pfParticleNetAK8LastJetTags = boostedJetONNXJetTagsProducer.clone();
      process.pfParticleNetAK8LastJetTags.src = cms.InputTag("pfParticleNetAK8LastJetTagInfos");
      process.pfParticleNetAK8LastJetTags.flav_names = cms.vstring('probHtt','probHtm','probHte','probHbb','probHcc','probHqq','probHgg','probQCD2hf','probQCD1hf','probQCD0hf','masscorr');
      process.pfParticleNetAK8LastJetTags.preprocess_json = cms.string('RecoBTag/Combined/data/ParticleNetAK8/Puppi/PNETUL/ClassReg/preprocess.json');
      process.pfParticleNetAK8LastJetTags.model_path = cms.FileInPath('RecoBTag/Combined/data/ParticleNetAK8/Puppi/PNETUL/ClassReg/particle-net.onnx');

      pnetDiscriminatorsAK8.extend([
          "pfParticleNetAK8LastJetTags:probHtt",
          "pfParticleNetAK8LastJetTags:probHtm",
          "pfParticleNetAK8LastJetTags:probHte",
          "pfParticleNetAK8LastJetTags:probHbb",
          "pfParticleNetAK8LastJetTags:probHcc",
          "pfParticleNetAK8LastJetTags:probHqq",
          "pfParticleNetAK8LastJetTags:probHgg",
          "pfParticleNetAK8LastJetTags:probQCD2hf",
          "pfParticleNetAK8LastJetTags:probQCD1hf",
          "pfParticleNetAK8LastJetTags:probQCD0hf",
          "pfParticleNetAK8LastJetTags:masscorr"
      ])

      process.slimmedJetsAK8UpdatedPNET = updatedPatJets.clone(
          jetSource = "updatedJetsAK8WithUserData",
          addJetCorrFactors = False,
          discriminatorSources = pnetDiscriminatorsAK8
      )

      from RecoBTag.FeatureTools.pfDeepBoostedJetTagInfos_cfi import pfDeepBoostedJetTagInfos
      process.pfParticleNetAK8JetTagInfos = pfDeepBoostedJetTagInfos.clone(
         jet_radius = 0.8,
          min_pt_for_track_properties = 0.95,
          min_jet_pt = 200, # Default value
          max_jet_eta = 2.5, # Default value
          use_puppiP4 = False,
          min_puppi_wgt = -1,
          vertices = "offlineSlimmedPrimaryVertices",
          secondary_vertices = "slimmedSecondaryVertices",
          pf_candidates = "packedPFCandidates",
          jets = "updatedJetsAK8WithUserData",
          puppi_value_map = "",
          vertex_associator = ""
      )

      process.pfParticleNetMassRegressionJetTags = boostedJetONNXJetTagsProducer.clone(
          src = 'pfParticleNetAK8JetTagInfos',
          preprocess_json = 'RecoBTag/Combined/data/ParticleNetAK8/MassRegression/V01/preprocess.json',
          model_path = 'RecoBTag/Combined/data/ParticleNetAK8/MassRegression/V01/particle-net.onnx',
          flav_names = ["mass"]
      )

      process.slimmedJetsAK8UpdatedPNET.discriminatorSources.extend(["pfParticleNetMassRegressionJetTags:mass"])

      process.finalJetsAK8.src = cms.InputTag("slimmedJetsAK8UpdatedPNET")
      process.lepInAK8JetVars.src = cms.InputTag("slimmedJetsAK8UpdatedPNET")
      process.fatJetTable.variables.PNET_Htt = Var("bDiscriminator('pfParticleNetAK8LastJetTags:probHtt')",float,precision=10)
      process.fatJetTable.variables.PNET_Htm = Var("bDiscriminator('pfParticleNetAK8LastJetTags:probHtm')",float,precision=10)
      process.fatJetTable.variables.PNET_Hte = Var("bDiscriminator('pfParticleNetAK8LastJetTags:probHte')",float,precision=10)
      process.fatJetTable.variables.PNET_Hbb = Var("bDiscriminator('pfParticleNetAK8LastJetTags:probHbb')",float,precision=10)
      process.fatJetTable.variables.PNET_Hcc = Var("bDiscriminator('pfParticleNetAK8LastJetTags:probHcc')",float,precision=10)
      process.fatJetTable.variables.PNET_Hqq = Var("bDiscriminator('pfParticleNetAK8LastJetTags:probHqq')",float,precision=10)
      process.fatJetTable.variables.PNET_Hgg = Var("bDiscriminator('pfParticleNetAK8LastJetTags:probHgg')",float,precision=10)
      process.fatJetTable.variables.PNET_QCD2hf = Var("bDiscriminator('pfParticleNetAK8LastJetTags:probQCD2hf')",float,precision=10)
      process.fatJetTable.variables.PNET_QCD1hf = Var("bDiscriminator('pfParticleNetAK8LastJetTags:probQCD1hf')",float,precision=10)
      process.fatJetTable.variables.PNET_QCD0hf = Var("bDiscriminator('pfParticleNetAK8LastJetTags:probQCD0hf')",float,precision=10)
      process.fatJetTable.variables.PNET_masscorr = Var("bDiscriminator('pfParticleNetAK8LastJetTags:masscorr')",float,precision=10)
      process.fatJetTable.variables.PNET_massregression = Var("bDiscriminator('pfParticleNetMassRegressionJetTags:mass')",float,precision=10)

      process.edTask.add(getattr(process,"slimmedJetsAK8UpdatedPNET"))
      process.edTask.add(getattr(process,"pfParticleNetAK8JetTagInfos"))
      process.edTask.add(getattr(process,"pfParticleNetMassRegressionJetTags"))
      process.edTask.add(getattr(process,"pfParticleNetAK8LastJetTags"))
      process.edTask.add(getattr(process,"pfParticleNetAK8LastJetTagInfos"))

    # For some reason these are lost when integrated as customization (even when not processing AK8), so re-add them to path.
    #process.edTask.add(getattr(process,"updatedPatJetsAK8WithDeepInfo"))
    #process.edTask.add(getattr(process,"selectedUpdatedPatJetsAK8WithDeepInfo"))
    #process.edTask.add(getattr(process,"updatedPatJetsTransientCorrectedAK8WithDeepInfo"))
    #process.edTask.add(getattr(process,"patJetCorrFactorsTransientCorrectedAK8WithDeepInfo"))
    #process.edTask.add(getattr(process,"pfParticleNetTagInfosAK8WithDeepInfo"))
    #process.edTask.add(getattr(process,"pfParticleNetMassRegressionJetTagsAK8WithDeepInfo"))
    #process.edTask.add(getattr(process,"patJetCorrFactorsAK8WithDeepInfo"))
    for key in process.__dict__.keys():
        if(type(getattr(process,key)).__name__=='EDProducer' or type(getattr(process,key)).__name__=='EDFilter') and "AK8WithDeepInfo" in key:
            process.edTask.add(getattr(process,key))

    process.edPath = cms.Path(process.edTask)

    # Schedule definition
    process.schedule = cms.Schedule(process.edPath,process.nanoAOD_step,process.endjob_step,process.NANOAODSIMoutput_step)

    return process
