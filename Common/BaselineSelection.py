import ROOT
import os
from .Utilities import *

initialized = False

ana_reco_object_collections = {
    "v12": [ "Electron", "Muon", "Tau", "Jet", "FatJet", "MET", "PuppiMET", "DeepMETResponseTune",
             "DeepMETResolutionTune", "SubJet" ],
    "v14": [ "Electron", "Muon", "Tau", "Jet", "FatJet", "PFMET", "PuppiMET", "DeepMETResponseTune",
             "DeepMETResolutionTune", "SubJet" ],
}
deepTauVersions = {"2p1":"2017", "2p5":"2018"}

def Initialize(loadTF=False, loadHHBtag=False):
    global initialized
    if not initialized:
        headers_dir = os.path.dirname(os.path.abspath(__file__))
        ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
        header_path_RootExt ="include/RootExt.h"
        header_path_GenLepton ="include/GenLepton.h"
        header_path_Gen ="include/BaselineGenSelection.h"
        header_path_Reco ="include/BaselineRecoSelection.h"
        header_path_HHbTag ="include/HHbTagScores.h"
        ROOT.gInterpreter.Declare(f'#include "{header_path_RootExt}"')
        ROOT.gInterpreter.Declare(f'#include "{header_path_GenLepton}"')
        ROOT.gInterpreter.Declare(f'#include "{header_path_Gen}"')
        ROOT.gInterpreter.Declare(f'#include "{header_path_Reco}"')
        for wpcl in [WorkingPointsTauVSe,WorkingPointsTauVSmu,WorkingPointsTauVSjet,WorkingPointsbTag, WorkingPointsMuonID]:
            ROOT.gInterpreter.Declare(f'{generate_enum_class(wpcl)}')
        if(loadTF):
            import RunKit.includeCMSSWlibs as IncludeLibs
            IncludeLibs.includeLibTool("tensorflow")
        if(loadHHBtag):
            lib_path = os.path.join(os.environ["FLAF_CMSSW_BASE"], "lib", os.environ["FLAF_CMSSW_ARCH"],
                                    "libHHToolsHHbtag.so")
            load_result = ROOT.gSystem.Load(lib_path)
            if load_result != 0:
                raise RuntimeError(f"HHBtagWrapper failed to load with status {load_result}")
            ROOT.gInterpreter.Declare(f'#include "{header_path_HHbTag}"')
            ROOT.gROOT.ProcessLine(f'HHBtagWrapper::Initialize("{os.environ["CMSSW_BASE"]}/src/HHTools/HHbtag/models/", 2)')

        initialized = True

def applyMETFlags(df, MET_flags):
    MET_flags_string = ' && '.join(MET_flags)
    return df.Filter(MET_flags_string, "MET filters")

def DefineGenObjects(df, isData=False, isHH=False, Hbb_AK4mass_mpv=125., p4_suffix='nano'):
    if isData:
        df = df.Define("genLeptons", "std::vector<reco_tau::gen_truth::GenLepton>()")
    else:
        df = df.Define("GenPart_daughters", "GetDaughters(GenPart_genPartIdxMother)")
        df = df.Define("genLeptons", """reco_tau::gen_truth::GenLepton::fromNanoAOD(GenPart_pt, GenPart_eta,
                                        GenPart_phi, GenPart_mass, GenPart_genPartIdxMother, GenPart_pdgId,
                                        GenPart_statusFlags, event)""")

    for lep in ["Electron", "Muon", "Tau"]:
        df = df.Define(f"{lep}_genMatchIdx",  f"MatchGenLepton({lep}_p4_{p4_suffix}, genLeptons, 0.2)")
        df = df.Define(f"{lep}_genMatch",  f"GetGenLeptonMatch({lep}_genMatchIdx, genLeptons)")
    if isData:
        return df

    if isHH:
        df = df.Define("genHttCandidate", """GetGenHTTCandidate(event, GenPart_pdgId, GenPart_daughters, GenPart_statusFlags, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, false)""")
        df = df.Define("genHbbIdx", """GetGenHBBIndex(event, GenPart_pdgId, GenPart_daughters, GenPart_statusFlags)""")
        df = df.Define("genHbb_isBoosted", "GenPart_pt[genHbbIdx]>550")
    for var in ["GenJet", "GenJetAK8", "SubGenJetAK8"]:
        df = df.Define(f"{var}_idx", f"CreateIndexes({var}_pt.size())")
        df = df.Define(f"{var}_p4", f"GetP4({var}_pt,{var}_eta,{var}_phi,{var}_mass, {var}_idx)")

    df = df.Define("GenJet_b_PF", "abs(GenJet_partonFlavour)==5")
    df = df.Define("GenJetAK8_b_PF", "abs(GenJetAK8_partonFlavour)==5")
    df = df.Define("GenJet_Hbb",f"FindTwoJetsClosestToMPV({Hbb_AK4mass_mpv}, GenJet_p4, GenJet_b_PF)")
    df = df.Define("GenJetAK8_Hbb", "FindGenJetAK8(GenJetAK8_mass, GenJetAK8_b_PF)")

    return df

def SelectRecoP4(df, syst_name='nano', nano_version="v12"):
    for obj in ana_reco_object_collections[nano_version]:
        df = df.Define(f"{obj}_p4", f"{obj}_p4_{syst_name}")
    return df

def CreateRecoP4(df, suffix='nano', nano_version="v12"):
    if len(suffix) > 0:
        suffix = "_" + suffix
    if("TrigObj_pt" in df.GetColumnNames()):
        df = df.Define(f"TrigObj_idx", f"CreateIndexes(TrigObj_pt.size())")
        if "TrigObj_mass" not in df.GetColumnNames():
            df = df.Define("TrigObj_mass", "RVecF(TrigObj_pt.size(), 0.f)")
        df = df.Define(f"TrigObj_p4", f"GetP4(TrigObj_pt,TrigObj_eta,TrigObj_phi, TrigObj_mass, TrigObj_idx)")
    for obj in ana_reco_object_collections[nano_version]:
        if "MET" in obj:
            df = df.Define(f"{obj}_p4{suffix}", f"LorentzVectorM({obj}_pt, 0., {obj}_phi, 0.)")
        else:
            df = df.Define(f"{obj}_idx", f"CreateIndexes({obj}_pt.size())")
            df = df.Define(f"{obj}_p4{suffix}", f"GetP4({obj}_pt, {obj}_eta, {obj}_phi, {obj}_mass, {obj}_idx)")
    return df
