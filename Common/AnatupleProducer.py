import ROOT
import numpy as np
import Common.BaselineSelection as Baseline
import Common.Utilities as Utilities
import Common.ReportTools as ReportTools
import Common.triggerSel as Triggers
import Corrections.Corrections as Corrections

deepTauScores= ["rawDeepTau2017v2p1VSe","rawDeepTau2017v2p1VSmu",
            "rawDeepTau2017v2p1VSjet", "rawDeepTau2018v2p5VSe", "rawDeepTau2018v2p5VSmu",
            "rawDeepTau2018v2p5VSjet",
            "idDeepTau2017v2p1VSe", "idDeepTau2017v2p1VSjet", "idDeepTau2017v2p1VSmu",
            "idDeepTau2018v2p5VSe","idDeepTau2018v2p5VSjet","idDeepTau2018v2p5VSmu",
            "decayMode"]
JetObservables = ["particleNetAK4_B", "particleNetAK4_CvsB",
                "particleNetAK4_CvsL","particleNetAK4_QvsG","particleNetAK4_puIdDisc",
                "btagDeepFlavB","btagDeepFlavCvB","btagDeepFlavCvL"]
JetObservablesMC = ["hadronFlavour","partonFlavour"]

colToSave = ["event","luminosityBlock","run",
                "MET_pt", "MET_phi","PuppiMET_pt", "PuppiMET_phi",
                "DeepMETResolutionTune_pt", "DeepMETResolutionTune_phi","DeepMETResponseTune_pt", "DeepMETResponseTune_phi",
                "MET_covXX", "MET_covXY", "MET_covYY", "PV_npvs",
                "Electron_genMatchIdx", "Muon_genMatchIdx","Tau_genMatchIdx" ]

def DefineAndAppend(df, varToDefine, varToCall):
    df = df.Define(f"{varToDefine}", f"{varToCall}")
    colToSave.append(varToDefine)
    return df

def addAllVariables(df, syst_name, isData, trigger_class):
    df = Baseline.SelectRecoP4(df, syst_name)
    df = Baseline.RecoLeptonsSelection(df)
    df = Baseline.RecoJetAcceptance(df)
    df = Baseline.RecoHttCandidateSelection(df)
    df = Baseline.RecoJetSelection(df)
    df = Baseline.RequestOnlyResolvedRecoJets(df)
    df = Baseline.ThirdLeptonVeto(df)
    df = Baseline.DefineHbbCand(df)
    if trigger_class is not None:
        df,hltBranches = trigger_class.ApplyTriggers(df, isData)
        colToSave.extend(hltBranches)
    df = DefineAndAppend(df, f"Tau_recoJetMatchIdx", f"FindMatching(Tau_p4, Jet_p4, 0.5)")
    df = DefineAndAppend(df, f"Muon_recoJetMatchIdx", f"FindMatching(Muon_p4, Jet_p4, 0.5)")
    df = DefineAndAppend(df, f"Electron_recoJetMatchIdx", f"FindMatching(Electron_p4, Jet_p4, 0.5)")
    df = DefineAndAppend(df,"channelId","static_cast<int>(httCand.channel())")
    jet_obs = JetObservables
    if not isData:
        jet_obs.extend(JetObservablesMC)
        colToSave.append("LHE_HT")
    for leg_idx in [0,1]:
        df = DefineAndAppend(df, f"tau{leg_idx+1}_pt", f"static_cast<float>(httCand.leg_p4[{leg_idx}].Pt())")
        df = DefineAndAppend(df, f"tau{leg_idx+1}_eta", f"static_cast<float>(httCand.leg_p4[{leg_idx}].Eta())")
        df = DefineAndAppend(df,f"tau{leg_idx+1}_phi", f"static_cast<float>(httCand.leg_p4[{leg_idx}].Phi())")
        df = DefineAndAppend(df,f"tau{leg_idx+1}_mass", f"static_cast<float>(httCand.leg_p4[{leg_idx}].M())")
        df = DefineAndAppend(df,f"tau{leg_idx+1}_charge", f"httCand.leg_charge[{leg_idx}]")
        df = df.Define(f"tau{leg_idx+1}_idx", f"httCand.leg_index[{leg_idx}]")
        df = df.Define(f"tau{leg_idx+1}_genMatchIdx", f"httCand.leg_genMatchIdx[{leg_idx}]")
        df = df.Define(f"tau{leg_idx+1}_recoJetMatchIdx", f"FindMatching(httCand.leg_p4[{leg_idx}], Jet_p4, 0.3)")
        df = DefineAndAppend(df, f"tau{leg_idx+1}_iso", f"httCand.leg_rawIso.at({leg_idx})")
        for deepTauScore in deepTauScores:
            df = DefineAndAppend(df, f"tau{leg_idx+1}_{deepTauScore}",
                                     f"httCand.leg_type[{leg_idx}] == Leg::tau ? Tau_{deepTauScore}.at(httCand.leg_index[{leg_idx}]) : -1;")

        if not isData:
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_kind", f"""tau{leg_idx+1}_genMatchIdx>=0 ? static_cast<int>(genLeptons.at(tau{leg_idx+1}_genMatchIdx).kind()) :
                                              static_cast<int>(GenLeptonMatch::NoMatch);""")
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_vis_pt", f"""tau{leg_idx+1}_genMatchIdx>=0? static_cast<float>(genLeptons.at(tau{leg_idx+1}_genMatchIdx).visibleP4().Pt()) :
                                                    -1.;""")
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_vis_eta", f"""tau{leg_idx+1}_genMatchIdx>=0? static_cast<float>(genLeptons.at(tau{leg_idx+1}_genMatchIdx).visibleP4().Eta()) :
                                                        -1.;""")
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_vis_phi", f"""tau{leg_idx+1}_genMatchIdx>=0? static_cast<float>(genLeptons.at(tau{leg_idx+1}_genMatchIdx).visibleP4().Phi()) :
                                                        -1.;""")
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_vis_mass", f"""tau{leg_idx+1}_genMatchIdx>=0? static_cast<float>(genLeptons.at(tau{leg_idx+1}_genMatchIdx).visibleP4().M()) :
                                                    -1.;""")
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_nChHad", f"""tau{leg_idx+1}_genMatchIdx>=0? genLeptons.at(tau{leg_idx+1}_genMatchIdx).nChargedHadrons() : 0;""")
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_nNeutHad", f"""tau{leg_idx+1}_genMatchIdx>=0? genLeptons.at(tau{leg_idx+1}_genMatchIdx).nNeutralHadrons() : 0;""")
            df = DefineAndAppend(df,f"tau{leg_idx+1}_gen_charge", f"""tau{leg_idx+1}_genMatchIdx>=0? genLeptons.at(tau{leg_idx+1}_genMatchIdx).charge() : -10;""")
            df = DefineAndAppend(df, f"tau{leg_idx+1}_seedingJet_partonFlavour",
                                        f"tau{leg_idx+1}_recoJetMatchIdx>=0 ? Jet_partonFlavour.at(tau{leg_idx+1}_recoJetMatchIdx) : -1;")
            df = DefineAndAppend(df, f"tau{leg_idx+1}_seedingJet_hadronFlavour",
                                    f"tau{leg_idx+1}_recoJetMatchIdx>=0 ? Jet_hadronFlavour.at(tau{leg_idx+1}_recoJetMatchIdx) : -1;")

        df = DefineAndAppend(df, f"tau{leg_idx+1}_seedingJet_pt",
                                    f"tau{leg_idx+1}_recoJetMatchIdx>=0 ? Jet_p4.at(tau{leg_idx+1}_recoJetMatchIdx).Pt() : -1;")
        df = DefineAndAppend(df, f"tau{leg_idx+1}_seedingJet_eta",
                                    f"tau{leg_idx+1}_recoJetMatchIdx>=0 ? Jet_p4.at(tau{leg_idx+1}_recoJetMatchIdx).Eta() : -1;")
        df = DefineAndAppend(df, f"tau{leg_idx+1}_seedingJet_phi",
                                    f"tau{leg_idx+1}_recoJetMatchIdx>=0 ? Jet_p4.at(tau{leg_idx+1}_recoJetMatchIdx).Phi() : -1;")
        df = DefineAndAppend(df, f"tau{leg_idx+1}_seedingJet_mass",
                                    f"tau{leg_idx+1}_recoJetMatchIdx>=0 ? Jet_p4.at(tau{leg_idx+1}_recoJetMatchIdx).M() : -1;")


        df = DefineAndAppend(df,f"b{leg_idx+1}_pt", f"HbbCandidate.leg_p4[{leg_idx}].Pt()")
        df = DefineAndAppend(df,f"b{leg_idx+1}_eta", f"HbbCandidate.leg_p4[{leg_idx}].Eta()")
        df = DefineAndAppend(df,f"b{leg_idx+1}_phi", f"HbbCandidate.leg_p4[{leg_idx}].Phi()")
        df = DefineAndAppend(df,f"b{leg_idx+1}_mass", f"HbbCandidate.leg_p4[{leg_idx}].M()")

        for jetVar in jet_obs:
            if(jetVar not in df.GetColumnNames()): continue
            df = DefineAndAppend(df,f"b{leg_idx+1}_{jetVar}", f"Jet_{jetVar}.at(HbbCandidate.leg_index[{leg_idx}])")
        df = DefineAndAppend(df,f"b{leg_idx+1}_HHbtag", f"Jet_HHBtagScore.at(HbbCandidate.leg_index[{leg_idx}])")
    return df


def createAnatuple(inFile, outFile, period, sample, X_mass, snapshotOptions,range, isData, evtIds, isHH, triggerFile,
                   store_noncentral):
    Baseline.Initialize(True, True)
    if not isData:
        Corrections.Initialize(period=period)

    trigger_class = Triggers.Triggers(triggerFile) if triggerFile is not None else None
    df = ROOT.RDataFrame("Events", inFile)
    if range is not None:
        df = df.Range(range)
    if len(evtIds) > 0:
        df = df.Filter(f"static const std::set<ULong64_t> evts = {{ {evtIds} }}; return evts.count(event) > 0;")
    df = DefineAndAppend(df,"sample_type", f"static_cast<int>(SampleType::{sample})")
    df = DefineAndAppend(df,"period", f"static_cast<int>(Period::{period})")
    df = DefineAndAppend(df,"X_mass", f"static_cast<int>({X_mass})")
    is_data = 'true' if isData else 'false'
    df = DefineAndAppend(df,"is_data", is_data)

    df = Baseline.CreateRecoP4(df)
    df = Baseline.DefineGenObjects(df, isData=isData, isHH=isHH)
    if isData:
        syst_dict = { 'nano' : 'Central' }
    else:
        df, syst_dict = Corrections.applyScaleUncertainties(df)

    df,weights = Corrections.getWeights(df)
    for syst_name, source_name in syst_dict.items():
        suffix = '' if syst_name in [ 'Central', 'nano' ] else f'_{syst_name}'
        if len(suffix) and not store_noncentral: continue
        df_syst = addAllVariables(df, syst_name, isData, trigger_class)
        report = df_syst.Report()
        histReport = ReportTools.SaveReport(report.GetValue(), reoprtName=f"Report{suffix}")
        varToSave = Utilities.ListToVector(colToSave)
        df_syst.Snapshot(f"Events{suffix}", outFile, varToSave, snapshotOptions)
        outputRootFile= ROOT.TFile(outFile, "UPDATE")
        outputRootFile.WriteTObject(histReport, f"Report{suffix}", "Overwrite")
        outputRootFile.Close()

if __name__ == "__main__":
    import argparse
    import os
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--period', type=str)
    parser.add_argument('--inFile', type=str)
    parser.add_argument('--outFile', type=str)
    parser.add_argument('--mass', type=int, default=-1)
    parser.add_argument('--sample_type', type=str)
    parser.add_argument('--compressionLevel', type=int, default=9)
    parser.add_argument('--compressionAlgo', type=str, default="LZMA")
    parser.add_argument('--nEvents', type=int, default=None)
    parser.add_argument('--evtIds', type=str, default='')
    parser.add_argument('--store-noncentral', action="store_true", help="Store ES variations.")
    parser.add_argument('--triggerFile', type=str, default=None)

    args = parser.parse_args()

    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "Common/GenTools.h"')
    isHH=False
    isData = False
    hh_samples = [ "GluGluToRadion", "GluGluToBulkGraviton", "VBFToRadion", "VBFToBulkGraviton", "HHnonRes", "TTHH" ]
    if args.mass>0 and args.sample_type in hh_samples:
        isHH = True
    if args.sample_type =='data':
        isData = True
    if os.path.exists(args.outFile):
        os.remove(args.outFile)

    snapshotOptions = ROOT.RDF.RSnapshotOptions()
    snapshotOptions.fOverwriteIfExists=True
    snapshotOptions.fMode="UPDATE"
    snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + args.compressionAlgo)
    snapshotOptions.fCompressionLevel = args.compressionLevel
    createAnatuple(args.inFile, args.outFile, args.period, args.sample_type, args.mass, snapshotOptions, args.nEvents,
                   isData, args.evtIds, isHH, args.triggerFile, args.store_noncentral)
