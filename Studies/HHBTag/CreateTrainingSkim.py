import ROOT
import numpy as np
import sys
import os
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])
import Common.Utilities as Utilities
import Common.ReportTools as ReportTools
import yaml
import Common.BaselineSelection as Baseline


jetVar_list = [ "pt", "eta", "phi", "mass", "btagDeepFlavB", "particleNetAK4_B", "btagDeepFlavCvB", "btagDeepFlavCvL", "btagDeepFlavQG", "particleNetAK4_CvsB", "particleNetAK4_CvsL", "particleNetAK4_QvsG", "HHBtagScore", "bRegRes", "genMatched", "hadronFlavour"]
def JetSavingCondition(df):
    df = df.Define('Jet_selIdx', 'ReorderObjects(Jet_btagDeepFlavB, Jet_idx[Jet_bCand])')
    for var in jetVar_list:
        df = df.Define(f"RecoJet_{var}", f"Take(Jet_{var}, Jet_selIdx)")
    return df
    
# genjetVar_list = ["pt","eta","phi","mass","hadronFlavour"]
# def GenJetSavingCondition(df):
    # for genvar in genjetVar_list:
        # df = df.Define(f"genjet_{genvar}",f"Take(GenJet_{genvar}, GenJet_idx)")
    # return df

def createSkim(inFile, outFile, period, sample, X_mass, node_index, mpv, config, snapshotOptions):
    Baseline.Initialize(True, True)

    df = ROOT.RDataFrame("Events", inFile)
    # df = df.Range(10)
    df = Baseline.CreateRecoP4(df)
    df = Baseline.SelectRecoP4(df)
    df = Baseline.DefineGenObjects(df, isHH=True, Hbb_AK4mass_mpv=mpv)

    df = df.Define("n_GenJet", "GenJet_idx.size()")
    df = Baseline.PassGenAcceptance(df)
    df = Baseline.GenJetSelection(df)
    df = Baseline.GenJetHttOverlapRemoval(df)
    df = Baseline.RequestOnlyResolvedGenJets(df)

    df = Baseline.RecoLeptonsSelection(df)
    # df = Baseline.RecoJetAcceptance(df)
    df = Baseline.RecoHttCandidateSelection(df, config["GLOBAL"])
    df = Baseline.RecoJetSelection(df)

    df = df.Define('genChannel', 'genHttCandidate->channel()')
    df = df.Define('recoChannel', 'HttCandidate.channel()')

    df = df.Filter("genChannel == recoChannel", "SameGenRecoChannels")
    df = df.Filter("GenRecoMatching(*genHttCandidate, HttCandidate, 0.2)", "SameGenRecoHTT")
    # df = Baseline.RequestOnlyResolvedRecoJets(df)

    df = Baseline.GenRecoJetMatching(df)
    df = df.Define("sample", f"static_cast<int>(SampleType::{sample})")
    df = df.Define("period", f"static_cast<int>(Period::Run2_{period})")
    df = df.Define("X_mass", f"static_cast<int>({X_mass})")
    df = df.Define("node_index", f"static_cast<int>({node_index})")

    df = Baseline.DefineHbbCand(df)

    df = df.Define("HttCandidate_leg0_pt", "HttCandidate.leg_p4[0].Pt()")
    df = df.Define("HttCandidate_leg0_eta", "HttCandidate.leg_p4[0].Eta()")
    df = df.Define("HttCandidate_leg0_phi", "HttCandidate.leg_p4[0].Phi()")
    df = df.Define("HttCandidate_leg0_mass", "HttCandidate.leg_p4[0].M()")
    df = df.Define("HttCandidate_leg1_pt", "HttCandidate.leg_p4[1].Pt()")
    df = df.Define("HttCandidate_leg1_eta", "HttCandidate.leg_p4[1].Eta()")
    df = df.Define("HttCandidate_leg1_phi", "HttCandidate.leg_p4[1].Phi()")
    df = df.Define("HttCandidate_leg1_mass", "HttCandidate.leg_p4[1].M()")
    df = df.Define("channel", "static_cast<int>(genChannel)")
    n_MoreThanTwoMatches = df.Filter("Jet_idx[Jet_genMatched].size()>2").Count()
    df = JetSavingCondition(df)
    # df = GenJetSavingCondition(df)

    report = df.Report()
    histReport=ReportTools.SaveReport(report.GetValue())
    if(n_MoreThanTwoMatches.GetValue()!=0) :
        raise RuntimeError('There are more than two jets matched! ')

    colToSave = ["event","luminosityBlock",
                "HttCandidate_leg0_pt", "HttCandidate_leg0_eta", "HttCandidate_leg0_phi", "HttCandidate_leg0_mass", "HttCandidate_leg1_pt", "HttCandidate_leg1_eta", "HttCandidate_leg1_phi","HttCandidate_leg1_mass",
                "channel","sample","period","X_mass", "node_index", "MET_pt", "MET_phi", "PuppiMET_pt", "PuppiMET_phi","DeepMETResolutionTune_pt", "DeepMETResolutionTune_phi","DeepMETResponseTune_pt", "DeepMETResponseTune_phi"]

    colToSave+=[f"RecoJet_{var}" for var in jetVar_list]
    # colToSave+=[f"genjet_{genvar}" for genvar in genjetVar_list]
    colToSave+=["GenJet_b_PF", "GenJetAK8_b_PF", "GenJet_Hbb" , "GenJetAK8_Hbb", "GenJet_idx"]

    varToSave = Utilities.ListToVector(colToSave)
    df.Snapshot("Event", outFile, varToSave, snapshotOptions)
    outputRootFile= ROOT.TFile(outFile, "UPDATE")
    outputRootFile.WriteTObject(histReport, "Report", "Overwrite")
    outputRootFile.Close()



if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser()
    parser.add_argument('--period', type=str)
    parser.add_argument('--inFile', type=str)
    parser.add_argument('--outFile', type=str)
    parser.add_argument('--mass', type=int)
    parser.add_argument('--node_index', type=int, default=-1)
    parser.add_argument('--config', required=True, type=str)
    parser.add_argument('--mpv', type=float, default=125)
    parser.add_argument('--sample', type=str)
    parser.add_argument('--compressionLevel', type=int, default=9)
    parser.add_argument('--compressionAlgo', type=str, default="LZMA")
    parser.add_argument('--particleFile', type=str,
                        default=f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt")
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)

    ROOT.gROOT.SetBatch(True)
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "include/GenTools.h"')
    ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{args.particleFile}\");")
    snapshotOptions = ROOT.RDF.RSnapshotOptions()
    snapshotOptions.fOverwriteIfExists=True
    snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + args.compressionAlgo)
    snapshotOptions.fCompressionLevel = args.compressionLevel
    createSkim(args.inFile, args.outFile, args.period, args.sample, args.mass, args.node_index, args.mpv, config, snapshotOptions)
