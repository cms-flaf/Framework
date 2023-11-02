import ROOT
import numpy as np
import Common.Utilities as Utilities
import Common.PrintGenChain as PrintGenChain
import Common.BaselineSelection as Baseline
import Studies.HHBTag.GetMPV as GetMPV
snapshotOptions = ROOT.RDF.RSnapshotOptions()
snapshotOptions.fOverwriteIfExists=True


def truthStudies(inFile, X_mass, mpv, run_nonClosest=False):
    Baseline.Initialize()

    df = ROOT.RDataFrame("Events", inFile)
    mpv = GetMPV.GetMPV(inFile)
    df = Baseline.DefineGenObjects(df, mpv)
    df = df.Define("GenJet_Hbb_v2","FindTwoJetsClosestToMPV(125., GenJet_p4, GenJet_b_PF)")
    df = df.Filter("GenJet_idx[GenJet_b_PF].size()>2")
    df = df.Define("n_overlaps", "GenJet_idx[GenJet_Hbb && GenJet_Hbb_v2].size()")
    h0 = df.Histo1D("n_overlaps")
    print(X_mass, h0.GetValue().GetBinContent(h0.GetValue().FindBin(2.))/ h0.GetValue().GetEntries())
    if run_nonClosest:
        df = df.Define("GenJet_idx_NotClosest", "GenJet_idx[GenJet_b_PF && !GenJet_Hbb]")
        df = df.Define("GenJet_idx_Closest", "GenJet_idx[GenJet_Hbb]")

        df = df.Define("invariantMass", "RVecF inv_mass; for(auto& i : GenJet_idx_NotClosest){for(auto& j : GenJet_idx_Closest){inv_mass.push_back((GenJet_p4[i]+GenJet_p4[j]).M())} } ")
        h0_1 = df.Histo1D("invariantMass")

    df = df.Define("n_GenJet", "GenJet_idx[GenJet_b_PF].size()")
    df = df.Define("n_GenJetAK8", "GenJetAK8_idx[GenJetAK8_b_PF].size()")
    df = df.Define("GenJet_b_invMass", "GenJet_p4[GenJet_b_PF][0].M()").Define("GenJetAK8_b_invMass", "GenJetAK8_p4[GenJetAK8_b_PF][0].M()").Define("Hbb_pt", "GenPart_pt[genHbbIdx]")
    h1 = df.Filter("n_GenJet==1").Histo1D(("GenJet_b_invMass_1GenJet", "GenJet_b_invMass_1GenJet",100, 0, X_mass),"GenJet_b_invMass")
    h2 = df.Filter("n_GenJet==2").Histo1D(("GenJet_b_invMass_2GenJet", "GenJet_b_invMass_2GenJet",100, 0, X_mass),"GenJet_b_invMass")
    h3 = df.Filter("n_GenJetAK8==1").Histo1D(("GenJet_b_invMass_1GenJetAK8", "GenJet_b_invMass_1GenJetAK8",100, 0, X_mass),"GenJet_b_invMass")
    h4 = df.Filter("n_GenJetAK8==2").Histo1D(("GenJet_b_invMass_2GenJetAK8", "GenJet_b_invMass_2GenJetAK8",100, 0, X_mass),"GenJet_b_invMass")
    h5 = df.Filter("n_GenJet==1").Histo1D(("Hbb_pt_1GenJet", "Hbb_pt_1GenJet",100, 0, X_mass),"Hbb_pt")
    h6 = df.Filter("n_GenJet==2").Histo1D(("Hbb_pt_2GenJet", "Hbb_pt_2GenJet",100, 0, X_mass),"Hbb_pt")
    h7 = df.Filter("n_GenJetAK8==1").Histo1D(("Hbb_pt_1GenJetAK8", "Hbb_pt_1GenJetAK8",100, 0, X_mass),"Hbb_pt")
    h8 = df.Filter("n_GenJetAK8==2").Histo1D(("Hbb_pt_2GenJetAK8", "Hbb_pt_2GenJetAK8",100, 0, X_mass),"Hbb_pt")
    h9 = df.Histo1D(f"n_GenJet")
    h10 = df.Histo1D(f"n_GenJetAK8")
    scatter_plot1 = df.Histo2D(("n_GenJetVSHbb_pt", "Hbb_ptVSGenJet_b_invMass", 100, 0., X_mass, 5, -0.5, 5.5),"Hbb_pt", "n_GenJet")
    scatter_plot2 = df.Histo2D(("n_GenJetAK8VSHbb_pt", "Hbb_ptVSGenJetAK8_b_invMass", 100, 0., X_mass, 4, -0.5, 3.5),"Hbb_pt", "n_GenJetAK8")
    df = df.Filter("n_GenJet==1 && n_GenJetAK8 == 1")
    scatter_plot3 = df.Histo2D(("Hbb_ptVSGenJet_b_invMass", "Hbb_ptVSGenJet_b_invMass", 100, 0., X_mass, 100, 0., 250.), "Hbb_pt", "GenJet_b_invMass")
    scatter_plot4 = df.Histo2D(("Hbb_ptVSGenJetAK8_b_invMass", "Hbb_ptVSGenJetAK8_b_invMass", 100, 0., X_mass, 100, 0., 250.), "Hbb_pt", "GenJetAK8_b_invMass")

    histFile = ROOT.TFile(f"output/GenJets_b_mass_m{X_mass}.root", "RECREATE")
    h0.GetValue().Write()
    if run_nonClosest:
        h0_1.GetValue().Write()
    h1.GetValue().Write()
    h2.GetValue().Write()
    h3.GetValue().Write()
    h4.GetValue().Write()
    h5.GetValue().Write()
    h6.GetValue().Write()
    h7.GetValue().Write()
    h8.GetValue().Write()
    h9.GetValue().Write()
    h10.GetValue().Write()

    scatter_plot1.GetValue().Write()
    scatter_plot2.GetValue().Write()
    scatter_plot3.GetValue().Write()
    scatter_plot4.GetValue().Write()

    histFile.Close()

if __name__ == "__main__":
    import argparse
    import os
    import re

    parser = argparse.ArgumentParser()
    parser.add_argument('--inFile', type=str)
    parser.add_argument('--mass', type=int)
    parser.add_argument('--mpv', type=float, default=122.8)
    parser.add_argument('--compressionLevel', type=int, default=9)
    parser.add_argument('--compressionAlgo', type=str, default="LZMA")
    parser.add_argument('--particleFile', type=str,
                        default=f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt")
    args = parser.parse_args()


    ROOT.gROOT.SetBatch(True)
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "include/GenTools.h"')
    ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{args.particleFile}\");")
    snapshotOptions = ROOT.RDF.RSnapshotOptions()
    snapshotOptions.fOverwriteIfExists=True
    snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + args.compressionAlgo)
    snapshotOptions.fCompressionLevel = args.compressionLevel

    truthStudies(args.inFile, args.mass, args.mpv, snapshotOptions)
    createSkim(args.inFile, args.outFile, args.period, args.sample, args.mass, args.mpv, snapshotOptions)
