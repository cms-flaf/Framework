import ROOT
import numpy as np
import Common.Utilities as Utilities

import Common.BaselineSelection as Baseline
def FindMPV(df):
    df = df.Define("bJetp4", " GenJet_p4[GenJet_B1]").Filter("bJetp4.size()==2").Filter("ROOT::Math::VectorUtil::DeltaR(bJetp4[0],bJetp4[1])>0.4").Define("Two_bGenJets_invMass", "(bJetp4[0]+bJetp4[1]).M()")
    histo = df.Histo1D(("Two_bGenJets_invMass", "Two_bGenJets_invMass", 801, -0.25, 400.25),"Two_bGenJets_invMass").GetValue()
    y_max = histo.GetMaximumBin()
    x_max = histo.GetXaxis().GetBinCenter(y_max)
    return x_max

def GetMPV(inFile):
    Baseline.Initialize()

    df = ROOT.RDataFrame("Events", inFile)
    df = Baseline.DefineGenObjects(df, 125.)
    df = df.Define("GenJet_B1","GenJet_pt > 20 && abs(GenJet_eta) < 2.5 && GenJet_b_PF")
    #df = df.Define("GenJet_B1","GenJet_pt > 50 && abs(GenJet_eta) < 2.5 && GenJet_b_PF")
    df = Baseline.PassGenAcceptance(df)
    x_max = FindMPV(df)
    return x_max


if __name__ == "__main__":
    import argparse
    import os
    import re

    parser = argparse.ArgumentParser()
    parser.add_argument('--inFile', type=str)
    parser.add_argument('--particleFile', type=str,
                        default=f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt")
    args = parser.parse_args()


    ROOT.gROOT.SetBatch(True)
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "include/GenTools.h"')
    ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{args.particleFile}\");")
    mpv = GetMPV(args.inFile)
    print(f"the mpv is {mpv}")
