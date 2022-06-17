# from Visual.HistTools import *
# from Studies.HHBTag.Utils import *
import ROOT
import numpy as np

import Common.BaselineSelection as Baseline

# #ROOT.EnableImplicitMT()
# _rootpath = os.path.abspath(os.path.dirname(__file__)+"/../../..")
# ROOT.gROOT.ProcessLine(".include "+_rootpath)
# particleFile = f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt"
# ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{particleFile}\");")
# ROOT.gROOT.SetBatch(True)
# ROOT.gStyle.SetOptStat(1111)
# import argparse


# snapshotOptions = ROOT.RDF.RSnapshotOptions()
# snapshotOptions.fMode = "UPDATE"
# snapshotOptions.fOverwriteIfExists=True


# if(args.inFile==''):
#     files=os.listdir(filesPath)
# else:
#     files=[f"{filesPath}/{args.inFile}"]
#     print(f"using the following files {files}")

# if(args.allMasses):
#     print("evaluating for all masses")

# outDir_prefix=f"{os.environ['ANALYSIS_PATH']}/data/output"
# if not os.path.exists(outDir_prefix):
#     os.makedirs(outDir_prefix)

# for file in files:
#     mass_start =  file.find('-')+1
#     mass_end = file.find('.root')
#     mass = file[ mass_start : mass_end]
#     if(args.allMasses==False and str(mass) not in args.masses.split(',')):
#         continue
#     print(f"evaluating for mass {mass} and file {file}")
#     df = ROOT.RDataFrame("Events", f"{filesPath}/{file}")

#     for ch in ['eTau']:#'muTau', 'tauTau']:
#         outDir=f"{outDir_prefix}/{ch}"
#         if not os.path.exists(outDir):
#             os.makedirs(outDir)
#         outFileName = args.outFile if(args.outFile!='') else f"output_{mass}.root"
#         outFile = f"{outDir}/{outFileName}"
#         print(outFile)
#         df_matched = DefineDataFrame(df, ch)
#         mpv = findMPV(df)
#         print(f"the mpv is {mpv}")
#         '''
#         df_matched = DefineDataFrame(df, ch)
#         df_GenJets_b = df_matched.Define("GenJet_b_PF", "RVecI GenJet_b_PF; for(int i =0 ; i<GenJet_partonFlavour.size(); i++){if (std::abs(GenJet_partonFlavour[i])==5){GenJet_b_PF.push_back(i);}} return GenJet_b_PF;").Define("GenJet_b_PF_size", "GenJet_b_PF.size()")

#         #df_withDaughters = GetDaughters(df_matched)
#         df_lastHadrons_fromHbb = df_matched.Define("lastHadronsIndices","GetLastHadrons(GenPart_pdgId, GenPart_genPartIdxMother, GenPart_daughters )")

#         df_Greater2_GenJets_b = df_GenJets_b.Filter("GenJet_b_PF_size>2")

#         df_Greater2_GenJets_b_2ClosestToMPVMass= df_Greater2_GenJets_b.Define("TwobJetsClosestToMPV", f"(FindTwoJetsClosestToMPV({mpv},GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass, GenJet_partonFlavour))").Define("Two_ClosestToMPV_bGenJets_invMass", "InvMassByIndices(TwobJetsClosestToMPV,GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass,GenJet_partonFlavour, true)")
#         hist_Greater2_GenJets_b_2ClosestToMPVMass = df_Greater2_GenJets_b_2ClosestToMPVMass.Histo1D(("Two_ClosestToMPV_bGenJets_invMass", "Two_ClosestToMPV_bGenJets_invMass;m_{jj} (GeV);N_{Events}", 30, 10, 350),"Two_ClosestToMPV_bGenJets_invMass").GetValue()

#         myfile = ROOT.TFile( outFile, 'RECREATE' )
#         hist_Greater2_GenJets_b_2ClosestToMPVMass.Write()
#         myfile.Close()

#         df_Greater2_GenJets_b_2ClosestToMPVMass.Snapshot("Events", outFile, "GenJet_b_PF_size",snapshotOptions)
#         '''

def createSkim(inFile, outFile):
    Baseline.Initialize()

    df = ROOT.RDataFrame("Events", inFile)

    df = Baseline.ApplyGenBaseline(df)
    df = Baseline.ApplyRecoBaseline0(df)
    df = Baseline.ApplyRecoBaseline1(df)
    df = Baseline.ApplyRecoBaseline2(df)
    df = df.Define('genChannel', 'genHttCand.channel()')
    df = df.Define('recoChannel', 'httCand.channel()')

    df = df.Filter("genChannel == recoChannel", "SameGenRecoChannels")
    df = df.Filter("GenRecoMatching(genHttCand, httCand, 0.2)", "SameGenRecoHTT")

    report = df.Report()
    channels = df.AsNumpy(['genChannel', 'recoChannel'])

    report.Print()

    ch, cnt = np.unique(channels['genChannel'], return_counts=True)
    print(ch)
    print(cnt)
    ch, cnt = np.unique(channels['recoChannel'], return_counts=True)
    print(ch)
    print(cnt)

    # print(df.Filter('genHttCand.channel() == Channel::eTau').Count().GetValue())
    # print(df.Filter('genHttCand.channel() == Channel::muTau').Count().GetValue())
    # print(df.Filter('genHttCand.channel() == Channel::tauTau').Count().GetValue())
    #df = Baseline.ApplyRecoBaselineL0(df)
    # df = ApplyRecoBaselineL1(df)
    # df = ApplyGenRecoMatch(df)
    # df = DefineDataFrame(df, ch)
    # mpv = findMPV(df)
    # print(f"the mpv is {mpv}")


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser()
    parser.add_argument('--inFile', type=str)
    parser.add_argument('--outFile', type=str)
    parser.add_argument('--particleFile', type=str,
                        default=f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt")
    args = parser.parse_args()

    ROOT.gROOT.SetBatch(True)
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "Common/GenTools.h"')
    ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{args.particleFile}\");")

    createSkim(args.inFile, args.outFile)