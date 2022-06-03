from Common.BaselineSelection import *
from Visual.HistTools import *
from Studies.HHBTag.Utils import *
# Enable multi-threading
import ROOT
ROOT.EnableImplicitMT()
ROOT.gROOT.SetBatch(True)
ROOT.gStyle.SetOptStat(1111)
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--masses', type=str, default = '300,500,800,1000')
parser.add_argument('--allMasses', type=bool, default = False)
parser.add_argument('--outFile', type=str, default = '')
parser.add_argument('--inFile', type=str, default = '')

args = parser.parse_args()

filesPath=f"{os.environ['ANALYSIS_PATH']}/data/nanoAOD"
snapshotOptions = ROOT.RDF.RSnapshotOptions()
snapshotOptions.fMode = "UPDATE"
snapshotOptions.fOverwriteIfExists=True


if(args.inFile==''):
    files=os.listdir(filesPath)
else:
    files=[f"{filesPath}/{args.inFile}"]
    print(f"using the following files {files}")

if(args.allMasses):
    print("evaluating for all masses")

outDir_prefix=f"{os.environ['ANALYSIS_PATH']}/data/output"
if not os.path.exists(outDir_prefix):
    os.makedirs(outDir_prefix)

for file in files:
    mass_start =  file.find('-')+1
    mass_end = file.find('.root')
    mass = file[ mass_start : mass_end]
    if(args.allMasses==False and str(mass) not in args.masses.split(',')):
        continue
    print(f"evaluating for mass {mass} and file {file}")
    df = ROOT.RDataFrame("Events", f"{filesPath}/{file}")

    for ch in ['eTau']:#'muTau', 'tauTau']:
        outDir=f"{outDir_prefix}/{ch}"
        if not os.path.exists(outDir):
            os.makedirs(outDir)
        outFileName = args.outFile if(args.outFile!='') else f"output_{mass}.root"
        outFile = f"{outDir}/{outFileName}"
        print(outFile)
        mpv = findMPV(df)
        print(f"the mpv is {mpv}")
        df_matched = DefineDataFrame(df, ch)


        df_GenJets_b = df_matched.Define("GenJet_b_PF", "RVecI GenJet_b_PF; for(int i =0 ; i<GenJet_partonFlavour.size(); i++){if (std::abs(GenJet_partonFlavour[i])==5){GenJet_b_PF.push_back(i);}} return GenJet_b_PF;").Define("GenJet_b_PF_size", "GenJet_b_PF.size()")


        df_lastHadrons_fromHbb = df_matched.Define("lastHadronsIndices","GetLastHadrons(GenPart_pdgId, GenPart_genPartIdxMother )")


        df_Greater2_GenJets_b = df_GenJets_b.Filter("GenJet_b_PF_size>2")


        df_Greater2_GenJets_b_2ClosestToMPVMass= df_Greater2_GenJets_b.Define("TwobJetsClosestToMPV", f"(FindTwoJetsClosestToMPV({mpv},GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass, GenJet_partonFlavour))").Define("Two_ClosestToMPV_bGenJets_invMass", "InvMassByIndices(TwobJetsClosestToMPV,GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass,GenJet_partonFlavour, true)")
        hist_Greater2_GenJets_b_2ClosestToMPVMass = df_Greater2_GenJets_b_2ClosestToMPVMass.Histo1D(("Two_ClosestToMPV_bGenJets_invMass", "Two_ClosestToMPV_bGenJets_invMass;m_{jj} (GeV);N_{Events}", 30, 10, 350),"Two_ClosestToMPV_bGenJets_invMass").GetValue()

        myfile = ROOT.TFile( outFile, 'RECREATE' )
        hist_Greater2_GenJets_b_2ClosestToMPVMass.Write()
        myfile.Close()

        df_Greater2_GenJets_b_2ClosestToMPVMass.Snapshot("Events", outFile, "GenJet_b_PF_size",snapshotOptions)
