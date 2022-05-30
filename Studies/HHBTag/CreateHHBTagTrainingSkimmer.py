from Common.BaselineSelection import *
from Visual.HistTools import *
# Enable multi-threading
import ROOT
ROOT.EnableImplicitMT()
ROOT.gROOT.SetBatch(True)
ROOT.gStyle.SetOptStat(1111)


filesPath=f"{os.environ['ANALYSIS_PATH']}/data/nanoAOD"

files=os.listdir(filesPath)
file=files[0]

mass_start =  file.find('-')+1
mass_end = file.find('.root')
mass = file[ mass_start : mass_end]
print(f"evaluating for mass {mass}")
df = ROOT.RDataFrame("Events", f"{filesPath}/{file}")

outDir=f"{os.environ['ANALYSIS_PATH']}/data/output"


for ch in ['eTau','muTau', 'tauTau']:#, 'muTau', 'tauTau']:#, 'eE', 'eMu', 'muMu']:
    df_matched = DefineDataFrame(df, ch)

    outDir = f"{outDir}/{ch}"

    if(not os.path.exists(outDir)):
        os.makedirs(outDir)

    df_GenJets_b = df_matched.Define("GenJet_b_PF", "vec_i GenJet_b_PF; for(int i =0 ; i<GenJet_partonFlavour.size(); i++){if (std::abs(GenJet_partonFlavour[i])==5){GenJet_b_PF.push_back(i);}} return GenJet_b_PF;").Define("GenJet_b_PF_size", "GenJet_b_PF.size()")
    df_lastHadrons_fromHbb = df_matched.Define("lastHadronsIndices","GetLastHadrons(GenPart_pdgId, GenPart_genPartIdxMother )")
    mpv = findMPV(df_GenJets_b.Fiter("GenJet_b_PF_size==2"))
    x_max = 121.75
    df_Greater2_GenJets_b = df_GenJets_b.Filter("GenJet_b_PF_size>2")
    df_Greater2_GenJets_b_2ClosestToMPVMass= df_Greater2_GenJets_b.Define("TwobJetsClosestToMPV", f"(FindTwoJetsClosestToMPV({x_max},GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass, GenJet_partonFlavour))").Define("Two_ClosestToMPV_bGenJets_invMass", "InvMassByIndices(TwobJetsClosestToMPV,GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass,GenJet_partonFlavour, true)")
    hist_Greater2_GenJets_b_2ClosestToMPVMass = df_Greater2_GenJets_b_2ClosestToMPVMass.Histo1D(("Two_ClosestToMPV_bGenJets_invMass", "Two_ClosestToMPV_bGenJets_invMass;m_{jj} (GeV);N_{Events}", 30, 10, 350),"Two_ClosestToMPV_bGenJets_invMass").GetValue()
    myfile = ROOT.TFile( f"output/{ch}/AllHistos.root", 'UPDATE' )
    hist_Greater2_GenJets_b_2ClosestToMPVMass.Write()
    myfile.Close()
    plot(hist_Greater2_GenJets_b_2ClosestToMPVMass, f"Two_bJet_ClosestToMPV_invMass_{ch}", ch,dirName)
