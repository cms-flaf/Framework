from Common.BaselineSelection import *
from Visual.HistTools import *
import ROOT
#ROOT.EnableImplicitMT()
_rootpath = os.path.abspath(os.path.dirname(__file__)+"/../../..")
ROOT.gROOT.ProcessLine(".include "+_rootpath)
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
mpv = findMPV(df)
print(f"mpv is {mpv}")


for ch in ['eTau','muTau', 'tauTau']:#, 'muTau', 'tauTau']:#, 'eE', 'eMu', 'muMu']:
    df_matched = DefineDataFrame(df, ch)
    dirName = f"output/{ch}"
    print(dirName, type(dirName))
    if(not os.path.exists(dirName)):
        os.makedirs(dirName)

    df_GenJets_b = df_matched.Define("GenJet_b_PF", "RVecI GenJet_b_PF; for(int i =0 ; i<GenJet_partonFlavour.size(); i++){if (std::abs(GenJet_partonFlavour[i])==5){GenJet_b_PF.push_back(i);}} return GenJet_b_PF;").Define("GenJet_b_PF_size", "GenJet_b_PF.size()")
    df_lastHadrons_fromHbb = df_matched.Define("lastHadronsIndices","GetLastHadrons(GenPart_pdgId, GenPart_genPartIdxMother )")
    '''
    # (1) 2 genJet with b-flavour for events with n GenJets with b = 2
    df_2_GenJets_b = df_GenJets_b.Filter("GenJet_b_PF_size==2").Define("Two_bGenJets_invMass", "InvMassByFalvour(GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass, GenJet_partonFlavour, true)")
    hist_Two_bGenJets_invMass = df_2_GenJets_b.Histo1D(("Two_bGenJets_invMass", "Two_bGenJets_invMass;m_{jj} (GeV);N_{Events}", 50, 0, 250),"Two_bGenJets_invMass").GetValue()
    myfile = ROOT.TFile( f"output/{ch}/AllHistos.root", 'RECREATE' )
    hist_Two_bGenJets_invMass.Write()
    myfile.Close()
    plot(hist_Two_bGenJets_invMass, f"Two_bGenJets_invMass_{ch}", ch,dirName)
    #print(f"{ch},  2 genJet with b-flavour for events with n GenJets with b = 2 ")
    #print(f"conf int with scipy = {EvaluateConfInt(hist_Two_bGenJets_invMass)}")
    #print(f"conf int with root and cumulative = {EvaluateDiffInt(hist_Two_bGenJets_invMass)}")

    # (2) 2 most energetic b-jets for events with n GenJets >2
    df_Greater2_GenJets_b = df_GenJets_b.Filter("GenJet_b_PF_size>2")
    df_Greater2_GenJets_b_2MostEnergeticsMass= df_Greater2_GenJets_b.Define("ReorderedJetsInPt", "ReorderObjects(GenJet_pt, GenJet_b_PF)").Define("two_most_energetic_bGenJets", "RVecI twoMostEnergeticJets; for(int i = 0; i<2; i++){twoMostEnergeticJets.push_back(ReorderedJetsInPt[i]);}  return twoMostEnergeticJets;").Define("Two_MostEnergetic_bGenJets_invMass", "InvMassByIndices(two_most_energetic_bGenJets,GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass,GenJet_partonFlavour, true)")
    hist_Two_MostEnergetic_bGenJets_invMass = df_Greater2_GenJets_b_2MostEnergeticsMass.Histo1D(("Two_MostEnergetic_bGenJets_invMass", "Two_MostEnergetic_bGenJets_invMass;m_{jj} (GeV);N_{Events}", 35, 0, 350),"Two_MostEnergetic_bGenJets_invMass").GetValue()
    myfile = ROOT.TFile( f"output/{ch}/AllHistos.root", 'UPDATE' )
    hist_Two_MostEnergetic_bGenJets_invMass.Write()
    myfile.Close()
    plot(hist_Two_MostEnergetic_bGenJets_invMass, f"TwoMostEnergetic_bGenJets_invMass_{ch}", ch,dirName)
    #print(f"{ch},  2 most energetic b-jets for events with n GenJets >2 ")
    #print(f"conf int with scipy = {EvaluateConfInt(hist_Greater2_GenJets_b_2MostEnergeticsMass)}")
    #print(f"conf int with root and cumulative = {EvaluateDiffInt(hist_Greater2_GenJets_b_2MostEnergeticsMass)}")

    # (3) all b-jets for events with n GenJets >2
    df_Greater2_GenJets_b = df_GenJets_b.Filter("GenJet_b_PF_size>2")
    df_Greater2_GenJets_b_allMass= df_Greater2_GenJets_b.Define("all_bGenJets_invMass", "InvMassByFalvour(GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass, GenJet_partonFlavour, true)")
    hist_Greater2_GenJets_b_allMass = df_Greater2_GenJets_b_allMass.Histo1D(("all_bGenJets_invMass", "all_bGenJets_invMass;m_{jj} (GeV);N_{Events}", 30, 10, 350),"all_bGenJets_invMass").GetValue()
    myfile = ROOT.TFile( f"output/{ch}/AllHistos.root", 'UPDATE' )
    hist_Greater2_GenJets_b_allMass.Write()
    myfile.Close()
    plot(hist_Greater2_GenJets_b_allMass, f"All_bGenJets_invMass_{ch}", ch,dirName)
    '''
    #print(f"{ch},  all b-jets for events with n GenJets >2 ")
    #print(f"conf int with scipy = {EvaluateConfInt(hist_Greater2_GenJets_b_allMass)}")
    #print(f"conf int with root and cumulative = {EvaluateDiffInt(hist_Greater2_GenJets_b_allMass)}")
    x_max = 121.75
    df_Greater2_GenJets_b = df_GenJets_b.Filter("GenJet_b_PF_size>2")
    df_Greater2_GenJets_b_2ClosestToMPVMass= df_Greater2_GenJets_b.Define("TwobJetsClosestToMPV", f"(FindTwoJetsClosestToMPV({x_max},GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass, GenJet_partonFlavour))").Define("Two_ClosestToMPV_bGenJets_invMass", "InvMassByIndices(TwobJetsClosestToMPV,GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass,GenJet_partonFlavour, true)")
    hist_Greater2_GenJets_b_2ClosestToMPVMass = df_Greater2_GenJets_b_2ClosestToMPVMass.Histo1D(("Two_ClosestToMPV_bGenJets_invMass", "Two_ClosestToMPV_bGenJets_invMass;m_{jj} (GeV);N_{Events}", 30, 10, 350),"Two_ClosestToMPV_bGenJets_invMass").GetValue()
    myfile = ROOT.TFile( f"output/{ch}/AllHistos.root", 'UPDATE' )
    hist_Greater2_GenJets_b_2ClosestToMPVMass.Write()
    myfile.Close()
    plot(hist_Greater2_GenJets_b_2ClosestToMPVMass, f"Two_bJet_ClosestToMPV_invMass_{ch}", ch,dirName)
    '''
    # (4) two final state hadrons from b  for events with n_final_b_had = 2
    df_2lastHadrons_fromHbb = df_lastHadrons_fromHbb.Filter("lastHadronsIndices.size()==2").Define("Two_LastHadrons_invMass", "InvMassByIndices(lastHadronsIndices, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass,GenPart_pdgId, false)")
    hist_Two_LastHadrons_invMass = df_2lastHadrons_fromHbb.Histo1D(("Two_LastHadrons_invMass", "Two_LastHadrons_invMass;m_{hadrons} (GeV);N_{Events}", 30, 0, 120),"Two_LastHadrons_invMass").GetValue()
    myfile = ROOT.TFile( f"output/{ch}/AllHistos.root", 'UPDATE' )
    hist_Two_LastHadrons_invMass.Write()
    myfile.Close()
    plot(hist_Two_LastHadrons_invMass, f"Two_LastHadrons_invMass_{ch}", ch,dirName)
    #print(f"{ch},  two final state hadrons from b  for events with n_final_b_had = 2 ")
    #print(f"conf int with scipy = {EvaluateConfInt(hist_Two_LastHadrons_invMass)}")
    #print(f"conf int with root and cumulative = {EvaluateDiffInt(hist_Two_LastHadrons_invMass)}")

    # (5) two most energetic final state hadrons from b  for events with n_final_b_had > 2
    df_Greater2lastHadrons_fromHbb = df_lastHadrons_fromHbb.Filter("lastHadronsIndices.size()>2")
    df_Greater2lastHadrons_fromHbb_2MostEnergeticsMass= df_lastHadrons_fromHbb.Define("ReorderedHadronsInPt", "ReorderObjects(GenPart_pt, lastHadronsIndices)").Define("two_most_energetic_HadronsFromHbb", "RVecI twoMostEnergeticHadrons; for(int i = 0; i<2; i++){twoMostEnergeticHadrons.push_back(ReorderedHadronsInPt[i]);} return twoMostEnergeticHadrons;").Define("Two_MostEnergetic_LastHadrons_invMass", "InvMassByIndices(two_most_energetic_HadronsFromHbb,GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass,GenPart_pdgId, false)")

    hist_Greater2lastHadrons_fromHbb_2MostEnergeticsMass = df_Greater2lastHadrons_fromHbb_2MostEnergeticsMass.Histo1D(("Two_MostEnergetic_LastHadrons_invMass", "Two_MostEnergetic_LastHadrons_invMass;m_{hadrons} (GeV);N_{Events}", 30, 0, 110),"Two_MostEnergetic_LastHadrons_invMass").GetValue()

    myfile = ROOT.TFile( f"output/{ch}/AllHistos.root", 'UPDATE' )
    hist_Greater2lastHadrons_fromHbb_2MostEnergeticsMass.Write()
    myfile.Close()
    plot(hist_Greater2lastHadrons_fromHbb_2MostEnergeticsMass, f"Two_MostEnergetic_LastHadrons_invMass_{ch}", ch,dirName)
    #print(f"{ch},  two most energetic final state hadrons from b  for events with n_final_b_had > 2 ")
    #print(f"conf int with scipy = {EvaluateConfInt(hist_Greater2lastHadrons_fromHbb_2MostEnergeticsMass)}")
    #print(f"conf int with root and cumulative = {EvaluateDiffInt(hist_Greater2lastHadrons_fromHbb_2MostEnergeticsMass)}")

    # (6) all final state hadrons from b  for events with n_final_b_had > 2
    df_Greater2lastHadrons_fromHbb_allMass = df_lastHadrons_fromHbb.Define("all_LastHadrons_invMass", "InvMassByIndices(lastHadronsIndices, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass,GenPart_pdgId, false)")
    hist_Greater2lastHadrons_fromHbb_allMass = df_Greater2lastHadrons_fromHbb_allMass.Histo1D(("all_LastHadrons_invMass", "all_LastHadrons_invMass;m_{hadrons} (GeV);N_{Events}", 30, 0, 120),"all_LastHadrons_invMass").GetValue()
    myfile = ROOT.TFile( f"output/{ch}/AllHistos.root", 'UPDATE' )
    hist_Greater2lastHadrons_fromHbb_allMass.Write()
    myfile.Close()
    plot(hist_Greater2lastHadrons_fromHbb_allMass, f"Greater2lastHadrons_fromHbb_allMass_{ch}", ch,dirName)
    #print(f"{ch},  all final state hadrons from b  for events with n_final_b_had > 2 ")
    #print(f"conf int with scipy = {EvaluateConfInt(hist_Greater2lastHadrons_fromHbb_allMass)}")
    #print(f"conf int with root and cumulative = {EvaluateDiffInt(hist_Greater2lastHadrons_fromHbb_allMass)}")
    '''
