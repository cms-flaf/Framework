from Common.BaselineSelection import *
from Visual.HistTools import *
from Studies.HHBTag.Utils import findMPV
import os
# Enable multi-threading
ROOT.EnableImplicitMT()
ROOT.gROOT.SetBatch(True)
ROOT.gStyle.SetOptStat(1111)
#ROOT.gInterpreter.Declare(f"static ParticleDB particleDB({particleDBFile}")
header_path_Utils = f"{os.environ['ANALYSIS_PATH']}/Studies/HHBTag/Utilities.h"
ROOT.gInterpreter.Declare('#include "{}"'.format(header_path_Utils))

def FindNumerator(df, score):#RVecI ReorderObjects(const RVecF& VarToOrder, const RVecI& index_vec, const unsigned nMax=std::numeric_limits<unsigned>::max()
    # order jets in decreasing order w.r.t. the score
    df_orderedJetsInScore = df.Define("AllRecoJetIndices", f"RVecI AllRecoJetIndices; for(int i=0; i<{score}.size();i++){{AllRecoJetIndices.push_back(i);}} return AllRecoJetIndices;").Define(f"JetsReorderedIn{score}", f"ReorderObjects({score}, AllRecoJetIndices)")
    df_correspondanceJetOrderedRecoJet = df_orderedJetsInScore.Define("CorrespondenceSum", f"int CorrespondenceSum=0; for(auto& i:RecoJetIndices){{if(i==JetsReorderedIn{score}[0] || i==JetsReorderedIn{score}[1]) {{ CorrespondenceSum++;}} /*std::cout << \" reco jet has index \"<< i << \" reordered jets have indices \" << JetsReorderedIn{score}[0] << \" and \" << JetsReorderedIn{score}[1]<< \" CorrespondanceSum is \" << CorrespondenceSum << std::endl; */ }} /*std::cout << std::endl;*/ return CorrespondenceSum;").Filter("CorrespondenceSum==2")
    return df_correspondanceJetOrderedRecoJet
    # ask that the sum of the jets corresponding to the reco selected indices is 2

#for file in os.listdir('nanoAOD'):
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
    print(f"channel is {ch}")
    # apply preselection
    df_matched = DefineDataFrame(df, ch)
    print(f"when applying preselection there are {df_matched.Count().GetValue()} events")
    # define b-GenJet indices
    df_GenJets_b = df_matched.Define("GenJet_b_PF", "RVecI GenJet_b_PF; for(int i =0 ; i<GenJet_partonFlavour.size(); i++){if (std::abs(GenJet_partonFlavour[i])==5){GenJet_b_PF.push_back(i);}} return GenJet_b_PF;").Define("GenJet_b_PF_size", "GenJet_b_PF.size()").Filter('GenJet_b_PF_size>=2')
    print(f"when requiring at least 2 bjets there are {df_GenJets_b.Count().GetValue()} events")
    # Tag 2 GenJet with b PartonFlavour with the MPV algo
    df_GenJets_b_2ClosestToMPVMass= df_GenJets_b.Define("TwobJetsClosestToMPV", f"(FindTwoJetsClosestToMPV({mpv},GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass, GenJet_partonFlavour))")
    print(f"when finding the two b-jet closest to MPV mass there are {df_GenJets_b_2ClosestToMPVMass.Count().GetValue()} events")
    # select events where DeltaR(tau, recoJet) > 0.5
    df_RecoJenJetCorrespondance = df_GenJets_b_2ClosestToMPVMass.Define("RecoJetIndices","FindRecoGenJetCorrespondence(Jet_genJetIdx, TwobJetsClosestToMPV)").Filter("RecoJetIndices.size()>=2")
    print(f"when searching at least 2 recoJets there are {df_RecoJenJetCorrespondance.Count().GetValue()} events")
    df_JetSeparatedFromLeptons = df_RecoJenJetCorrespondance.Filter("JetLepSeparation(leg2_p4, Jet_eta, Jet_phi, RecoJetIndices)")
    if(ch=='tauTau'):
        df_JetSeparatedFromLeptons = df_JetSeparatedFromLeptons.Filter("JetLepSeparation(leg1_p4, Jet_eta, Jet_phi, RecoJetIndices)")
    denum = df_JetSeparatedFromLeptons.Count().GetValue()
    print(f"when searching at least 2 recoJets separated from reco taus there are {denum} events")
    print()
    #Jet_btagCSVV2	Jet_btagDeepB	Jet_btagDeepCvB	Jet_btagDeepFlavB	Jet_btagDeepFlavCvB
    for score in ["Jet_btagCSVV2","Jet_btagDeepB","Jet_btagDeepFlavB","Jet_btagDeepFlavCvB"]:
        print(f"evaluating numerator for {score}")
        df_Final = FindNumerator(df_JetSeparatedFromLeptons, score)
        num = df_Final.Count().GetValue()
        eff = num/denum
        print(f"numerator for {score} is {num}, hence efficiency is {eff}")
    print()
print()
