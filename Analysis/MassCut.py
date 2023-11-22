import ROOT
import os
import sys
import numpy as np
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities

def defineP4(df, name):
    df = df.Define(f"{name}_p4", f"ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>({name}_pt,{name}_eta,{name}_phi,{name}_mass)")
    return df

inFiles = Utilities.ListToVector(["/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-1000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-1250/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-1500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-1750/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-2000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-250/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-2500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-260/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-270/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-280/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-300/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-3000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-320/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-350/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-400/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-450/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-550/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-600/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-650/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-700/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-750/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-800/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-850/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-900/nano.root" ,"/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-1000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-1250/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-1500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-1750/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-2000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-250/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-2500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-260/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-270/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-280/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-300/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-3000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-320/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-350/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-400/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-450/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-550/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-600/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-650/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-700/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-750/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-800/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-850/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v7_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-900/nano.root"])
df_initial = ROOT.RDataFrame("Events", inFiles)
#print(df.Count().GetValue())
df_boosted = df_initial.Define("SelectedFatJet_size","SelectedFatJet_pt.size()").Filter("SelectedFatJet_size>0")
#print(df.Count().GetValue())

df_boosted = df_boosted.Define("FatJet_atLeast1BHadron","SelectedFatJet_nBHadrons>0").Filter("SelectedFatJet_pt[FatJet_atLeast1BHadron].size()==1")
#print(df.Count().GetValue())

df_boosted= df_boosted.Define("SelectedFatJet_pNetmass","SelectedFatJet_particleNet_mass[FatJet_atLeast1BHadron][0]")
df_boosted= df_boosted.Define("SelectedFatJet_pt1had","SelectedFatJet_pt[FatJet_atLeast1BHadron][0]")
np_dict_boosted = df_boosted.AsNumpy(["SelectedFatJet_pNetmass","SelectedFatJet_pt1had"])
np_array_mass_boosted = np_dict_boosted["SelectedFatJet_pNetmass"]
np_array_pt_boosted = np_dict_boosted["SelectedFatJet_pt1had"]
#print(len(np_array))
print("quantile max for pt boosted = ", np.quantile(np_array_pt_boosted, 1-0.005))
print("quantile min for pt boosted = ",np.quantile(np_array_pt_boosted, 0.005))
print("quantile max for mass boosted = ", np.quantile(np_array_mass_boosted, 1-0.005))
print("quantile min for mass boosted = ",np.quantile(np_array_mass_boosted, 0.005))



#df_resolved = df_initial.Define("is_resolved","SelectedFatJet_pt.size()").Filter("SelectedFatJet_size==0 && b1_pt >0 && b2_pt>0 && b2_hadronFlavour==5 && b2_hadronFlavour==5")
df_resolved = df_initial.Filter("b1_pt >0 && b2_pt>0 && b1_hadronFlavour==5 && b2_hadronFlavour==5")
for idx in [0,1]:
    df_resolved = defineP4(df_resolved, f"tau{idx+1}")
    df_resolved = defineP4(df_resolved, f"b{idx+1}")
df_resolved = df_resolved.Define("tautau_m_vis", "static_cast<float>((tau1_p4+tau2_p4).M())")
df_resolved = df_resolved.Define("bb_m_vis", """static_cast<float>((b1_p4+b2_p4).M())""")
np_dict_resolved = df_resolved.AsNumpy(["tautau_m_vis","bb_m_vis"])
np_array_mass_bb_resolved = np_dict_resolved["bb_m_vis"]
np_array_mass_tt_resolved = np_dict_resolved["tautau_m_vis"]
print("quantile max for bb mass resolved = ", np.quantile(np_array_mass_bb_resolved, 1-0.005))
print("quantile min for bb mass resolved = ", np.quantile(np_array_mass_bb_resolved, 0.005))
print("quantile max for tautau mass resolved = ",np.quantile(np_array_mass_tt_resolved, 1-0.005))
print("quantile min for tautau mass resolved = ",np.quantile(np_array_mass_tt_resolved, 0.005))