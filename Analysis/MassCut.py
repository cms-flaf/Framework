import ROOT
import os
import sys
import yaml
import numpy as np
import matplotlib; import matplotlib.pyplot as plt
import matplotlib.colors as colors
import mplhep as hep
plt.style.use(hep.style.ROOT)

if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *
def defineP4(df, name):
    df = df.Define(f"{name}_p4", f"ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>({name}_pt,{name}_eta,{name}_phi,{name}_mass)")
    return df

inFiles = Utilities.ListToVector(["/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-1000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-1250/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-1500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-1750/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-2000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-250/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-2500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-260/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-270/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-280/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-300/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-3000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-320/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-350/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-400/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-450/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-550/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-600/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-650/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-700/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-750/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-800/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-850/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-900/nano.root" ,"/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-1000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-1250/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-1500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-1750/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-2000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-250/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-2500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-260/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-270/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-280/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-300/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-3000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-320/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-350/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-400/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-450/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-550/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-600/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-650/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-700/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-750/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-800/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-850/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-900/nano.root"])
df_initial = ROOT.RDataFrame("Events", inFiles)

df_initial = df_initial.Define("nSelBtag", f"int(b1_idbtagDeepFlavB >=1) + int(b2_idbtagDeepFlavB >=1)")
df_initial = df_initial.Define("OS", "tau1_charge*tau2_charge < 0")
df_resolved = df_initial.Filter(f"b1_pt >0 && b2_pt>0 && b1_hadronFlavour==5 && b2_hadronFlavour==5 && tau2_idDeepTau2017v2p1VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value} && OS && nSelBtag >1")
#df_resolved = df_initial.Filter(f"b1_pt >0 && b2_pt>0 && b1_hadronFlavour==5 && b2_hadronFlavour==5 && nSelBtag >1 && OS")
for idx in [0,1]:
    df_resolved = defineP4(df_resolved, f"tau{idx+1}")
    df_resolved = defineP4(df_resolved, f"b{idx+1}")
df_resolved = df_resolved.Define("tautau_m_vis", "static_cast<float>((tau1_p4+tau2_p4).M())")
df_resolved = df_resolved.Define("bb_m_vis", """static_cast<float>((b1_p4+b2_p4).M())""")
np_dict_resolved = df_resolved.AsNumpy(["bb_m_vis","tautau_m_vis"])
np_array_mass_bb_resolved = np_dict_resolved["bb_m_vis"]
np_array_mass_tt_resolved = np_dict_resolved["tautau_m_vis"]

max_bb_mass =np.quantile(np_array_mass_bb_resolved, 1-0.005)
min_bb_mass =np.quantile(np_array_mass_bb_resolved, 0.005)

max_bb_mass_int = math.ceil(max_bb_mass / 10) * 10
min_bb_mass_int = math.floor(min_bb_mass / 10) * 10

df_resolved_bb = df_resolved.Filter(f"bb_m_vis > {min_bb_mass_int} && bb_m_vis < {max_bb_mass_int}")

np_dict_resolved_bb = df_resolved_bb.AsNumpy(["tautau_m_vis"])
np_array_mass_tt_resolved_bb = np_dict_resolved_bb["tautau_m_vis"]
max_tt_mass =np.quantile(np_array_mass_tt_resolved_bb, 1-0.005)
min_tt_mass =np.quantile(np_array_mass_tt_resolved_bb, 0.005)

max_tt_mass_int = math.ceil(max_tt_mass / 10) * 10
min_tt_mass_int = math.floor(min_tt_mass / 10) * 10

print("quantile max (99.5%) for bb mass resolved = ", max_bb_mass, max_bb_mass_int)
print("quantile min (99.5%) for bb mass resolved = ", min_bb_mass, min_bb_mass_int)
print("quantile max (99.5%) for tt mass resolved = ", max_tt_mass, max_tt_mass_int)
print("quantile min (99.5%) for tt mass resolved = ", min_tt_mass, min_tt_mass_int)


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
'''


histcfg = '/afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/config/plot/histograms.yaml'
hist_cfg_dict = {}
with open(histcfg, 'r') as f:
    hist_cfg_dict = yaml.safe_load(f)


x_bins = np.array(hist_cfg_dict['bb_m_vis']['x_bins'][0:12])
y_bins = np.array(hist_cfg_dict['tautau_m_vis']['x_bins'][0:12])


fig, ax = plt.subplots()

np_array_mass_bb_resolved_int = np_array_mass_bb_resolved.astype(int)
np_array_mass_tt_resolved_int = np_array_mass_tt_resolved.astype(int)

H, xedges, yedges = np.histogram2d(np_array_mass_bb_resolved, np_array_mass_tt_resolved, bins=(x_bins, y_bins))

hep.hist2dplot(H, xedges, yedges, flow="show", cmap='jet',cmin=0)
# in alternativa: jet, ocean, rainbow, viridis, plasma


x_p = [min_bb_mass_int, max_bb_mass_int, max_bb_mass_int, min_bb_mass_int]
y_p = [min_tt_mass_int, min_tt_mass_int, max_tt_mass_int, max_tt_mass_int]

x_p = np.append(x_p, x_p[0])
y_p = np.append(y_p, y_p[0])

hep.cms.text('Preliminary', fontsize=30)
hep.cms.lumitext((r"138 $fb^{-1}$ (13 TeV)"))
#hep.cms.label(loc=0)

ylabel = r"$m_{{\tau\tau}}^{{vis}}$  [GeV]"
xlabel = r"$m_{{bb}}^{{vis}}$  [GeV]"
plt.xlabel(xlabel)
plt.ylabel(ylabel)

plt.plot(x_p,y_p, c='red')

plt.show()
plt.savefig('MassCut_limits.png')

'''