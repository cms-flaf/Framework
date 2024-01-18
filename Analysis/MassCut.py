import ROOT
import os
import sys
import yaml
import numpy as np
import matplotlib; import matplotlib.pyplot as plt
import matplotlib.colors as colors

if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *
def defineP4(df, name):
    df = df.Define(f"{name}_p4", f"ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>({name}_pt,{name}_eta,{name}_phi,{name}_mass)")
    return df

inFiles = Utilities.ListToVector(["/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-1000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-1250/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-1500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-1750/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-2000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-250/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-2500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-260/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-270/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-280/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-300/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-3000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-320/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-350/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-400/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-450/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-550/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-600/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-650/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-700/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-750/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-800/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-850/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToBulkGravitonToHHTo2B2Tau_M-900/nano.root" ,"/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-1000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-1250/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-1500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-1750/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-2000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-250/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-2500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-260/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-270/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-280/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-300/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-3000/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-320/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-350/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-400/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-450/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-500/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-550/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-600/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-650/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-700/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-750/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-800/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-850/nano.root","/eos/home-k/kandroso/cms-hh-bbtautau/anaTuples/Run2_2018/v9_deepTau2p1/GluGluToRadionToHHTo2B2Tau_M-900/nano.root"])
df_initial = ROOT.RDataFrame("Events", inFiles)

df_initial = df_initial.Define("nSelBtag", f"int(b1_idbtagDeepFlavB >=2) + int(b2_idbtagDeepFlavB >=2)")
df_resolved = df_initial.Filter(f"b1_pt >0 && b2_pt>0 && b1_hadronFlavour==5 && b2_hadronFlavour==5 && tau2_idDeepTau2017v2p1VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value} && nSelBtag >1")
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

histcfg = '/afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/config/plot/histograms.yaml'
hist_cfg_dict = {}
with open(histcfg, 'r') as f:
    hist_cfg_dict = yaml.safe_load(f)


x_bins = hist_cfg_dict['bb_m_vis']['x_bins']
y_bins = hist_cfg_dict['tautau_m_vis']['x_bins']

plt.style.use(hep.style.ROOT)
ylabel = r"$m_{{\tau\tau}}^{{vis}}$  [GeV]"
xlabel = r"$m_{{bb}}^{{vis}}$  [GeV]"
#cmin=10,
plt.hist2d(np_array_mass_bb_resolved, np_array_mass_tt_resolved, bins=(100, 100), range = [[0, 300.],[0,300.]], cmap=plt.cm.jet)

plt.xlabel(xlabel)
plt.ylabel(ylabel)

x_p = [50,50,275, 275]
y_p = [20,130,130,20]
x_p = np.append(x_p, x_p[0])
y_p = np.append(y_p, y_p[0])

plt.plot(x_p,y_p, c='red')

plt.show()
plt.savefig('MassCut_limits.png')