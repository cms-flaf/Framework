import ROOT
import os
import sys
import yaml
import numpy as np
import matplotlib; import matplotlib.pyplot as plt
import matplotlib.colors as colors
import mplhep as hep
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
#df_resolved = df_initial.Filter(f"b1_pt >0 && b2_pt>0 && b1_hadronFlavour==5 && b2_hadronFlavour==5 && tau2_idDeepTau2017v2p1VSjet >= {Utilities.WorkingPointsTauVSjet.Medium.value} && OS && nSelBtag >1")
df_resolved = df_initial.Filter(f"b1_pt >0 && b2_pt>0 && b1_hadronFlavour==5 && b2_hadronFlavour==5 && nSelBtag >1 ")
for idx in [0,1]:
    df_resolved = defineP4(df_resolved, f"tau{idx+1}")
    df_resolved = defineP4(df_resolved, f"b{idx+1}")
df_resolved = df_resolved.Define("tautau_m_vis", "static_cast<float>((tau1_p4+tau2_p4).M())")
df_resolved = df_resolved.Define("bb_m_vis", """static_cast<float>((b1_p4+b2_p4).M())""")
np_dict_resolved = df_resolved.AsNumpy(["tautau_m_vis","bb_m_vis"])
np_array_mass_bb_resolved = np_dict_resolved["bb_m_vis"]
np_array_mass_tt_resolved = np_dict_resolved["tautau_m_vis"]
print("quantile max (95%) for bb mass resolved = ", np.quantile(np_array_mass_bb_resolved, 1-0.05))
print("quantile min (95%) for bb mass resolved = ", np.quantile(np_array_mass_bb_resolved, 0.05))
print("quantile max (95%)  for tautau mass resolved = ",np.quantile(np_array_mass_tt_resolved, 1-0.05))
print("quantile min (95%)  for tautau mass resolved = ",np.quantile(np_array_mass_tt_resolved, 0.05))
print()

print("quantile max (99.5%) for bb mass resolved = ", np.quantile(np_array_mass_bb_resolved, 1-0.005))
print("quantile min (99.5%) for bb mass resolved = ", np.quantile(np_array_mass_bb_resolved, 0.005))
print("quantile max (99.5%) for tautau mass resolved = ",np.quantile(np_array_mass_tt_resolved, 1-0.005))
print("quantile min (99.5%) for tautau mass resolved = ",np.quantile(np_array_mass_tt_resolved, 0.005))

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
hep.cms.text('Preliminary', fontsize=40)
hep.cms.lumitext((r"138 $fb^{-1}$ (13 TeV)"))
fig, ax = plt.subplots()

ax.hist2d(np_array_mass_bb_resolved, np_array_mass_tt_resolved, bins=(100, 100), range = [[0, 1000.],[0,1000.]], cmap=plt.cm.jet)

cbar = hep.hist2dplot(values, histogram.axes[0].edges, histogram.axes[1].edges,
                        flow=None, norm=colors.LogNorm(vmin=1, vmax=values.max()))
cbar.cbar.ax.set_ylabel("No. Events", rotation=90, labelpad=0.5, loc='top')
rect = matplotlib.patches.Rectangle((245,473), 100, 20, color='white')
fig = plt.figure(figsize=(wsize, hsize),)
ax = plt.subplot(111)
ax.title.set_size(100)
ax.add_patch(rect)

plt.legend(loc="upper right", facecolor="white", edgecolor="white", framealpha=1)
ax.set_xlabel(xlabel)
ax.set_ylabel(ylabel)

x_p = [50,50,270, 270]
y_p = [20,130,130,20]
x_p = np.append(x_p, x_p[0])
y_p = np.append(y_p, y_p[0])

plt.plot(x_p,y_p, c='red')

plt.show()
plt.savefig('MassCut_limits.png')