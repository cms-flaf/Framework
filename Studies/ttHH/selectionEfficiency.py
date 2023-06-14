import ROOT

import os
import sys

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  base_dir = os.path.dirname(file_dir)
  if base_dir not in sys.path:
    sys.path.append(base_dir)
  __package__ = os.path.split(file_dir)[-1]

from Common.Utilities import *

def compute_eff(input_dir):
  df = ROOT.RDataFrame("Events", f'{input_dir}/nano_0.root')
  df_notsel = ROOT.RDataFrame("EventsNotSelected", f'{input_dir}/nano_0.root')

  n_total_sel = df.Count()
  n_total_notsel = df_notsel.Count()

  df = df.Define("Electron_sel", f"""
                 Electron_pt > 10 && abs(Electron_eta) < 2.3 && abs(Electron_dz) < 0.2 && abs(Electron_dxy) < 0.045
                 && Electron_mvaNoIso_WP90 && Electron_pfRelIso03_all < 0.3
  """)

  df = df.Define("Muon_sel", f"""
                 Muon_pt > 10 && abs(Muon_eta) < 2.3 && abs(Muon_dz) < 0.2 && abs(Muon_dxy) < 0.045
                 && Muon_mediumId && Muon_pfRelIso04_all < 0.3
  """)
  df = df.Define("Tau_sel", f"""
                 Tau_pt > 20 && Tau_eta < 2.5 && abs(Tau_dz) < 0.2 && Tau_decayMode != 5 && Tau_decayMode != 6
                 && Tau_idDeepTau2018v2p5VSe >= {WorkingPointsTauVSe.VVLoose.value}
                 && Tau_idDeepTau2018v2p5VSmu >= {WorkingPointsTauVSmu.VLoose.value}
                 && Tau_idDeepTau2018v2p5VSjet >= {WorkingPointsTauVSjet.VLoose.value}
  """)

  df = df.Define("nElectrons", "Electron_pt[Electron_sel].size()")
  df = df.Define("nMuons", "Muon_pt[Muon_sel].size()")
  df = df.Define("nTaus", "Tau_pt[Tau_sel].size()")
  df = df.Define("nLeptons", "Electron_pt[Electron_sel].size() + Muon_pt[Muon_sel].size() + Tau_pt[Tau_sel].size()")

  sel_cnt = df.Filter("nLeptons >= 2 && nTaus >= 1").Count()
  sel_lep_cnt = df.Filter("nLeptons >= 2 && nTaus == 0").Count()
  had_sel_cnt = df.Filter("nTaus >= 2 && nElectrons == 0 && nMuons == 0").Count()
  had_lep_sel_cnt = df.Filter("nTaus >= 2 && nElectrons + nMuons > 0").Count()
  lep_cnt = {}
  n_max = 6
  for n in range(n_max):
    cmp = '==' if n < 4 else '>='
    lep_cnt[n] = df.Filter(f"nLeptons {cmp} {n}").Count()

  llep_cnt = {}
  for n in range(n_max):
    cmp = '==' if n < 4 else '>='
    llep_cnt[n] = df.Filter(f"nElectrons + nMuons {cmp} {n}").Count()

  tau_cnt = {}
  for n in range(n_max):
    cmp = '==' if n < 4 else '>='
    tau_cnt[n] = df.Filter(f"nTaus {cmp} {n}").Count()

  n_total = n_total_sel.GetValue() + n_total_notsel.GetValue()
  print(f'Total events: {n_total}')
  print(f'Pre-selected events: {n_total_sel.GetValue()}')
  for n in range(n_max):
    print(f'{n} leptons: {lep_cnt[n].GetValue()} ({lep_cnt[n].GetValue() / n_total * 100:.2f}%)')
  for n in range(n_max):
    print(f'{n} light leptons: {llep_cnt[n].GetValue()} ({llep_cnt[n].GetValue() / n_total * 100:.2f}%)')
  for n in range(n_max):
    print(f'{n} hadronic taus: {tau_cnt[n].GetValue()} ({tau_cnt[n].GetValue() / n_total * 100:.2f}%)')
  print(f'>=2 leptons with at least 1 hadronic tau: {sel_cnt.GetValue()} ({sel_cnt.GetValue() / n_total * 100:.2f}%)')
  print(f'>=2 leptons with no hadronic tau: {sel_lep_cnt.GetValue()} ({sel_lep_cnt.GetValue() / n_total * 100:.2f}%)')
  print(f'>=2 hadronic taus (no e/mu): {had_sel_cnt.GetValue()} ({had_sel_cnt.GetValue() / n_total * 100:.2f}%)')
  print(f'>=2 hadronic taus (with e/mu): {had_lep_sel_cnt.GetValue()} ({had_lep_sel_cnt.GetValue() / n_total * 100:.2f}%)')

if __name__ == "__main__":

  import argparse
  parser = argparse.ArgumentParser(description='ttHH Selection efficiency.')
  parser.add_argument('--input', required=True, type=str, help="Input directory")
  args = parser.parse_args()

  compute_eff(args.input)
