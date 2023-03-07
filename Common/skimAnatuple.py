import ROOT
import os
headers_dir = os.path.dirname(os.path.abspath(__file__))
header_path_AnalysisTools = os.path.join(headers_dir, "AnalysisTools.h")
ROOT.gInterpreter.Declare(f'#include "{header_path_AnalysisTools}"')

def skimAnatuple(df):
    for tau_idx in [1,2]:
        df = df.Filter(f'tau{tau_idx}_pt>40 && abs(tau{tau_idx}_eta)<2.1')
    df = df.Filter('DeltaR(tau1_eta,tau1_phi, tau2_eta,tau2_phi) > 0.5')
    df = df.Filter('tau1_charge+tau2_charge==0')
    return df

def findInvMass(df):
    for idx in [0,1]:
        df = df.Define(f"tau{idx+1}_p4", f"ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>(tau{idx+1}_pt,tau{idx+1}_eta,tau{idx+1}_phi,tau{idx+1}_mass)")
    df = df.Define("tau_inv_mass", "(tau1_p4+tau2_p4).M()")
    return df