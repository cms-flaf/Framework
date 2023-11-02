import ROOT
import os
from .Utilities import *

initialized = False

def Initialize():
    global initialized
    global tau1_p4
    global tau2_p4
    global b1_p4
    global b2_p4
    global MET_p4
    if initialized:
        raise RuntimeError('HH KinFitSel already initialized')
    headers_dir = os.path.dirname(os.path.abspath(__file__))
    header_path_AnalysisTools = "include/AnalysisTools.h"
    ROOT.gInterpreter.Declare(f'#include "{header_path_AnalysisTools}"')
    header_path_HHKinFit = "include/KinFitInterface.h"
    ROOT.gInterpreter.Declare(f'#include "{header_path_HHKinFit}"')
    header_path_SVFit = "include/SVfitAnaInterface.h"
    ROOT.gInterpreter.Declare(f'#include "{header_path_SVFit}"')
    header_path_MT2 = "include/MT2.h"
    ROOT.gInterpreter.Declare(f'#include "{header_path_MT2}"')
    header_path_Lester_mt2_bisect = "include/Lester_mt2_bisect.cpp"
    ROOT.gInterpreter.Declare(f'#include "{header_path_Lester_mt2_bisect}"')
    tau1_p4 = "ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>(tau1_pt,tau1_eta,tau1_phi,tau1_mass)"
    tau2_p4 = "ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>(tau2_pt,tau2_eta,tau2_phi,tau2_mass)"
    b1_p4 = "ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>(b1_pt,b1_eta,b1_phi,b1_mass)"
    b2_p4 = "ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>(b2_pt,b2_eta,b2_phi,b2_mass)"
    MET_p4 = "ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>(met_pt, 0., met_phi, 0.)"
    initialized = True

def GetMT2(df):
    if not initialized:
        raise RuntimeError("Legacy Variables not initialized!")
    df = df.Define('MT2', f'float(analysis::Calculate_MT2({tau1_p4}, {tau2_p4}, {b1_p4},{b2_p4}, {MET_p4}))')
    return df,['MT2']

def GetKinFit(df):
    if not initialized:
        raise RuntimeError("Legacy Variables not initialized!")
    df = df.Define("bjet1_JER", f"b1_ptRes*{b1_p4}.E()")
    df = df.Define("bjet2_JER", f"b2_ptRes*{b2_p4}.E()")
    df = df.Define("kinFit_result", f"""kin_fit::FitProducer::Fit({tau1_p4}, {tau2_p4},{b1_p4}, {b2_p4},
                                                       {MET_p4}, MET_covXX, MET_covXY, MET_covYY,
                                                       bjet1_JER,bjet2_JER, 0)""")

    df = df.Define('kinFit_convergence', 'kinFit_result.convergence')
    df = df.Define('kinFit_m', 'float(kinFit_result.mass)')
    df = df.Define('kinFit_chi2', 'float(kinFit_result.chi2)')
    return df,['kinFit_convergence','kinFit_m','kinFit_chi2']

def GetSVFit(df):
    df = df.Define('SVfit_result',f"""sv_fit::FitProducer::Fit({tau1_p4}, static_cast<Leg>(tau1_legType), tau1_decayMode,
                                                {tau2_p4}, static_cast<Leg>(tau2_legType), tau2_decayMode,
                                                {MET_p4}, MET_covXX, MET_covXY, MET_covYY)""")
    df = df.Define('SVfit_valid', 'int(SVfit_result.has_valid_momentum)')
    df = df.Define('SVfit_pt', 'float(SVfit_result.momentum.pt())')
    df = df.Define('SVfit_eta', 'float(SVfit_result.momentum.eta())')
    df = df.Define('SVfit_phi', 'float(SVfit_result.momentum.phi())')
    df = df.Define('SVfit_m', 'float(SVfit_result.momentum.mass())')
    df = df.Define('SVfit_pt_error', 'float(SVfit_result.momentum_error.pt())')
    df = df.Define('SVfit_eta_error', 'float(SVfit_result.momentum_error.eta())')
    df = df.Define('SVfit_phi_error', 'float(SVfit_result.momentum_error.phi())')
    df = df.Define('SVfit_m_error', 'float(SVfit_result.momentum_error.mass())')
    df = df.Define('SVfit_mt', 'float(SVfit_result.transverseMass)')
    df = df.Define('SVfit_mt_error', 'float(SVfit_result.transverseMass_error)')
    return df,['SVfit_valid', 'SVfit_pt', 'SVfit_eta', 'SVfit_phi', 'SVfit_m', 'SVfit_pt_error', 'SVfit_eta_error', 'SVfit_phi_error', 'SVfit_m_error', 'SVfit_mt', 'SVfit_mt_error']


