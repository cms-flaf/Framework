#yumeng
import datetime
import os
import sys
import ROOT
import shutil
import zlib

import argparse
import traceback

ROOT.gROOT.ProcessLine(".include " + os.environ['ANALYSIS_PATH'])
header_path_RootExt = "include/RootExt.h"
header_path_GenLepton = "include/GenLepton.h"
header_path_Gen = "include/BaselineGenSelection.h"
header_path_Reco = "include/BaselineRecoSelection.h"
header_path_HHbTag = "include/HHbTagScores.h"
ROOT.gInterpreter.Declare(f'#include "{header_path_RootExt}"')
ROOT.gInterpreter.Declare(f'#include "{header_path_GenLepton}"')
ROOT.gInterpreter.Declare(f'#include "{header_path_Gen}"')
ROOT.gInterpreter.Declare(f'#include "{header_path_Reco}"')

parser = argparse.ArgumentParser()
parser.add_argument('--particleFile', type=str,
                    default=f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt")

args = parser.parse_args()

ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{args.particleFile}\");")

def check_root_file(file_path):
    if not os.path.exists(file_path):
        print(f"Error: The file '{file_path}' does not exist.")
        return False

    root_file = ROOT.TFile.Open(file_path)
    if not root_file or root_file.IsZombie():
        print(f"Error: The file '{file_path}' can not be opened.")
        return False

    print(f"Success: The file '{file_path}' is valid")
    root_file.Close()
    return True

def get_event_counts(df):
    try:
        # if df.Count().GetValue() > 0:
        #     return df.Count().GetValue()
        # else:
        #     return 0
        return df.Count().GetValue()
    except Exception as e:
        print(f"Error counting events: {e}")
        #traceback.print_exc()
        #return 0

def process_tree_Gen(file_path, tree_name):
    df = ROOT.RDataFrame(tree_name, file_path)

    df = df.Define("GenJet_p4", "RVecLV()")
    df = df.Define("GenPart_daughters", "GetDaughters(GenPart_genPartIdxMother)")
    
    if tree_name == "Events":
        df = df.Define("genLeptons", """reco_tau::gen_truth::GenLepton::fromNanoAOD(GenPart_pt, GenPart_eta,
                                    GenPart_phi, GenPart_mass, GenPart_genPartIdxMother, GenPart_pdgId,
                                    GenPart_statusFlags, event)""")
        df = df.Define("H_to_VV", """GetGenHVVCandidate(event, genLeptons, GenPart_pdgId, GenPart_daughters, 
                                  GenPart_statusFlags, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, GenJet_p4, true)""")
    else:
        df = df.Define("genLeptons", """reco_tau::gen_truth::GenLepton::fromNanoAOD(GenPart_pt, GenPart_eta,
                                    GenPart_phi, GenPart_mass, GenPart_genPartIdxMother, GenPart_pdgId,
                                    GenPart_statusFlags,-1)""")
        df = df.Define("H_to_VV", """GetGenHVVCandidate(-1,genLeptons, GenPart_pdgId, GenPart_daughters, 
                                  GenPart_statusFlags, GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass, GenJet_p4, true)""")
    
    df = df.Define("Leg0_Kind", "H_to_VV.legs.at(0).leg_kind.at(0)")
    df= df.Define("GenLep0_p4","H_to_VV.legs.at(0).leg_vis_p4.at(0)")
    
    genSL_count = 0
    gen_accept_count = 0

    if mychannel == "sle":
        df_GenSL=df.Filter("Leg0_Kind==Vleg::PromptElectron|| Leg0_Kind==Vleg::TauDecayedToElectron", "")
        genSL_count = get_event_counts(df_GenSL)

        df_Gen_Accept = df_GenSL.Filter("GenLep0_p4.pt()>10 && abs (GenLep0_p4.eta()) <2.5", "")
        gen_accept_count = get_event_counts(df_Gen_Accept)
    
    if mychannel == "slmuon":
        df_GenSL=df.Filter("Leg0_Kind==Vleg::PromptMuon|| Leg0_Kind==Vleg::TauDecayedToMuon", "")
        genSL_count = get_event_counts(df_GenSL)

        df_Gen_Accept = df_GenSL.Filter("GenLep0_p4.pt()>15 && abs (GenLep0_p4.eta()) <2.4", "")
        gen_accept_count = get_event_counts(df_Gen_Accept)

    return df_GenSL, df_Gen_Accept, genSL_count, gen_accept_count

def RecoHWWCandidateSelection(df):
    def add_filter(df, base_cut, and_cut, label):
        full_cut = f"Sum({base_cut} && {and_cut}) > 0"
        df = df.Filter(full_cut, label)
        return df, f"{base_cut} && {and_cut}"
    
    if mychannel == "sle":
        base_cut = "Electron_pt > 10"
        df = df.Filter("Sum(Electron_pt > 10) > 0", "Reco_e_pt>10")
        df, base_cut = add_filter(df, base_cut, "abs(Electron_eta) <2.5", "Reco_e_eta<2.5")
        df, base_cut = add_filter(df, base_cut, "abs(Electron_dz) <0.1", "Reco_e_dz<0.1")
        df, base_cut = add_filter(df, base_cut, "abs(Electron_dxy) <0.05", "Reco_e_dxy<0.05")
        df, base_cut = add_filter(df, base_cut, "Electron_sip3d <= 8", "Reco_e_sip3d <= 8")
        df, base_cut = add_filter(df, base_cut, "Electron_miniPFRelIso_all < 0.4", "Reco_e_miniPFRelIso_all < 0.4")
        df, base_cut = add_filter(df, base_cut, "Electron_mvaIso_WP90", "Reco_e_mvaIso_WP90")
        df, base_cut = add_filter(df, base_cut, "Electron_mvaIso_WP80", "Reco_e_mvaIso_WP80")

    # #double check:
    #     df = df.Filter("Sum(Electron_pt > 10) > 0", "Reco_e_pt>10")
    #     df = df.Filter("Sum(Electron_pt > 10 && abs(Electron_eta) <2.5) > 0", "Reco_e_eta<2.5")
    #     df = df.Filter("Sum(Electron_pt > 10 && abs(Electron_eta) <2.5 && abs(Electron_dz) <0.1) > 0", "Reco_e_dz<0.1")

    if mychannel== "slmuon":
        base_cut = "Muon_pt > 15"
        df = df.Filter("Sum(Muon_pt > 15) > 0", "Reco_mu_pt>15")
        df, base_cut = add_filter(df, base_cut, "abs(Muon_eta) <2.4", "Reco_mu_eta<2.4")
        df, base_cut = add_filter(df, base_cut, "abs(Muon_dz) <0.1", "Reco_mu_dz<0.1")
        df, base_cut = add_filter(df, base_cut, "abs(Muon_dxy) <0.05", "Reco_mu_dxy<0.05")
        df, base_cut = add_filter(df, base_cut, "Muon_sip3d <= 8", "Reco_mu_sip3d <= 8")
        df, base_cut = add_filter(df, base_cut, "Muon_miniPFRelIso_all < 0.4", "Reco_mu_miniPFRelIso_all < 0.4")
        df, base_cut = add_filter(df, base_cut, "Muon_looseId", "Mu_looseId")
        df, base_cut = add_filter(df, base_cut, "Muon_tightId", "Muon_tightId")
    return df

#20240910: This definition has not been used.
def RecoHWWJetSelection(df):
    df = df.Define("Jet_Incl", "Jet_pt>20 && abs(Jet_eta) < 2.5 && ( Jet_jetId & 2 )")
    df = df.Define("FatJet_Incl", "FatJet_pt >200 && abs(FatJet_eta) < 2.5 ) && ( FatJet_jetId & 2 ) && (FatJet_msoftdrop > 30) ")
    ######from here not modified
    df = df.Define("Jet_sel", """return RemoveOverlaps(Jet_p4, Jet_Incl,HwwCandidate.getLegP4s(), 0.4);""")
    df = df.Define("FatJet_sel", """return RemoveOverlaps(FatJet_p4, FatJet_Incl,HwwCandidate.getLegP4s(), 0.4);""")
    df = df.Define("Jet_cleaned", " RemoveOverlaps(Jet_p4, Jet_sel,{ {FatJet_p4[FatJet_sel][0], },}, 1, 0.8)")

    df = df.Define("n_eff_Jets", "(FatJet_p4[FatJet_sel].size()*2)+(Jet_p4[Jet_cleaned].size())")
    df = df.Define("n_eff_jets_SL","(is_SL && n_eff_Jets>=3)")
    df = df.Define("n_eff_jets_DL","(!is_SL && n_eff_Jets>=2)")

    return df.Filter(" (n_eff_jets_SL || n_eff_jets_DL)", "Reco bjet candidates")

def Report_cutflow_hist(canvas, mass_point, report, initial_count, genSL_count,  gen_accept_count, skim_count, reportName="Cutflow", printOut=False):
    cuts = [c for c in report]
    num_cuts = len(cuts)

    hist_eff = ROOT.TH1D(reportName + f"__M-{mass_point}" + "_Efficiency", reportName +" Efficiency" + "_for Masspoints", num_cuts + 4, 0, num_cuts + 4)
    hist_eff.GetXaxis().SetBinLabel(1, "Initial_NoSkim")
    hist_eff.SetBinContent(1, 1.0)
    # hist_eff.SetBinError(1, 0.0) 
    hist_eff.SetMinimum(0)

    if mychannel== "sle":
        hist_eff.GetXaxis().SetBinLabel(2, "GenSLe")
        hist_eff.SetBinContent(2, genSL_count/initial_count)

        hist_eff.GetXaxis().SetBinLabel(3, "Gen_e_Accept")
        hist_eff.SetBinContent(3, gen_accept_count/genSL_count)

        hist_eff.GetXaxis().SetBinLabel(4, "Skim")
        hist_eff.SetBinContent(4,skim_count/gen_accept_count)

    if mychannel== "slmuon":
        hist_eff.GetXaxis().SetBinLabel(2, "GenSLMu")
        hist_eff.SetBinContent(2, genSL_count/initial_count)

        hist_eff.GetXaxis().SetBinLabel(3, "Gen_Mu_Accept")
        hist_eff.SetBinContent(3, gen_accept_count/genSL_count)

        hist_eff.GetXaxis().SetBinLabel(4, "Skim")
        hist_eff.SetBinContent(4,skim_count/gen_accept_count)

    for c_id, cut in enumerate(cuts):

        # p = cut.GetEff() / 100
        # n = cut.GetAll()
        # binomial_error = ROOT.TMath.Sqrt(p * (1 - p) / n)

        hist_eff.SetBinContent(c_id + 5, cut.GetEff()/100)
        hist_eff.GetXaxis().SetBinLabel(c_id + 5, cut.GetName())
        #hist_eff.SetBinError(c_id + 5, binomial_error)
    
    hist_eff.GetXaxis().SetLabelSize(0.03);  
    
    #hist_eff.GetXaxis().LabelsOption("v");
    canvas.SetBottomMargin(0.25);

    return hist_eff

def genDistribution_hist(df):
    if var == "pt":
        df = df.Define("GenLep0_pt", "GenLep0_p4.pt()") 

        # pt_min = df.Min("GenLep0_pt").GetValue()
        # pt_max = df.Max("GenLep0_pt").GetValue()
        pt_min = 0
        #pt_max = 2150 for eventstree 5000, >1000 for eventstree 3000
        pt_max = 500

        bins = int((pt_max - pt_min)/4)
        model = ROOT.RDF.TH1DModel(f"GenLep0_pT_M-{mass_point}", f"GenLep0 pT for Masspoints ({mychannel})", bins, pt_min, pt_max)
        hist_pt = df.Histo1D(model, "GenLep0_pt")
        #df.Foreach(lambda pt: hist_pt.Fill(pt), ["GenLep0_pt"])
        hist_pt.GetXaxis().SetTitle(f"{mychannel} pT(GeV)")
        hist_pt.GetYaxis().SetTitle("Normalized Distribution")
        return hist_pt
    
    if var == "eta":
        df = df.Define("GenLep0_eta", "GenLep0_p4.eta()") 

        # eta_min = df.Min("GenLep0_eta").GetValue()
        # eta_max = df.Max("GenLep0_eta").GetValue()
        eta_min = -6 #theta/eta 0/5~180/-5
        eta_max = 6

        bins=10 #normally bins=50, bins = int((eta_max - eta_min)/4)
        model = ROOT.RDF.TH1DModel(f"GenLep0_eta_M-{mass_point}", f"GenLep0 eta for Masspoints ({mychannel})", bins, eta_min, eta_max)
        hist_eta = df.Histo1D(model, "GenLep0_eta")
        #df.Foreach(lambda pt: hist_eta.Fill(eta), ["GenLep0_eta"])
        hist_eta.GetXaxis().SetTitle(f"{mychannel} eta")
        hist_eta.GetYaxis().SetTitle("Normalized Distribution")
        return hist_eta

######################################################################def of main functions starts:
######################################################################common configs:
######################################################################change save paths:
def main(mychannel, var, hist_type):
    mass_points = [250, 450, 650, 1000, 3000, 5000]
    colors = [ROOT.kRed, ROOT.kBlue, ROOT.kGreen, ROOT.kMagenta, ROOT.kCyan, ROOT.kOrange]
    # mass_points = [250]

    histograms = []
    canvas = ROOT.TCanvas("canvas", "Mass Points Comparison", 800, 600)
    canvas.SetGrid()
    legend = ROOT.TLegend(0.75, 0.9, 1, 1)
    legend.SetNColumns(2)

    for i, mass_point in enumerate(mass_points):
        file_path = f"/eos/cms/store/group/phys_higgs/HLepRare/skim_2024_v1/Run3_2022/GluGlutoRadiontoHHto2B2Vto2B2JLNu_M-{mass_point}/nano_0.root"
        if check_root_file(file_path):
            df_eventstree = ROOT.RDataFrame("Events", file_path)
            df_notselected = ROOT.RDataFrame("EventsNotSelected", file_path)
            eventscount=get_event_counts(df_eventstree)
            notselectedcount=get_event_counts(df_notselected)
        initial_count = eventscount + notselectedcount

        df_GenSL_Eventstree, df_GenAccept_Eventstree,  genSL_count_eventstree, gen_accept_count_eventstree = process_tree_Gen(file_path, "Events")
        df_GenSL_Notselected, df_GenAccept_Notselected,  genSL_count_notselected, gen_accept_count_notselected = process_tree_Gen(file_path, "EventsNotSelected")

        if hist_type =="cutflow":
            hist=process_cutflow_hist(canvas, mass_point, initial_count, genSL_count_eventstree, genSL_count_notselected, gen_accept_count_eventstree, gen_accept_count_notselected, df_GenAccept_Eventstree)
        if hist_type =="genDistribution":
            hist=process_genDistribution_hist(mass_point, df_GenSL_Eventstree, df_GenSL_Notselected)

        legend.AddEntry(hist, f"M-{mass_point}", "l")

        hist.SetLineColor(colors[i])
        histograms.append(hist)

        if i == 0:
            hist.Draw("HIST")
            ROOT.gStyle.SetOptStat(0)
        else:
            hist.Draw("HIST SAME")
            ROOT.gStyle.SetOptStat(0)

    legend.Draw()

    if hist_type =="cutflow":
            canvas.SaveAs(f"plots/MiniGenRecoCut_Combine_{mychannel}.png")
    if hist_type =="genDistribution": 
            canvas.SaveAs(f"plots/cutplusGen_{var}_Combine_{mychannel}.png")
     
######################################################################def of process cutflow hist
def process_cutflow_hist(canvas, mass_point, initial_count, genSL_count_eventstree, genSL_count_notselected, gen_accept_count_eventstree, gen_accept_count_notselected, df_GenAccept_Eventstree):
    genSL_count= genSL_count_eventstree + genSL_count_notselected
    gen_accept_count= gen_accept_count_eventstree + gen_accept_count_notselected
    skim_count = gen_accept_count_eventstree

    df_cutflow=RecoHWWCandidateSelection(df_GenAccept_Eventstree)
    #df_cutflow=RecoHWWJetSelection(df_cutflow)

    report=df_cutflow.Report()

    hist = Report_cutflow_hist(canvas, mass_point, report, initial_count, genSL_count, gen_accept_count, skim_count)
    return hist

######################################################################def of process genDistribution hist
def process_genDistribution_hist(mass_point, df_GenSL_Eventstree, df_GenSL_Notselected):
    #if no need for adding Events and EventsNotSelected, only 2 lines below
    hist1 = genDistribution(df_GenSL_Eventstree)
    #hist = hist1.GetValue()  # Ensure the histogram is fully evaluated
    
    hist1_evaluated = hist1.GetValue() 
    hist2 = genDistribution(df_GenSL_Notselected)
    hist2_evaluated = hist2.GetValue()
    hist = hist1_evaluated.Clone(f"GenLep0_add_{mass_point}")
    hist.Add(hist2_evaluated)

    #if normalization
    integral = hist.Integral()
    print(f"Integral of the histogram: {integral}")
    if integral != 0:
        hist.Scale(1.0 / integral)
    else:
        print("Warning: Histogram has zero integral, cannot normalize.")
    
    if var == "eta":
        hist.GetYaxis().SetRangeUser(0, 0.5) 

######################################################################run!
mychannel = "sle"
#mychannel = "slmuon"

var = "pt"
#var = "eta"

hist_type ="cutflow"
#hist_type = "genDistribution"

main(mychannel, var, hist_type)



    

    
    


    






