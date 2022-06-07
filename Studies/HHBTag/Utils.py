 # includere cose
# Enable multi-threading
import ROOT
from Common.BaselineSelection import *
_rootpath = os.path.abspath(os.path.dirname(__file__)+"/../../..")
ROOT.gROOT.ProcessLine(".include "+_rootpath)
ROOT.gStyle.SetOptStat(1111)

#header_analysis_tools = os.path.join(os.sep, os.environ['ANALYSIS_PATH'] + os.sep, "Common"+ os.sep,"AnalysisTools.h")
header_path_utils =os.path.join(os.sep, os.environ['ANALYSIS_PATH'] + os.sep, "Studies"+ os.sep,"HHBTag"+ os.sep,"Utilities.h")


ROOT.gInterpreter.Declare('#include "{}"'.format(header_path_utils))
#ROOT.gInterpreter.Declare('#include "{}"'.format(header_analysis_tools))

#def GetMPV(df, drawHisto = False ):
def findMPV(df):
    df_eTau = DefineDataFrame(df, "eTau")
    df_muTau = DefineDataFrame(df, "muTau")
    df_tauTau = DefineDataFrame(df, "tauTau")

    df_eTau_2bGenJets = df_eTau.Define("GenJet_b_PF", "RVecI GenJet_b_PF; for(int i =0 ; i<GenJet_partonFlavour.size(); i++){if (std::abs(GenJet_partonFlavour[i])==5){GenJet_b_PF.push_back(i);}} return GenJet_b_PF;").Define("GenJet_b_PF_size", "GenJet_b_PF.size()").Filter("GenJet_b_PF_size==2").Define("Two_bGenJets_invMass", "InvMassByFalvour(GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass, GenJet_partonFlavour, true)")
    histo_eTau_2bGenJets = df_eTau_2bGenJets.Histo1D(("Two_bGenJets_invMass_eTau", "Two_bGenJets_invMass_eTau", 400, -0.5, 199.5),"Two_bGenJets_invMass").GetValue()
    df_muTau_2bGenJets = df_muTau.Define("GenJet_b_PF", "RVecI GenJet_b_PF; for(int i =0 ; i<GenJet_partonFlavour.size(); i++){if (std::abs(GenJet_partonFlavour[i])==5){GenJet_b_PF.push_back(i);}} return GenJet_b_PF;").Define("GenJet_b_PF_size", "GenJet_b_PF.size()").Filter("GenJet_b_PF_size==2").Define("Two_bGenJets_invMass", "InvMassByFalvour(GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass, GenJet_partonFlavour, true)")
    histo_muTau_2bGenJets = df_muTau_2bGenJets.Histo1D(("Two_bGenJets_invMass_muTau", "Two_bGenJets_invMass_muTau", 400, -0.5, 199.5),"Two_bGenJets_invMass").GetValue()
    df_tauTau_2bGenJets = df_tauTau.Define("GenJet_b_PF", "RVecI GenJet_b_PF; for(int i =0 ; i<GenJet_partonFlavour.size(); i++){if (std::abs(GenJet_partonFlavour[i])==5){GenJet_b_PF.push_back(i);}} return GenJet_b_PF;").Define("GenJet_b_PF_size", "GenJet_b_PF.size()").Filter("GenJet_b_PF_size==2").Define("Two_bGenJets_invMass", "InvMassByFalvour(GenJet_pt, GenJet_eta, GenJet_phi, GenJet_mass, GenJet_partonFlavour, true)")
    histo_tauTau_2bGenJets = df_tauTau_2bGenJets.Histo1D(("Two_bGenJets_invMass_tauTau", "Two_bGenJets_invMass_tauTau", 400, -0.5, 199.5),"Two_bGenJets_invMass").GetValue()

    histo_tot = ROOT.TH1D(histo_eTau_2bGenJets)
    histo_tot.Add(histo_tot,histo_muTau_2bGenJets)
    histo_tot.Add(histo_tot,histo_tauTau_2bGenJets)
    #if(drawHisto):
    #    histo_tot_canvas = ROOT.TCanvas()
    #    histo_tot_canvas.cd()
    #    histo_tot.Draw()
    #    histo_tot_canvas.Update()
    #    input()

    #print(histo_tot.GetBinWidth(2))
    y_max = histo_tot.GetMaximumBin()
    x_max = histo_tot.GetXaxis().GetBinCenter(y_max)
    return x_max

def EvaluateDiffInt(array):
    step = 0.001
    q = np.arange(0, 1 + step, step)
    x_q = np.quantile(array, step)

    interval = 0.68
    quantiles=[]
    differences =[]
    diff_quantiles =[]
    for i in q:
        for j in q:
            if((i-j)==0.68):
                differences.append([j, i])
                quantiles.append([np.quantile(array, j),np.quantile(array, i)])
                diff_quantiles.append(np.quantile(array, i)-np.quantile(array, j))
    minimum=min(diff_quantiles)
    min_index= diff_quantiles.index(minimum)
    min_diff = differences[min_index ]
    min_quantiles= quantiles[min_index  ]
    #print(f"min_index = {min_index}, minimum = {minimum}, min_diff = {min_diff}, min_quantiles = {min_quantiles} ")

    return min_index, minimum, min_diff, min_quantiles
