import ROOT
import numpy as np
import Common.Utilities as Utilities
import Common.PrintGenChain as PrintGenChain
import Common.BaselineSelection as Baseline
 
snapshotOptions = ROOT.RDF.RSnapshotOptions()
snapshotOptions.fOverwriteIfExists=True
 

def createSkim(inFile, outFile, period, sample, X_mass, mpv):
    Baseline.Initialize()

    df = ROOT.RDataFrame("Events", inFile)

    df = Baseline.ApplyGenBaseline0(df)
    for var in ["GenJet", "GenJetAK8"]:   
        df = df.Define(f"{var}_idx", f"CreateIndexes({var}_pt.size())")
        df = df.Define(f"{var}_p4", f"GetP4({var}_pt,{var}_eta,{var}_phi,{var}_mass, {var}_idx)")
        df = df.Define(f"{var}_b_PF",f"abs({var}_partonFlavour)==5")
        df = df.Define(f"{var}_idx_1", f"RemoveOverlaps({var}_p4, {var}_b_PF,{{{{genHttCand.leg_p4[0], genHttCand.leg_p4[1]}},}}, 2, 0.5)").Define(f"{var}_bJetp4", f"{var}_p4[{var}_idx_1]").Define(f"n_b{var}", f"{var}_bJetp4.size()")
    h1 = df.Histo1D(f"n_bGenJet").GetValue()

    #print(df.GetColumnNames())
    h2 = df.Filter("n_bGenJet==1").Define("GenJet_b_invMass", "GenJet_bJetp4[0].M()").Histo1D(f"GenJet_b_invMass").GetValue()
    h3 = df.Filter("n_bGenJet==1").Define("GenJet_invMass", "GenJet_p4[0].M()").Histo1D(f"GenJet_invMass").GetValue()

    df = df.Filter("n_bGenJet==1 && n_bGenJetAK8 == 1").Define("GenJetAK8_b_invMass", "GenJetAK8_bJetp4[0].M()").Define("GenJetAK8_invMass", "GenJetAK8_p4[0].M()")
    
    df=df.Define("hbbInvMass", "(genHbbCand.leg_p4[0]+genHbbCand.leg_p4[1]).M()")
    df = df.Filter(" GenJetAK8_b_invMass>80 ")
    h4 = df.Histo1D(f"GenJetAK8_b_invMass").GetValue()
    h5 = df.Histo1D(f"GenJetAK8_invMass").GetValue()
    y_max_4 = h4.GetMaximumBin()
    x_max_4 = h4.GetXaxis().GetBinCenter(y_max_4)  
    y_max_5 = h5.GetMaximumBin()
    x_max_5 = h5.GetXaxis().GetBinCenter(y_max_5)  
    print(f"x_max for GenJetAK8_b_invMass is {x_max_4} \n x_max for GenJetAK8_invMass is {x_max_5} ")
    h_hbb = df.Histo1D("hbbInvMass").GetValue()
    scatter_plot = df.Histo2D(("hbbVSgenJetAK8mass", "hbbVSgenJetAK8mass", 100, 0., 250., 100, 0., 250.), "hbbInvMass", "GenJetAK8_invMass").GetValue()
    scatter_plot2 = df.Histo2D(("hbbVSbgenJetAK8mass", "hbbVSbgenJetAK8mass", 100, 0., 250., 100, 0., 250.), "hbbInvMass", "GenJetAK8_b_invMass").GetValue()

    histFile = ROOT.TFile(f"GenJets_b_mass_m{X_mass}.root", "RECREATE") 
    h1.Write()
    h2.Write()
    h3.Write()
    h4.Write()
    h5.Write()
    h_hbb.Write()
    scatter_plot.Write() 
    scatter_plot2.Write() 
    histFile.Close()
    '''
    df.Display({"GenJetAK8_b_invMass", "event"}).Print()
     
    event= 269 # 50.968750  
    out = f"output/GenChain_mass{X_mass}_evt{event}.txt"
    df=df.Filter("event==269")
    PrintGenChain.PrintDecayChain(df, str(event),out)
    df.Display({"GenJetAK8_b_invMass","GenJetAK8_invMass" }).Print() 
    df.Display({"GenJetAK8_mass","GenJetAK8_pt"}).Print()
    df.Display({"GenJetAK8_eta","GenJetAK8_phi"}).Print()
     "GenJetAK8_pt","GenJetAK8_eta","GenJetAK8_phi",
    h_hbb = df.Histo1D("hbbInvMass").GetValue()
    scatter_plot = df.Histo2D(("hbbVSgenJetAK8mass", "hbbVSgenJetAK8mass", 100, 0., 250., 100, 0., 250.), "hbbInvMass", "GenJetAK8_invMass").GetValue()
    scatter_plot2 = df.Histo2D(("hbbVSbgenJetAK8mass", "hbbVSbgenJetAK8mass", 100, 0., 250., 100, 0., 250.), "hbbInvMass", "GenJetAK8_b_invMass").GetValue()

    histFile = ROOT.TFile(f"GenJets_b_mass_m{X_mass}.root", "RECREATE") 
    h1.Write()
    h2.Write()
    h3.Write()
    h4.Write()
    h5.Write()
    h_hbb.Write()
    scatter_plot.Write() 
    scatter_plot2.Write() 
    histFile.Close()
     df = df.Define("GenJet_bJetp4", "GenJet_p4[GenJet_b_PF]").Define("n_bJet", "GenJet_bJetp4.size()")
    h2 = df.Histo1D("n_bJet").GetValue()
    df = df.Define("GenJetAK8_bJetp4", "GenJetAK8_p4[GenJetAK8_b_PF]").Define("GenJetAK8_bJet_size","GenJetAK8_bJetp4.size()")  
    df = df.Define("First_GenJetAK8_b_invMass", "GenJetAK8_bJetp4[0].M()")  
    df = df.Define("FirstTwo_GenJetAK8_b_invMass", "(GenJetAK8_bJetp4[0]+GenJetAK8_bJetp4[1]).M()")  

    h1 = df.Filter("GenJetAK8_bJet_idx.size()==1").Define("First_GenJetAK8_b_invMass", "GenJetAK8_bJetp4[0].M()").Histo1D("First_GenJetAK8_b_invMass").GetValue()
    h2 = df.Filter("GenJetAK8_bJet_idx.size()==2").Define("FirstTwo_GenJetAK8_b_invMass", "(GenJetAK8_bJetp4[0]+GenJetAK8_bJetp4[1]).M()").Histo1D("FirstTwo_GenJetAK8_b_invMass").GetValue()
    h2 = df.Filter("GenJetAK8_bJet_idx.size()==3").Define("FirstThree_GenJetAK8_b_invMass", "(GenJetAK8_bJetp4[0]+GenJetAK8_bJetp4[1]).M()").Histo1D("FirstTwo_GenJetAK8_b_invMass").GetValue()
    
 
    df = df.Define("GenJet_bJetp4", "GenJet_p4[GenJet_b_PF]").Filter("GenJet_bJetp4.size()==2").Define("GenJet_b_invMass", "(GenJet_bJetp4[0]+GenJet_bJetp4[1]).M()")  

    h3 = df.Histo1D("GenJet_b_invMass").GetValue()
    h4 = df.Histo1D("nGenJetAK8").GetValue()
    h5 = df.Histo1D("GenJetAK8_bJet_size").GetValue()
    y_max_1 = h1.GetMaximumBin()
    x_max_1 = h1.GetXaxis().GetBinCenter(y_max_1) 
    y_max_2 = h2.GetMaximumBin()
    x_max_2 = h2.GetXaxis().GetBinCenter(y_max_2) 
    y_max_3 = h3.GetMaximumBin()
    x_max_3 = h3.GetXaxis().GetBinCenter(y_max_3)
    print(f"x_max for First_GenJetAK8_b_invMass is {x_max_1} \n x_max for FirstTwo_GenJetAK8_b_invMass is {x_max_2} \n x_max for GenJet_b_invMass is {x_max_3}")
    histFile = ROOT.TFile("GenJets_b_mass.root", "RECREATE")
    h1.Write()
    h2.Write()
    h3.Write()
    h4.Write()
    histFile.Close()

    '''

if __name__ == "__main__":
    import argparse
    import os
    import re 

    parser = argparse.ArgumentParser()
    parser.add_argument('--period', type=str)
    parser.add_argument('--inFile', type=str)
    parser.add_argument('--outFile', type=str)
    parser.add_argument('--mass', type=int)
    parser.add_argument('--mpv', type=float, default=120.75) 
    parser.add_argument('--sample', type=str)
    parser.add_argument('--compressionLevel', type=int, default=9)
    parser.add_argument('--compressionAlgo', type=str, default="kLZMA")
    parser.add_argument('--particleFile', type=str,
                        default=f"{os.environ['ANALYSIS_PATH']}/config/pdg_name_type_charge.txt")
    args = parser.parse_args()
    
    
    ROOT.gROOT.SetBatch(True)
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "Common/GenTools.h"')
    ROOT.gInterpreter.ProcessLine(f"ParticleDB::Initialize(\"{args.particleFile}\");") 
    snapshotOptions.fCompressionLevel=args.compressionLevel 
    setattr(snapshotOptions, 'fCompressionAlgorithm', Utilities.compression_algorithms[args.compressionAlgo])
    createSkim(args.inFile, args.outFile, args.period, args.sample, args.mass, args.mpv)
        