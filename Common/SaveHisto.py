
def CreateHisto(list, labels, name, file):
    canvas = ROOT.TCanvas(name, name, 800, 600)
    n_points = len(list)
    graph = ROOT.TGraph(n_points)
    graph.SetNameTitle(name, name)
    for i in range(0, n_points):
        graph.SetPoint(i, i + 1., list[i])   
    h = graph.GetHistogram().Clone()
    h = h.Rebin(int(h.GetNbinsX()/(n_points)))
    for i in range(0, n_points):
        h.SetBinContent(i+1, list[i])
        h.GetXaxis().SetBinLabel(i+1, labels[i])    
    h.Draw() 
    canvas.SetBottomMargin(0.18)
    canvas.SetRightMargin(0.15)
    canvas.Update()
    canvas.Write()
    return 
 



if __name__ == "__main__":
    import argparse
    import os
    import re 
    from Common.Utilities import *
    parser = argparse.ArgumentParser()  
    parser.add_argument('--inFile', type=str) 
    parser.add_argument('--outFile', type=str) 
    args = parser.parse_args()
    num_vec, den_vec, cumul_vec, eff_vec, x_axes_vec = GetReportInfo(args.inFile)  
    file = ROOT.TFile.Open(args.outFile, "UPDATE")

    CreateHisto(num_vec, x_axes_vec, "numerator", file)
    
    CreateHisto(den_vec, x_axes_vec, "denumerator", file) 

    CreateHisto(eff_vec, x_axes_vec, "efficiency", file) 
    CreateHisto(cumul_vec, x_axes_vec, "cumulative", file) 
    file.Close()    
        