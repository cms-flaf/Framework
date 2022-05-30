import ROOT
import os
from scipy import stats
import numpy as np
def plot(hist, title,ch, outDir):
    # Canvas and general style options
    ROOT.gStyle.SetTextFont(42)
    d = ROOT.TCanvas("", "", 800, 700)
    # Make sure the canvas stays in the list of canvases after the macro execution
    ROOT.SetOwnership(d, False)
    d.SetLeftMargin(0.15)
    hist.SetTitle("")
    hist.GetXaxis().SetTitleSize(0.04)
    hist.GetYaxis().SetTitleSize(0.04)
    hist.SetLineWidth(2)
    #hist.SetMaximum(18)
    hist.SetLineWidth(2)
    hist.SetFillStyle(1001)
    hist.SetLineColor(ROOT.kBlack)
    hist.SetFillColor(ROOT.kAzure - 9)

    # Draw histograms
    hist.SetTitle(title)
    hist.SetName(title)
    hist.Draw("HIST")

    # Add legend
    legend = ROOT.TLegend(0.62, 0.70, 0.82, 0.88)
    legend.SetHeader(title)

    # Save plot
    if(not os.path.exists(outDir)):
        os.makedirs(outDir)
    d.SaveAs(f"{outDir}/{title}.pdf")
