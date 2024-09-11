import ROOT as rt
import CMS_lumi, tdrstyle
import array

import mplhep as hep
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import matplotlib.patches as patches

#plt.rcParams.update({
#    "text.usetex": True,
    #"font.family": "sans-serif",
    #"font.sans-serif": "Helvetica",
#})

period_dict = {
    "Run2_2018" : "59.83",
    "Run2_2017" : "41.48",
    "Run2_2016" : "16.8",
    "Run2_2016_HIPM" : "19.5",
    "all": "138.8 fb^{-1} (13 TeV)",

}
def plot_2D_histogram(histogram, title, xlabel, ylabel, bin_labels, filename, period, rectangle_coordinates):
    hep.style.use("CMS")
    plt.figure(figsize=(25, 15))
    plt.xlabel(xlabel, fontsize=40)
    plt.ylabel(ylabel, fontsize=40)
    hep.hist2dplot(histogram, cmap='cool', cmax=histogram.GetMaximum())
    #hep.cms.text("work in progress")
    #hep.cms.text("")
    #hep.cms.label("Preliminary")
    hep.cms.label("Preliminary", lumi=period_dict[period], year=period.split('_')[1], fontsize=40)
    if bin_labels:
        textstr = '\n'.join([f"{i}. {label}" for i, label in enumerate(bin_labels)])
        plt.gcf().text(0.8, 0.5, textstr, fontsize=14)


    #plt.grid(True)
    fig = plt.gcf()
    ax = fig.gca()
    print(rectangle_coordinates)
    x1,y1,lenx,leny = rectangle_coordinates
    rect = patches.Rectangle((x1, y1), lenx, leny, linewidth=2, edgecolor='r', facecolor='none')
    ax.add_patch(rect)
    plt.gca().set_aspect('auto')
    plt.tight_layout(rect=[0, 0, 0.8, 1])
    plt.savefig(filename, bbox_inches='tight')
    plt.show()

def create_2D_histogram(histogram, canvas, output_file, global_cfg_dict,period,cat='other'):
    """
    Function to create and save a 2D histogram plot with CMS style.

    Args:
    histogram: TH2F, 2D histogram object (MC or data)
    canvas: TCanvas, canvas to draw on
    output_file: str, name of the output file to save the histogram (e.g. 'output.root')

    Returns:
    Saves the canvas in a ROOT file.
    """

    # Set the TDR style
    tdrstyle.setTDRStyle()

    # CMS Lumi settings
    CMS_lumi.extraText = "Preliminary"

    CMS_lumi.relPosX = 0.12

    # Canvas and margin settings
    H_ref = 900
    W_ref = 900
    W = W_ref
    H = H_ref

    T = 0.15 * H_ref
    B = 0.15 * H_ref
    L = 0.25 * W_ref
    R = 0.1 * W_ref

    # Configure canvas
    canvas.SetFillColor(0)
    canvas.SetBorderMode(0)
    canvas.SetFrameFillStyle(0)
    canvas.SetFrameBorderMode(0)
    canvas.SetLeftMargin(L / W)
    canvas.SetRightMargin(R / W)
    canvas.SetTopMargin(T / H)
    canvas.SetBottomMargin(B / H)

    canvas.SetTickx(0)
    canvas.SetTicky(0)
    #canvas.SetTickz(0)


    for xbin in range(1,histogram.GetNbinsX()+1):
        if int(histogram.GetXaxis().GetBinCenter(xbin))== global_cfg_dict['mass_cut_limits']['bb_m_vis'][cat][0]:
            x1 = histogram.GetXaxis().GetBinLowEdge(xbin)
        if int(histogram.GetXaxis().GetBinLowEdge(xbin))== global_cfg_dict['mass_cut_limits']['bb_m_vis'][cat][0]:
            x1 = histogram.GetXaxis().GetBinLowEdge(xbin)
        if int(histogram.GetXaxis().GetBinCenter(xbin))==global_cfg_dict['mass_cut_limits']['bb_m_vis'][cat][1]:
            x2 = histogram.GetXaxis().GetBinLowEdge(xbin)
        if int(histogram.GetXaxis().GetBinUpEdge(xbin))==global_cfg_dict['mass_cut_limits']['bb_m_vis'][cat][1]:
            x2 = histogram.GetXaxis().GetBinUpEdge(xbin)
            #print(histogram.GetXaxis().GetBinCenter(xbin))
            #print( global_cfg_dict['mass_cut_limits']['bb_m_vis']['other'][0])
            #print(histogram.GetXaxis().GetBinLowEdge(xbin))
            #print( global_cfg_dict['mass_cut_limits']['bb_m_vis']['other'][1])

    for ybin in range(0,histogram.GetNbinsY()+1):
        if histogram.GetYaxis().GetBinCenter(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][0]:
            y1 = histogram.GetYaxis().GetBinLowEdge(ybin)
        if histogram.GetYaxis().GetBinLowEdge(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][0]:
            y1 = histogram.GetYaxis().GetBinLowEdge(ybin)
        if histogram.GetYaxis().GetBinCenter(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][1]:
            y2 = histogram.GetYaxis().GetBinLowEdge(ybin)
        if histogram.GetYaxis().GetBinUpEdge(ybin)==global_cfg_dict['mass_cut_limits']['tautau_m_vis'][1]:
            y2 = histogram.GetYaxis().GetBinUpEdge(ybin)

    box = rt.TBox(x1, y1, x2, y2)
    box.SetLineColor(rt.kRed)  # Colore del bordo rosso
    box.SetLineWidth(3)        # Spessore della linea
    box.SetFillStyle(0)        # Nessun riempimento


    # Draw the 2D histogram
    histogram.Draw("COLZ")
    box.Draw("same")           # Disegna il rettangolo sulla canvas

    # Set axis titles and adjust offsets
    histogram.GetXaxis().SetTitleOffset(0.7)
    histogram.GetYaxis().SetTitleOffset(0.7)

    histogram.GetXaxis().SetTitleSize(0.06)
    histogram.GetYaxis().SetTitleSize(0.06)
    # Add CMS and lumi text
    CMS_lumi.CMS_lumi(canvas, period, 0)

    # Update and save the canvas as a PDF file
    canvas.Update()
    canvas.SaveAs(output_file)

    print(f"2D histogram saved to {output_file}")
