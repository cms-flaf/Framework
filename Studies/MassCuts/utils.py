import ROOT
import CMS_lumi, tdrstyle
import array
import os
import mplhep as hep
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import matplotlib.patches as patches
hep.style.use("CMS")

# https://matplotlib.org/stable/users/explain/colors/colormaps.html
# https://mplhep.readthedocs.io/en/latest/api.html
# https://www.desy.de/~tadej/tutorial/matplotlib_tutorial.html

#plt.rcParams.update({
#    "text.usetex": True,
    #"font.family": "sans-serif",
    #"font.sans-serif": "Helvetica",
#})
# inFiles = Utilities.ListToVector([
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1000/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1250/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1500/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-1750/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-2000/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-250/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-2500/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-260/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-270/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-280/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-300/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-3000/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-320/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-350/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-400/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-450/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-500/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-550/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-600/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-650/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-700/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-750/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-800/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-850/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToBulkGravitonToHHTo2B2Tau_M-900/nanoHTT_0.root" , f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1000/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1250/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1500/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-1750/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-2000/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-250/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-2500/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-260/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-270/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-280/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-300/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-3000/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-320/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-350/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-400/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-450/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-500/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-550/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-600/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-650/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-700/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-750/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-800/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-850/nanoHTT_0.root",
#         f"/eos/user/a/aciocci/HHbbTauTauRes/anaTuples/v13_deepTau2p1_HTT/SC/Run2_{args.year}/GluGluToRadionToHHTo2B2Tau_M-900/nanoHTT_0.root"
#     ])

def getLabels(cat,year,channel,mass,resonance,labels):
    bins = GetCorrectBinning()

    cat_name = cat.split('_')[0]
    # if cat_name == 'baseline':
    #     cat_name == cat
    outDir = f"output/Masses_histograms/Run2_{year}/{cat_name}/{channel}"
    if not os.path.isdir(outDir):
        os.makedirs(outDir)
    # outFileName = f"{outDir}/M_{mass}"
    # print(resonance)

    if resonance != 'both' :
        outFileName = (f"{outDir}/{resonance}_M-{mass}")
    # catname = cat.split("_")[0]
    spin = 0 if resonance == 'rad' else 2
    channelnames = {
        "eTau":"bbe$\\tau$",
        "muTau":"bb$\\mu\\tau$",
        "tauTau":"bb$\\tau\\tau$",
    }
    channelname = channelnames[channel]
    title = f"{channelname} {cat_name}"
    # print(cat,channelname,mass,spin,outFileName)
    return bins,labels,cat_name,channelname,title,spin,outFileName

def GetCorrectBinning():
    # Valori dei centri (già ordinati)
    centri = [250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000, 1050, 1100, 1150, 1200, 1250, 1300, 1350, 1400, 1450, 1500, 1550, 1600, 1650, 1700, 1750, 1800, 1850, 1900, 1950, 2000, 2050, 2100, 2150, 2200, 2250, 2300, 2350, 2400, 2450, 2500, 2550, 2600, 2650, 2700, 2750, 2800, 2850, 2900, 2950, 3000]

    # Definisci gli intervalli
    bins = [centri[i] for i in range(len(centri) - 1)] + [centri[-1] + 1]  # Aggiungi un valore massimo per il bin finale
    # Calcola i limiti dei bin
    limits = [centri[0] - (centri[1] - centri[0]) / 2]  # Limite inferiore del primo bin

    for i in range(len(centri) - 1):
        limits.append((centri[i] + centri[i + 1]) / 2)

    limits.append(centri[-1] + (centri[-1] - centri[-2]) / 2)  # Limite superiore dell'ultimo bin

    # Converte in un array NumPy
    #limits = np.array(limits)
    return limits



period_dict = {
    "Run2_2018" : "59.83",
    "Run2_2017" : "41.48",
    "Run2_2016" : "16.8",
    "Run2_2016_HIPM" : "19.5",
    "Run2_all": "138.8",

}
def plot_2D_histogram(histogram, title, xlabel, ylabel, bin_labels, filename, period, rectangle_coordinates):
    hep.style.use("CMS")
    plt.figure(figsize=(25, 15))
    plt.xlabel(xlabel, fontsize=40)
    plt.ylabel(ylabel, fontsize=40)
    hep.hist2dplot(histogram, cmap='Blues', cmax=histogram.GetMaximum())
    #hep.cms.text("work in progress")
    #hep.cms.text("")
    #hep.cms.label("Simulation Preliminary")
    # labels https://mplhep.readthedocs.io/en/latest/api.html#experiment-label-helpers
    hep.cms.label("Preliminary", lumi=period_dict[period], year=period.split('_')[1], fontsize=40)
    if bin_labels:
        textstr = '\n'.join([f"{i}. {label}" for i, label in enumerate(bin_labels)])
        plt.gcf().text(0.8, 0.5, textstr, fontsize=14)


    #plt.grid(True)
    # rectangle: https://matplotlib.org/stable/api/_as_gen/matplotlib.patches.Rectangle.html
    fig = plt.gcf()
    ax = fig.gca()
    #print(rectangle_coordinates)
    x1,y1,lenx,leny = rectangle_coordinates
    rect = patches.Rectangle((x1, y1), lenx, leny, linewidth=2, edgecolor='r', facecolor='none')
    ax.add_patch(rect)
    plt.gca().set_aspect('auto')
    plt.tight_layout(rect=[0, 0, 0.8, 1])
    plt.savefig(f"{filename}.pdf", format="pdf", bbox_inches="tight")
    plt.savefig(f"{filename}.png", bbox_inches="tight")
    plt.show()


def plot_1D_histogram(histograms, labels, bins,title, bin_labels, filename, period, mass, spin):
    fig, ax = plt.subplots(figsize=(16,10))
    #plt.xlabel(xlabel, fontsize=40)
    plt.ylabel('Events', fontsize=25)
    plt.xlabel('$m_{HH}$(GeV)', fontsize=25)
    alpha=0.8
    linewidth=2

    colors = ["salmon", "cornflowerblue", "limegreen",]
    colors_edges = ["orangered","blue","green"]
    for histogram,label,color,coloredge in zip(histograms,labels,colors,colors_edges):
        bins_x = []
        bins_y = []
        #print(histogram.GetTitle(), label)
        good_binnum = 0
        for binnum in range(1,histogram.GetNbinsX()+1):
            strint_val = str(int(histogram.GetXaxis().GetBinCenter(binnum)))
            if strint_val == mass :
                #print(f"good binnum is {binnum}")
                good_binnum = binnum
            #print(f"binCenter = {strint_val}, mass = {mass}")
            bins_x.append(histogram.GetXaxis().GetBinCenter(binnum))
            bins_y.append(histogram.GetBinContent(binnum))
        hep.histplot(
            np.array(bins_y),
            bins=np.array(bins),
            histtype="fill",
            color=color,
            alpha=alpha,
            edgecolor=coloredge,
            label=label,
            linewidth=linewidth,
            ax=ax,
        )
        alpha-=0.2
        linewidth+=1
        #print(good_binnum)
    #ax.axvline(x = mass, color = 'black', linestyle = '--', label=f'm(res) = {mass} GeV')
    year_name = year=period.split('_')[1]
    year_str = year_name if year_name != 'all' else ''
    ax.axvline(x=bins_x[good_binnum-1], color='black', linestyle='--',linewidth=2, label=f'$m_{{X}}$ = {mass} GeV\nspin={spin}')
    hep.cms.label("Preliminary", lumi=period_dict[period], year=year_str, fontsize=25)

    # plt.legend(title = "Legend Title")
    # plt.title("Line Graph - Geeksforgeeks")
    ax.legend(title= title,fontsize=25)
    plt.show()
    plt.gca().set_aspect('auto')
    plt.savefig(f"{filename}.pdf", format="pdf", bbox_inches="tight")
    # plt.savefig(f"{filename}.png", format="png", bbox_inches="tight")