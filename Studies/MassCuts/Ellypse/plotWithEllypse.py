import matplotlib.patches as patches
from matplotlib.patches import Ellipse
import matplotlib.pyplot as plt
import mplhep as hep

period_dict = {
    "Run2_2018" : "59.83",
    "Run2_2017" : "41.48",
    "Run2_2016" : "16.8",
    "Run2_2016_HIPM" : "19.5",
    "Run2_all": "138.8",
}
def plot_2D_histogram(histogram, title, xlabel, ylabel, bin_labels, filename, year, ellipse_parameters=None):
    hep.style.use("CMS")
    plt.figure(figsize=(25, 15))
    plt.title(title, fontsize=40)
    plt.xlabel(xlabel, fontsize=40)
    plt.ylabel(ylabel, fontsize=40)
    hep.hist2dplot(histogram, cmap='Blues', cmax=histogram.GetMaximum())
    period = f"Run2_{year}"
    # Etichette e stile CMS
    hep.cms.label("Preliminary", lumi=period_dict[period], year=year, fontsize=40)

    # Eventuali bin labels
    if bin_labels:
        textstr = '\n'.join([f"{i}. {label}" for i, label in enumerate(bin_labels)])
        plt.gcf().text(0.8, 0.5, textstr, fontsize=14)

    # Aggiunta dell'ellisse se specificata
    fig = plt.gcf()
    ax = fig.gca()
    if ellipse_parameters:
        Mc_bb, Mc_tt, a_bb, a_tt = ellipse_parameters  # Centro e semiassi
        ellipse = Ellipse(
            (Mc_bb, Mc_tt),  # Centro
            2 * a_bb,  # Larghezza (semiasse maggiore * 2)
            2 * a_tt,  # Altezza (semiasse minore * 2)
            edgecolor='r',  # Colore del bordo
            facecolor='none',  # Nessun riempimento
            linewidth=2
        )
        ax.add_patch(ellipse)

    plt.gca().set_aspect('auto')
    plt.tight_layout(rect=[0, 0, 0.8, 1])
    plt.savefig(f"{filename}.pdf", format="pdf", bbox_inches="tight")
    plt.savefig(f"{filename}.png", bbox_inches="tight")
    plt.show()
