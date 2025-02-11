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

def plot_2D_histogram(histogram, title, xlabel, ylabel, bin_labels, filename, year, ellipse_parameters=None,rectangle_coordinates=None):
    hep.style.use("CMS")
    plt.figure(figsize=(25, 15))
    plt.title(title, fontsize=40)
    plt.xlabel(xlabel, fontsize=40)
    plt.ylabel(ylabel, fontsize=40)
    hep.hist2dplot(histogram, cmap='Blues', cmax=histogram.GetMaximum())
    period = f"Run2_{year}"
    hep.cms.label("Preliminary", lumi=period_dict[period], year=year, fontsize=40)

    # Eventuali bin labels
    if bin_labels:
        textstr = '\n'.join([f"{i}. {label}" for i, label in enumerate(bin_labels)])
        plt.gcf().text(0.8, 0.5, textstr, fontsize=14)

    # Aggiunta dell'ellisse se specificata
    fig = plt.gcf()
    ax = fig.gca()
    if ellipse_parameters:
        A, B, C, D = ellipse_parameters  # Centro (A, C) e semiassi (B, D)
        ellipse = Ellipse(
            (C, A),  # Centro in coordinate (mbb, mtt)
            2 * D,   # Larghezza = 2 * semiasse lungo mbb
            2 * B,   # Altezza = 2 * semiasse lungo mtt
            edgecolor='r',  # Colore del bordo
            facecolor='none',  # Nessun riempimento
            linewidth=2
        )
        ax.add_patch(ellipse)
    if rectangle_coordinates:
        m1_max, m1_min, m2_max, m2_min = rectangle_coordinates
        #print(rectangle_coordinates)
        # Aggiungi il quadrato
        width = m1_max - m1_min  # Larghezza del quadrato
        height = m2_max - m2_min  # Altezza del quadrato
        square = patches.Rectangle(
            (m1_min, m2_min),  # Coordinate in basso a sinistra
            width,             # Larghezza
            height,            # Altezza
            linewidth=3, edgecolor='g', facecolor='none', label="Quadrato"
        )
        plt.gca().add_patch(square)
        plt.gca().set_aspect('auto')
        plt.tight_layout(rect=[0, 0, 0.8, 1])

    plt.gca().set_aspect('auto')
    plt.tight_layout(rect=[0, 0, 0.8, 1])
    plt.savefig(f"{filename}.pdf", format="pdf", bbox_inches="tight")
    plt.savefig(f"{filename}.png", bbox_inches="tight")
    plt.show()
