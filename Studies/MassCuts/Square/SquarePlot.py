import mplhep as hep
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import mplhep as hep
import matplotlib.pyplot as plt
import matplotlib.patches as patches
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
    m1_max, m1_min, m2_max, m2_min = rectangle_coordinates
    #print(rectangle_coordinates)
    # Aggiungi il quadrato
    width = m1_max - m1_min  # Larghezza del quadrato
    height = m2_max - m2_min  # Altezza del quadrato
    square = patches.Rectangle(
        (m1_min, m2_min),  # Coordinate in basso a sinistra
        width,             # Larghezza
        height,            # Altezza
        linewidth=2, edgecolor='r', facecolor='none', label="Quadrato"
    )
    plt.gca().add_patch(square)
    plt.gca().set_aspect('auto')
    plt.tight_layout(rect=[0, 0, 0.8, 1])
    plt.savefig(f"{filename}.pdf", format="pdf", bbox_inches="tight")
    plt.savefig(f"{filename}.png", bbox_inches="tight")
    plt.show()
