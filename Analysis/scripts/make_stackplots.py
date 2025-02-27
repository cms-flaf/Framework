import os

era = "Run3_2022"
ver = "SF"
indir = f"/eos/user/t/toakhter/HH_bbtautau_Run3/histograms/{ver}/{era}/merged/"
plotdir = f"/eos/user/t/toakhter/HH_bbtautau_Run3/histograms/{ver}/{era}/plots/"

varnames = ["tau1_pt", "tau2_pt", "tautau_m_vis", "bb_m_vis", "bbtautau_mass"] 

channellist = ["eE", "eMu", "muMu", "eTau", "muTau", "tauTau"]

cat = "inclusive"

using_uncertainties = False #When we turn on Up/Down, the file storage changes due to renameHists.py

for var in varnames:
    for channel in channellist:
        filename = os.path.join(indir, var, f"{var}.root")
        print("Loading fname ", filename)
        os.makedirs(plotdir, exist_ok=True)
        outname = os.path.join(plotdir, f"HHbbtautau_{channel}_{var}_StackPlot.pdf")

        if not using_uncertainties:
            os.system(f"python3 ../HistPlotter.py --inFile {filename} --bckgConfig ../../config/HH_bbtautau/{era}/background_samples.yaml --globalConfig ../../config/HH_bbtautau/global.yaml --outFile {outname} --var {var} --category {cat} --channel {channel} --uncSource Central --wantData --year {era} --wantQCD False --rebin False --analysis HH_bbtautau --qcdregion OS_Iso --sigConfig ../../config/HH_bbtautau/{era}/samples.yaml")

        else:
            filename = os.path.join(indir, var, 'tmp', f"all_histograms_{var}_hadded.root")
            os.system(f"python3 ../HistPlotter.py --inFile {filename} --bckgConfig ../../config/HH_bbtautau/{era}/background_samples.yaml --globalConfig ../../config/HH_bbtautau/global.yaml --outFile {outname} --var {var} --category {cat} --channel {channel} --uncSource Central --wantData --year {era} --wantQCD False --rebin False --analysis HH_bbtautau --qcdregion OS_Iso --sigConfig ../../config/HH_bbtautau/{era}/samples.yaml")
