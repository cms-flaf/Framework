import os


indir = "/eos/user/d/daebi/ANA_FOLDER_DEV/histograms/Run32022_13Nov24/Run3_2022/merged/"

varnames = ["lep1_pt", "lep1_eta", "lep2_pt", "lep2_eta", "bjet1_btagPNetB", "bjet2_btagPNetB"]
varnames = ["lep1_pt"]
channellist = ["e", "eE", "eMu", "mu", "muMu"]
channellist = ["mu", "muMu"]

era = "Run3_2022"
plotdir = "plots_19Nov_NoMuonSF/"

for var in varnames:
    for channel in channellist:
        filename = os.path.join(indir, var, f"{var}.root")
        print("Loading fname ", filename)
        os.makedirs(plotdir, exist_ok=True)
        outname = os.path.join(plotdir, f"HHbbWW_{channel}_{var}_StackPlot.pdf")
        os.system(f"python3 ../HistPlotter.py --inFile {filename} --bckgConfig ../../config/HH_bbWW/background_samples.yaml --globalConfig ../../config/HH_bbWW/global.yaml --outFile {outname} --var {var} --category inclusive --channel {channel} --uncSource Central --wantData True --year Run3_2022 --wantQCD False --rebin False --analysis HH_bbWW --qcdregion OS_Iso --sigConfig ../../config/HH_bbWW/{era}/samples.yaml")
