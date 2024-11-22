import os


indir = "/eos/user/d/daebi/ANA_FOLDER_DEV/histograms/Run32022_13Nov24_NoMuonSF/Run3_2022/merged/"

varnames = ["lep1_pt", "lep1_eta", "lep2_pt", "lep2_eta", "bjet1_btagPNetB", "bjet2_btagPNetB"]
varnames = ["lep1_pt", "lep1_eta", "lep2_pt", "lep2_eta"]
channellist = ["e", "eE", "eMu", "mu", "muMu"]
channellist = ["mu", "muMu"]

plotdir = "plots_19Nov_NoMuonSF/"

for var in varnames:
    for channel in channellist:
        filename = os.path.join(indir, var, f"tmp/all_histograms_{var}_Central.root")
        print("Loading fname ", filename)
        os.makedirs(plotdir, exist_ok=True)
        outname = os.path.join(plotdir, f"HHbbWW_{channel}_{var}_StackPlot.pdf")
        os.system(f"python3 HistPlotter.py --inFile {filename} --bckgConfig ../config/HH_bbWW/background_samples.yaml --globalConfig ../config/HH_bbWW/global.yaml --sampleConfig ../config/Run3_2022/samples.yaml --outFile {outname} --var {var} --category inclusive --channel {channel} --uncSource Central --wantData True --year Run3_2022 --wantQCD False --rebin False --analysis HH_bbWW")
