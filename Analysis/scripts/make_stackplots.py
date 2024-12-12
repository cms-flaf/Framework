import os


#indir = "/eos/user/d/daebi/ANA_FOLDER_DEV/histograms/Run32022_13Nov24_MediumMuonSF/Run3_2022/merged/"
#indir = "/eos/user/d/daebi/ANA_FOLDER_DEV/histograms/Run32022EE_27Nov24_MuonSF_PU/Run3_2022EE/merged/"
indir = "/eos/user/d/daebi/ANA_FOLDER_DEV/histograms/Run32022_27Nov24_MuonSF_PU/Run3_2022/merged/"

varnames = ["lep1_pt", "lep1_eta", "lep2_pt", "lep2_eta", "bjet1_btagPNetB", "bjet2_btagPNetB"]
varnames = ["lep1_pt", "diLep_mass", "MT_lep1", "MT_lep2", "MT_tot"]
channellist = ["e", "eE", "eMu", "mu", "muMu"]
channellist = ["mu", "muMu"]

#era = "Run3_2022EE"
era = "Run3_2022"
#plotdir = "plots_2Dec_MedMuonSF/"
plotdir = "plots_27Nov_MuonSF_PU_2022/"
cat = "inclusive"

for var in varnames:
    for channel in channellist:
        filename = os.path.join(indir, var, f"{var}.root")
        print("Loading fname ", filename)
        os.makedirs(plotdir, exist_ok=True)
        outname = os.path.join(plotdir, f"HHbbWW_{channel}_{var}_StackPlot.pdf")
        os.system(f"python3 ../HistPlotter.py --inFile {filename} --bckgConfig ../../config/HH_bbWW/background_samples.yaml --globalConfig ../../config/HH_bbWW/global.yaml --outFile {outname} --var {var} --category {cat} --channel {channel} --uncSource Central --wantData True --year {era} --wantQCD False --rebin False --analysis HH_bbWW --qcdregion OS_Iso --sigConfig ../../config/HH_bbWW/{era}/samples.yaml")
