import ROOT
import sys
import os
import math
import shutil
import json
import time
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

from Analysis.hh_bbtautau import *
import Common.Utilities
def AddMissingHistograms(fileInitial, all_histnames):
    missing_hists = {}
    inFile = ROOT.TFile.Open(fileInitial, "UPDATE")
    channels =[str(key.GetName()) for key in inFile.GetListOfKeys()]
    for channel in channels:
        missing_hists[channel] = {}
        dir_0 = inFile.Get(channel)
        # dir_1 = dir_0.Get("OS_Iso")
        keys_categories = [str(key.GetName()) for key in dir_0.GetListOfKeys()]
        for cat in keys_categories:
            missing_hists[channel][cat] = {}
            dir_2= dir_0.Get(cat)
            keys_process = [str(key.GetName()) for key in dir_2.GetListOfKeys()]
            for proc in all_histnames.keys():
                for histname in all_histnames[proc]:
                    if histname not in keys_process:
                        if proc not in missing_hists[channel][cat].keys():
                            missing_hists[channel][cat][proc] = []
                        missing_hists[channel][cat][proc].append(histname)
    # print(missing_hists)
    for channel in missing_hists.keys():
        dir_0 = inFile.Get(channel)
        for cat in missing_hists[channel].keys():
            dir_2= dir_0.Get(cat)
            for proc in missing_hists[channel][cat].keys():
                histToClone = dir_2.Get(proc)
                for histstoadd in missing_hists[channel][cat][proc]:
                    histCloned = histToClone.Clone(histstoadd)
                    key_dir = channel,cat
                    dir_name = '/'.join(key_dir)
                    dir_ptr = Utilities.mkdir(inFile,dir_name)
                    dir_ptr.WriteTObject(histCloned, histstoadd, "Overwrite")
            keys_process = [str(key.GetName()) for key in dir_2.GetListOfKeys()]
            # print(keys_process)




unc_names = [
    "CMS_btag_HF{scale}", "CMS_btag_LF{scale}", "CMS_btag_cferr1{scale}", "CMS_btag_cferr2{scale}", "CMS_eff_t_id_syst_alleras{scale}", "CMS_eff_t_id_syst_highpT_bin1{scale}", "CMS_eff_t_id_syst_highpT_bin2{scale}", "CMS_eff_t_id_syst_highpT_extrap{scale}", "CMS_scale_j_Abs{scale}", "CMS_scale_j_BBEC1{scale}", "CMS_scale_j_EC2{scale}", "CMS_scale_j_FlavQCD{scale}", "CMS_scale_j_HF{scale}", "CMS_scale_j_RelBal{scale}", "CMS_QCD_norm_{year}{scale}", "CMS_btag_hfstats1_{year}{scale}", "CMS_btag_hfstats2_{year}{scale}", "CMS_btag_lfstats1_{year}{scale}", "CMS_btag_lfstats2_{year}{scale}", "CMS_eff_j_PUJET_id_{year}{scale}", "CMS_eff_m_id_iso_{year}{scale}", "CMS_eff_m_id_{year}{scale}", "CMS_eff_e_{year}{scale}", "CMS_eff_t_id_stat1_DM0_{year}{scale}", "CMS_eff_t_id_stat1_DM10_{year}{scale}", "CMS_eff_t_id_stat1_DM11_{year}{scale}", "CMS_eff_t_id_stat1_DM1_{year}{scale}", "CMS_eff_t_id_stat2_DM0_{year}{scale}", "CMS_eff_t_id_stat2_DM10_{year}{scale}", "CMS_eff_t_id_stat2_DM11_{year}{scale}", "CMS_eff_t_id_stat2_DM1_{year}{scale}", "CMS_eff_t_id_syst_{year}{scale}", "CMS_eff_t_id_syst_{year}_DM0{scale}", "CMS_eff_t_id_syst_{year}_DM10{scale}", "CMS_eff_t_id_syst_{year}_DM11{scale}", "CMS_eff_t_id_syst_{year}_DM1{scale}", "CMS_eff_t_id_etauFR_barrel_{year}{scale}", "CMS_eff_t_id_etauFR_endcaps_{year}{scale}", "CMS_eff_t_id_mutauFR_eta0p4to0p8_{year}{scale}", "CMS_eff_t_id_mutauFR_eta0p8to1p2_{year}{scale}", "CMS_eff_t_id_mutauFR_eta1p2to1p7_{year}{scale}", "CMS_eff_t_id_mutauFR_etaGt1p7_{year}{scale}", "CMS_eff_t_id_mutauFR_etaLt0p4_{year}{scale}", "CMS_eff_t_id_stat_highpT_bin1_{year}{scale}", "CMS_eff_t_id_stat_highpT_bin2_{year}{scale}", "CMS_eff_m_id_reco_{year}{scale}", "CMS_eff_m_id_highpt_{year}{scale}", "CMS_eff_m_id_reco_highpt_{year}{scale}", "CMS_l1_prefiring_{year}{scale}", "CMS_pu_lumi_MC_{year}{scale}", "CMS_scale_t_DM0_{year}{scale}", "CMS_scale_t_DM1_{year}{scale}", "CMS_scale_t_3prong_{year}{scale}", "CMS_scale_t_eFake_DM0_{year}{scale}", "CMS_scale_t_eFake_DM1_{year}{scale}", "CMS_scale_t_muFake_{year}{scale}", "CMS_res_j_{year}{scale}", "CMS_scale_j_Abs_{year}{scale}", "CMS_scale_j_BBEC1_{year}{scale}", "CMS_scale_j_EC2_{year}{scale}", "CMS_scale_j_HF_{year}{scale}", "CMS_scale_j_RelSample_{year}{scale}", "CMS_norm_qcd_{year}{scale}","CMS_scale_qcd_{year}{scale}", "CMS_pnet_{year}{scale}", "CMS_bbtt_trig_MET_{year}{scale}", "CMS_bbtt_trig_singleTau_{year}{scale}", "CMS_bbtt_trig_ele_{year}{scale}", "CMS_bbtt_trig_SL_ele_{year}{scale}", "CMS_bbtt_trig_cross_ele_{year}{scale}", "CMS_bbtt_trig_mu_{year}{scale}", "CMS_bbtt_trig_SL_mu_{year}{scale}", "CMS_bbtt_trig_cross_mu_{year}{scale}", "CMS_bbtt_trig_tau_{year}{scale}"]



histograms_initials = {
    "2016":"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/histograms/New_massCut/SC/SR/Run2_2016/merged/kinFit_m/kinFit_m.root",
    "2016_HIPM":"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/histograms/New_massCut/SC/SR/Run2_2016_HIPM/merged/kinFit_m/kinFit_m.root",
    "2017":"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/histograms/New_massCut/SC/SR/Run2_2017/merged/kinFit_m/kinFit_m.root",
    "2018":"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/histograms/New_massCut/SC/SR/Run2_2018/merged/kinFit_m/kinFit_m.root"
}
histograms_initials_boosted = {
    "2016": "/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/histograms/New_massCut/SC/SR/Run2_2016/merged/bbtautau_mass_met_boosted/bbtautau_mass_met_boosted.root",
    "2016_HIPM": "/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/histograms/New_massCut/SC/SR/Run2_2016_HIPM/merged/bbtautau_mass_met_boosted/bbtautau_mass_met_boosted.root",
    "2017":"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/histograms/New_massCut/SC/SR/Run2_2017/merged/bbtautau_mass_met_boosted/bbtautau_mass_met_boosted.root",
    "2018":"/eos/user/v/vdamante/HH_bbtautau_resonant_Run2/histograms/New_massCut/SC/SR/Run2_2018/merged/bbtautau_mass_met_boosted/bbtautau_mass_met_boosted.root"
}



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    # parser.add_argument('--year', required=False, type=str, default='2016_HIPM')
    parser.add_argument('--outFile', required=True, type=str, default='')
    args = parser.parse_args()
    years = ["2016", "2016_HIPM", "2017","2018"]
    all_histnames = {}
    for proc in processes:
        all_histnames[proc] = [proc]
        for uncname in unc_names:
            for scale in ['Up','Down']:
                for year in years:
                    all_histnames[proc].append(proc+"_"+uncname.format(scale=scale,year=year))

    hadd_str = f'hadd -f209 -n 0 {args.outFile} '

    all_files = []
    for year in years:
        AddMissingHistograms(histograms_initials[year], all_histnames)
        all_files.append(histograms_initials[year])
        AddMissingHistograms(histograms_initials_boosted[year], all_histnames)
        all_files.append(histograms_initials_boosted[year])


    hadd_str += ' '.join(f for f in all_files)
    if len(all_files) > 1:
        ps_call([hadd_str], True)
    else:
        shutil.copy(all_files[0],args.outFile)
    # if os.path.exists(args.outFile) and args.remove_files:
    #     for histFile in all_files:
    #         if histFile == args.outFile: continue
    #         os.remove(histFile)