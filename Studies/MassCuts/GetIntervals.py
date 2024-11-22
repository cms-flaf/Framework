import numpy as np
import math
def GetMassCut(dfWrapped,global_cfg_dict,channels=['tauTau'], categories=['boosted','inclusive', 'res2b', 'res1b'],quantile_max=0.99, wantSequential=False):
    for cat in categories:
        for channel in channels:
            df_cat = dfWrapped.df.Filter(f"OS_Iso && {cat} && {channel}")
            if cat == 'boosted':
                df_cat = df_cat.Define("FatJet_atLeast1BHadron",
                "SelectedFatJet_nBHadrons>0").Filter("SelectedFatJet_p4[FatJet_atLeast1BHadron].size()>0")
            else:
                df_cat = df_cat.Filter("b1_hadronFlavour==5 && b2_hadronFlavour==5 ")
            np_dict_cat = df_cat.AsNumpy(["bb_m_vis",
            "tautau_m_vis"])

            np_array_mass_bb_cat = np_dict_cat['bb_m_vis']
            np_array_mass_tt_cat = np_dict_cat['tautau_m_vis']

            max_bb_mass =np.quantile(np_array_mass_bb_cat, quantile_max)
            min_bb_mass =np.quantile(np_array_mass_bb_cat, 1-quantile_max)

            max_bb_mass_int = math.ceil(max_bb_mass / 10) * 10
            min_bb_mass_int = math.floor(min_bb_mass / 10) * 10
            df_cat_bb = df_cat#.Filter(f"bb_m_vis > {min_bb_mass_int} && bb_m_vis < {max_bb_mass_int}")
            if wantSequential:
                df_cat_bb = df_cat.Filter(f"bb_m_vis > {min_bb_mass_int} && bb_m_vis < {max_bb_mass_int}")

            np_dict_cat_bb = df_cat_bb.AsNumpy(["tautau_m_vis"])
            np_array_mass_tt_cat_bb = np_dict_cat_bb["tautau_m_vis"]
            max_tt_mass =np.quantile(np_array_mass_tt_cat_bb, quantile_max)
            min_tt_mass =np.quantile(np_array_mass_tt_cat_bb, 1-quantile_max)

            max_tt_mass_int = math.ceil(max_tt_mass / 10) * 10
            min_tt_mass_int = math.floor(min_tt_mass / 10) * 10

            print(f"quantile max {round(quantile_max,3)} for bb mass in category {cat} and channel {channel} =  {max_bb_mass}, {max_bb_mass_int}")
            print(f"quantile min {round(1-quantile_max,3)} for bb mass  in category {cat} and channel {channel} =  {min_bb_mass}, {min_bb_mass_int}")
            print(f"quantile max {round(quantile_max,3)} for tt mass  in category {cat} and channel {channel} =  {max_tt_mass}, {max_tt_mass_int}")
            print(f"quantile min {round(1-quantile_max,3)} for tt mass  in category {cat} and channel {channel} =  {min_tt_mass}, {min_tt_mass_int}")


