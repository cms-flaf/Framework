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

            df_cat_low = df_cat.Filter(f"tautau_m_vis > 20 && bb_m_vis > 50")

            np_dict_cat_up = df_cat_low.AsNumpy(["tautau_m_vis","bb_m_vis"])
            np_array_mass_tt_cat_up = np_dict_cat_up["tautau_m_vis"]
            np_array_mass_bb_cat_up = np_dict_cat_up["bb_m_vis"]

            len_tt_init = len(np_array_mass_tt_cat_up)
            len_bb_init = len(np_array_mass_bb_cat_up)
            #print(f"len initial {len_tt_init}")
            #print(f"{cat}, {channel}")
            percentage = 0.
            # [50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100, 105, 110, 15, 120, 125, 130, 135, 140, 145, 150, 155, 160, 165, 170, 175, 180, 185, 190, 195, 200, 205, 215, 220, 230, 240, 250]
            for tt_mass_step in [110, 115, 120, 125, 130, 135, 140, 150]:
                for bb_mass_step in [180, 185, 190, 200]: #205, 210,215, 220, 225, 230, 250, 270, 290, 310]:
                    df_cat_up= df_cat_low.Filter(f"tautau_m_vis < {tt_mass_step} && bb_m_vis < {bb_mass_step}")
                    np_dict_cat_low = df_cat_up.AsNumpy(["tautau_m_vis","bb_m_vis"])
                    np_array_mass_tt_cat_low = np_dict_cat_low["tautau_m_vis"]
                    np_array_mass_bb_cat_low = np_dict_cat_low["bb_m_vis"]
                    len_tt_upper = len(np_array_mass_tt_cat_low)
                    len_bb_upper = len(np_array_mass_bb_cat_low)
                    percentage = len_tt_upper/len_tt_init
                    #if percentage >0.98:
                    print(f"percentage = {percentage}")
                    print(f"bb_mass_step = {bb_mass_step}, tt_mass_step= {tt_mass_step}")
                    print(f"len_tt_upper = {len_tt_upper}, len_bb_upper= {len_bb_upper}")
