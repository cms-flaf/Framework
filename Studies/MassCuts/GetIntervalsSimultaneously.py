import numpy as np
import math
def GetMassCut(dfWrapped,global_cfg_dict,channels=[], categories=[],quantile_max=0.99):

    for cat in categories:

        df_cat = dfWrapped.df.Filter(f"OS_Iso")
        if cat != 'boosted':
            #df_cat = dfWrapped.df.Filter(f"OS_Iso && {cat}")
            df_cat = df_cat.Filter("b1_hadronFlavour==5 && b2_hadronFlavour==5 ")
        else:
            df_cat = df_cat.Define("FatJet_atLeast2BHadron",
            "SelectedFatJet_nBHadrons>=2").Filter("SelectedFatJet_p4[FatJet_atLeast2BHadron].size()>0")
        for channel in channels:
            lower_cut_tt = 15
            lower_cut_bb = 40
            lower_cut_bb_boosted = 30
            lower_cut_tt_boosted = 15
            upper_cut_tt_boosted = 130
            df_ch = df_cat.Filter(channel)
            gen_kind = 5
            if channel=='eTau': gen_kind = 3
            if channel=='muTau': gen_kind = 4
            df_ch = df_ch.Filter(f"tau1_gen_kind == {gen_kind} && tau2_gen_kind==5")

            if cat=='boosted':
                ### boosted_cat_3 redefinition: to apply also mass cuts ####
                df_ch = df_ch.Define("boosted_cat_3","boosted && !(res2b_cat3 && SVfit_m > 15 && SVfit_m < 130 && bb_m_vis < 270 && bb_m_vis>20)").Filter("boosted_cat_3")

                masses_boosted = ['bb_m_vis_boosted_fjmass', 'bb_m_vis_softdrop', 'bb_m_vis_pnet']
                for mass_boosted in masses_boosted:
                    print(f"using {mass_boosted}")
                    np_dict_ch = df_ch.AsNumpy(["SVfit_m",mass_boosted])
                    np_array_mass_tt_ch = np_dict_ch["SVfit_m"]
                    np_array_mass_bb_ch = np_dict_ch[mass_boosted]
                    ### denumerator definition ####
                    len_tt_init = len(np_array_mass_tt_ch)
                    len_bb_init = len(np_array_mass_bb_ch)

                    for bb_mass_step in [400, 410, 420, 430, 440, 450, 460, 470, 480, 490, 500, 600, 700, 800]: #[200, 205, 210, 215, 220, 225, 230, 235, 240, 245, 250, 255, 260, 265, 270, 275, 280, 285, 290, 295,300, 310, 320, 330, 340, 350, 360, 380, 390, 400, 410, 420, 430, 440, 450, 460, 470, 480, 490, 500, 600, 700, 800]:
                        mask_tautau_up = np_dict_ch['SVfit_m'] < upper_cut_tt_boosted
                        mask_tautau_low = np_dict_ch['SVfit_m'] > lower_cut_tt_boosted

                        mask_bb_up = np_dict_ch[mass_boosted] < bb_mass_step
                        mask_bb_low = np_dict_ch[mass_boosted] > lower_cut_bb_boosted

                        combined_mask = mask_tautau_up & mask_tautau_low & mask_bb_up & mask_tautau_low
                        combined_mask = mask_tautau_up & mask_tautau_low & mask_bb_up & mask_bb_low
                        np_dict_ch_uppercut = {
                            'SVfit_m': np_dict_ch['SVfit_m'][combined_mask],
                            mass_boosted: np_dict_ch[mass_boosted][combined_mask]
                        }

                        np_array_mass_tt_ch_uppercut = np_dict_ch_uppercut["SVfit_m"]
                        np_array_mass_bb_ch_uppercut = np_dict_ch_uppercut[mass_boosted]
                        len_tt_upper = len(np_array_mass_tt_ch_uppercut)

                        len_bb_upper = len(np_array_mass_bb_ch_uppercut)
                        percentage = len_tt_upper/len_tt_init
                        percentage = len_bb_upper/len_bb_init
                        if percentage > quantile_max-0.1 and percentage < quantile_max+0.1:
                            print(f"percentage = {percentage}")
                            print(f"for category {cat} and channel {channel}")
                            print(f"condition: {mass_boosted} < {bb_mass_step}")
                            print(f"bb_mass_step = {bb_mass_step}")
                            print(len_bb_upper)
            else:
                # df_ch = df_ch.Define("boosted_cat3_SR","boosted && !(res2b_cat3 && SVfit_m > 15 && SVfit_m < 130 && bb_m_vis < 270 && bb_m_vis>20) && (res2b_cat3 && SVfit_m > 15 && SVfit_m < 130 && bb_m_vis_softdrop < 450 && bb_m_vis_softdrop>30)").Define("res1b_cat3_SR", "!(boosted_cat3_SR)").Filter("res1b_cat3_SR")

                np_dict_ch = df_ch.AsNumpy(["SVfit_m","bb_m_vis"])
                np_array_mass_tt_ch = np_dict_ch["SVfit_m"]
                np_array_mass_bb_ch = np_dict_ch["bb_m_vis"]
                ### denumerator definition ####
                len_tt_init = len(np_array_mass_tt_ch)
                len_bb_init = len(np_array_mass_bb_ch)
                percentage = 0.
                for tt_mass_step in [30, 40, 50, 60, 70, 80, 90, 100, 110, 115, 120, 125, 130, 135, 140, 145, 150, 155, 160, 165, 170, 175, 180, 185, 190, 195, 200, 205, 210, 215,]:
                    for bb_mass_step in [160, 165, 170, 175, 180, 185, 190, 195, 200, 205, 210, 215, 220, 225, 230, 235, 240, 245, 250, 255, 260, 265, 270, 275, 280, 285, 290, 295, 300, 305, 310, 315, 320, 325, 330, 335]:
                        mask_tautau_up = np_dict_ch['SVfit_m'] < tt_mass_step
                        mask_tautau_low =  np_dict_ch['SVfit_m'] > lower_cut_tt

                        mask_bb_up = np_dict_ch['bb_m_vis'] < bb_mass_step
                        mask_bb_low =  np_dict_ch['bb_m_vis'] > lower_cut_bb
                        combined_mask = mask_tautau_up & mask_tautau_low & mask_bb_up & mask_bb_low
                        np_dict_ch_uppercut = {
                            'SVfit_m': np_dict_ch['SVfit_m'][combined_mask],
                            'bb_m_vis': np_dict_ch['bb_m_vis'][combined_mask]
                        }
                        np_array_mass_tt_ch_uppercut = np_dict_ch_uppercut["SVfit_m"]
                        np_array_mass_bb_ch_uppercut = np_dict_ch_uppercut["bb_m_vis"]
                        len_tt_upper = len(np_array_mass_tt_ch_uppercut)
                        len_bb_upper = len(np_array_mass_bb_ch_uppercut)
                        percentage = len_tt_upper/len_tt_init
                        if (percentage > quantile_max-0.04 and percentage < quantile_max+0.05): # or (bb_mass_step ==280 or bb_mass_step==290 or bb_mass_step==300 ):
                            print()
                            print(f"quantile_max-0.04 = {quantile_max-0.04}")
                            print(f"quantile_max+0.05 = {quantile_max+0.05}")
                            print(f"quantile_max = {quantile_max} percentage = {percentage}")
                            print(f"for category {cat} and channel {channel}")
                            print(f"condition: SVfit_m < {tt_mass_step} && bb_m_vis < {bb_mass_step}")
                            print(f"bb_mass_step = {bb_mass_step}, tt_mass_step= {tt_mass_step}")
                            print(f"len_tt_upper = {len_tt_upper}, len_bb_upper= {len_bb_upper}")
                print()
        print()
    print()

