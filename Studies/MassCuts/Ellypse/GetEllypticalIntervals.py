import numpy as np
import math

from scipy.optimize import minimize

# Funzione per calcolare la frazione di punti dentro l'ellisse
def fraction_in_ellipse(data, M_c1, M_c2, a, b):
    x, y = data[:, 0], data[:, 1]
    ellipse = ((x - M_c1)**2 / a**2) + ((y - M_c2)**2 / b**2)
    return np.mean(ellipse <= 1)

# Funzione obiettivo: minimizza la deviazione dai target
def objective(params, dataset_tt, dataset_sig, fraction_b, fraction_sig):
    M_c1, M_c2, a, b = params
    frac_tt = fraction_in_ellipse(dataset_tt, M_c1, M_c2, a, b)  # Dataset A: frazione inclusa
    frac_sig = fraction_in_ellipse(dataset_sig, M_c1, M_c2, a, b)  # Dataset B: frazione inclusa
    fraction_bckg = 1-fraction_b
    # Penalizzazioni: Dataset A deve essere incluso al 10%, Dataset B al 68%
    penalty_tt = abs(frac_tt - 0.01)  # Solo 10% di Dataset A deve rientrare
    penalty_sig = abs(frac_sig - 0.68)  # 68% di Dataset B deve rientrare
    total_penalty = penalty_tt + penalty_sig
    print(f"Params: M_c1={M_c1:.2f}, M_c2={M_c2:.2f}, a={a:.2f}, b={b:.2f} | Penalty: {total_penalty:.4f}")
    print(f"   Frazione ttbar: {frac_tt:.4f} (Target: {fraction_b})")
    print(f"   Frazione segnale: {frac_sig:.4f} (Target: {fraction_sig})")
    return total_penalty

def optimize_ellypse(dataset_tt,dataset_sig, fraction_b, fraction_sig):
    # Valori iniziali per [M_c1, M_c2, a, b]
    initial_guess = [116, 111, 35, 45]

    # Ottimizzazione
    # result = minimize(objective, initial_guess, bounds=[(None, None), (None, None), (0.1, None), (0.1, None)])
    result = minimize(
        objective,
        initial_guess,
        args=(dataset_tt, dataset_sig, fraction_b, fraction_sig),
        bounds=[(None, None), (None, None), (0.1, None), (0.1, None)],
         options={'disp': True}
    )
    # Risultati finali
    M_c1_opt, M_c2_opt, a_opt, b_opt = result.x
    print(f"Centri ottimali: M_c1 = {M_c1_opt}, M_c2 = {M_c2_opt}")
    print(f"Semiassi ottimali: a = {a_opt}, b = {b_opt}")

    # Frazione finale nei dataset
    frac_A_final = fraction_in_ellipse(dataset_tt, M_c1_opt, M_c2_opt, a_opt, b_opt)
    frac_B_final = fraction_in_ellipse(dataset_sig, M_c1_opt, M_c2_opt, a_opt, b_opt)
    print(f"Frazione in Dataset A (deve essere ~10%): {frac_A_final:.3f}")
    print(f"Frazione in Dataset B (deve essere ~68%): {frac_B_final:.3f}")
    return M_c1_opt, M_c2_opt, a_opt, b_opt




def GetEllypticalMassCut(dfWrapped_sig,dfWrapped_tt,global_cfg_dict,channel, cat,quantile_sig=0.68,quantile_ttbar=0.99):
    df_cat_sig = dfWrapped_sig.df.Filter(f"OS_Iso && {cat} && {channel}")
    df_cat_tt = dfWrapped_tt.df.Filter(f"OS_Iso && {cat} && {channel}")
    if cat == 'boosted':
        df_cat_sig = df_cat_sig.Define("FatJet_atLeast1BHadron",
        "SelectedFatJet_nBHadrons>0").Filter("SelectedFatJet_p4[FatJet_atLeast1BHadron].size()>0")
        df_cat_tt = df_cat_tt.Define("FatJet_atLeast1BHadron",
        "SelectedFatJet_nBHadrons>0").Filter("SelectedFatJet_p4[FatJet_atLeast1BHadron].size()>0")
    else:
        df_cat_sig = df_cat_sig.Filter("b1_hadronFlavour==5 && b2_hadronFlavour==5 ")
        df_cat_tt = df_cat_tt.Filter("b1_hadronFlavour==5 && b2_hadronFlavour==5 ")
    np_dict_cat_sig = df_cat_sig.AsNumpy(["tautau_m_vis","bb_m_vis"])
    mbb_list_sig = np_dict_cat_sig["bb_m_vis"]
    mtt_list_sig = np_dict_cat_sig["tautau_m_vis"]
    dataset_sig = np.column_stack((mtt_list_sig, mbb_list_sig))  # Signal dataset

    np_dict_cat_tt = df_cat_tt.AsNumpy(["tautau_m_vis","bb_m_vis"])
    mbb_list_tt = np_dict_cat_tt["bb_m_vis"]
    mtt_list_tt = np_dict_cat_tt["tautau_m_vis"]
    dataset_tt = np.column_stack((mtt_list_tt, mbb_list_tt))  # Background dataset


    print(cat, channel)

    print(quantile_sig)
    print(quantile_ttbar)
    # Per il dataset A

    M_c1, M_c2, a, b = optimize_ellypse(dataset_tt,dataset_sig, quantile_ttbar, quantile_sig)

    print(f"Centro asse m_tt = {M_c1}, Centro asse m_bb = {M_c2}, Semiasse maggiore (m_tt) = {a}, Semiasse minore (m_bb) = {b}")
    return M_c1, M_c2, a, b


