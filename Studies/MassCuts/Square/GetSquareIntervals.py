import numpy as np
import math
import numpy as np
import math

def GetMassesQuantilesJoint(df_cat, tt_mass, bb_mass, quantile_max):
    # Estrai i dati come array numpy
    np_dict_cat = df_cat.AsNumpy([tt_mass, bb_mass])
    mbb_list = np_dict_cat[bb_mass]
    mtt_list = np_dict_cat[tt_mass]

    # Combina m_bb e m_tt in un array bidimensionale
    data = np.column_stack((mtt_list, mbb_list))

    # Calcola il punto mediano (o centrale) nello spazio bidimensionale
    median_point = np.median(data, axis=0)

    # Calcola le distanze euclidee di ogni punto dal mediano
    distances = np.linalg.norm(data - median_point, axis=1)

    # Ordina i dati per distanza crescente
    sorted_indices = np.argsort(distances)
    sorted_data = data[sorted_indices]

    # Seleziona la frazione di punti corrispondente al quantile massimo
    n_points = int(len(data) * quantile_max)
    selected_data = sorted_data[:n_points]

    print(len(selected_data)/len(data))
    # Trova i limiti massimi e minimi del sottoinsieme selezionato
    min_tt, max_tt = np.min(selected_data[:, 0]), np.max(selected_data[:, 0])
    min_bb, max_bb = np.min(selected_data[:, 1]), np.max(selected_data[:, 1])

    # Arrotonda i limiti ai 10 più vicini (se necessario)
    min_tt_int = math.floor(min_tt / 10) * 10
    max_tt_int = math.ceil(max_tt / 10) * 10
    min_bb_int = math.floor(min_bb / 10) * 10
    max_bb_int = math.ceil(max_bb / 10) * 10

    # print(f"Quantile target: {quantile_max}")
    # print(f"bb masses: min = {min_bb_int}, max = {max_bb_int}")
    # print(f"tt masses: min = {min_tt_int}, max = {max_tt_int}")

    return min_bb_int, max_bb_int, min_tt_int, max_tt_int


def GetMassesQuantiles(df_cat,tt_mass,bb_mass,quantile_max,wantSequential):
    np_dict_cat = df_cat.AsNumpy([tt_mass,bb_mass])
    mbb_list = np_dict_cat[bb_mass]
    mtt_list = np_dict_cat[tt_mass]
    max_bb_mass =np.quantile(mbb_list, quantile_max)
    min_bb_mass =np.quantile(mbb_list, 1-quantile_max)
    max_bb_mass_int = math.ceil(max_bb_mass / 10) * 10
    min_bb_mass_int = math.floor(min_bb_mass / 10) * 10
    if wantSequential:
        df_cat_bb = df_cat.Filter(f"{bb_mass} > {min_bb_mass_int} && {bb_mass} < {max_bb_mass_int}")
        np_dict_cat_bb = df_cat_bb.AsNumpy([tt_mass])
        mtt_list = np_dict_cat_bb[tt_mass]
    max_tt_mass =np.quantile(mtt_list, quantile_max)
    min_tt_mass =np.quantile(mtt_list, 1-quantile_max)

    max_tt_mass_int = math.ceil(max_tt_mass / 10) * 10
    min_tt_mass_int = math.floor(min_tt_mass / 10) * 10
    print(f"quantile is {quantile_max}")
    min_bb = min(min_bb_mass_int, max_bb_mass_int)
    max_bb = max(min_bb_mass_int, max_bb_mass_int)
    min_tt = min(min_tt_mass_int, max_tt_mass_int)
    max_tt = max(min_tt_mass_int, max_tt_mass_int)
    print(f"bb masses: min = {min_bb}, max = {max_bb}")
    print(f"tt masses: min = {min_tt}, max = {max_tt}")
    return min_bb,max_bb,min_tt,max_tt


def GetSquareSignalMassCut(dfWrapped_sig,global_cfg_dict,channel, cat,bb_mass, tt_mass = "tautau_m_vis",quantile_sig=0.68,wantSequential=False):
    df_cat_sig = dfWrapped_sig.df.Filter(f"OS_Iso && {cat} && {channel}")
    # df_cat_tt = dfWrapped_bckg.df.Filter(f"OS_Iso && {cat} && {channel}")
    if cat == 'boosted':
        df_cat_sig = df_cat_sig.Define("FatJet_atLeast1BHadron",
        "SelectedFatJet_nBHadrons>0").Filter("SelectedFatJet_p4[FatJet_atLeast1BHadron].size()>0")
    else:
        df_cat_sig = df_cat_sig.Filter("b1_hadronFlavour==5 && b2_hadronFlavour==5 ")
    return GetMassesQuantiles(df_cat_sig,tt_mass,bb_mass,quantile_sig,wantSequential)


def GetSquareSignalAndBckgMassCut(dfWrapped_sig,dfWrapped_bckg,global_cfg_dict,channel, cat,bb_mass, tt_mass = "tautau_m_vis",quantile_sig=0.90,quantile_bckg=0.68,wantSequential=False):

    df_cat_sig = dfWrapped_sig.df.Filter(f"OS_Iso && {cat} && {channel}")
    df_cat_bckg = dfWrapped_bckg.df.Filter(f"OS_Iso && {cat} && {channel}")
    n_in_sig = df_cat_sig.Count().GetValue()
    n_in_bckg = df_cat_bckg.Count().GetValue()
    print(f"n_initial sig = {n_in_sig}")
    print(f"n_initial bckg = {n_in_bckg}")
    if cat == 'boosted':
        df_cat_sig = df_cat_sig.Define("FatJet_atLeast1BHadron",
        "SelectedFatJet_nBHadrons>0").Filter("SelectedFatJet_p4[FatJet_atLeast1BHadron].size()>0")
        df_cat_bckg = df_cat_bckg.Define("FatJet_atLeast1BHadron",
        "SelectedFatJet_nBHadrons>0").Filter("SelectedFatJet_p4[FatJet_atLeast1BHadron].size()>0")
    else:
        df_cat_sig = df_cat_sig.Filter("b1_hadronFlavour==5 && b2_hadronFlavour==5 ")
        df_cat_bckg = df_cat_bckg.Filter("b1_hadronFlavour==5 && b2_hadronFlavour==5 ")
    print("SIGNALS")
    masses_sig  = GetMassesQuantiles(df_cat_sig,tt_mass,bb_mass,quantile_sig,wantSequential)
    masses_bckg = GetMassesQuantiles(df_cat_bckg,tt_mass,bb_mass,1-quantile_bckg,wantSequential)
    min_bb,max_bb,min_tt,max_tt = masses_sig
    n_fin_sig = df_cat_sig.Filter(f"{tt_mass} > {min_tt} && {tt_mass} < {max_tt} && {bb_mass}>{min_bb} && {bb_mass} < {max_bb}").Count().GetValue() # min_bb,max_bb,min_tt,max_tt
    min_bb,max_bb,min_tt,max_tt = masses_bckg
    n_fin_bckg = df_cat_bckg.Filter(f"{tt_mass} > {min_tt} && {tt_mass} < {max_tt} && {bb_mass}>{min_bb} && {bb_mass} < {max_bb}").Count().GetValue() # min_bb,max_bb,min_tt,max_tt
    n_in_bckg = df_cat_bckg.Count().GetValue()
    print(f"n_initial sig = {n_fin_sig}")
    print(f"n_initial bckg = {n_fin_bckg}")

    print("\nBACKGROUNDS")

    return masses_sig, masses_bckg

'''
def fraction_in_square(data, M_c1, M_c2, side):
    x, y = data[:, 0], data[:, 1]
    half_side = side / 2
    in_square = (M_c1 - half_side <= x) & (x <= M_c1 + half_side) & \
                (M_c2 - half_side <= y) & (y <= M_c2 + half_side)
    return np.mean(in_square)

def objective_square(params, dataset_tt, dataset_sig, fraction_b, fraction_sig):
    M_c1, M_c2, side = params
    frac_A = fraction_in_square(dataset_tt, M_c1, M_c2, side)  # Dataset A: frazione inclusa
    frac_B = fraction_in_square(dataset_sig, M_c1, M_c2, side)  # Dataset B: frazione inclusa

    # Penalizzazioni
    penalty_A = abs(frac_A - (1 - fraction_b))  # Target frazione di ttbar esclusa
    penalty_B = abs(frac_B - fraction_sig)      # Target frazione di segnale inclusa

    return penalty_A + penalty_B

def optimize_square(dataset_tt, dataset_sig, fraction_b, fraction_sig):
    dataset_sig = np.column_stack((mtt_list_sig, mbb_list_sig))  # Signal dataset
    dataset_tt = np.column_stack((mtt_list_tt, mbb_list_tt))  # Background dataset

    # Valori iniziali per [M_c1, M_c2, side]
    initial_guess = [125, 125, 50]

    # Ottimizzazione
    result = minimize(
        objective_square,
        initial_guess,
        args=(dataset_tt, dataset_sig, fraction_b, fraction_sig),
        bounds=[(None, None), (None, None), (0.1, None)],
        method='Nelder-Mead'
    )

    # Risultati finali
    M_c1_opt, M_c2_opt, side_opt = result.x
    print(f"Centro quadrato: M_c1 = {M_c1_opt}, M_c2 = {M_c2_opt}")
    print(f"Lato quadrato: side = {side_opt}")

    # Frazione finale nei dataset
    frac_A_final = fraction_in_square(dataset_tt, M_c1_opt, M_c2_opt, side_opt)
    frac_B_final = fraction_in_square(dataset_sig, M_c1_opt, M_c2_opt, side_opt)
    print(f"Frazione in Dataset A (ttbar, esclusa al 99%): {frac_A_final:.3f}")
    print(f"Frazione in Dataset B (segnale, inclusa al 68%): {frac_B_final:.3f}")
    return M_c1_opt, M_c2_opt, side_opt


'''