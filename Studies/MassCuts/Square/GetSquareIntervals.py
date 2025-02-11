import numpy as np
import math

from scipy.stats import chi2
from sklearn.covariance import MinCovDet
import matplotlib.pyplot as plt
'''
def GetMassesQuantilesJoint(df_cat, tt_mass, bb_mass, quantile_max):
    np_dict_cat = df_cat.AsNumpy([tt_mass, bb_mass])
    mbb = np_dict_cat[bb_mass]
    mtt = np_dict_cat[tt_mass]
    data = np.vstack((mbb, mtt)).T  # Matrice Nx2

    lower, upper = 0.16, 0.84  # Limiti iniziali in percentili
    den = df_cat.Count().GetValue()
    num = den
    perc = 1
    p_low_mtt, p_low_mbb = np.min(data, axis=0)
    p_high_mtt, p_high_mbb = np.max(data, axis=0)
    print(f"initially: bb in {p_low_mbb}, {p_high_mbb}")
    print(f"initially: tt in {p_low_mtt}, {p_high_mtt}")


    while perc < quantile_max:  # Espandiamo finché non superiamo quantile_max
        print("perc, quantile_max, perc - quantile_max")
        print(perc, quantile_max, perc - quantile_max)
        print("upper, lower")
        print(upper, lower)
        p_low = np.percentile(data, lower, axis=0)
        p_high = np.percentile(data, upper, axis=0)
        p_low_mbb, p_low_mtt = p_low
        p_high_mbb, p_high_mtt = p_high

        print(f"during: bb in {p_low_mbb}, {p_high_mbb}")
        print(f"during: tt in {p_low_mtt}, {p_high_mtt}")
        num = df_cat.Filter(f"{tt_mass} >= {p_low_mtt} && {tt_mass} < {p_high_mtt}")\
                    .Filter(f"{bb_mass} >= {p_low_mbb} && {bb_mass} < {p_high_mbb}")\
                    .Count().GetValue()
        print(num)
        perc = num / den
        # print(f"Dati selezionati: {num} su {den} (percentuale: {perc}%)")
        # print(f"mbb = {p_low[0]}, {p_high[0]}")
        # print(f"mtt = {p_low[1]}, {p_high[1]}")
        if perc < quantile_max:
            upper += (100 - upper) * 0.1  # Espandi i limiti
            print("sto espandendo i limiti")
            print(upper, lower)
        else:
            print("sto restringendo i limiti")
            upper -= (upper - lower) * 0.1  # Restringi i limiti
            print(upper, lower)
        print(p_low_mbb, p_low_mtt)
        print(p_high_mbb, p_high_mtt)
    # Arrotondiamo i limiti finali
    min_tt_int = math.floor(p_low_mtt / 10) * 10
    max_tt_int = math.ceil(p_high_mtt / 10) * 10
    min_bb_int = math.floor(p_low_mbb / 10) * 10
    max_bb_int = math.ceil(p_high_mbb / 10) * 10
    print(f" final fraction = {perc}")
    print(f"mbb = {min_bb_int}, {max_bb_int}")
    print(f"mtt = {min_tt_int}, {max_tt_int}")

    return min_tt_int, max_tt_int, min_bb_int, max_bb_int  # Restituisce i limiti finali

import numpy as np
import math
from sklearn.covariance import MinCovDet
from scipy.stats import chi2
'''
import numpy as np
import math

def GetMassesQuantilesJoint(df_cat, tt_mass, bb_mass, quantile_max):
    np_dict_cat = df_cat.AsNumpy([tt_mass, bb_mass])
    mbb = np_dict_cat[bb_mass]
    mtt = np_dict_cat[tt_mass]
    data = np.vstack((mbb, mtt)).T  # Matrice Nx2

    lower, upper = 0, 1  # Limiti iniziali in percentili
    den = df_cat.Count().GetValue()
    num = den
    perc = 0. # num / den

    # Inizializziamo i limiti per evitare UnboundLocalError
    p_low_mtt, p_low_mbb = np.min(data, axis=0)
    p_high_mtt, p_high_mbb = np.max(data, axis=0)

    # if perc >= quantile_max:
    #     print("Percentuale già superiore al quantile massimo richiesto.")
    #     return

    while perc < quantile_max:  # Continua fino a superare quantile_max
        p_low = np.percentile(data, lower, axis=0)
        p_high = np.percentile(data, upper, axis=0)
        p_low_mbb, p_low_mtt = p_low
        p_high_mbb, p_high_mtt = p_high

        num = df_cat.Filter(f"{tt_mass} >= {p_low_mtt} && {tt_mass} < {p_high_mtt}")\
                    .Filter(f"{bb_mass} >= {p_low_mbb} && {bb_mass} < {p_high_mbb}")\
                    .Count().GetValue()
        perc = num / den
        # print(f"Dati selezionati: {num} su {den} (percentuale: {perc:.3f})")
        # print(f"mbb = {p_low[0]:.2f}, {p_high[0]:.2f}")
        # print(f"mtt = {p_low[1]:.2f}, {p_high[1]:.2f}")

        if perc < quantile_max:
            upper += (100 - upper) * 0.1  # Espandi i limiti per includere più dati

    # Arrotondiamo i limiti finali
    min_tt_int = math.floor(p_low_mtt / 10) * 10
    max_tt_int = math.ceil(p_high_mtt / 10) * 10
    min_bb_int = math.floor(p_low_mbb / 10) * 10
    max_bb_int = math.ceil(p_high_mbb / 10) * 10

    # print(f"mbb = {min_bb_int}, {max_bb_int}")
    # print(f"mtt = {min_tt_int}, {max_tt_int}")

    return min_tt_int,max_tt_int,min_bb_int,max_bb_int


def GetEllipticCut(df_cat, tt_mass, bb_mass, quantile_max):
    np_dict_cat = df_cat.AsNumpy([tt_mass, bb_mass])
    mbb = np_dict_cat[bb_mass]
    mtt = np_dict_cat[tt_mass]
    data = np.vstack((mtt, mbb)).T  # Matrice Nx2 con ordine (mtt, mbb)

    den = df_cat.Count().GetValue()
    num = den  # Inizialmente includiamo tutti i dati
    perc = 0. #num / den

    # Stima della media robusta e della matrice di covarianza con MinCovDet
    mcd = MinCovDet(support_fraction=0.68).fit(data)
    center = mcd.location_  # Centro dell'ellisse (A, C)
    covariance = mcd.covariance_  # Matrice di covarianza robusta

    # Calcoliamo gli autovalori per determinare i semiassi iniziali
    eigvals, eigvecs = np.linalg.eigh(covariance)

    # Chi-quadro per selezionare un livello di confidenza iniziale
    chi2_val = np.sqrt(chi2.ppf(quantile_max, df=2))  # Valore di chi-quadro per il livello desiderato
    B, D = chi2_val * np.sqrt(eigvals)  # Semiassi iniziali

    scale_factor = 1.0  # Fattore di scala per espandere/ridurre l'ellisse

    while perc < quantile_max:  # Espandiamo finché non superiamo quantile_max
        # Definiamo la funzione ellittica
        mask = ((data[:, 0] - center[0])**2 / (B * scale_factor)**2 +
                (data[:, 1] - center[1])**2 / (D * scale_factor)**2) < 1

        num = np.sum(mask)  # Conta i punti dentro l'ellisse
        perc = num / den

        if perc < quantile_max:
            scale_factor += 0.05  # Aumentiamo la dimensione dell'ellisse

        # Valori finali
        B_final = B * scale_factor
        D_final = D * scale_factor
        A, C = center  # Centro dell'ellisse

        # **Filtro corretto nel dataframe**
        num = df_cat.Filter(f"(({tt_mass} - {A})*({tt_mass} - {A}) / ({B_final}*{B_final}) + ({bb_mass} - {C})*({bb_mass} - {C}) / ({D_final}*{D_final})) < 1").Count().GetValue()

        perc = num / den  # Percentuale aggiornata dopo il filtro

    return A, B_final, C, D_final  # Restituisce i parametri dell'ellisse


