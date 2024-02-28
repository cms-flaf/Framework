
tau_pt_limits_dict = {
    "2016":
    {
        "eTau":[26,26,26],
        "muTau":[25, 20, 25],
        "tauTau":[40, 40, 40],
    },
    "2017":
    {
        "eTau":[33, 25, 35],
        "muTau":[28, 21, 32],
        "tauTau":[40, 40, 40],
    },
    "2018":
    {
        "eTau":[33, 25, 35],
        "muTau":[25, 21, 32],
        "tauTau":[40, 40, 40],
    }
}

ggR_samples = [ "GluGluToRadionToHHTo2B2Tau_M-250", "GluGluToRadionToHHTo2B2Tau_M-260", "GluGluToRadionToHHTo2B2Tau_M-270", "GluGluToRadionToHHTo2B2Tau_M-280", "GluGluToRadionToHHTo2B2Tau_M-300", "GluGluToRadionToHHTo2B2Tau_M-320", "GluGluToRadionToHHTo2B2Tau_M-350", "GluGluToRadionToHHTo2B2Tau_M-450", "GluGluToRadionToHHTo2B2Tau_M-500", "GluGluToRadionToHHTo2B2Tau_M-550", "GluGluToRadionToHHTo2B2Tau_M-600", "GluGluToRadionToHHTo2B2Tau_M-650", "GluGluToRadionToHHTo2B2Tau_M-700", "GluGluToRadionToHHTo2B2Tau_M-750", "GluGluToRadionToHHTo2B2Tau_M-800", "GluGluToRadionToHHTo2B2Tau_M-850", "GluGluToRadionToHHTo2B2Tau_M-900", "GluGluToRadionToHHTo2B2Tau_M-1000", "GluGluToRadionToHHTo2B2Tau_M-1250", "GluGluToRadionToHHTo2B2Tau_M-1500", "GluGluToRadionToHHTo2B2Tau_M-1750", "GluGluToRadionToHHTo2B2Tau_M-2000", "GluGluToRadionToHHTo2B2Tau_M-2500", "GluGluToRadionToHHTo2B2Tau_M-3000"]

ggBG_samples = [
    'GluGluToBulkGravitonToHHTo2B2Tau_M-250', 'GluGluToBulkGravitonToHHTo2B2Tau_M-260', 'GluGluToBulkGravitonToHHTo2B2Tau_M-270', 'GluGluToBulkGravitonToHHTo2B2Tau_M-280', 'GluGluToBulkGravitonToHHTo2B2Tau_M-300', 'GluGluToBulkGravitonToHHTo2B2Tau_M-320', 'GluGluToBulkGravitonToHHTo2B2Tau_M-350', #'GluGluToBulkGravitonToHHTo2B2Tau_M-400',
    'GluGluToBulkGravitonToHHTo2B2Tau_M-450', 'GluGluToBulkGravitonToHHTo2B2Tau_M-500', 'GluGluToBulkGravitonToHHTo2B2Tau_M-550', 'GluGluToBulkGravitonToHHTo2B2Tau_M-600', 'GluGluToBulkGravitonToHHTo2B2Tau_M-650', 'GluGluToBulkGravitonToHHTo2B2Tau_M-700', 'GluGluToBulkGravitonToHHTo2B2Tau_M-750', 'GluGluToBulkGravitonToHHTo2B2Tau_M-800', 'GluGluToBulkGravitonToHHTo2B2Tau_M-850', 'GluGluToBulkGravitonToHHTo2B2Tau_M-900',
    'GluGluToBulkGravitonToHHTo2B2Tau_M-1000', 'GluGluToBulkGravitonToHHTo2B2Tau_M-1250', 'GluGluToBulkGravitonToHHTo2B2Tau_M-1500', 'GluGluToBulkGravitonToHHTo2B2Tau_M-1750', 'GluGluToBulkGravitonToHHTo2B2Tau_M-2000', 'GluGluToBulkGravitonToHHTo2B2Tau_M-2500','GluGluToBulkGravitonToHHTo2B2Tau_M-3000'
    ]


def get_regions_dict(channel, singleTau_pt_limits=[190,190], met_th=180, bigtau=False):
    regions = {}
    tau1_eta_sel = {
        "eTau": "abs(tau1_eta) <= 2.5",
        "muTau": "abs(tau1_eta) <= 2.4",
        "tauTau": "abs(tau1_eta)<=2.1"
    }
    tau1_2p1 = "abs(tau1_eta)<=2.1"
    tau2_2p1 = "abs(tau2_eta)<=2.1"
    tau_pt_limits = tau_pt_limits_dict["2018"][channel]
    first_tau_sel = "tau1_pt >= {0} && {1}"
    second_tau_sel = "tau2_pt >= {0} && {1}"

    second_tau_sel_singleTau = second_tau_sel.format(singleTau_pt_limits[1], tau2_2p1)
    first_tau_sel_singleTau = first_tau_sel.format(singleTau_pt_limits[0], tau1_eta_sel[channel])
    first_tau_sel_other = first_tau_sel.format(tau_pt_limits[0], tau1_eta_sel[channel])
    second_tau_sel_other = second_tau_sel.format(tau_pt_limits[1], tau2_2p1)
    single_lepton_validity = first_tau_sel.format(tau_pt_limits[0], tau1_eta_sel[channel])
    cross_lepton_validity_first = first_tau_sel.format(tau_pt_limits[1], tau1_2p1)
    cross_lepton_validity_second = second_tau_sel.format(tau_pt_limits[2], tau2_2p1)
    cross_lepton_validity  = f"{cross_lepton_validity_first} && {cross_lepton_validity_second}"


    if bigtau:
        if channel == "tauTau":
            #second_tau_sel_singleTau = second_tau_sel.format(singleTau_pt_limits[1], tau2_2p1)
            #first_tau_sel_singleTau = first_tau_sel.format(singleTau_pt_limits[0], tau1_eta_sel[channel])
            regions["singleTau_region"] = f"""(({first_tau_sel_singleTau}) || ({second_tau_sel_singleTau}))"""

            #first_tau_sel_other = first_tau_sel.format(tau_pt_limits[0], tau1_eta_sel[channel])
            #second_tau_sel_other = second_tau_sel.format(tau_pt_limits[1], tau2_2p1)
            regions["other_trg_region"] = f"""(({first_tau_sel_other}) &&( {second_tau_sel_other})) && !({regions["singleTau_region"]})"""
        else:
            #second_tau_sel_singleTau = second_tau_sel.format(singleTau_pt_limits[1], tau2_2p1)
            regions["singleTau_region"] = second_tau_sel_singleTau

            #single_lepton_validity = first_tau_sel.format(tau_pt_limits[0], tau1_eta_sel[channel])
            #cross_lepton_validity_first = first_tau_sel.format(tau_pt_limits[1], tau1_2p1)
            #cross_lepton_validity_second = second_tau_sel.format(tau_pt_limits[2], tau2_2p1)
            #cross_lepton_validity  = f"{cross_lepton_validity_first} && {cross_lepton_validity_second}"
            regions['other_trg_region'] = f"""(({single_lepton_validity}) || ({cross_lepton_validity})) && !({regions["singleTau_region"]})"""

    else:
        if channel == "tauTau":
            #first_tau_sel_other = first_tau_sel.format(tau_pt_limits[0], tau1_eta_sel[channel])
            #second_tau_sel_other = second_tau_sel.format(tau_pt_limits[1], tau2_2p1)
            #second_tau_sel_singleTau = second_tau_sel.format(singleTau_pt_limits[1], tau2_2p1)
            #first_tau_sel_singleTau = first_tau_sel.format(singleTau_pt_limits[0], tau1_eta_sel[channel])
            regions["other_trg_region"] = f"""(({first_tau_sel_other}) && ({second_tau_sel_other}))"""
            regions["singleTau_region"] = f"""(({first_tau_sel_singleTau}) || ({second_tau_sel_singleTau}))&& !({regions["other_trg_region"]})"""

        else:
            #single_lepton_validity = first_tau_sel.format(tau_pt_limits[0], tau1_eta_sel[channel])
            #cross_lepton_validity_first = first_tau_sel.format(tau_pt_limits[1], tau1_2p1)
            #cross_lepton_validity_second = second_tau_sel.format(tau_pt_limits[2], tau2_2p1)
            #cross_lepton_validity  = f"{cross_lepton_validity_first} && {cross_lepton_validity_second}"
            #second_tau_sel_singleTau = second_tau_sel.format(singleTau_pt_limits[1], tau2_2p1)
            regions["other_trg_region"] = f"""(({single_lepton_validity}) || ({cross_lepton_validity}))"""
            regions["singleTau_region"] = f"""({second_tau_sel_singleTau}) &&!({regions["other_trg_region"]}) """

    regions["MET_region"] = f"""(met_pt > {met_th} && !( ({regions["other_trg_region"]}) || ( {regions["singleTau_region"]} ) ) )"""
    return regions


def GetTrgValidityRegion(trg):
    trg_validity_regions = {
        "HLT_ditau":"(tau1_pt>40 && abs(tau1_eta) < 2.1) && (tau2_pt>40 && abs(tau2_eta) < 2.1)",
        "HLT_singleTau":"(tau1_pt > 190 && abs(tau1_eta) < 2.1) || (tau2_pt>190 && abs(tau2_eta) < 2.1 )",
        "HLT_MET": "metnomu_pt > 120",
        "HLT_singleMu":"tau1_pt>26",
        "HLT_singleMu50":"tau1_pt>52",
        "HLT_mutau":"tau1_pt > 22 && abs(tau1_eta) < 2.1 && tau2_pt > 22 && abs(tau2_eta) < 2.1",
        "HLT_singleEle":"tau1_pt>34 && abs(tau1_eta)<2.5",
        "HLT_etau":"tau1_pt > 26 && abs(tau1_eta) < 2.1 && tau2_pt > 35 && abs(tau2_eta) < 2.1",
    }
    return trg_validity_regions[trg]

'''
def GetETauDict(dfWrappedInitial):
    df_eTau = dfWrapped_central.df.Filter('eTau').Filter("tau1_gen_kind==3 && tau2_gen_kind==5").Filter(os_iso_filtering[args.deepTauWP])
    nInitial_eTau = df_eTau.Count().GetValue()
'''