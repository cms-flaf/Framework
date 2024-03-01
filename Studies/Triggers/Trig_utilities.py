
import matplotlib.pyplot as plt
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
        "eTau":[34, 26, 35],
        "muTau":[26, 22, 32],
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

    first_tau_sel_singleTau = first_tau_sel.format(singleTau_pt_limits[0], tau1_eta_sel[channel])
    second_tau_sel_singleTau = second_tau_sel.format(singleTau_pt_limits[1], tau2_2p1)

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


def GetTrgValidityRegion(trg, singleTau_pt_limits=[190,190], met_th=150, year="2018"):
    trg_validity_regions = {
        "HLT_ditau":f"""(tau1_pt>{tau_pt_limits_dict[year]["tauTau"][0]} && abs(tau1_eta) < 2.1) && (tau2_pt>{tau_pt_limits_dict[year]["tauTau"][1]} && abs(tau2_eta) < 2.1)""",
        "HLT_singleTau":f"""(tau1_pt > {singleTau_pt_limits[0]} && abs(tau1_eta) < 2.1) || (tau2_pt>{singleTau_pt_limits[1]} && abs(tau2_eta) < 2.1 )""",
        "HLT_MET": f"metnomu_pt > {met_th}",
        "HLT_singleMu":f"""tau1_pt>{tau_pt_limits_dict[year]["muTau"][0]}""",
        "HLT_singleMu50":"tau1_pt>52",
        "HLT_mutau":f"""tau1_pt > {tau_pt_limits_dict[year]["muTau"][1]} && abs(tau1_eta) < 2.1 && tau2_pt > {tau_pt_limits_dict[year]["muTau"][2]} && abs(tau2_eta) < 2.1""",
        "HLT_singleEle":f"""tau1_pt>{tau_pt_limits_dict[year]["eTau"][0]} && abs(tau1_eta)<2.5""",
        "HLT_etau":f"""tau1_pt > {tau_pt_limits_dict[year]["eTau"][1]} && abs(tau1_eta) < 2.1 && tau2_pt > {tau_pt_limits_dict[year]["eTau"][2]} && abs(tau2_eta) < 2.1""",
    }
    return trg_validity_regions[trg]

def AddEfficiencyToDict(df,trg_list, channel, n_initial_channel,eff_key, eff_dict,verbose=False):
    filter_expr = f' ('
    filter_expr+= ' || '.join(trg for trg in trg_list)
    filter_expr+= ')'
    n_channel = df.Filter(filter_expr).Count().GetValue()
    eff_channel = n_channel / n_initial_channel
    if verbose:
        print(f"with {filter_expr} : n_initial{channel} = {n_initial_channel}, n_{channel} = {n_channel}, eff_{channel} = {round(eff_channel,2)}")
    if eff_key not in eff_dict.keys():
        eff_dict[eff_key] = []
    eff_dict[eff_key].append(round(eff_channel,2))

def makeplot(eff_dict, labels, linestyles, channel, x_values, sample, deepTauWP, deepTauVersion, ratio_ref,suffix='', colors=[], wantLegend=True):
    if colors==[]:
        colors = ['blue', 'green', 'red', 'orangered', 'purple', 'pink', 'yellow', 'cyan','black','brown','lime','navy','crimson']
    markers = ['o', '^', 's', 'D', 'x', 'v', 'p', '*','o']
    plt.figure(figsize=(20,15))
    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True)
    for i, (key, values) in enumerate(eff_dict.items()):
        label = None
        if i < len(labels):
            label = labels[i]
        ratio_values = [(val/ref) for val,ref in zip(values,ratio_ref)]
        for mass,ratio,value in zip(x_values,ratio_values,values):
            print(f"mass = {mass} and value = {ratio} and value = {value}")

        #print(colors[i % len(colors)],markers[i % len(markers)],linestyles[i % len(markers)], label)
        ax1.plot(x_values, values, color=colors[i % len(colors)],marker=markers[i % len(markers)],linestyle=linestyles[i % len(markers)], label=label)
        ax2.plot(x_values, ratio_values, color=colors[i % len(colors)],marker=markers[i % len(markers)],linestyle=linestyles[i % len(markers)])#,label=label)

    #### Legend + titles + axis adjustment ####
    #plt.title(f'efficiencies for channel {channel} and {sample}')
    plt.xlabel(r'$m_X$ (GeV/$c^2$)')
    ax1.set_ylabel('efficiency')
    ax2.set_ylabel('ratio')
    ax1.set_ylim(0., 1.05)
    #ax2.legend(loc='upper right',  bbox_to_anchor=(0.5, 0.8))

    #fig.legend(bbox_to_anchor=(0.5, -0.05),
          #fancybox=True, shadow=True)
    #ax2.set_yscale('log')
    ax2.set_xscale('log')
    xticks = [250, 500, 800, 1000, 1500, 2000, 2500 ,3000]  # Esempio di numeri da visualizzare sull'asse logaritmico
    ax2.set_xticks(xticks)
    ax2.set_xticklabels([str(num) for num in xticks])

    figName = f'Studies/Triggers/eff_{channel}_{sample}_{deepTauWP}_{deepTauVersion}{suffix}'
    #fig.legend(bbox_to_anchor=(1.3, 0.6))
    #ax1.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1), shadow=True, ncol=1)
    if wantLegend:
        ax1.legend(loc='center left', bbox_to_anchor=(1, 0.5), shadow=True, ncol=1)
    plt.tight_layout()
    if wantLegend:
        figName += 'WithLeg'
    plt.savefig(f'{figName}.png')

'''
def GetETauDict(dfWrappedInitial):
    df_eTau = dfWrapped_central.df.Filter('eTau').Filter("tau1_gen_kind==3 && tau2_gen_kind==5").Filter(os_iso_filtering[args.deepTauWP])
    nInitial_eTau = df_eTau.Count().GetValue()
'''

'''
#### eTau efficiencies ####

pass_eleTau = pass_other_trg.format("HLT_etau")
pass_eleTau_validity = f"""HLT_etau && ({GetTrgValidityRegion("HLT_etau")})"""
pass_singleEle = pass_other_trg.format("HLT_singleEle")
pass_singleEle_validity = f"""HLT_singleEle && ({GetTrgValidityRegion("HLT_singleEle")})"""


trg_eTau_list_1 = [pass_singleEle,pass_eleTau]
etau_labels.append("singleEle||eTau")
etau_linestyles.append('solid')
AddEfficiencyToDict(dataframes_channel['eTau'], trg_eTau_list_1, 'eTau', nInitial_eTau,'eff_eTau_1', eff_etau)

trg_eTau_list_2 = [pass_singleEle,pass_eleTau, pass_singleTau ]
etau_labels.append("singleEle||eTau||singleTau")
etau_linestyles.append('solid')
AddEfficiencyToDict(dataframes_channel['eTau'], trg_eTau_list_2, 'eTau', nInitial_eTau,'eff_eTau_2', eff_etau)

trg_eTau_list_3 = [pass_singleEle,pass_eleTau, pass_met ]
etau_labels.append("singleEle||eTau||MET")
etau_linestyles.append('solid')
AddEfficiencyToDict(dataframes_channel['eTau'], trg_eTau_list_3, 'eTau', nInitial_eTau,'eff_eTau_3', eff_etau)

trg_eTau_list_4 = [pass_singleEle,pass_eleTau, pass_singleTau, pass_met ]
etau_labels.append("full OR app region")
etau_linestyles.append('solid')
AddEfficiencyToDict(dataframes_channel['eTau'], trg_eTau_list_4, 'eTau', nInitial_eTau,'eff_eTau_4', eff_etau)


trg_eTau_list_5 = ['HLT_singleEle','HLT_etau', 'HLT_singleTau', 'HLT_MET']
etau_labels.append("full OR")
etau_linestyles.append('dashed')
AddEfficiencyToDict(dataframes_channel['eTau'], trg_eTau_list_5, 'eTau', nInitial_eTau,'eff_eTau_5', eff_etau)

############################################################

trg_eTau_list_noreg_1 = [pass_singleEle_validity, pass_eleTau_validity]
etau_labels_noreg.append("singleEle||eTau")
AddEfficiencyToDict(dataframes_channel['eTau'], trg_eTau_list_noreg_1, 'eTau', nInitial_eTau,'eff_eTau_noreg_1', eff_etau_noreg)

trg_eTau_list_noreg_2 = [pass_singleEle,pass_eleTau,pass_singleTau_validity]
etau_labels_noreg.append("singleEle||eTau||singleTau")
AddEfficiencyToDict(dataframes_channel['eTau'], trg_eTau_list_noreg_2, 'eTau', nInitial_eTau,'eff_eTau_noreg_2', eff_etau_noreg)

trg_eTau_list_noreg_3 = [pass_singleEle,pass_eleTau,pass_MET_validity]
etau_labels_noreg.append("singleEle,eTau,MET")
AddEfficiencyToDict(dataframes_channel['eTau'], trg_eTau_list_noreg_3, 'eTau', nInitial_eTau,'eff_eTau_noreg_3', eff_etau_noreg)

trg_eTau_list_noreg_4 = [pass_singleEle,pass_eleTau,pass_singleTau_validity,pass_MET_validity]
etau_labels_noreg.append("allOR")
AddEfficiencyToDict(dataframes_channel['eTau'], trg_eTau_list_noreg_4, 'eTau', nInitial_eTau,'eff_eTau_noreg_4', eff_etau_noreg,True)



#### muTau efficiencies ####
pass_muTau = pass_other_trg.format("HLT_mutau")
pass_singleMu = pass_other_trg.format("HLT_singleMu")
pass_singleMu50 = pass_other_trg.format("HLT_singleMu50")

pass_muTau_validity = f"""HLT_mutau && ({GetTrgValidityRegion("HLT_mutau")})"""
pass_singleMu_validity = f"""HLT_singleMu && ({GetTrgValidityRegion("HLT_singleMu")})"""
pass_singleMu50_validity = f"""HLT_singleMu50 && ({GetTrgValidityRegion("HLT_singleMu50")})"""



trg_muTau_list_1 = [pass_singleMu, pass_muTau]
mutau_labels.append("singleMu,muTau")
mutau_linestyles.append('solid')
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_1, 'muTau', nInitial_muTau,'eff_muTau_1', eff_mutau)

trg_muTau_list_2 = [pass_singleMu, pass_muTau,pass_singleMu50]
mutau_labels.append("singleMu,muTau,singleMu50")
mutau_linestyles.append('solid')
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_2, 'muTau', nInitial_muTau,'eff_muTau_2', eff_mutau)

trg_muTau_list_3 = [pass_singleMu, pass_muTau, pass_singleTau]
mutau_labels.append("singleMu,muTau,singleTau")
mutau_linestyles.append('solid')
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_3, 'muTau', nInitial_muTau,'eff_muTau_3', eff_mutau)

trg_muTau_list_4 = [pass_singleMu, pass_muTau, pass_met]
mutau_labels.append("singleMu,muTau,MET")
mutau_linestyles.append('solid')
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_4, 'muTau', nInitial_muTau,'eff_muTau_4', eff_mutau)

trg_muTau_list_5 = [pass_singleMu, pass_muTau, pass_singleTau,pass_met]
mutau_labels.append("singleMu,muTau,singleTau,MET")
mutau_linestyles.append('solid')
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_5, 'muTau', nInitial_muTau,'eff_muTau_5', eff_mutau)

trg_muTau_list_6 = [pass_singleMu, pass_muTau,pass_singleMu50, pass_singleTau]
mutau_labels.append("singleMu,muTau,singleMu50,singleTau")
mutau_linestyles.append('solid')
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_6, 'muTau', nInitial_muTau,'eff_muTau_6', eff_mutau)

trg_muTau_list_7 = [pass_singleMu, pass_muTau,pass_singleMu50, pass_met]
mutau_labels.append("singleMu,muTau,singleMu50,MET")
mutau_linestyles.append('solid')
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_7, 'muTau', nInitial_muTau,'eff_muTau_7', eff_mutau)

trg_muTau_list_8 = [pass_singleMu, pass_singleMu50, pass_muTau, pass_met,pass_singleTau]
mutau_labels.append("full OR")
mutau_linestyles.append('solid')
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_8, 'muTau', nInitial_muTau,'eff_muTau_8', eff_mutau)

trg_muTau_list_9 = ['HLT_singleMu', 'HLT_mutau','HLT_singleMu50', 'HLT_singleTau', 'HLT_MET']
mutau_labels.append("full OR no app region")
mutau_linestyles.append('dotted')
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_9, 'muTau', nInitial_muTau,'eff_muTau_9', eff_mutau)
######################



trg_muTau_list_noreg_1 = [pass_singleMu_validity, pass_muTau_validity]
mutau_labels_noreg.append("singleMu,muTau")
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_noreg_1, 'muTau', nInitial_muTau,'eff_muTau_noreg_1', eff_mutau_noreg)

trg_muTau_list_noreg_2 = [pass_singleMu_validity, pass_muTau_validity,pass_singleMu50_validity]
mutau_labels_noreg.append("singleMu,muTau,singleMu50")
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_noreg_2, 'muTau', nInitial_muTau,'eff_muTau_noreg_2', eff_mutau_noreg)

trg_muTau_list_noreg_3 = [pass_singleMu_validity, pass_muTau_validity, pass_singleTau_validity]
mutau_labels_noreg.append("singleMu,muTau,singleTau")
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_noreg_3, 'muTau', nInitial_muTau,'eff_muTau_noreg_3', eff_mutau_noreg)

trg_muTau_list_noreg_4 = [pass_singleMu_validity, pass_muTau_validity, pass_MET_validity]
mutau_labels_noreg.append("singleMu,muTau,MET")
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_noreg_4, 'muTau', nInitial_muTau,'eff_muTau_noreg_4', eff_mutau_noreg)

trg_muTau_list_noreg_5 = [pass_singleMu_validity, pass_muTau_validity, pass_singleTau_validity,pass_MET_validity]
mutau_labels_noreg.append("singleMu,muTau,singleTau,MET")
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_noreg_5, 'muTau', nInitial_muTau,'eff_muTau_noreg_5', eff_mutau_noreg)

trg_muTau_list_noreg_6 = [pass_singleMu_validity, pass_muTau_validity,pass_singleMu50_validity, pass_singleTau_validity]
mutau_labels_noreg.append("singleMu,muTau,singleMu50,singleTau")
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_noreg_6, 'muTau', nInitial_muTau,'eff_muTau_noreg_6', eff_mutau_noreg)

trg_muTau_list_noreg_7 = [pass_singleMu_validity, pass_muTau_validity,pass_singleMu50_validity, pass_MET_validity]
mutau_labels_noreg.append("singleMu,muTau,singleMu50,MET")
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_noreg_7, 'muTau', nInitial_muTau,'eff_muTau_noreg_7', eff_mutau_noreg)

trg_muTau_list_noreg_8 = [pass_singleMu_validity, pass_singleMu50_validity, pass_muTau_validity, pass_MET_validity,pass_singleTau_validity]
mutau_labels_noreg.append("full OR")
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_noreg_8, 'muTau', nInitial_muTau,'eff_muTau_noreg_8', eff_mutau_noreg)

trg_muTau_list_noreg_9 = ['HLT_singleMu', 'HLT_mutau','HLT_singleMu50', 'HLT_singleTau', 'HLT_MET']
mutau_labels_noreg.append("full OR no val region")
mutau_linestyles.append('dotted')
AddEfficiencyToDict(dataframes_channel['muTau'], trg_muTau_list_noreg_9, 'muTau', nInitial_muTau,'eff_muTau_noreg_9', eff_mutau_noreg)

'''

'''
trg_tauTau_list_noreg_diTauSingleTau = [pass_ditau_validity,pass_singleTau_validity]
tautau_labels_noreg.append("ditau,singleTau")
AddEfficiencyToDict(dataframes_channel['tauTau'], trg_tauTau_list_noreg_diTauSingleTau, 'tauTau', nInitial_tauTau,'eff_tauTau_noreg_2', eff_tautau_noreg)

trg_tauTau_list_noreg_diTauMET = [pass_ditau_validity,pass_MET_validity]
tautau_labels_noreg.append("ditau,MET")
AddEfficiencyToDict(dataframes_channel['tauTau'], trg_tauTau_list_noreg_diTauMET, 'tauTau', nInitial_tauTau,'eff_tauTau_noreg_3', eff_tautau_noreg)

trg_tauTau_list_fullORwithoutValidity_onlyToCheck = ['HLT_ditau','HLT_singleTau','HLT_MET']
#tautau_labels.append("ditau,singleTau,MET")
tautau_linestyles.append('dashed')
AddEfficiencyToDict(dataframes_channel['tauTau'], trg_tauTau_list_fullORwithoutValidity_onlyToCheck, 'tauTau', nInitial_tauTau,'eff_tauTau_6', eff_tautau)

''''''
trg_tauTau_list_1 = [pass_diTau]
tautau_labels.append("diTau")
tautau_linestyles.append('solid')
AddEfficiencyToDict(dataframes_channel['tauTau'], trg_tauTau_list_1, 'tauTau', nInitial_tauTau,'eff_tauTau_1', eff_tautau)

trg_tauTau_list_2 = [pass_diTau, pass_met]
tautau_labels.append("diTau,MET")
tautau_linestyles.append('solid')
AddEfficiencyToDict(dataframes_channel['tauTau'], trg_tauTau_list_2, 'tauTau', nInitial_tauTau,'eff_tauTau_2', eff_tautau)

trg_tauTau_list_3 = [pass_diTau, pass_singleTau]
tautau_labels.append("diTau,singleTau")
tautau_linestyles.append('solid')
AddEfficiencyToDict(dataframes_channel['tauTau'], trg_tauTau_list_3, 'tauTau', nInitial_tauTau,'eff_tauTau_3', eff_tautau)

#print(eff_etau)
#print(etau_labels)
#print(len(masses))

#makeplot(eff_etau, etau_labels,etau_linestyles, 'eTau', x_values, args.sample, args.wantBigTau, args.deepTauWP, args.deepTauVersion, False, eff_etau_noreg['eff_eTau_noreg_1'])
#makeplot(eff_mutau, mutau_labels,mutau_linestyles, 'muTau', x_values, args.sample, args.wantBigTau, args.deepTauWP, args.deepTauVersion, False, eff_mutau_noreg['eff_muTau_noreg_1'])
print("for tauTau with reg")
makeplot(eff_tautau, tautau_labels,tautau_linestyles, 'tauTau', x_values, args.sample, args.wantBigTau, args.deepTauWP, args.deepTauVersion,False, eff_tautau_noreg['eff_tauTau_noreg_1'])
print("for tauTau bigTau")


#makeplot(eff_etau_noreg, etau_labels_noreg,etau_linestyles, 'eTau', x_values, args.sample, args.wantBigTau, args.deepTauWP, args.deepTauVersion, True,eff_etau_noreg['eff_eTau_noreg_1'])

#makeplot(eff_mutau_noreg, mutau_labels_noreg,mutau_linestyles, 'muTau', x_values, args.sample, args.wantBigTau, args.deepTauWP, args.deepTauVersion, True,eff_mutau_noreg['eff_muTau_noreg_1'])#print("for tauTau no reg")
#makeplot(eff_tautau_noreg, tautau_labels_noreg,tautau_linestyles, 'tauTau', x_values, args.sample, args.wantBigTau, args.deepTauWP, args.deepTauVersion, True,eff_tautau_noreg['eff_tauTau_noreg_1'])

'''
