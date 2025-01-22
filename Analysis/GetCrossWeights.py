import math
import ROOT
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])
# ROOT.gROOT.ProcessLine('#include "include/AnalysisTools.h"')

ROOT.gInterpreter.Declare(
    """
    float get_scale_factor_error(const float& effData, const float& effMC, const float& errData, const float& errMC, std::string err_name) {

    float SF_error = 0.;

    if (errData==0. && errMC==0.) {
        //std::cout<<"WARNING : uncertainty on data and MC = 0, for " << err_name << ", can not calculate uncertainty on scale factor. Uncertainty set to 0." << std::endl;
        return 0.;
    }

    if (effData==0. || effMC==0.) {
        //std::cout<<"WARNING : uncertainty on OR and MC = 0, for " << err_name << ",can not calculate uncertainty on scale factor. Uncertainty set to 0." << std::endl;
        return 0.;
    }
    else {
        SF_error = pow((errData/effData),2) + pow((errMC/effMC),2);
        SF_error = pow(SF_error, 0.5)*(effData/effMC);
    }
    return SF_error;
    }

    float SelectCorrectDM(const float& tauDM, const float& effDM0, const float& effDM1, const float& eff3Prong){
        if(tauDM == 0){
            return effDM0;
        }
        if(tauDM == 1){
            return effDM1;
        }
        if(tauDM == 10 || tauDM == 11){
            return eff3Prong;
        }

        return 0.;
    }
    float getCorrectSingleLepWeight(const float& lep1_pt, const float& lep1_eta, const bool& lep1_matching, const float& lep1_weight,const float& lep2_pt, const float& lep2_eta, const bool& lep2_matching, const float& lep2_weight){
        if(lep1_pt > lep2_pt){
            return lep1_matching? lep1_weight : 1.f;
        }
        else if(lep1_pt == lep2_pt){
            if(abs(lep1_eta) < abs(lep2_eta)){
                return lep1_matching? lep1_weight : 1.f;
            }
            else if(abs(lep1_eta) > abs(lep2_eta)){
                return lep2_matching? lep2_weight : 1.f;
            }
        }
        else{
            return lep2_matching? lep2_weight : 1.f;
        }
    throw std::invalid_argument("ERROR: no suitable single lepton candidate");

    }
    """
)

def get_scale_factor_error(eff_data, eff_mc, err_data, err_mc):
        SF_error = 0.0

        if err_data == 0. and err_mc == 0.:
            print("WARNING: uncertainty on data and MC = 0, cannot calculate uncertainty on scale factor. Uncertainty set to 0.")

        if eff_data == 0. or eff_mc == 0.:
            print("WARNING: efficiency in data or MC = 0, cannot calculate uncertainty on scale factor. Uncertainty set to 0.")
            return 0.0
        else:
            SF_error = math.pow((err_data / eff_data), 2) + math.pow((err_mc / eff_mc), 2)
            SF_error = math.sqrt(SF_error) * (eff_data / eff_mc)

        return SF_error

def defineTriggerWeightsErrors(dfBuilder):
    passSingleMu = "SingleMu_region"
    passCrossMuTau = "CrossMuTau_region"
    passSingleEle = "SingleEle_region"
    passCrossEleTau = "CrossEleTau_region"
    if 'muTau' in dfBuilder.config['channels_to_consider']:
        ######## MuTau #########
        ########## Errors ##########
        Eff_SL_mu_Data_Err = "eff_data_tau1_Trg_singleMuUp-eff_data_tau1_Trg_singleMuCentral"
        Eff_SL_mu_MC_Err = "eff_MC_tau1_Trg_singleMuUp-eff_MC_tau1_Trg_singleMuCentral"
        dfBuilder.df = dfBuilder.df.Define("Eff_SL_mu_Data_Err", f"""{Eff_SL_mu_Data_Err}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_SL_mu_MC_Err", f"""{Eff_SL_mu_MC_Err}""")

        Eff_cross_mu_Data_Err = "eff_data_tau1_Trg_mutau_muUp-eff_data_tau1_Trg_mutau_muCentral"
        Eff_cross_mu_MC_Err = "eff_MC_tau1_Trg_mutau_muUp-eff_MC_tau1_Trg_mutau_muCentral"
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_mu_Data_Err", f"""{Eff_cross_mu_Data_Err}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_mu_MC_Err", f"""{Eff_cross_mu_MC_Err}""")

        Err_Data_SL_mu = f"{passSingleMu} * Eff_SL_mu_Data_Err - {passCrossMuTau} * {passSingleMu} * (Eff_cross_mu_Data > Eff_SL_mu_Data) * Eff_SL_mu_Data_Err * Eff_cross_tau_mutau_Data; "
        Err_MC_SL_mu = f"{passSingleMu} * Eff_SL_mu_MC_Err - {passCrossMuTau} * {passSingleMu} * (Eff_cross_mu_MC > Eff_SL_mu_MC) * Eff_SL_mu_MC_Err * Eff_cross_tau_mutau_MC;"

        dfBuilder.df = dfBuilder.df.Define("Err_Data_SL_mu", f"""{Err_Data_SL_mu}""")
        dfBuilder.df = dfBuilder.df.Define("Err_MC_SL_mu", f"""{Err_MC_SL_mu}""")

        ######### Scale factors ########

        Err_Data_expression_cross_mu =  f" - {passCrossMuTau} * {passSingleMu} * (Eff_cross_mu_Data <= Eff_SL_mu_Data) * Eff_cross_mu_Data_Err * Eff_cross_tau_mutau_Data + {passCrossMuTau} * Eff_cross_mu_Data_Err * Eff_cross_tau_mutau_Data; "
        Err_MC_expression_cross_mu =  f" - {passCrossMuTau} * {passSingleMu} * (Eff_cross_mu_MC <= Eff_SL_mu_MC) * Eff_cross_mu_MC_Err * Eff_cross_tau_mutau_MC + {passCrossMuTau} * Eff_cross_mu_MC_Err * Eff_cross_tau_mutau_MC; "


        ######### tau leg #########

        Eff_cross_tau_mutau_Data_Up = "SelectCorrectDM(tau2_decayMode, eff_data_tau2_Trg_mutau_DM0Up, eff_data_tau2_Trg_mutau_DM1Up, eff_data_tau2_Trg_mutau_3ProngUp)"
        Eff_cross_tau_mutau_MC_Up = "SelectCorrectDM(tau2_decayMode, eff_MC_tau2_Trg_mutau_DM0Up, eff_MC_tau2_Trg_mutau_DM1Up, eff_MC_tau2_Trg_mutau_3ProngUp)"
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_tau_mutau_Data_Up", f"""{Eff_cross_tau_mutau_Data_Up}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_tau_mutau_MC_Up", f"""{Eff_cross_tau_mutau_MC_Up}""")

        Eff_Data_mutau_tau_Up = f"{passSingleMu} * Eff_SL_mu_Data - {passCrossMuTau} * {passSingleMu} * std::min(Eff_cross_mu_Data, Eff_SL_mu_Data) * Eff_cross_tau_mutau_Data_Up   + {passCrossMuTau} * Eff_cross_mu_Data * Eff_cross_tau_mutau_Data_Up;"
        Eff_MC_mutau_tau_Up   = f"{passSingleMu} * Eff_SL_mu_MC   - {passCrossMuTau} * {passSingleMu} * std::min(Eff_cross_mu_MC  , Eff_SL_mu_MC)   * Eff_cross_tau_mutau_MC_Up     + {passCrossMuTau} * Eff_cross_mu_MC   * Eff_cross_tau_mutau_MC_Up;"
        dfBuilder.df = dfBuilder.df.Define("Eff_Data_mutau_tau_Up", f"""{Eff_Data_mutau_tau_Up}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_MC_mutau_tau_Up", f"""{Eff_MC_mutau_tau_Up}""")

        Eff_cross_tau_mutau_Data_Err = "(Eff_Data_mutau_tau_Up - Eff_Data_mutau)"
        Eff_cross_tau_mutau_MC_Err = "(Eff_MC_mutau_tau_Up - Eff_MC_mutau)"

        dfBuilder.df = dfBuilder.df.Define("Err_Data_cross_mu", f"""{Err_Data_expression_cross_mu}""")
        dfBuilder.df = dfBuilder.df.Define("Err_MC_cross_mu", f"""{Err_MC_expression_cross_mu}""")
        Err_Data_mutau_mu = "(Err_Data_SL_mu + Err_Data_cross_mu)"
        Err_MC_mutau_mu   = "(Err_MC_SL_mu   + Err_MC_cross_mu)"
        dfBuilder.df = dfBuilder.df.Define("Err_Data_mutau_mu", f"""{Err_Data_mutau_mu}""")
        dfBuilder.df = dfBuilder.df.Define("Err_MC_mutau_mu", f"""{Err_MC_mutau_mu}""")
        if dfBuilder.period == 'Run2_2016' or dfBuilder.period == 'Run2_2016_HIPM':
            Err_Data_ele = "Err_Data_SL_mu"
            Err_MC_ele   = "Err_MC_SL_mu"

        if dfBuilder.period == 'Run2_2016' or dfBuilder.period == 'Run2_2016_HIPM':
            Err_Data_ele = "Err_Data_SL_mu"
            Err_MC_ele   = "Err_MC_SL_mu"
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_tau_mutau_Data_Err", f"""{Eff_cross_tau_mutau_Data_Err}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_tau_mutau_MC_Err", f"""{Eff_cross_tau_mutau_MC_Err}""")
        dfBuilder.df = dfBuilder.df.Define("trigSF_SL_mu_err", f"""get_scale_factor_error(Eff_Data_mutau, Eff_MC_mutau, Err_Data_SL_mu, Err_MC_SL_mu,"trigSF_SL_mu_err")""")
        dfBuilder.df = dfBuilder.df.Define("trigSF_cross_mu_err", f"""get_scale_factor_error(Eff_Data_mutau, Eff_MC_mutau, Err_Data_cross_mu, Err_MC_cross_mu,"trigSF_cross_mu_err")""")
        dfBuilder.df = dfBuilder.df.Define("trigSF_tau_mutau_err", f"""get_scale_factor_error(Eff_Data_mutau, Eff_MC_mutau, Eff_cross_tau_mutau_Data_Err, Eff_cross_tau_mutau_MC_Err,"trigSF_tau_mutau_err")""")
        dfBuilder.df = dfBuilder.df.Define("trigSF_mu_err", f"""get_scale_factor_error(Eff_Data_mutau, Eff_MC_mutau, Err_Data_mutau_mu, Err_MC_mutau_mu,"trigSF_mu_err")""")
        dfBuilder.df = dfBuilder.df.Define("muTau_trigSF_tauUp", "weight_trigSF_muTau+trigSF_tau_mutau_err") # questo ci importa
        dfBuilder.df = dfBuilder.df.Define("muTau_trigSF_tauDown", "weight_trigSF_muTau-trigSF_tau_mutau_err") # questo ci importa
        dfBuilder.df = dfBuilder.df.Define("trigSF_muUp", "weight_trigSF_muTau+trigSF_mu_err") # questo ci importa
        dfBuilder.df = dfBuilder.df.Define("trigSF_muDown", "weight_trigSF_muTau-trigSF_mu_err") # questo ci importa
        dfBuilder.df = dfBuilder.df.Define("trigSF_SL_muUp", "weight_trigSF_muTau+trigSF_SL_mu_err") # questo ci importa
        dfBuilder.df = dfBuilder.df.Define("trigSF_SL_muDown", "weight_trigSF_muTau-trigSF_SL_mu_err") # questo ci importa
        dfBuilder.df = dfBuilder.df.Define("trigSF_cross_muUp", "weight_trigSF_muTau+trigSF_cross_mu_err") # questo ci importa
        dfBuilder.df = dfBuilder.df.Define("trigSF_cross_muDown", "weight_trigSF_muTau-trigSF_cross_mu_err") # questo ci importa
        for scale in ['Up','Down']:
            dfBuilder.df = dfBuilder.df.Define(f"trigSF_mu{scale}_rel", f"if ((HLT_singleMu || HLT_mutau) && Legacy_region ) {{return trigSF_mu{scale}/weight_trigSF_muTau;}} return 1.f; ")
            dfBuilder.df = dfBuilder.df.Define(f"trigSF_SL_mu{scale}_rel", f"if ((HLT_singleMu || HLT_mutau) && Legacy_region ) {{return trigSF_SL_mu{scale}/weight_trigSF_muTau;}} return 1.f; ")
            dfBuilder.df = dfBuilder.df.Define(f"trigSF_cross_mu{scale}_rel", f"if ((HLT_singleMu || HLT_mutau) && Legacy_region ) {{return trigSF_cross_mu{scale}/weight_trigSF_muTau;}} return 1.f; ")
            dfBuilder.df = dfBuilder.df.Define(f"muTau_trigSF_tau{scale}_rel", f"if ((HLT_singleMu || HLT_mutau) && Legacy_region ) {{return muTau_trigSF_tau{scale}/weight_trigSF_muTau;}} return 1.f; ")


    if 'eTau' in dfBuilder.config['channels_to_consider']:
        ######## EleTau #########
        ########## Errors ##########
        Eff_SL_ele_Data_Err = "eff_data_tau1_Trg_singleMuUp-eff_data_tau1_Trg_singleMuCentral"
        Eff_SL_ele_MC_Err = "eff_MC_tau1_Trg_singleMuUp-eff_MC_tau1_Trg_singleMuCentral"
        dfBuilder.df = dfBuilder.df.Define("Eff_SL_ele_Data_Err", f"""{Eff_SL_ele_Data_Err}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_SL_ele_MC_Err", f"""{Eff_SL_ele_MC_Err}""")

        Eff_cross_ele_Data_Err = "eff_data_tau1_Trg_etau_eleUp-eff_data_tau1_Trg_etau_eleCentral"
        Eff_cross_ele_MC_Err = "eff_MC_tau1_Trg_etau_eleUp-eff_MC_tau1_Trg_etau_eleCentral"
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_ele_Data_Err", f"""{Eff_cross_ele_Data_Err}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_ele_MC_Err", f"""{Eff_cross_ele_MC_Err}""")

        Err_Data_SL_ele = f"{passSingleMu} * Eff_SL_ele_Data_Err - {passCrossEleTau} * {passSingleMu} * (Eff_cross_ele_Data > Eff_SL_ele_Data) * Eff_SL_ele_Data_Err * Eff_cross_tau_etau_Data; "
        Err_MC_SL_ele = f"{passSingleMu} * Eff_SL_ele_MC_Err - {passCrossEleTau} * {passSingleMu} * (Eff_cross_ele_MC > Eff_SL_ele_MC) * Eff_SL_ele_MC_Err * Eff_cross_tau_etau_MC;"

        dfBuilder.df = dfBuilder.df.Define("Err_Data_SL_ele", f"""{Err_Data_SL_ele}""")
        dfBuilder.df = dfBuilder.df.Define("Err_MC_SL_ele", f"""{Err_MC_SL_ele}""")

        ######### Scale factors ########

        Err_Data_expression_cross_ele =  f" - {passCrossEleTau} * {passSingleMu} * (Eff_cross_ele_Data <= Eff_SL_ele_Data) * Eff_cross_ele_Data_Err * Eff_cross_tau_etau_Data + {passCrossEleTau} * Eff_cross_ele_Data_Err * Eff_cross_tau_etau_Data; "
        Err_MC_expression_cross_ele =  f" - {passCrossEleTau} * {passSingleMu} * (Eff_cross_ele_MC <= Eff_SL_ele_MC) * Eff_cross_ele_MC_Err * Eff_cross_tau_etau_MC + {passCrossEleTau} * Eff_cross_ele_MC_Err * Eff_cross_tau_etau_MC; "


        ######### tau leg #########

        Eff_cross_tau_etau_Data_Up = "SelectCorrectDM(tau2_decayMode, eff_data_tau2_Trg_etau_DM0Up, eff_data_tau2_Trg_etau_DM1Up, eff_data_tau2_Trg_etau_3ProngUp)"
        Eff_cross_tau_etau_MC_Up = "SelectCorrectDM(tau2_decayMode, eff_MC_tau2_Trg_etau_DM0Up, eff_MC_tau2_Trg_etau_DM1Up, eff_MC_tau2_Trg_etau_3ProngUp)"
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_tau_etau_Data_Up", f"""{Eff_cross_tau_etau_Data_Up}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_tau_etau_MC_Up", f"""{Eff_cross_tau_etau_MC_Up}""")

        Eff_Data_etau_tau_Up = f"{passSingleMu} * Eff_SL_ele_Data - {passCrossEleTau} * {passSingleMu} * std::min(Eff_cross_ele_Data, Eff_SL_ele_Data) * Eff_cross_tau_etau_Data_Up   + {passCrossEleTau} * Eff_cross_ele_Data * Eff_cross_tau_etau_Data_Up;"
        Eff_MC_etau_tau_Up   = f"{passSingleMu} * Eff_SL_ele_MC   - {passCrossEleTau} * {passSingleMu} * std::min(Eff_cross_ele_MC  , Eff_SL_ele_MC)   * Eff_cross_tau_etau_MC_Up     + {passCrossEleTau} * Eff_cross_ele_MC   * Eff_cross_tau_etau_MC_Up;"
        dfBuilder.df = dfBuilder.df.Define("Eff_Data_etau_tau_Up", f"""{Eff_Data_etau_tau_Up}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_MC_etau_tau_Up", f"""{Eff_MC_etau_tau_Up}""")

        Eff_cross_tau_etau_Data_Err = "(Eff_Data_etau_tau_Up - Eff_Data_etau)"
        Eff_cross_tau_etau_MC_Err = "(Eff_MC_etau_tau_Up - Eff_MC_etau)"

        dfBuilder.df = dfBuilder.df.Define("Err_Data_cross_ele", f"""{Err_Data_expression_cross_ele}""")
        dfBuilder.df = dfBuilder.df.Define("Err_MC_cross_ele", f"""{Err_MC_expression_cross_ele}""")
        Err_Data_etau_ele = "(Err_Data_SL_ele + Err_Data_cross_ele)"
        Err_MC_etau_ele   = "(Err_MC_SL_ele   + Err_MC_cross_ele)"
        dfBuilder.df = dfBuilder.df.Define("Err_Data_etau_ele", f"""{Err_Data_etau_ele}""")
        dfBuilder.df = dfBuilder.df.Define("Err_MC_etau_ele", f"""{Err_MC_etau_ele}""")
        if dfBuilder.period == 'Run2_2016' or dfBuilder.period == 'Run2_2016_HIPM':
            Err_Data_ele = "Err_Data_SL_ele"
            Err_MC_ele   = "Err_MC_SL_ele"

        if dfBuilder.period == 'Run2_2016' or dfBuilder.period == 'Run2_2016_HIPM':
            Err_Data_ele = "Err_Data_SL_ele"
            Err_MC_ele   = "Err_MC_SL_ele"
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_tau_etau_Data_Err", f"""{Eff_cross_tau_etau_Data_Err}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_tau_etau_MC_Err", f"""{Eff_cross_tau_etau_MC_Err}""")
        dfBuilder.df = dfBuilder.df.Define("trigSF_SL_ele_err", f"""get_scale_factor_error(Eff_Data_etau, Eff_MC_etau, Err_Data_SL_ele, Err_MC_SL_ele,"trigSF_SL_ele_err")""")
        dfBuilder.df = dfBuilder.df.Define("trigSF_cross_ele_err", f"""get_scale_factor_error(Eff_Data_etau, Eff_MC_etau, Err_Data_cross_ele, Err_MC_cross_ele,"trigSF_cross_ele_err")""")
        dfBuilder.df = dfBuilder.df.Define("trigSF_tau_etau_err", f"""get_scale_factor_error(Eff_Data_etau, Eff_MC_etau, Eff_cross_tau_etau_Data_Err, Eff_cross_tau_etau_MC_Err,"trigSF_tau_etau_err")""")
        dfBuilder.df = dfBuilder.df.Define("trigSF_ele_err", f"""get_scale_factor_error(Eff_Data_etau, Eff_MC_etau, Err_Data_etau_ele, Err_MC_etau_ele,"trigSF_ele_err")""")
        dfBuilder.df = dfBuilder.df.Define("eTau_trigSF_tauUp", "weight_trigSF_eTau+trigSF_tau_etau_err") # questo ci importa
        dfBuilder.df = dfBuilder.df.Define("eTau_trigSF_tauDown", "weight_trigSF_eTau-trigSF_tau_etau_err") # questo ci importa
        dfBuilder.df = dfBuilder.df.Define("trigSF_eleUp", "weight_trigSF_eTau+trigSF_ele_err") # questo ci importa
        dfBuilder.df = dfBuilder.df.Define("trigSF_eleDown", "weight_trigSF_eTau-trigSF_ele_err") # questo ci importa
        dfBuilder.df = dfBuilder.df.Define("trigSF_SL_eleUp", "weight_trigSF_eTau+trigSF_SL_ele_err") # questo ci importa
        dfBuilder.df = dfBuilder.df.Define("trigSF_SL_eleDown", "weight_trigSF_eTau-trigSF_SL_ele_err") # questo ci importa
        dfBuilder.df = dfBuilder.df.Define("trigSF_cross_eleUp", "weight_trigSF_eTau+trigSF_cross_ele_err") # questo ci importa
        dfBuilder.df = dfBuilder.df.Define("trigSF_cross_eleDown", "weight_trigSF_eTau-trigSF_cross_ele_err") # questo ci importa
        for scale in ['Up','Down']:
            dfBuilder.df = dfBuilder.df.Define(f"trigSF_ele{scale}_rel", f"if ((HLT_singleMu || HLT_etau) && Legacy_region ) {{return trigSF_ele{scale}/weight_trigSF_eTau;}} return 1.f; ")
            dfBuilder.df = dfBuilder.df.Define(f"trigSF_SL_ele{scale}_rel", f"if ((HLT_singleMu || HLT_etau) && Legacy_region ) {{return trigSF_SL_ele{scale}/weight_trigSF_eTau;}} return 1.f; ")
            dfBuilder.df = dfBuilder.df.Define(f"trigSF_cross_ele{scale}_rel", f"if ((HLT_singleMu || HLT_etau) && Legacy_region ) {{return trigSF_cross_ele{scale}/weight_trigSF_eTau;}} return 1.f; ")
            dfBuilder.df = dfBuilder.df.Define(f"eTau_trigSF_tau{scale}_rel", f"if ((HLT_singleMu || HLT_etau) && Legacy_region ) {{return eTau_trigSF_tau{scale}/weight_trigSF_eTau;}} return 1.f; ")


    for scale in ['Up', 'Down']:
        ### diTau - for tauTau ###
        dfBuilder.df = dfBuilder.df.Define(
            f"tauTau_trigSF_tau{scale}_rel",
            f"if (HLT_ditau && Legacy_region && tauTau) {{return (SelectCorrectDM(tau1_decayMode, weight_tau1_TrgSF_ditau_DM0{scale}_rel, weight_tau1_TrgSF_ditau_DM1{scale}_rel, weight_tau1_TrgSF_ditau_3Prong{scale}_rel)*SelectCorrectDM(tau2_decayMode, weight_tau2_TrgSF_ditau_DM0{scale}_rel, weight_tau2_TrgSF_ditau_DM1{scale}_rel, weight_tau2_TrgSF_ditau_3Prong{scale}_rel)); }}return 1.f;"
        )
            ### singleTau ###
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_singleTau{scale}_rel", f"""if (HLT_singleTau && (tauTau ) && SingleTau_region && !(Legacy_region)) {{return getCorrectSingleLepWeight(tau1_pt, tau1_eta, tau1_HasMatching_singleTau, weight_tau1_TrgSF_singleTau{scale}_rel,tau2_pt, tau2_eta, tau2_HasMatching_singleTau, weight_tau2_TrgSF_singleTau{scale}_rel); }} else if (HLT_singleTau && (eTau || muTau ) && SingleTau_region && !(Legacy_region)) {{return weight_tau2_TrgSF_singleTau{scale}_rel;}} ;return 1.f;""")
        ### MET ###
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_MET{scale}_rel", f"if(HLT_MET && !(SingleTau_region) && !(Legacy_region)) {{return weight_TrgSF_MET{scale}_rel;}} return 1.f;")
        #### final tau trig sf #####
        dfBuilder.df = dfBuilder.df.Define(f"trigSF_tau{scale}_rel", f"""if (Legacy_region && eTau){{return eTau_trigSF_tau{scale}_rel;}} else if (Legacy_region && muTau){{return muTau_trigSF_tau{scale}_rel;}} else if(Legacy_region && tauTau){{return tauTau_trigSF_tau{scale}_rel;}} return 1.f;""")
        # ### singleEle only - for eE ###
        # dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_singleEle{scale}_rel", f"""(HLT_singleEle && SingleEle_region && eE) {{return getCorrectSingleLepWeight(tau1_pt, tau1_eta, tau1_HasMatching_singleEle, weight_tau1_TrgSF_singleEle{scale}_rel,tau2_pt, tau2_eta, tau2_HasMatching_singleEle, weight_tau2_TrgSF_singleEle{scale}_rel);}} return 1.f;""")
        # dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_singleMu{scale}_rel", f"""if (HLT_singleMu && SingleMu_region && muMu) {{return getCorrectSingleLepWeight(tau1_pt, tau1_eta, tau1_HasMatching_singleMu, weight_tau1_TrgSF_singleMu{scale}_rel,tau2_pt, tau2_eta, tau2_HasMatching_singleMu, weight_tau2_TrgSF_singleMu{scale}_rel) ;}} return 1.f;""")
        # dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_eMu{scale}_rel", f"""if (((HLT_singleMu && SingleMu_region) || (HLT_singleEle && SingleEle_region)) && weight_tau1_TrgSF_singleEle{scale}_rel!=1.f && eMu) {{return (weight_tau1_TrgSF_singleEle{scale}_rel*weight_tau2_TrgSF_singleMu{scale}_rel);}} return 1.f;""")
        # finally, comulate tau weights errors

def defineTriggerWeights(dfBuilder): # needs application region def
    passSingleMu = "SingleMu_region"
    passCrossMuTau = "CrossMuTau_region"
    passSingleEle = "SingleEle_region"
    passCrossEleTau = "CrossEleTau_region"
    if 'muTau' in dfBuilder.config['channels_to_consider']:
        ######## MuTau ##########
        ####### Efficiencies ######
        Eff_SL_mu_Data = "eff_data_tau1_Trg_singleMuCentral"
        Eff_SL_mu_MC = "eff_MC_tau1_Trg_singleMuCentral"
        dfBuilder.df = dfBuilder.df.Define("Eff_SL_mu_Data", f"""{Eff_SL_mu_Data}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_SL_mu_MC", f"""{Eff_SL_mu_MC}""")

        Eff_cross_mu_Data = "eff_data_tau1_Trg_mutau_muCentral"
        Eff_cross_mu_MC = "eff_MC_tau1_Trg_mutau_muCentral"
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_mu_Data", f"""{Eff_cross_mu_Data}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_mu_MC", f"""{Eff_cross_mu_MC}""")

        Eff_cross_tau_mutau_Data = "SelectCorrectDM(tau2_decayMode, eff_data_tau2_Trg_mutau_DM0Central, eff_data_tau2_Trg_mutau_DM1Central, eff_data_tau2_Trg_mutau_3ProngCentral)"
        Eff_cross_tau_mutau_MC = "SelectCorrectDM(tau2_decayMode, eff_MC_tau2_Trg_mutau_DM0Central, eff_MC_tau2_Trg_mutau_DM1Central, eff_MC_tau2_Trg_mutau_3ProngCentral)"
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_tau_mutau_Data", f"""{Eff_cross_tau_mutau_Data}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_tau_mutau_MC", f"""{Eff_cross_tau_mutau_MC}""")

        Eff_Data_expression_mutau = (
                    f"{passSingleMu} * {Eff_SL_mu_Data} - {passCrossMuTau} * {passSingleMu} * std::min({Eff_cross_mu_Data}, {Eff_SL_mu_Data}) * {Eff_cross_tau_mutau_Data} + {passCrossMuTau} * {Eff_cross_mu_Data} * {Eff_cross_tau_mutau_Data};"
                )
        Eff_MC_expression_mutau = (
            f"{passSingleMu} * {Eff_SL_mu_MC} - {passCrossMuTau} * {passSingleMu} * std::min({Eff_cross_mu_MC}, {Eff_SL_mu_MC}) * {Eff_cross_tau_mutau_MC} + {passCrossMuTau} * {Eff_cross_mu_MC} * {Eff_cross_tau_mutau_MC};"
        )
        dfBuilder.df = dfBuilder.df.Define("Eff_Data_mutau", f"""{Eff_Data_expression_mutau}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_MC_mutau", f"""{Eff_MC_expression_mutau}""")
        weight_muTau_expression = "if ( (HLT_singleMu || HLT_mutau) && Legacy_region && Eff_MC_mutau!=0) {return static_cast<float>(Eff_Data_mutau/Eff_MC_mutau);} return 1.f;"
        if dfBuilder.period == 'Run2_2016' or dfBuilder.period == 'Run2_2016_HIPM':
            weight_muTau_expression = "if (HLT_singleMu && SingleMu_region) {return (weight_tau1_TrgSF_singleMuCentral ) ;} return 1.f; "
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_muTau", weight_muTau_expression)

    if 'eTau' in dfBuilder.config['channels_to_consider']:
        ######## EleTau ##########
        ####### Efficiencies ######
        Eff_SL_ele_Data = "eff_data_tau1_Trg_singleEleCentral"
        Eff_SL_ele_MC = "eff_MC_tau1_Trg_singleEleCentral"
        dfBuilder.df = dfBuilder.df.Define("Eff_SL_ele_Data", f"""{Eff_SL_ele_Data}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_SL_ele_MC", f"""{Eff_SL_ele_MC}""")

        Eff_cross_ele_Data = "eff_data_tau1_Trg_etau_eleCentral"
        Eff_cross_ele_MC = "eff_MC_tau1_Trg_etau_eleCentral"
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_ele_Data", f"""{Eff_cross_ele_Data}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_ele_MC", f"""{Eff_cross_ele_MC}""")

        Eff_cross_tau_etau_Data = "SelectCorrectDM(tau2_decayMode, eff_data_tau2_Trg_etau_DM0Central, eff_data_tau2_Trg_etau_DM1Central, eff_data_tau2_Trg_etau_3ProngCentral)"
        Eff_cross_tau_etau_MC = "SelectCorrectDM(tau2_decayMode, eff_MC_tau2_Trg_etau_DM0Central, eff_MC_tau2_Trg_etau_DM1Central, eff_MC_tau2_Trg_etau_3ProngCentral)"
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_tau_etau_Data", f"""{Eff_cross_tau_etau_Data}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_cross_tau_etau_MC", f"""{Eff_cross_tau_etau_MC}""")

        Eff_Data_expression_etau = (
                    f"{passSingleEle} * {Eff_SL_ele_Data} - {passCrossEleTau} * {passSingleEle} * std::min({Eff_cross_ele_Data}, {Eff_SL_ele_Data}) * {Eff_cross_tau_etau_Data} + {passCrossEleTau} * {Eff_cross_ele_Data} * {Eff_cross_tau_etau_Data};"
                )
        Eff_MC_expression_etau = (
            f"{passSingleEle} * {Eff_SL_ele_MC} - {passCrossEleTau} * {passSingleEle} * std::min({Eff_cross_ele_MC}, {Eff_SL_ele_MC}) * {Eff_cross_tau_etau_MC} + {passCrossEleTau} * {Eff_cross_ele_MC} * {Eff_cross_tau_etau_MC};"
        )
        dfBuilder.df = dfBuilder.df.Define("Eff_Data_etau", f"""{Eff_Data_expression_etau}""")
        dfBuilder.df = dfBuilder.df.Define("Eff_MC_etau", f"""{Eff_MC_expression_etau}""")
        weight_eTau_expression = "if ( (HLT_singleEle || HLT_etau) && Legacy_region && Eff_MC_etau!=0) {return static_cast<float>(Eff_Data_etau/Eff_MC_etau);} return 1.f;"
        if dfBuilder.period == 'Run2_2016' or dfBuilder.period == 'Run2_2016_HIPM':
            weight_eTau_expression = "if (HLT_singleEle && SingleEle_region) {return (weight_tau1_TrgSF_singleEleCentral ) ;} return 1.f; "
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_eTau", weight_eTau_expression)

    # *********************** tauTau ***********************
    if 'tauTau' in dfBuilder.config['channels_to_consider']:
        dfBuilder.df = dfBuilder.df.Define(
                    f"weight_trigSF_diTau",
                    f"if (HLT_ditau && Legacy_region && tauTau) {{return (SelectCorrectDM(tau1_decayMode, weight_tau1_TrgSF_ditau_DM0Central, weight_tau1_TrgSF_ditau_DM1Central, weight_tau1_TrgSF_ditau_3ProngCentral)*SelectCorrectDM(tau2_decayMode, weight_tau2_TrgSF_ditau_DM0Central, weight_tau2_TrgSF_ditau_DM1Central, weight_tau2_TrgSF_ditau_3ProngCentral)); }}return 1.f;"
                )
    # *********************** singleTau ***********************

    # dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_singleTau", f"if  (HLT_singleTau && (tauTau || eTau || muTau ) && SingleTau_region && !(Legacy_region)) {{return getCorrectSingleLepWeight(tau1_pt, tau1_eta, tau1_HasMatching_singleTau, weight_tau1_TrgSF_singleTauCentral,tau2_pt, tau2_eta, tau2_HasMatching_singleTau, weight_tau2_TrgSF_singleTauCentral);}} return 1.f;")
    # if 'tauTau' in dfBuilder.config['channels_to_consider'] or 'muTau' in dfBuilder.config['channels_to_consider'] or 'eTau' in dfBuilder.config['channels_to_consider']  :
    dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_singleTau", f"""if (HLT_singleTau && (tauTau ) && SingleTau_region && !(Legacy_region)) {{return getCorrectSingleLepWeight(tau1_pt, tau1_eta, tau1_HasMatching_singleTau, weight_tau1_TrgSF_singleTauCentral,tau2_pt, tau2_eta, tau2_HasMatching_singleTau, weight_tau2_TrgSF_singleTauCentral); }} else if (HLT_singleTau && (eTau || muTau ) && SingleTau_region && !(Legacy_region)) {{return weight_tau2_TrgSF_singleTauCentral;}} ;return 1.f;""")
    # *********************** MET ***********************
    # if 'tauTau' in dfBuilder.config['channels_to_consider'] or 'muTau' in dfBuilder.config['channels_to_consider'] or 'eTau' in dfBuilder.config['channels_to_consider']  :
    dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_MET", "if (HLT_MET && (tauTau || eTau || muTau ) && !(SingleTau_region) && !(Legacy_region)) { return (weight_TrgSF_METCentral) ;} return 1.f;")
    # *********************** singleEle ***********************
    # dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_singleEle", "if (HLT_singleEle && SingleEle_region) {return weight_tau1_TrgSF_singleEleCentral*weight_tau2_TrgSF_singleEleCentral ;} return 1.f;")
    # if 'eE' in dfBuilder.config['channels_to_consider']  :
    dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_singleEle", "if (HLT_singleEle && SingleEle_region && eE) {return getCorrectSingleLepWeight(tau1_pt, tau1_eta, tau1_HasMatching_singleEle, weight_tau1_TrgSF_singleEleCentral,tau2_pt, tau2_eta, tau2_HasMatching_singleEle, weight_tau2_TrgSF_singleEleCentral) ;} return 1.f;")
    # *********************** singleMu ***********************
    # dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_singleMu", "if (HLT_singleMu && SingleMu_region) {return weight_tau1_TrgSF_singleMuCentral*weight_tau2_TrgSF_singleMuCentral ;} return 1.f;")
    # if 'muMu' in dfBuilder.config['channels_to_consider']  :
    dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_singleMu", "if (HLT_singleMu && SingleMu_region && muMu) {return getCorrectSingleLepWeight(tau1_pt, tau1_eta, tau1_HasMatching_singleMu, weight_tau1_TrgSF_singleMuCentral,tau2_pt, tau2_eta, tau2_HasMatching_singleMu, weight_tau2_TrgSF_singleMuCentral) ;} return 1.f;")
    # *********************** singleLepPerEMu ***********************
    # if 'eMu' in dfBuilder.config['channels_to_consider']  :
    dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_eMu", "if (((HLT_singleMu && SingleMu_region) || (HLT_singleEle && SingleEle_region)) && weight_tau1_TrgSF_singleEleCentral!=1.f && eMu) {return (weight_tau1_TrgSF_singleEleCentral*weight_tau2_TrgSF_singleMuCentral);} return 1.f;")
        #dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_muE", f"if (((HLT_singleMu && SingleMu_region) || (HLT_singleEle && SingleEle_region)) && weight_tau2_TrgSF_singleEleCentral!=1.f) return (weight_tau2_TrgSF_singleEleCentral*weight_tau1_TrgSF_singleMuCentral); return 1.f;")

def AddTriggerWeightsAndErrors(dfBuilder,WantErrors):
    defineTriggerWeights(dfBuilder)
    if WantErrors:
        defineTriggerWeightsErrors(dfBuilder)