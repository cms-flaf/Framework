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

    float SelectCorrectEfficiency(const float& tauDM, const float& effDM0, const float& effDM1, const float& eff3Prong){
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
    # must pass SingleMu_region - Mu leg
    Eff_SL_mu_Data = "eff_data_tau1_Trg_singleMuCentral"
    Eff_SL_mu_MC = "eff_MC_tau1_Trg_singleMuCentral"

    # must pass CrossMuTau_region - Mu leg
    Eff_cross_mu_Data = "eff_data_tau1_Trg_mutau_muCentral"
    Eff_cross_mu_MC = "eff_MC_tau1_Trg_mutau_muCentral"


    # must pass CrossMuTau_region - Tau leg
    Eff_cross_tau_Data = "eff_data_tau2_Trg_mutau_3ProngCentral*eff_data_tau2_Trg_mutau_DM0Central*eff_data_tau2_Trg_mutau_DM1Central"
    Eff_cross_tau_MC = "eff_MC_tau2_Trg_mutau_3ProngCentral*eff_MC_tau2_Trg_mutau_DM0Central*eff_MC_tau2_Trg_mutau_DM1Central"

    # ************************************

    Eff_SL_ele_Data = "eff_data_tau1_Trg_singleEleCentral"
    Eff_SL_ele_MC = "eff_MC_tau1_Trg_singleEleCentral"

    # must pass CrossEleTau_region - Ele leg
    Eff_cross_ele_Data = "eff_data_tau1_Trg_etau_eleCentral"
    Eff_cross_ele_MC = "eff_MC_tau1_Trg_etau_eleCentral"


    for scale in ['Up', 'Down']:
        dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_ditau_DM0{scale}_rel", f"if (HLT_ditau && Legacy_region) {{return (weight_tau1_TrgSF_ditau_DM0{scale}_rel*weight_tau2_TrgSF_ditau_DM0{scale}_rel); }}return 1.f;")
        dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_ditau_DM1{scale}_rel", f"if (HLT_ditau && Legacy_region) {{return (weight_tau1_TrgSF_ditau_DM1{scale}_rel*weight_tau2_TrgSF_ditau_DM1{scale}_rel); }}return 1.f;")
        dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_ditau_3Prong{scale}_rel", f"if (HLT_ditau && Legacy_region) {{return (weight_tau1_TrgSF_ditau_3Prong{scale}_rel*weight_tau2_TrgSF_ditau_3Prong{scale}_rel); }}return 1.f;")
        dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_singleTau_{scale}_rel", f"if (HLT_singleTau && SingleTau_region && !Legacy_region) {{return (weight_tau1_TrgSF_singleTau{scale}_rel*weight_tau1_TrgSF_singleTau{scale}_rel) ;}} return 1.f;")
        dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_MET{scale}_rel", f"if(HLT_MET && !(SingleTau_region) && !(Legacy_region)) {{return weight_TrgSF_MET{scale}_rel;}} return 1.f;")
        dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_singleEle{scale}_rel", f"if (HLT_singleEle && SingleEle_region) {{return (weight_tau1_TrgSF_singleEle{scale}_rel*weight_tau2_TrgSF_singleEle{scale}_rel);}} return 1.f;")
        dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_singleMu{scale}_rel", f"if (HLT_singleMu && SingleMu_region) {{return (weight_tau1_TrgSF_singleMu{scale}_rel*weight_tau2_TrgSF_singleMu{scale}_rel);}} return 1.f;")
        dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_eMu{scale}_rel", f"if (((HLT_singleMu && SingleMu_region) || (HLT_singleEle && SingleEle_region)) && weight_tau1_TrgSF_singleEleCentral!=1.f) {{return (weight_tau1_TrgSF_singleEle{scale}_rel*weight_tau2_TrgSF_singleMu{scale}_rel);}} return 1.f;")

    Eff_SL_mu_Data_Err = "(eff_data_tau2_Trg_singleMuUp-eff_data_tau1_Trg_singleMuCentral)"
    Eff_SL_mu_MC_Err = "(eff_MC_tau2_Trg_singleMuUp-eff_MC_tau1_Trg_singleMuCentral)"
    Err_Data_SL_mu = f"{passSingleMu} * {Eff_SL_mu_Data_Err} - {passCrossMuTau} * {passSingleMu} * ({Eff_cross_mu_Data} > {Eff_SL_mu_Data}) * {Eff_SL_mu_Data_Err} * {Eff_cross_tau_Data}"
    Err_MC_SL_mu = f"{passSingleMu} * {Eff_SL_mu_MC_Err} - {passCrossMuTau} * {passSingleMu} * ({Eff_cross_mu_MC} > {Eff_SL_mu_MC}) * {Eff_SL_mu_MC_Err} * {Eff_cross_tau_MC}"
    dfBuilder.df = dfBuilder.df.Define(f"Err_Data_SL_mu", Err_Data_SL_mu)
    dfBuilder.df = dfBuilder.df.Define(f"Err_MC_SL_mu", Err_MC_SL_mu)
    dfBuilder.df = dfBuilder.df.Define("trigSF_SL_mu_err" , """get_scale_factor_error(Eff_Data_mutau, Eff_MC_mutau, Err_Data_SL_mu, Err_MC_SL_mu, "trigSF_SL_mu_err")""")

    Eff_cross_mu_Data_Err = "(eff_data_tau1_Trg_mutau_muUp - eff_data_tau1_Trg_mutau_muCentral)"
    Eff_cross_mu_MC_Err = "(eff_MC_tau1_Trg_mutau_muUp - eff_MC_tau1_Trg_mutau_muCentral)"
    Err_Data_cross_mu = f"- {passCrossMuTau} * {passSingleMu} * ({Eff_cross_mu_Data} <= {Eff_SL_mu_Data}) * {Eff_cross_mu_Data_Err} * {Eff_cross_tau_Data} + {passCrossMuTau} * {Eff_cross_mu_Data_Err} * {Eff_cross_tau_Data};"
    Err_MC_cross_mu = f"- {passCrossMuTau} * {passSingleMu} * ({Eff_cross_mu_MC} <= {Eff_SL_mu_MC}) * {Eff_cross_mu_MC_Err} * {Eff_cross_tau_MC} + {passCrossMuTau} * {Eff_cross_mu_MC_Err} * {Eff_cross_tau_MC};"
    dfBuilder.df = dfBuilder.df.Define(f"Err_Data_cross_mu", Err_Data_cross_mu)
    dfBuilder.df = dfBuilder.df.Define(f"Err_MC_cross_mu", Err_MC_cross_mu)
    dfBuilder.df = dfBuilder.df.Define(f"trigSF_cross_mu_err", """get_scale_factor_error(Eff_Data_mutau, Eff_MC_mutau, Err_Data_cross_mu, Err_MC_cross_mu,"trigSF_cross_mu_err")""")

    dfBuilder.df = dfBuilder.df.Define("Eff_Data_mutau_Up", "SelectCorrectEfficiency(tau2_decayMode, eff_data_tau2_Trg_mutau_DM0Up, eff_data_tau2_Trg_mutau_DM1Up, eff_data_tau2_Trg_mutau_3ProngUp)")

    dfBuilder.df = dfBuilder.df.Define("Eff_MC_mutau_Up", "SelectCorrectEfficiency(tau2_decayMode, eff_MC_tau2_Trg_mutau_DM0Up, eff_MC_tau2_Trg_mutau_DM1Up, eff_MC_tau2_Trg_mutau_3ProngUp)")

    dfBuilder.df = dfBuilder.df.Define("trigSF_err_dm_mutau", """get_scale_factor_error(Eff_Data_mutau, Eff_MC_mutau, Eff_Data_mutau_Up - Eff_Data_mutau, Eff_MC_mutau_Up - Eff_MC_mutau,"trigSF_err_dm_mutau")""")
    Err_Data_mu = "Err_Data_SL_mu + Err_Data_cross_mu"
    Err_MC_mu   = "Err_MC_SL_mu   + Err_MC_cross_mu"
    if dfBuilder.period == 'Run2_2016' or dfBuilder.period == 'Run2_2016_HIPM':
        Err_Data_mu = "Err_Data_SL_mu"
        Err_MC_mu   = "Err_MC_SL_mu"
    dfBuilder.df = dfBuilder.df.Define("Err_Data_mu", Err_Data_mu)
    dfBuilder.df = dfBuilder.df.Define("Err_MC_mu", Err_MC_mu)
    dfBuilder.df = dfBuilder.df.Define("trigSF_mu_err", """get_scale_factor_error(Eff_Data_mutau, Eff_MC_mutau, Err_Data_mu, Err_MC_mu,"trigSF_mu_err")""")

    if dfBuilder.period == 'Run2_2016' or dfBuilder.period == 'Run2_2016_HIPM':
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_cross_mu_Up", " 1.f ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_cross_mu_Down", " 1.f ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_SL_mu_Up", "if ((HLT_singleMu) && Legacy_region ) {return weight_tau1_TrgSF_singleMuUp_rel*weight_tau1_TrgSF_singleMuCentral;} return 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_SL_mu_Down", "if ((HLT_singleMu) && Legacy_region ) {return weight_tau1_TrgSF_singleMuUp_rel*weight_tau1_TrgSF_singleMuCentral;} return 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_mu_Up", " 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_mu_Down", " 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_mutau_tau_Up", "  1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_mutau_tau_Down", "  1.f; ")
    else:
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_cross_mu_Up", "if ((HLT_singleMu || HLT_mutau) && Legacy_region && Eff_MC_mutau!=0) {return weight_HLT_muTau + trigSF_cross_mu_err;} return 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_cross_mu_Down", "if ((HLT_singleMu || HLT_mutau) && Legacy_region && Eff_MC_mutau!=0) {return weight_HLT_muTau - trigSF_cross_mu_err;} return 1.f; ")

        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_SL_mu_Up", "if ((HLT_singleMu || HLT_mutau) && Legacy_region && Eff_MC_mutau!=0) {return weight_HLT_muTau + trigSF_SL_mu_err;} return 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_SL_mu_Down", "if ((HLT_singleMu || HLT_mutau) && Legacy_region && Eff_MC_mutau!=0) {return weight_HLT_muTau - trigSF_SL_mu_err;} return 1.f; ")

        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_mu_Up", "if ((HLT_singleMu || HLT_mutau) && Legacy_region && Eff_MC_mutau!=0) {return weight_HLT_muTau + trigSF_mu_err;} return 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_mu_Down", "if ((HLT_singleMu || HLT_mutau) && Legacy_region && Eff_MC_mutau!=0) {return weight_HLT_muTau - trigSF_mu_err;} return 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_mutau_tau_Up", "if ((HLT_singleMu || HLT_mutau) && Legacy_region && Eff_MC_mutau!=0) {return weight_HLT_muTau + trigSF_err_dm_mutau;} return 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_mutau_tau_Down", "if ((HLT_singleMu || HLT_mutau) && Legacy_region && Eff_MC_mutau!=0) {return weight_HLT_muTau - trigSF_err_dm_mutau;} return 1.f; ")


    # must pass Crossetau_region - Tau leg

    # must pass CrossEleTau_region - Tau leg
    Eff_cross_tau_Data = "eff_data_tau2_Trg_etau_3ProngCentral*eff_data_tau2_Trg_etau_DM0Central*eff_data_tau2_Trg_etau_DM1Central"
    Eff_cross_tau_MC = "eff_MC_tau2_Trg_etau_3ProngCentral*eff_MC_tau2_Trg_etau_DM0Central*eff_MC_tau2_Trg_etau_DM1Central"

    #Eff_cross_tau_Data = "eff_data_tau2_Trg_etauCentral"
    #Eff_cross_tau_MC = "eff_MC_tau2_Trg_etauCentral"
    Eff_SL_ele_Data_Err = "(eff_data_tau2_Trg_singleEleUp-eff_data_tau1_Trg_singleEleCentral)"
    Eff_SL_ele_MC_Err = "(eff_MC_tau2_Trg_singleEleUp-eff_MC_tau1_Trg_singleEleCentral)"
    Err_Data_SL_ele = f"{passSingleEle} * {Eff_SL_ele_Data_Err} - {passCrossEleTau} * {passSingleEle} * ({Eff_cross_ele_Data} > {Eff_SL_ele_Data}) * {Eff_SL_ele_Data_Err} * {Eff_cross_tau_Data}"
    Err_MC_SL_ele = f"{passSingleEle} * {Eff_SL_ele_MC_Err} - {passCrossEleTau} * {passSingleEle} * ({Eff_cross_ele_MC} > {Eff_SL_ele_MC}) * {Eff_SL_ele_MC_Err} * {Eff_cross_tau_MC}"
    dfBuilder.df = dfBuilder.df.Define(f"Err_Data_SL_ele", Err_Data_SL_ele)
    dfBuilder.df = dfBuilder.df.Define(f"Err_MC_SL_ele", Err_MC_SL_ele)
    dfBuilder.df = dfBuilder.df.Define("trigSF_SL_ele_err" , """get_scale_factor_error(Eff_Data_etau, Eff_MC_etau, Err_Data_SL_ele, Err_MC_SL_ele, "trigSF_SL_ele_err")""")

    Eff_cross_ele_Data_Err = "(eff_data_tau1_Trg_etau_eleUp - eff_data_tau1_Trg_etau_eleCentral)"
    Eff_cross_ele_MC_Err = "(eff_MC_tau1_Trg_etau_eleUp - eff_MC_tau1_Trg_etau_eleCentral)"
    Err_Data_cross_ele = f"- {passCrossEleTau} * {passSingleEle} * ({Eff_cross_ele_Data} <= {Eff_SL_ele_Data}) * {Eff_cross_ele_Data_Err} * {Eff_cross_tau_Data} + {passCrossEleTau} * {Eff_cross_ele_Data_Err} * {Eff_cross_tau_Data};"
    Err_MC_cross_ele = f"- {passCrossEleTau} * {passSingleEle} * ({Eff_cross_ele_MC} <= {Eff_SL_ele_MC}) * {Eff_cross_ele_MC_Err} * {Eff_cross_tau_MC} + {passCrossEleTau} * {Eff_cross_ele_MC_Err} * {Eff_cross_tau_MC};"
    dfBuilder.df = dfBuilder.df.Define(f"Err_Data_cross_ele", Err_Data_cross_ele)
    dfBuilder.df = dfBuilder.df.Define(f"Err_MC_cross_ele", Err_MC_cross_ele)
    dfBuilder.df = dfBuilder.df.Define(f"trigSF_cross_ele_err", """get_scale_factor_error(Eff_Data_etau, Eff_MC_etau, Err_Data_cross_ele, Err_MC_cross_ele,"trigSF_cross_ele_err")""")

    dfBuilder.df = dfBuilder.df.Define("Eff_Data_etau_Up", "SelectCorrectEfficiency(tau2_decayMode, eff_data_tau2_Trg_etau_DM0Up, eff_data_tau2_Trg_etau_DM1Up, eff_data_tau2_Trg_etau_3ProngUp)")

    dfBuilder.df = dfBuilder.df.Define("Eff_MC_etau_Up", "SelectCorrectEfficiency(tau2_decayMode, eff_MC_tau2_Trg_etau_DM0Up, eff_MC_tau2_Trg_etau_DM1Up, eff_MC_tau2_Trg_etau_3ProngUp)")

    dfBuilder.df = dfBuilder.df.Define("trigSF_err_dm_etau", """get_scale_factor_error(Eff_Data_etau, Eff_MC_etau, Eff_Data_etau_Up - Eff_Data_etau, Eff_MC_etau_Up - Eff_MC_etau,"trigSF_err_dm_etau")""")
    Err_Data_ele = "Err_Data_SL_ele + Err_Data_cross_ele"
    Err_MC_ele   = "Err_MC_SL_ele   + Err_MC_cross_ele"
    if dfBuilder.period == 'Run2_2016' or dfBuilder.period == 'Run2_2016_HIPM':
        Err_Data_ele = "Err_Data_SL_ele"
        Err_MC_ele   = "Err_MC_SL_ele"
    dfBuilder.df = dfBuilder.df.Define("Err_Data_ele", Err_Data_ele)
    dfBuilder.df = dfBuilder.df.Define("Err_MC_ele", Err_MC_ele)
    dfBuilder.df = dfBuilder.df.Define("trigSF_ele_err", """get_scale_factor_error(Eff_Data_etau, Eff_MC_etau, Err_Data_ele, Err_MC_ele,"trigSF_ele_err")""")

    if dfBuilder.period == 'Run2_2016' or dfBuilder.period == 'Run2_2016_HIPM':
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_cross_ele_Up", " 1.f ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_cross_ele_Down", " 1.f ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_SL_ele_Up", "if ((HLT_singleEle) && Legacy_region ) {return weight_tau1_TrgSF_singleEleUp_rel*weight_tau1_TrgSF_singleEleCentral;} return 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_SL_ele_Down", "if ((HLT_singleEle) && Legacy_region ) {return weight_tau1_TrgSF_singleEleUp_rel*weight_tau1_TrgSF_singleEleCentral;} return 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_ele_Up", " 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_ele_Down", " 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_etau_tau_Up", "  1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_etau_tau_Down", "  1.f; ")
    else:
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_cross_ele_Up", "if ((HLT_singleEle || HLT_etau) && Legacy_region && Eff_MC_etau!=0) {return weight_HLT_eTau + trigSF_cross_ele_err;} return 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_cross_ele_Down", "if ((HLT_singleEle || HLT_etau) && Legacy_region && Eff_MC_etau!=0) {return weight_HLT_eTau - trigSF_cross_ele_err;} return 1.f; ")

        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_SL_ele_Up", "if ((HLT_singleEle || HLT_etau) && Legacy_region && Eff_MC_etau!=0) {return weight_HLT_eTau + trigSF_SL_ele_err;} return 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_SL_ele_Down", "if ((HLT_singleEle || HLT_etau) && Legacy_region && Eff_MC_etau!=0) {return weight_HLT_eTau - trigSF_SL_ele_err;} return 1.f; ")

        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_ele_Up", "if ((HLT_singleEle || HLT_etau) && Legacy_region && Eff_MC_etau!=0) {return weight_HLT_eTau + trigSF_ele_err;} return 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_ele_Down", "if ((HLT_singleEle || HLT_etau) && Legacy_region && Eff_MC_etau!=0) {return weight_HLT_eTau - trigSF_ele_err;} return 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_etau_tau_Up", "if ((HLT_singleEle || HLT_etau) && Legacy_region && Eff_MC_etau!=0) {return weight_HLT_eTau + trigSF_err_dm_etau;} return 1.f; ")
        dfBuilder.df = dfBuilder.df.Define(f"weight_trigSF_etau_tau_Down", "if ((HLT_singleEle || HLT_etau) && Legacy_region && Eff_MC_etau!=0) {return weight_HLT_eTau - trigSF_err_dm_etau;} return 1.f; ")

def defineTriggerWeights(dfBuilder): # needs application region def

    # *********************** tauTau ***********************
    dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_diTau", "if (HLT_ditau && Legacy_region) {return (weight_tau1_TrgSF_ditau_3ProngCentral*weight_tau1_TrgSF_ditau_DM0Central*weight_tau1_TrgSF_ditau_DM1Central*weight_tau2_TrgSF_ditau_3ProngCentral*weight_tau2_TrgSF_ditau_DM0Central*weight_tau2_TrgSF_ditau_DM1Central); }return 1.f;")
    # *********************** singleTau ***********************
    # dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_singleTau", "if (HLT_singleTau && SingleTau_region && !Legacy_region) {return (weight_tau1_TrgSF_singleTauCentral*weight_tau2_TrgSF_singleTauCentral) ;} return 1.f;")
    dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_singleTau", "if (HLT_singleTau && SingleTau_region && !(Legacy_region)) {return getCorrectSingleLepWeight(tau1_pt, tau1_eta, tau1_HasMatching_singleTau, weight_tau1_TrgSF_singleTauCentral,tau2_pt, tau2_eta, tau2_HasMatching_singleTau, weight_tau2_TrgSF_singleTauCentral); } return 1.f;")
    # *********************** MET ***********************
    dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_MET", "if (HLT_MET && !(SingleTau_region) && !(Legacy_region)) { return (weight_TrgSF_METCentral) ;} return 1.f;")
    # *********************** singleEle ***********************
    # dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_singleEle", "if (HLT_singleEle && SingleEle_region) {return weight_tau1_TrgSF_singleEleCentral*weight_tau2_TrgSF_singleEleCentral ;} return 1.f;")
    dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_singleEle", "if (HLT_singleEle && SingleEle_region) {return getCorrectSingleLepWeight(tau1_pt, tau1_eta, tau1_HasMatching_singleEle, weight_tau1_TrgSF_singleEleCentral,tau2_pt, tau2_eta, tau2_HasMatching_singleEle, weight_tau2_TrgSF_singleEleCentral) ;} return 1.f;")
    # *********************** singleMu ***********************
    # dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_singleMu", "if (HLT_singleMu && SingleMu_region) {return weight_tau1_TrgSF_singleMuCentral*weight_tau2_TrgSF_singleMuCentral ;} return 1.f;")
    dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_singleMu", "if (HLT_singleMu && SingleMu_region) {return getCorrectSingleLepWeight(tau1_pt, tau1_eta, tau1_HasMatching_singleMu, weight_tau1_TrgSF_singleMuCentral,tau2_pt, tau2_eta, tau2_HasMatching_singleMu, weight_tau2_TrgSF_singleMuCentral) ;} return 1.f;")
    # *********************** singleLepPerEMu ***********************
    dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_eMu", "if (((HLT_singleMu && SingleMu_region) || (HLT_singleEle && SingleEle_region)) && weight_tau1_TrgSF_singleEleCentral!=1.f) {return (weight_tau1_TrgSF_singleEleCentral*weight_tau2_TrgSF_singleMuCentral);} return 1.f;")
    #dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_muE", f"if (((HLT_singleMu && SingleMu_region) || (HLT_singleEle && SingleEle_region)) && weight_tau2_TrgSF_singleEleCentral!=1.f) return (weight_tau2_TrgSF_singleEleCentral*weight_tau1_TrgSF_singleMuCentral); return 1.f;")


    # *********************** muTau ***********************
    passSingleLep = "SingleMu_region"
    passCrossLep = "CrossMuTau_region"
    # must pass SingleMu_region - Mu leg
    Eff_SL_mu_Data = "eff_data_tau1_Trg_singleMuCentral"
    Eff_SL_mu_MC = "eff_MC_tau1_Trg_singleMuCentral"

    # must pass CrossMuTau_region - Mu leg
    Eff_cross_mu_Data = "eff_data_tau1_Trg_mutau_muCentral"
    Eff_cross_mu_MC = "eff_MC_tau1_Trg_mutau_muCentral"


    # must pass CrossMuTau_region - Tau leg
    Eff_cross_tau_Data = "eff_data_tau2_Trg_mutau_3ProngCentral*eff_data_tau2_Trg_mutau_DM0Central*eff_data_tau2_Trg_mutau_DM1Central"
    Eff_cross_tau_MC = "eff_MC_tau2_Trg_mutau_3ProngCentral*eff_MC_tau2_Trg_mutau_DM0Central*eff_MC_tau2_Trg_mutau_DM1Central"
    # efficiency expression
    Eff_Data_expression_mu = f"{passSingleLep} * {Eff_SL_mu_Data} - {passCrossLep} * {passSingleLep} * std::min({Eff_cross_mu_Data}, {Eff_SL_mu_Data}) * {Eff_cross_tau_Data} + {passCrossLep} * {Eff_cross_mu_Data} * {Eff_cross_tau_Data};"
    Eff_MC_expression_mu = f"{passSingleLep} * {Eff_SL_mu_MC}   - {passCrossLep} * {passSingleLep} * std::min({Eff_cross_mu_MC}  , {Eff_SL_mu_MC})   * {Eff_cross_tau_MC}   + {passCrossLep} * {Eff_cross_mu_MC} * {Eff_cross_tau_MC};"

    dfBuilder.df = dfBuilder.df.Define(f"Eff_Data_mutau", Eff_Data_expression_mu)
    dfBuilder.df = dfBuilder.df.Define(f"Eff_MC_mutau", Eff_MC_expression_mu)
    weight_muTau_expression = "if ( (HLT_singleMu || HLT_mutau) && Legacy_region && Eff_MC_mutau!=0) {return static_cast<float>(Eff_Data_mutau/Eff_MC_mutau);} return 1.f;"
    if dfBuilder.period == 'Run2_2016' or dfBuilder.period == 'Run2_2016_HIPM':
        weight_muTau_expression = "if (HLT_singleMu && SingleMu_region) {return (weight_tau1_TrgSF_singleMuCentral ) ;} return 1.f; "
    dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_muTau", weight_muTau_expression)


    # *********************** etau ***********************

    passSingleLep = "SingleEle_region"
    passCrossLep = "CrossEleTau_region"
    # must pass SingleEle_region - Mu leg
    Eff_SL_ele_Data = "eff_data_tau1_Trg_singleEleCentral"
    Eff_SL_ele_MC = "eff_MC_tau1_Trg_singleEleCentral"

    # must pass CrossEleTau_region - Mu leg
    Eff_cross_ele_Data = "eff_data_tau1_Trg_etau_eleCentral"
    Eff_cross_ele_MC = "eff_MC_tau1_Trg_etau_eleCentral"


    # must pass CrossEleTau_region - Tau leg
    Eff_cross_tau_Data = "eff_data_tau2_Trg_etau_3ProngCentral*eff_data_tau2_Trg_etau_DM0Central*eff_data_tau2_Trg_etau_DM1Central"
    Eff_cross_tau_MC = "eff_MC_tau2_Trg_etau_3ProngCentral*eff_MC_tau2_Trg_etau_DM0Central*eff_MC_tau2_Trg_etau_DM1Central"
    # efficiency expression
    Eff_Data_expression_ele = f"{passSingleLep} * {Eff_SL_ele_Data} - {passCrossLep} * {passSingleLep} * std::min({Eff_cross_ele_Data}, {Eff_SL_ele_Data}) * {Eff_cross_tau_Data} + {passCrossLep} * {Eff_cross_ele_Data} * {Eff_cross_tau_Data};"
    Eff_MC_expression_ele = f"{passSingleLep} * {Eff_SL_ele_MC}   - {passCrossLep} * {passSingleLep} * std::min({Eff_cross_ele_MC}  , {Eff_SL_ele_MC})   * {Eff_cross_tau_MC}   + {passCrossLep} * {Eff_cross_ele_MC} * {Eff_cross_tau_MC};"

    dfBuilder.df = dfBuilder.df.Define(f"Eff_Data_etau", Eff_Data_expression_ele)
    dfBuilder.df = dfBuilder.df.Define(f"Eff_MC_etau", Eff_MC_expression_ele)
    weight_eleTau_expression = "if ( (HLT_singleEle || HLT_etau) && Legacy_region && Eff_MC_etau!=0) {return static_cast<float>(Eff_Data_etau/Eff_MC_etau);} return 1.f;"
    if dfBuilder.period == 'Run2_2016' or dfBuilder.period == 'Run2_2016_HIPM':
        weight_eleTau_expression = "if (HLT_singleEle && SingleEle_region) {return (weight_tau1_TrgSF_singleEleCentral ) ;} return 1.f; "
    dfBuilder.df = dfBuilder.df.Define(f"weight_HLT_eTau", weight_eleTau_expression)



def defineTotalTriggerWeight(dfBuilder):
    dfBuilder.df = dfBuilder.df.Define(f"weight_trg_tauTau", """if (HLT_ditau && Legacy_region) { return weight_HLT_diTau; }if (HLT_singleTau && SingleTau_region && !Legacy_region){ return weight_HLT_singleTau; } if (HLT_MET && !(SingleTau_region) && !(Legacy_region)){ return weight_HLT_MET ; }return 1.f;""")
    dfBuilder.df = dfBuilder.df.Define(f"weight_trg_muTau", """if ((HLT_singleMu || HLT_mutau) && Legacy_region) { return weight_HLT_muTau; }if (HLT_singleTau && SingleTau_region && !Legacy_region){ return weight_HLT_singleTau; } if (HLT_MET && !(SingleTau_region) && !(Legacy_region)){ return weight_HLT_MET ; }return 1.f;""")
    dfBuilder.df = dfBuilder.df.Define(f"weight_trg_eTau", """if ((HLT_singleEle || HLT_etau) && Legacy_region) { return weight_HLT_eTau; }if (HLT_singleTau && SingleTau_region && !Legacy_region){ return weight_HLT_singleTau; } if (HLT_MET && !(SingleTau_region) && !(Legacy_region)){ return weight_HLT_MET ; }return 1.f;""")
    dfBuilder.df = dfBuilder.df.Define(f"weight_trg_eE", "if (HLT_singleEle && SingleEle_region) {return weight_HLT_singleEle ;} return 1.f;")
    dfBuilder.df = dfBuilder.df.Define(f"weight_trg_muMu", "if (HLT_singleMu && SingleMu_region) {return weight_HLT_singleMu ;} return 1.f;")
    dfBuilder.df = dfBuilder.df.Define(f"weight_trg_eMu", "if (((HLT_singleMu && SingleMu_region) || (HLT_singleEle && SingleEle_region)) && weight_tau1_TrgSF_singleEleCentral!=1.f) {return (weight_tau1_TrgSF_singleEleCentral*weight_tau2_TrgSF_singleMuCentral);} return 1.f;")
