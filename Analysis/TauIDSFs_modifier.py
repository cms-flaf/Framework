import os
import ROOT

header_path_RootExt = os.path.join(os.environ['ANALYSIS_PATH'], "PlotKit/include/RootExt.h")
ROOT.gInterpreter.Declare(f'#include "{header_path_RootExt}"')
ROOT.gInterpreter.Declare(
    """
    static std::map<std::string, std::string> histNamesMap ;
    void FillHistNamesMap(std::string variable, std::string histName){
        if(!(histNamesMap.find(variable)!= histNamesMap.end())){
            histNamesMap[variable] = histName;
        }
    }
    float GetSFValue(float value, std::string fileName, std::string variable){
        auto inputFile = root_ext::OpenRootFile(fileName);
        auto hist = root_ext::ReadCloneObject<TH1>(*inputFile, histNamesMap.at(variable), "", true);
        return hist->GetBinContent(hist->FindFixBin(value));
        }
    """
)


def GetNewSFs_Pt(df, weights_to_apply, deepTauVersion='v2p1'):
    hist_path =  os.path.join(os.environ['ANALYSIS_PATH'], "Corrections/data/TAU/2018_UL/TauID_SF_pt_DeepTau2017v2p1VSjet_VSjetMedium_VSeleVVLoose_Mar07.root")
    ROOT.FillHistNamesMap('pt',"DMinclusive_2018_hist")
    for tau_idx in [1,2]:
        df = df.Define(f"weight_tau{tau_idx}ID_Central_new",f"""GetSFValue(tau{tau_idx}_pt,"{hist_path}","pt")""")
        weights_to_apply.append(f"weight_tau{tau_idx}ID_Central_new")
    return df

def GetNewSFs_DM(df,weights_to_apply, deepTauVersion='v2p1'):
    decayModes= ['0', '1' , '10', '11']
    histNames_map_string= {}
    if(deepTauVersion=='v2p1'):
        hist_path =  os.path.join(os.environ['ANALYSIS_PATH'], "Corrections/data/TAU/2018_UL/TauID_SF_dm_DeepTau2017v2p1VSjet_VSjetMedium_VSeleVVLoose_Mar07.root")
        infile = ROOT.TFile.Open(hist_path, "READ")
        for decayMode in decayModes:
            ROOT.FillHistNamesMap(decayMode, f"DM{decayMode}_2018_hist")
        for tau_idx in [1,2]:
            df = df.Define(f'weight_tau{tau_idx}ID_Central_new_DM{decayMode}',f"""GetSFValue(tau{tau_idx}_pt,"{hist_path}",std::to_string(tau{tau_idx}_decayMode))""")
            weights_to_apply.append(f"weight_tau{tau_idx}ID_Central_new_DM{decayMode}")
    elif deepTauVersion=='v2p5':
        SF_DM_dict = {
                      '0': 0.8861654,
                      '1': 0.9226725,
                      '10': 0.9329327,
                      '11': 0.7653489
                      }
        for tau_idx in [1,2]:
            df = df.Define(f'weight_tau{tau_idx}ID_Central_new_DM{decayMode}', f"""{SF_DM_dict["tau{tau_idx}_decayMode"]}""")
            weights_to_apply.append(f"weight_tau{tau_idx}ID_Central_new_DM{decayMode}")
    else:
        raise RuntimeError("Error: unknown deepTauVersion")
    return df