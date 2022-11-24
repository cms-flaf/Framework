import ROOT
import Common.Utilities as Utilities
import Common.BaselineSelection as Baseline 
syst_names = {"_Central":Baseline.ana_reco_object_collections, "_TauESUp":["Tau", "MET"], "_TauESDown":["Tau", "MET"]}


def applyTriggers(df, yaml_dict= None):
    Baseline.Initialize(False, False)
    df, syst_list = Baseline.CreateRecoP4(df, None)
    df = Baseline.DefineGenObjects(df, isData=False, isHH=True) #for the moment, testing on MC HH events 
    df = Baseline.RecoLeptonsSelection(df) 
    df = Baseline.RecoJetAcceptance(df)
    df = Baseline.RecoHttCandidateSelection(df) 
    all_or_strings = []
    dict_legtypes = {"Electron":"Leg::e", "Muon":"Leg::mu", "Tau":"Leg::tau"}
    if yaml_dict is None:
        return df 
    for ch in ['tauTau', 'eTau', 'muTau']: 
        total_or_string = f"( httCand.channel() == Channel::{ch} && "
        total_or_paths = []
        total_objects_matched = []
        for item in yaml_dict[ch].items():  
            or_paths = '( '
            or_paths += " || ".join(path for path in item[1]['path_MC'] )
            or_paths += ' ) ' 
            for leg_tuple in item[1]['legs'].items():
                leg_dict = leg_tuple[1]
                # define the offline cuts before asking for matching 
                leg_dict_offline= leg_dict["offline_obj"] 
                var_name_offline = f"""{leg_dict_offline["type"]}_offlineCut_{leg_tuple[0]}_{item[0]}_{ch}""" 
                df = df.Define(f"{var_name_offline}", f"""{leg_dict_offline["cut"]}""")
                if leg_dict['doMatching'] == False:  
                    if(leg_dict_offline['type']!='MET'):
                        var_name_offline = f"""{leg_dict_offline['type']}_idx[{var_name_offline}].size()>=0"""
                    total_or_paths.append(f"""({or_paths} &&  {var_name_offline})""") 
                    continue
                # define online cuts
                leg_dict_online= leg_dict["online_obj"] 
                var_name_online = f"""{leg_dict_offline["type"]}_onlineCut_{leg_tuple[0]}_{item[0]}_{ch}"""  
                df = df.Define(f"""{var_name_online}""",f"""{leg_dict_online["cut"]}""")
                # find matching online <-> offline
                matching_var = f"""{leg_dict_offline["type"]}_Matching_{leg_tuple[0]}_{item[0]}_{ch}"""  
                df = df.Define(f"{matching_var}", f"""FindMatching({var_name_offline}, {var_name_online},
                TrigObj_eta, TrigObj_phi, {leg_dict_offline["type"]}_eta,{leg_dict_offline["type"]}_phi, 0.2 )""")
                total_objects_matched.append([f"""{dict_legtypes[leg_dict_offline["type"]]}""", matching_var]) 
            # build the legVector 
            if(f"""({or_paths} &&  {var_name_offline})""" in total_or_paths): continue
            legVector = '{ '
            for legVector_element in total_objects_matched:
                legVector_elements = '{ '
                legVector_elements += ", ".join(element for element in legVector_element )
                legVector_elements += ' }'
                legVector += legVector_elements
                legVector += ' , '
            legVector += ' }'
            df = df.Define(f"""hasHttCandCorrespondance_{item[0]}_{ch}""", f"""HasHttMatching(httCand, {legVector} )""")
            total_or_paths.append(f"""({or_paths} &&  hasHttCandCorrespondance_{item[0]}_{ch})""")
        total_or_string += ' ( '
        total_or_string += " || ".join(path for path in total_or_paths) 
        total_or_string += ' ) '
        total_or_string += ' ) ' 
        all_or_strings.append(total_or_string)
    final_or_string = " || ".join(total_or for total_or in all_or_strings)   
    df = df.Filter(final_or_string)
    print(df.Count().GetValue()) 
    return df
    

if __name__ == "__main__":
    import argparse
    import yaml 
    import os
    parser = argparse.ArgumentParser() 
    parser.add_argument('--period', type=str)
    parser.add_argument('--inFile', type=str)    
    parser.add_argument('--yamlFile', type=str)    
    parser.add_argument('--nEvents', type=int, default=None)
    parser.add_argument('--evtIds', type=str, default='') 
    args = parser.parse_args() 
    
    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "Common/GenTools.h"') 
    df = ROOT.RDataFrame("Events", args.inFile)
    yaml_dict = None
    with open(f"{args.yamlFile}", "r") as stream:
        try:
            #print(yaml.safe_load(stream))
            yaml_dict= yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc) 
    if args.nEvents is not None:
        df = df.Range(args.nEvents)
    if len(args.evtIds) > 0:
        df = df.Filter(f"static const std::set<ULong64_t> evts = {{ {args.evtIds} }}; return evts.count(event) > 0;") 
    df = applyTriggers(df, yaml_dict)


  
#eTau:
#  singleEle:
#  EleTau:
#  singleTau:
#  tauMET:

# regioni accettanza trigger






''' 
                
                df = df.Define(f"isInHttCand_{var_name_offline}", f""" RVecB isInHttCand({var_name_offline}.size(), false);
                            for(size_t leg_idx = 0; leg_idx < HTTCand::n_legs; leg_idx++){{ 
                                if({var_name_offline}[httCand.leg_index[leg_idx]]!=0) isInHttCand[httCand.leg_index[leg_idx]] = true;
                            }}
                            return isInHttCand;""")
                # apply online cuts
                leg_dict_online= leg_dict["online_obj"]
                var_name_online = f"""{leg_dict_offline["type"]}_onlineCut_{leg_tuple[0]}_{item[0]}""" 
                df = df.Define(f"""{var_name_online}""",f"""{leg_dict_online["cut"]}""")
                #find matching 
                matching_var = f"""{leg_dict_offline["type"]}_Matching_{leg_tuple[0]}_{item[0]}""" 
                df = df.Define(f"""{matching_var}""", f"""FindMatching({var_name_offline}, {var_name_online},
                TrigObj_eta, TrigObj_phi, {leg_dict_offline["type"]}_eta,{leg_dict_offline["type"]}_phi, 0.2 )""")
                df = df.Define(f"""hasHttCandCorrespondance_{item[0]}_{leg_tuple[0]}""", f"""for(size_t leg_idx = 0; leg_idx < HTTCand::n_legs; leg_idx++){{ 
                                if({matching_var}[httCand.leg_index[leg_idx]]!=0) return true;
                            }}
                            return false;""")
                total_or_paths.append(f"""({or_paths} &&  hasHttCandCorrespondance_{item[0]}_{leg_tuple[0]})""")  
            '''