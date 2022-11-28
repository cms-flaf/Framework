import ROOT
import Common.BaselineSelection as Baseline
syst_names = {"_Central":Baseline.ana_reco_object_collections, "_TauESUp":["Tau", "MET"], "_TauESDown":["Tau", "MET"]}


def ApplyTriggers(df, yaml_dict= None, isData = False):
    total_objects_matched = []
    all_or_strings = []
    total_or_paths = []
    dict_legtypes = {"Electron":"Leg::e", "Muon":"Leg::mu", "Tau":"Leg::tau"}
    if yaml_dict is None:
        return df
    for path in yaml_dict:
        path_dict = yaml_dict[path]
        #channel_or_string = '( '
        #channel_or_string += ' || '.join(f"httCand.channel() == Channel::{ch} " for ch in path_dict['channels'])
        #channel_or_string += ' )'
        or_paths = '( '
        if('path' in path_dict):
            or_paths += " || ".join(path for path in path_dict['path'] )
        else:
            str_data = 'data' if isData else 'MC'
            or_paths += " || ".join(path for path in path_dict[f"path_{str_data}"] )
        or_paths += ' ) '

        leg_dict = path_dict['legs']
        k=0
        for leg_tuple in leg_dict:
            k+=1
            # define offline cuts
            leg_dict_offline= leg_tuple["offline_obj"]
            var_name_offline = f"""{leg_dict_offline["type"]}_offlineCut_{k}_{path}"""
            type_name_offline = leg_dict_offline["type"]
            df = df.Define(f"{var_name_offline}", f"""{leg_dict_offline["cut"]}""")
            if leg_tuple['doMatching'] == False:
                    if(leg_dict_offline['type']!='MET'):
                        var_name_offline = f"""{leg_dict_offline['type']}_idx[{var_name_offline}].size()>=0"""
                    total_or_paths.append(f"""({or_paths} &&  {var_name_offline})""")
                    continue
            # require that isLeg
            df = df.Define(f"{type_name_offline}_{var_name_offline}_sel", f"""httCand.isLeg({type_name_offline}_idx, {dict_legtypes[type_name_offline]}) && ({var_name_offline})""")
            df = df.Define(f"isHttLegAndPassOfflineSel_{var_name_offline}_{type_name_offline}", f"{var_name_offline} && {type_name_offline}_{var_name_offline}_sel")
            leg_dict_online= leg_tuple["online_obj"]
            var_name_online =  f"""{leg_dict_offline["type"]}_onlineCut_{k}_{path}"""
            df = df.Define(f"""{var_name_online}""",f"""{leg_dict_online["cut"]}""")
            matching_var = f"""{leg_dict_offline["type"]}_Matching_{k}_{path}"""
            # find matching online <-> offline
            df = df.Define(f"{matching_var}", f"""FindMatchingOnlineIndices(isHttLegAndPassOfflineSel_{var_name_offline}_{type_name_offline}, {var_name_online},
                                           TrigObj_eta, TrigObj_phi, {leg_dict_offline["type"]}_eta,{leg_dict_offline["type"]}_phi, 0.4 )""")

            total_objects_matched.append([matching_var])
        if(f"""({or_paths} &&  {var_name_offline})""" in total_or_paths):
            continue

        legVector = '{ '
        for legVector_element in total_objects_matched:
            legVector_elements = '{ '
            legVector_elements += ", ".join(element for element in legVector_element )
            legVector_elements += ' }'
            legVector += legVector_elements
            legVector += ' , '
        legVector += ' }'
        # find solution
        df = df.Define(f"""hasHttCandCorrespondance_{path}""", f"""HasHttMatching({legVector} )""")
        total_or_paths.append(f"""({or_paths} &&  hasHttCandCorrespondance_{path})""")
    total_or_string = ' || '.join(or_path for or_path in total_or_paths)
    df = df.Define(f"HLT_{path}", f"{total_or_string}")

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
    parser.add_argument('--isData', type=bool, default=False)
    args = parser.parse_args()

    ROOT.gROOT.ProcessLine(".include "+ os.environ['ANALYSIS_PATH'])
    ROOT.gROOT.ProcessLine('#include "Common/GenTools.h"')
    df = ROOT.RDataFrame("Events", args.inFile)
    yaml_dict = None
    with open(f"{args.yamlFile}", "r") as stream:
        try:
            yaml_dict= yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    if args.nEvents is not None:
        df = df.Range(args.nEvents)
    if len(args.evtIds) > 0:
        df = df.Filter(f"static const std::set<ULong64_t> evts = {{ {args.evtIds} }}; return evts.count(event) > 0;")

    Baseline.Initialize(False, False)
    df, syst_list = Baseline.CreateRecoP4(df, None)
    df = Baseline.DefineGenObjects(df, args.isData, isHH=True) #for the moment, testing on MC HH events
    df = Baseline.RecoLeptonsSelection(df)
    df = Baseline.RecoJetAcceptance(df)
    df = Baseline.RecoHttCandidateSelection(df)
    df = ApplyTriggers(df, yaml_dict, args.isData)
