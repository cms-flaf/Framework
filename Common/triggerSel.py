import yaml
class Triggers():
    dict_legtypes = {"Electron":"Leg::e", "Muon":"Leg::mu", "Tau":"Leg::tau", "Jet":"Leg::jet"}

    def __init__(self, triggerFile, deltaR_matching=0.4):
        with open(triggerFile, "r") as stream:
            self.trigger_dict= yaml.safe_load(stream)
        self.deltaR_matching = deltaR_matching

    def ApplyTriggers(self, df, offline_lepton_legs, channel='Htt', isData = False, isSignal=False, default=0):

        hltBranches = []
        matchedObjectsBranches= []
        for path, path_dict in self.trigger_dict.items():
            path_key = 'path'
            if 'path' not in path_dict:
                path_key += '_data' if isData else '_MC'
            keys = [k for k in path_dict[path_key]]
            # check that HLT path exists:
            for key in keys:
                trigger_string = key.split(' ')
                trigName = trigger_string[0]
                if trigName not in df.GetColumnNames():
                    path_dict[path_key].remove(key)
            or_paths = " || ".join(f'({p})' for p in path_dict[path_key])
            or_paths = f' ( { or_paths } ) '
            additional_conditions = [""]
            total_objects_matched = []
            for leg_id, leg_tuple in enumerate(path_dict['legs']):
                if 'ref_leg' in leg_tuple:
                    leg_tuple = path_dict['legs'][leg_tuple['ref_leg']]
                leg_dict_offline= leg_tuple["offline_obj"]
                ########################
                type_name_offline = leg_dict_offline["type"] #if not "Jet" in leg_dict_offline else "ExtraJet"
                offlinecut = leg_dict_offline["cut"] #if not "Jet" in leg_dict_offline["type"] else leg_dict_offline["cut"].replace("Jet", "ExtraJet")
                # print(f'type_name_offline : {type_name_offline}')
                # print(f'offlinecut : {offlinecut}')
                # print(f'leg_dict_offline["cut"] : {leg_dict_offline["cut"]}')
                ########################
                var_name_offline = f'{type_name_offline}_offlineCut_{leg_id+1}_{path}'
                
                df = df.Define(var_name_offline, offlinecut)
                df.Display({var_name_offline}).Print()
                if not leg_tuple["doMatching"]:
                    if not leg_dict_offline["type"].startswith('MET'):
                        var_name_offline = f'{leg_dict_offline["type"]}_idx[{var_name_offline}].size()>0'
                    additional_conditions.append(var_name_offline)
                else:
                    # print(f'{channel}Candidate')
                    df = df.Define(f'{type_name_offline}_{var_name_offline}_sel', f'{channel}Candidate.isLeg({type_name_offline}_idx, {self.dict_legtypes[type_name_offline]}) && ({var_name_offline})')
                    
                    # df.Display({f'{type_name_offline}_idx'}).Print()
                    # df.Display({f'{type_name_offline}_{var_name_offline}_sel'}).Print()
                    
                    leg_dict_online= leg_tuple["online_obj"]
                    var_name_online =  f'{leg_dict_offline["type"]}_onlineCut_{leg_id+1}_{path}'
                    cut_vars = []
                    cuts = leg_dict_online["cuts"] if "cuts" in leg_dict_online else [ { "cut" : leg_dict_online["cut"] } ]
                    for cut_idx, online_cut in enumerate(cuts):
                        preCondition = online_cut.get('preCondition', 'true')
                        cut_var_name =  f'{leg_dict_offline["type"]}_onlineCut_{leg_id+1}_{path}_{cut_idx}'
                        df = df.Define(cut_var_name, f"!({preCondition}) || (({preCondition}) && ({online_cut['cut']}))")
                        cut_vars.append(cut_var_name)
                    df = df.Define(var_name_online, ' && '.join(cut_vars))
                    matching_var = f'{leg_dict_offline["type"]}_Matching_{leg_id+1}_{path}'
                    df = df.Define(matching_var, f"""FindMatchingSet( {type_name_offline}_{var_name_offline}_sel,{var_name_online},{leg_dict_offline["type"]}_p4, TrigObj_p4,{self.deltaR_matching} )""")
                    # df.Display({matching_var}).Print()
                    total_objects_matched.append(f'{{ {self.dict_legtypes[type_name_offline]}, {matching_var} }}')
            legVector = f'{{ { ", ".join(total_objects_matched)} }}'
            print(legVector)
            df = df.Define(f'hasOOMatching_{path}_details', f'HasOOMatching({legVector})')
            df = df.Define(f'hasOOMatching_{path}', f'hasOOMatching_{path}_details.first')
            df.Display({f'hasOOMatching_{path}_details'}).Print()
            df.Display({f'hasOOMatching_{path}'}).Print()
            for offline_leg_id, offline_leg_name in enumerate(offline_lepton_legs):
                matching_var_bool = f'{offline_leg_name}_HasMatching_{path}'
                cond = f"{channel}Candidate.leg_type.size() > {offline_leg_id}"
                df = df.Define(matching_var_bool, f'{cond} ? (hasOOMatching_{path}_details.second.count(LegIndexPair({channel}Candidate.leg_type.at({offline_leg_id}), {channel}Candidate.leg_index.at({offline_leg_id}) ) ) > 0 ): 0')
                df.Display({matching_var_bool}).Print()
                matchedObjectsBranches.append(matching_var_bool)
            fullPathSelection = f'{or_paths} &&  hasOOMatching_{path}'
            fullPathSelection += ' && '.join(additional_conditions)
            hltBranch = f'HLT_{path}'
            hltBranches.append(hltBranch)
            df = df.Define(hltBranch, fullPathSelection)
            # df.Display({hltBranch}).Print()
        total_or_string = ' || '.join(hltBranches)
        if not isSignal:
            df = df.Filter(total_or_string, "trigger application")
        hltBranches.extend(matchedObjectsBranches)
        print("hltBranches : ", hltBranches)
        return df,hltBranches
    
    def ApplyTriggers_newVersion(self, df, offline_legs, isData = False, isSignal=False):
        hltBranches = []
        matchedObjectsBranches= []
        for path, path_dict in self.trigger_dict.items():
            path_key = 'path'
            if 'path' not in path_dict:
                path_key += '_data' if isData else '_MC'
            keys = [k for k in path_dict[path_key]]
            # check that HLT path exists:
            for key in keys:
                trigger_string = key.split(' ')
                trigName = trigger_string[0]
                if trigName not in df.GetColumnNames():
                    path_dict[path_key].remove(key)
            or_paths = " || ".join(f'({p})' for p in path_dict[path_key])
            or_paths = f' ( { or_paths } ) '
            additional_conditions = [""]
            total_objects_matched = []
            for leg_id, leg_tuple in enumerate(path_dict['legs']):
                leg_dict_offline= leg_tuple["offline_obj"]
                for obj in offline_legs:
                    offline_cut = leg_dict_offline["cut"].format(obj=obj)
                    if(obj == "ExtraJet"):
                        offline_cut = offline_cut.replace("ExtraJet_legType == Leg::", "").replace("e &&", "").replace("mu &&", "").replace("tau &&", "").replace("jet &&", "")
                    print(offline_cut)
                    var_name_offline = f'{obj}_offlineCut_{leg_id+1}_{path}'
                
                    df = df.Define(var_name_offline, offline_cut)
            
                    leg_dict_online= leg_tuple["online_obj"]
                    var_name_online =  f'{obj}_onlineCut_{leg_id+1}_{path}'
                    cut_vars = []
                    cuts = leg_dict_online["cuts"] if "cuts" in leg_dict_online else [ { "cut" : leg_dict_online["cut"] } ]
                    for cut_idx, online_cut in enumerate(cuts):
                        preCondition = online_cut.get('preCondition', 'true')
                        cut_var_name =  f'{obj}_onlineCut_{leg_id+1}_{path}_{cut_idx}'
                        df = df.Define(cut_var_name, f"!({preCondition}) || (({preCondition}) && ({online_cut['cut']}))")
                        cut_vars.append(cut_var_name)
                    df = df.Define(var_name_online, ' && '.join(cut_vars))
                    matching_var = f'{obj}_Matching_{leg_id+1}_{path}'
                    
                    df = df.Define(matching_var, f"""FindMatchingSet( {var_name_offline}, {var_name_online}, {obj}_p4, TrigObj_p4, {self.deltaR_matching} )""")

            for obj in offline_legs:
                matching_var_bool = f'{obj}_HasMatching_{path}'
                if (not obj=="ExtraJet"):
                    matching_var_bool_str = " || ".join(f"{obj}_Matching_{i+1}_{path}" for i in range(len(path_dict['legs'])))
                    df = df.Define(matching_var_bool, f'({matching_var_bool_str})')
                else:
                    # la variabile printata qui non è nel formato che mi aspetto controlla meglio
                    df.Display([f"{obj}_Matching_1_{path}"]).Print()
                    df = df.Define(matching_var_bool, f'Sum({obj}_Matching_1_{path})>0')
                matchedObjectsBranches.append(matching_var_bool)
            df = df.Define(f"HasOOMatching_{path}",  " || ".join(f'({obj}_HasMatching_{path})' for obj in offline_legs))
            fullPathSelection = f'{or_paths} &&  HasOOMatching_{path}'
            fullPathSelection += ' && '.join(additional_conditions)
            hltBranch = f'HLT_{path}'
            hltBranches.append(hltBranch)
            df = df.Define(hltBranch, fullPathSelection)
        total_or_string = ' || '.join(hltBranches)
        if not isSignal:
            df = df.Filter(total_or_string, "trigger application")
        hltBranches.extend(matchedObjectsBranches)
        print("hltBranches : ", hltBranches)
        return df,hltBranches