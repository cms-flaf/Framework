import yaml
class Triggers():
    dict_legtypes = {"Electron":"Leg::e", "Muon":"Leg::mu", "Tau":"Leg::tau"}

    def __init__(self, triggerFile):
        self.trigger_dict = None

        with open(triggerFile, "r") as stream:
            try:
                self.trigger_dict= yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)


    def ApplyTriggers(self, df, isData = False):
        total_objects_matched = []
        total_or_paths = []
        hltBranches = []
        if self.trigger_dict is None:
            return df
        for path, path_dict in self.trigger_dict.items():
            path_key = 'path'
            if 'path' not in path_dict:
                path_key += '_data' if isData else '_MC'
            or_paths = " || ".join(path for path in path_dict[path_key])
            or_paths = f' ( { or_paths } ) '
            additional_conditions = [""]
            total_objects_matched = []
            already_considered_legs = []
            isDifferentLeg = 'false'
            for leg_id, leg_tuple in enumerate(path_dict['legs']):
                # define offline cuts
                leg_dict_offline= leg_tuple["offline_obj"]
                type_name_offline = leg_dict_offline["type"]
                if(leg_id == 0):
                    already_considered_legs.append(type_name_offline)
                if(type_name_offline not in already_considered_legs):
                    isDifferentLeg = 'true'
                var_name_offline = f'{type_name_offline}_offlineCut_{leg_id+1}_{path}'
                df = df.Define(var_name_offline, leg_dict_offline["cut"])
                if not leg_tuple["doMatching"]:
                        if(leg_dict_offline["type"]!='MET'):
                            var_name_offline = f'{leg_dict_offline["type"]}_idx[{var_name_offline}].size()>=0'
                        additional_conditions.append(f' {var_name_offline} ')
                else:
                    # require that isLeg
                    df = df.Define(f'{type_name_offline}_{var_name_offline}_sel', f'httCand.isLeg({type_name_offline}_idx, {self.dict_legtypes[type_name_offline]}) && ({var_name_offline})')
                    df = df.Define(f'isHttLegAndPassOfflineSel_{var_name_offline}_{type_name_offline}', f'{var_name_offline} && {type_name_offline}_{var_name_offline}_sel')
                    leg_dict_online= leg_tuple["online_obj"]
                    var_name_online =  f'{leg_dict_offline["type"]}_onlineCut_{leg_id+1}_{path}'
                    df = df.Define(f'{var_name_online}',f'{leg_dict_online["cut"]}')
                    matching_var = f'{leg_dict_offline["type"]}_Matching_{leg_id+1}_{path}'
                    # find matching online <-> offline
                    df = df.Define(f'{matching_var}', f"""FindMatchingOnlineIndices(isHttLegAndPassOfflineSel_{var_name_offline}_{type_name_offline}, {var_name_online},
                                                TrigObj_eta, TrigObj_phi, {leg_dict_offline["type"]}_eta,{leg_dict_offline["type"]}_phi, 0.4 )""")
                    total_objects_matched.append(f'{{ {isDifferentLeg}, {matching_var} }}')

            legVector = f'{{ { ", ".join(total_objects_matched)} }}'
            # find solution
            df = df.Define(f'hasHttCandCorrespondance_{path}', f'HasHttMatching({legVector} )')
            stringToAppend = f'{or_paths} &&  hasHttCandCorrespondance_{path}'
            stringToAppend += ' && '.join(additional_conditions)
            hltBranches.append(stringToAppend)
            df = df.Define(f'HLT_{path}', stringToAppend)
            total_or_paths.append(f' ( {stringToAppend} )')
        total_or_string = ' || '.join(or_path for or_path in total_or_paths)
        df = df.Define(f'HLT_total_triggers', f'{total_or_string}')
        hltBranches.append('HLT_total_triggers')
        return df,hltBranches
