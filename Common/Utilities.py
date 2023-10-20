from enum import Enum
import ROOT
import copy

class WorkingPointsTauVSmu(Enum):
    VLoose = 1
    Loose = 2
    Medium = 3
    Tight = 4

class WorkingPointsTauVSjet(Enum):
   VVVLoose = 1
   VVLoose = 2
   VLoose = 3
   Loose = 4
   Medium = 5
   Tight = 6
   VTight = 7
   VVTight = 8

class WorkingPointsTauVSe(Enum):
    VVVLoose = 1
    VVLoose = 2
    VLoose = 3
    Loose = 4
    Medium = 5
    Tight = 6
    VTight = 7
    VVTight = 8

class WorkingPointsBoostedTauVSjet(Enum):
   VVLoose = 1
   VLoose = 2
   Loose = 3
   Medium = 4
   Tight = 5
   VTight = 6
   VVTight = 7

class WorkingPointsbTag(Enum):
    Loose = 1
    Medium = 2
    Tight = 3

class WorkingPointsMuonID(Enum):
    HighPtID = 1
    LooseID = 2
    MediumID = 3
    MediumPromptID = 4
    SoftID = 5
    TightID = 6
    TrkHighPtID = 7

deepTauVersions = {"2p1":"2017", "2p5":"2018"}

def ListToVector(list, type="string"):
	vec = ROOT.std.vector(type)()
	for item in list:
		vec.push_back(item)
	return vec

def generate_enum_class(cls):
    enum_string = "enum class {} : int {{\n".format(cls.__name__)
    for item in cls:
        enum_string += "    {} = {},\n".format(item.name, item.value)
    enum_string += "};"
    return enum_string

class DataFrameWrapper:
    def __init__(self, df, defaultColToSave=[]):
        self.df = df
        self.colToSave = copy.deepcopy(defaultColToSave)

    def Define(self, varToDefine, varToCall):
        self.df = self.df.Define(f"{varToDefine}", f"{varToCall}")

    def Filter(self, filter_str, filter_name=""):
        self.df = self.df.Filter(filter_str, filter_name)

    def DefineAndAppend(self, varToDefine, varToCall):
        self.Define(varToDefine, varToCall)
        self.colToSave.append(varToDefine)

    def Apply(self, func, *args, **kwargs):
        result = func(self.df, *args, **kwargs)
        if isinstance(result, tuple):
            self.df = result[0]
            if len(result) == 2:
                return result[1]
            return result[1:]
        else:
            self.df = result

def ApplyConfigCustomisations(config_dict, customisations):
    customisations_to_apply = customisations.split(',')
    for customisation in customisations_to_apply:
        substrings = customisation.split('=')
        if len(substrings) != 2 :
            raise RuntimeError("len of substring is not 2!")
        value = substrings[-1]
        key_entries = substrings[0].split('.')
        cfg_entry = config_dict
        for key in key_entries[:-1]:
            cfg_entry = cfg_entry[key]
        entry_type = type(cfg_entry[key_entries[-1]])
        cfg_entry[key_entries[-1]] = entry_type(value)

def GetValues(collection):
    for key, value in collection.items():
        if isinstance(value, dict):
            GetValues(value)
        else:
            collection[key] = value.GetValue()
    return collection

def GetKeyNames(filee, dir = "" ):
        if dir != "":
            filee.cd(dir)
        return [str(key.GetName()) for key in ROOT.gDirectory.GetListOfKeys()]
