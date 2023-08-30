import sys
import math
import ROOT
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities


col_type_dict = {
  'Float_t':'float',
  'Bool_t':'bool',
  'Int_t' :'int',
  'ULong64_t' :'unsigned long long',
  'Long64_t' : 'long long',
  'Long_t' :'long',
  'UInt_t' :'unsigned int',
  'Char_t' : 'char',
  'ROOT::VecOps::RVec<Float_t>':'ROOT::VecOps::RVec<float>',
  'ROOT::VecOps::RVec<Int_t>':'ROOT::VecOps::RVec<int>',
  'ROOT::VecOps::RVec<UChar_t>':'ROOT::VecOps::RVec<unsigned char>',
  'ROOT::VecOps::RVec<float>':'ROOT::VecOps::RVec<float>',
  'ROOT::VecOps::RVec<int>':'ROOT::VecOps::RVec<int>',
  'ROOT::VecOps::RVec<unsigned char>':'ROOT::VecOps::RVec<unsigned char>'
  }
def defineP4(df, name):
    df = df.Define(f"{name}_p4", f"ROOT::Math::LorentzVector<ROOT::Math::PtEtaPhiM4D<double>>({name}_pt,{name}_eta,{name}_phi,{name}_mass)")
    return df

def defineAllP4(df):
    for idx in [0,1]:
        df = defineP4(df, f"tau{idx+1}")
        df = defineP4(df, f"b{idx+1}")
        #df = defineP4(df, f"tau{idx+1}_seedingJet")
    return df

def createInvMass(df):
    df = df.Define("tautau_m_vis", "(tau1_p4+tau2_p4).M()")
    df = df.Define("bb_m_vis", "(b1_p4+b2_p4).M()")
    df = df.Define("bbtautau_mass", "(b1_p4+b2_p4+tau1_p4+tau2_p4).M()")
    df = df.Define("dR_tautau", 'ROOT::Math::VectorUtil::DeltaR(tau1_p4, tau2_p4)')
    return df

def RenormalizeHistogram(histogram, norm, include_overflows=True):
    integral = histogram.Integral(0, histogram.GetNbinsX()+1) if include_overflows else histogram.Integral()
    histogram.Scale(norm / integral)

def FixNegativeContributions(histogram):
    correction_factor = 0.

    ss_debug = ""
    ss_negative = ""

    original_Integral = histogram.Integral(0, histogram.GetNbinsX()+1)
    ss_debug += "\nSubtracted hist for '{}'.\n".format(histogram.GetName())
    ss_debug += "Integral after bkg subtraction: {}.\n".format(original_Integral)
    if original_Integral < 0:
        print(ss_debug)
        print("Integral after bkg subtraction is negative for histogram '{}'".format(histogram.GetName()))
        return False,ss_debug, ss_negative

    for n in range(1, histogram.GetNbinsX()+1):
        if histogram.GetBinContent(n) >= 0:
            continue
        prefix = "WARNING" if histogram.GetBinContent(n) + histogram.GetBinError(n) >= 0 else "ERROR"

        ss_negative += "{}: {} Bin {}, content = {}, error = {}, bin limits=[{},{}].\n".format(
            prefix, histogram.GetName(), n, histogram.GetBinContent(n), histogram.GetBinError(n),
            histogram.GetBinLowEdge(n), histogram.GetBinLowEdge(n+1))

        error = correction_factor - histogram.GetBinContent(n)
        new_error = math.sqrt(math.pow(error, 2) + math.pow(histogram.GetBinError(n), 2))
        histogram.SetBinContent(n, correction_factor)
        histogram.SetBinError(n, new_error)

    RenormalizeHistogram(histogram, original_Integral, True)
    return True, ss_debug, ss_negative

class DataFrameBuilderBase:
    def CreateColumnTypes(self):
        colNames = [str(c) for c in self.df.GetColumnNames()]
        entryIndexIdx = colNames.index("entryIndex")
        colNames[entryIndexIdx], colNames[0] = colNames[0], colNames[entryIndexIdx]
        self.colNames = colNames
        self.colTypes = [str(self.df.GetColumnType(c)) for c in self.colNames]

    def __init__(self, df):
        self.df = df
        self.colNames=[]
        self.colTypes=[]
        self.var_list = []
        self.CreateColumnTypes()


    def GetEventsFromShifted(self, df_central):
        df_final = df_central.Filter(""" std::find ( analysis::GetEntriesVec().begin(), analysis::GetEntriesVec().end(),
                                     entryIndex ) != analysis::GetEntriesVec().end()""")
        self.df=df_final

    def CreateFromDelta(self,var_list,central_columns,central_col_types):
        for var_idx,var_name in enumerate(self.colNames):
            if not var_name.endswith("Diff"):
                continue
            var_name_forDelta = var_name.removesuffix("Diff")
            central_col_idx = central_columns.index(var_name_forDelta)
            if central_columns[central_col_idx]!=var_name_forDelta:
                raise RuntimeError(f"CreateFromDelta: {central_columns[central_col_idx]} != {var_name_forDelta}")
            self.df = self.df.Define(f"{var_name_forDelta}", f"""analysis::FromDelta({var_name},
                                     analysis::GetEntriesMap()[entryIndex]->GetValue<{col_type_dict[self.colTypes[var_idx]]}>({central_col_idx}) )""")
            var_list.append(f"{var_name_forDelta}")
        for central_col_idx,central_col in enumerate(central_columns):
            if central_col in var_list or central_col in self.colNames: continue
            #if central_col != 'channelId' : continue # this is for a bugfix that I still haven't figured out !!
            self.df = self.df.Define(central_col, f"""analysis::GetEntriesMap()[entryIndex]->GetValue<{central_col_types[central_col_idx]}>({central_col_idx})""")


def createModel(hist_cfg, var):
    hists = {}
    x_bins = hist_cfg[var]['x_bins']
    if type(hist_cfg[var]['x_bins'])==list:
        x_bins_vec = Utilities.ListToVector(x_bins, "double")
        model = ROOT.RDF.TH1DModel("", "", x_bins_vec.size()-1, x_bins_vec.data())
    else:
        n_bins, bin_range = x_bins.split('|')
        start,stop = bin_range.split(':')
        model = ROOT.RDF.TH1DModel("", "",int(n_bins), float(start), float(stop))
    return model






def GetWeight(channel,cat , btag_wp):
    btag_weight = "1"
    if cat!='inclusive':
        btag_weight = f"weight_bTagSF_{btag_wp}_Central"
    trg_weights_dict = {
        'eTau':["weight_tau1_TrgSF_singleEle_Central","weight_tau2_TrgSF_singleEle_Central"],
        'muTau':["weight_tau1_TrgSF_singleMu_Central","weight_tau2_TrgSF_singleMu_Central"],
        'tauTau':["weight_tau1_TrgSF_ditau_Central","weight_tau2_TrgSF_ditau_Central"]
        }
    weights_to_apply = [ "weight_Jet_PUJetID_Central_b1", "weight_Jet_PUJetID_Central_b2", "weight_TauID_Central", btag_weight, "weight_tau1_EleidSF_Central", "weight_tau1_MuidSF_Central", "weight_tau2_EleidSF_Central", "weight_tau2_MuidSF_Central","weight_total"]
    #weights_to_apply = [  "weight_TauID_Central",btag_weight, "weight_tau1_EleidSF_Central", "weight_tau1_MuidSF_Central", "weight_tau2_EleidSF_Central", "weight_tau2_MuidSF_Central","weight_total"]
    #weights_to_apply = [ "weight_total"]

    weights_to_apply.extend(trg_weights_dict[channel])
    total_weight = '*'.join(weights_to_apply)
    return total_weight

def GetRelativeWeights(column_names):
    return [col for col in column_names if "weight" in col and col.endswith("rel")]
