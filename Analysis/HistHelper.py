import sys
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities

deepTauYears = {'v2p1':'2017','v2p5':'2018'}
QCDregions = ['OS_Iso', 'SS_Iso', 'OS_AntiIso', 'SS_AntiIso']
categories = ['res2b', 'res1b', 'inclusive']#, 'boosted']
#categories = ['res2b', 'res1b', 'inclusive', 'boosted']
channels = {'eTau':13, 'muTau':23, 'tauTau':33}
triggers = {'eTau':'HLT_singleEle', 'muTau':'HLT_singleMu', 'tauTau':"HLT_ditau"}

col_type_dict = {
  'Float_t':'float',
  'Bool_t':'bool',
  'Int_t' :'int',
  'ULong64_t' :'unsigned long long',
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

def defineWeights(df_dict, use_new_weights=False, deepTauVersion='v2p1'):
    for sample in df_dict.keys():
        weight_names=[]
        if sample != "data":
            if(use_new_weights):
                df_dict[sample] = GetNewSFs_DM(df_dict[sample],weights_to_apply, deepTauVersion)
            '''
            else:
                if "weight_tauID_Central" not in weights_to_apply:
                    weights_to_apply.append("weight_tauID_Central")
            '''
        for weight in weights_to_apply:
            weight_names.append(weight if sample!="data" else "1")
            if weight == 'weight_Central' and weight in df_dict[sample].GetColumnNames():
                weight_names.append("1000")
                if(sample=="TT"):
                    weight_names.append('791. / 687.')
        weight_str = " * ".join(weight_names)
        df_dict[sample]=df_dict[sample].Define("weight",weight_str)

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
        print(debug_info)
        print("Integral after bkg subtraction is negative for histogram '{}'".format(histogram.GetName()))
        return False

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

def GetValues(collection):
    for key, value in collection.items():
        if isinstance(value, dict):
            GetValues(value)
        else:
            collection[key] = value.GetValue()
    return collection

def Estimate_QCD(histograms, sums):
    hist_data = histograms['data']
    sum_data = sums['data']
    hist_data_B = hist_data['region_B']
    n_C = sum_data['region_C']
    n_D = sum_data['region_D']
    for sample in histograms.keys():
        if sample=='data' or sample in signals:
            continue
        # find kappa value
        n_C -= sums[sample]['region_C']
        n_D -= sums[sample]['region_D']
        hist_data_B.Add(histograms[sample]['region_B'], -1)
    kappa = n_C/n_D
    if n_C <= 0 or n_D <= 0:
        raise  RuntimeError(f"transfer factor <=0 ! {kappa}")
    hist_data_B.Scale(kappa)
    fix_negative_contributions,debug_info,negative_bins_info = FixNegativeContributions(hist_data_B)
    if not fix_negative_contributions:
        print(debug_info)
        print(negative_bins_info)
        raise RuntimeError("Unable to estimate QCD")
    return hist_data_B

def createHistograms(df_dict, var):
    hists = {}
    x_bins = plotter.hist_cfg[var]['x_bins']
    if type(plotter.hist_cfg[var]['x_bins'])==list:
        x_bins_vec = Utilities.ListToVector(x_bins, "double")
        model = ROOT.RDF.TH1DModel("", "", x_bins_vec.size()-1, x_bins_vec.data())
    else:
        n_bins, bin_range = x_bins.split('|')
        start,stop = bin_range.split(':')
        model = ROOT.RDF.TH1DModel("", "",int(n_bins), float(start), float(stop))
    for sample in df_dict.keys():
        hists[sample] = {}
        for region in QCDregions:
            hists[sample][f"region_{region}"] = df_dict[sample].Filter(f"region_{region}").Histo1D(model,var, "weight")
    return hists

def createSums(df_dict):
    sums = {}
    for sample in df_dict.keys():
        sums[sample] = {}
        for region in QCDregions:
            sums[sample][f"region_{region}"] = df_dict[sample].Filter(f"region_{region}").Sum("weight")
    return sums