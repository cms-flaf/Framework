#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import ROOT


# In[ ]:


#df_gg = ROOT.RDataFrame("Events", "/eos/user/t/toakhter/HH_bbtautau_Run3/anaTuples/dev/Run3_2022/GluGlutoHHto2B2Tau_kl-1p00_kt-1p00_c2-0p00/nano_0.root")
df_gg = ROOT.RDataFrame("Events", "/eos/user/t/toakhter/HH_bbtautau_Run3/anaTuples/dev/Run2_2016/GluGluToRadionToHHTo2B2Tau_M-300/nanoHTT_0.root")
df_VBF = ROOT.RDataFrame("Events", "/eos/user/t/toakhter/HH_bbtautau_Run3/anaTuples/dev/Run3_2022/VBFHHto2B2Tau_CV_1_C2V_1_C3_1/nano_0.root")


# In[ ]:


print("number of events: gg: ", df_gg.Count().GetValue())
print("number of events: VBF: ", df_VBF.Count().GetValue())


# In[ ]:


def denominator_tau_func(df):
    
    df = df.Define(f"denominator_tau", f"""
                     (tau1_charge * tau2_charge)==-1 &&
                     tau2_idDeepTau2018v2p5VSjet >= 5 &&
                     genLepton2_legType == 3 &&
                     genLepton1_pt > 20 && genLepton2_pt > 20 &&
                     abs(genLepton1_eta) < 2.5 && abs(genLepton2_eta) < 2.5
                """)
    print("denominator_tau", df.Filter("denominator_tau").Count().GetValue())
    return df

def numerator_tau_func(df):

    for idx in range(1,3):
        df = df.Define(f"matchedRecoGenLep{idx}", f"tau{idx}_gen_kind == genLepton{idx}_legType+2")
        print(idx, df.Filter(f"matchedRecoGenLep{idx}").Count().GetValue())

    df = df.Define("matchedRecoGenLeptons", "matchedRecoGenLep1 && matchedRecoGenLep2")
    print("matchedRecoGenLeptons", df.Filter("matchedRecoGenLeptons").Count().GetValue())
    
    df = df.Define(f"correctTauReco", f"tau1_legType+2 == tau1_gen_kind && tau2_legType+2 == tau2_gen_kind")
    print("correctTauReco", df.Filter("correctTauReco").Count().GetValue())
    
    df = df.Define(f"numerator_tau", "denominator_tau && matchedRecoGenLeptons && correctTauReco")
    print("numerator tau", df.Filter("numerator_tau").Count().GetValue())
    
    return df

def tau_purity(df, purity):

    df = denominator_tau_func(df)
    df = numerator_tau_func(df)
    
    num = df.Filter("numerator_tau").Count().GetValue()
    denom = df.Filter("denominator_tau").Count().GetValue()
    
    print("denominator", denom)
    print("numerator", num)
    purity = num / denom
    print("purity", purity)
    
    return purity

def denominator_b_func(df):
    
    df = df.Define(f"denominator_b", f"numerator_tau && nJetFromGenHbb == 2")
    #df = df.Define(f"denominator_b", f"correctTauReco && nJetFromGenHbb == 2")
    print("denominator_b", df.Filter("denominator_b").Count().GetValue())
    
    return df

def numerator_b_func(df):

    df = df.Define(f"numerator_b", f"denominator_b && b1_fromGenHbb && b2_fromGenHbb")
    print("numerator b", df.Filter("numerator_b").Count().GetValue())
    
    return df

def b_purity(df, purity):

    df = denominator_tau_func(df)
    df = numerator_tau_func(df)
    df = denominator_b_func(df)
    df = numerator_b_func(df)
    
    num = df.Filter("numerator_b").Count().GetValue()
    denom = df.Filter("denominator_b").Count().GetValue()
    
    print("denominator", denom)
    print("numerator", num)
    purity = num / denom
    print("purity", purity)
    
    return purity


# In[ ]:


#file_gg = "/eos/user/t/toakhter/HH_bbtautau_Run3/anaTuples/dev/Run3_2022/GluGlutoHHto2B2Tau_kl-1p00_kt-1p00_c2-0p00/nano_0.root"
file_gg = "/eos/user/t/toakhter/HH_bbtautau_Run3/anaTuples/dev/Run2_2016/GluGluToRadionToHHTo2B2Tau_M-300/nanoHTT_0.root"
df_gg = ROOT.RDataFrame("Events", file_gg)

tau_purity_value = tau_purity(df_gg, purity=0)
print("tau purity", tau_purity_value)

print("\n\n\n")

b_purity_value = b_purity(df_gg, purity=0)
print("b purity", b_purity_value)


# In[ ]:


file_VBF = "/eos/user/t/toakhter/HH_bbtautau_Run3/anaTuples/dev/Run3_2022/VBFHHto2B2Tau_CV_1_C2V_1_C3_1/nano_0.root"
df_VBF = ROOT.RDataFrame("Events", file_VBF)

tau_purity_value_VBF = tau_purity(df_VBF, purity=0)
print("tau purity", tau_purity_value_VBF)

print("\n\n\n")

b_purity_value_VBF = b_purity(df_VBF, purity=0)
print("b purity", b_purity_value_VBF)


# In[ ]:




