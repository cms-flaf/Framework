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
    conf_interval = [round(x-purity, 4) for x in proportion_confint(num, denom, 1-0.68, 'beta')]

    print("purity", purity, "confidence interval (68%): ", conf_interval)
        
    return round(purity, 4), conf_interval

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
    conf_interval = [round(x-purity, 4) for x in proportion_confint(num, denom, 1-0.68, 'beta')]

    print("purity", purity, "confidence interval (68%): ", conf_interval)
        
    return round(purity, 4), conf_interval


if __name__ == "__main__":
    import argparse
    import os, sys

    parser = argparse.ArgumentParser()
    parser.add_argument('--inFile', type=str)
    parser.add_argument('--outFile', type=str)
    args = parser.parse_args()

    import ROOT
    from statsmodels.stats.proportion import proportion_confint

    if os.path.exists(args.outFile):
        os.remove(args.outFile)
    outDir = os.path.dirname(args.outFile)
    if len(outDir) > 0 and not os.path.exists(outDir):
        os.makedirs(outDir)

    orig_stdout = sys.stdout
    f = open(args.outFile, 'w')
    sys.stdout = f

    df = ROOT.RDataFrame("Events", args.inFile)

    print("======tau purity")
    tau_purity_value = tau_purity(df, purity=0)
    print("======(tau purity, [confidence interval (68%)])", tau_purity_value)

    print("\n=========================\n")

    print("======b purity")
    b_purity_value = b_purity(df, purity=0)
    print("======(b purity, [confidence interval (68%)])", b_purity_value)

    sys.stdout = orig_stdout
    f.close()