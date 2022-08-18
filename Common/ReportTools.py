import ROOT
def SaveReport(report,PrintOut=False):
    cuts = [c for c in report] 
    hist = ROOT.TH1D("Report","Report", len(cuts)+1, 0, len(cuts)+1)
    hist.GetXaxis().SetBinLabel(1, "Initial")
    hist.SetBinContent(1, cuts[0].GetAll())
    for c_id, cut in enumerate(cuts):       
        hist.SetBinContent(c_id+2, cut.GetPass())
        hist.GetXaxis().SetBinLabel(c_id+2, cut.GetName())
        if(PrintOut): 
            print(cut.GetName())
            print(cut.GetPass())
            print(cut.GetAll())
            print(cut.GetEff())    
    return hist
    

