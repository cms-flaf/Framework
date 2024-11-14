
def select(df):
     #MET Filters double check ?
     df = df.Filter('lep2_pt == 0 ')  #Exactly one tight leptons
     df = df.Define('lep1_leading_ptcut','((lep1_pt  > 25 && lep1_type == 2) || (lep1_pt  > 32 && lep1_type == 1))')  #HLT cut on lepton
     df = df.Filter('lep1_leading_ptcut')  #Exactly one tight leptons
     ## HLT
     df = df.Define ('HLT_Mu','(HLT_singleIsoMu || HLT_singleIsoMuHT)')
     df = df.Define ('HLT_El','(HLT_singleIsoEleHT || HLT_singleEleWpTight )')
     df = df.Filter ('((HLT_Mu && lep1_type == 2 ) || (HLT_El && lep1_type == 1))') #single lep HLT
     #ak4 jet
     df = df.Define('ak4_sel','centralJet_pt > 25 && abs(centralJet_eta) < 2.4 ')
     df = df.Define('ak4_sel_batgged', 'ak4_sel && centralJet_btagPNetB > 0.2450')
     df = df.Define('Nak4_sel','centralJet_pt[ak4_sel].size()')
     df = df.Define('Nak4_sel_batgged','centralJet_pt[ak4_sel_batgged].size()')
     #ak8 jet
     df = df.Define('Fatjet_tau21','  (SelectedFatJet_tau2 / SelectedFatJet_tau1)')
     df = df.Define('Fatjet_sel','(abs(SelectedFatJet_eta) < 2.4 && SelectedFatJet_msoftdrop < 210 && SelectedFatJet_SubJet1_pt > 20  && SelectedFatJet_SubJet2_pt > 20 && abs(SelectedFatJet_SubJet1_eta) < 2.4 && abs(SelectedFatJet_SubJet2_pt) < 2.4 && Fatjet_tau21 <= 0.75 )')
     df = df.Define('Fatjet_sel_btagged','Fatjet_sel && ((SelectedFatJet_SubJet1_btagDeepB > 0.4184 && SelectedFatJet_SubJet1_pt > 30) || (SelectedFatJet_SubJet2_btagDeepB > 0.4184 && SelectedFatJet_SubJet2_pt > 30))')
     df = df.Define('NSelectedFatjet','SelectedFatJet_pt[Fatjet_sel].size()')
     df = df.Define('NSelectedFatjet_batgged','SelectedFatJet_pt[Fatjet_sel].size()')
     #Resolved
     df = df.Define('Resolved','Nak4_sel >= 2 && Nak4_sel_batgged >= 1 && NSelectedFatjet_batgged == 0 ')
     df = df.Define('Boosted', 'NSelectedFatjet_batgged >= 1')
     df = df.Filter('Boosted || Resolved')
     return df