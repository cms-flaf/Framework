
def select(df):
     # #MET Filters double check ?
     df = df.Filter('((lep1_pt > 15 && lep2_pt > 15) && (abs(lep1_eta) < 2.5 && abs(lep2_eta) < 2.5) && (lep1_pt > 25 || lep2_pt > 25))')  #tighter pT/eta cut on lepton to match sync excercise #6359
     df = df.Filter('(lep1_charge*lep2_charge) < 0') # leptons opposite charge #6284
     #df = df.Define ('lep1tight',' (lep1_Muon_tightId==1 || lep1_Electron_mvaIso_WP90==1)')
     #df = df.Define ('lep2tight',' (lep2_Muon_tightId==1 || lep2_Electron_mvaIso_WP90==1)')
     #df = df.Filter("lep1tight && lep2tight ")
     df = df.Define('lep1_p4', 'ROOT::Math::PtEtaPhiMVector(lep1_pt, lep1_eta, lep1_phi, lep1_mass)')
     df = df.Define('lep2_p4', 'ROOT::Math::PtEtaPhiMVector(lep2_pt, lep2_eta, lep2_phi, lep2_mass)')
     df = df.Define('m_ll', '(lep1_p4 + lep2_p4).mass()')
     df = df.Filter('m_ll > 12') #6035
     #df = df.Filter('(lep1_type==lep2_type && (abs(m_ll - 91.1880)) > 10)') #ZResonanceVeto #2927
     #HLT
     df = df.Define ('HLT_Mu','(HLT_singleIsoMu || HLT_singleIsoMuHT || HLT_diMuon)')
     df = df.Define ('HLT_El','(HLT_singleIsoEleHT || HLT_singleEleWpTight || HLT_diElec )')
     df = df.Define ('HLT_EMU','(HLT_EMu || HLT_MuE )')
     df = df.Define ('HLT',"(HLT_Mu || HLT_El || HLT_EMU )")
     df = df.Filter ('HLT == 1') #single or double muon HLT should fire in event #5927
     #ak4 jet
     df = df.Define('ak4_sel','centralJet_pt > 25 && abs(centralJet_eta) < 2.4 ')
     df = df.Define('ak4_sel_batgged', 'ak4_sel && centralJet_btagPNetB > 0.2450')
     df = df.Define('Nak4_sel','centralJet_pt[ak4_sel].size()')
     df = df.Define('Nak4_sel_batgged','centralJet_pt[ak4_sel_batgged].size()')
     # #ak8 jet
     df = df.Define('Fatjet_tau21','  (SelectedFatJet_tau2 / SelectedFatJet_tau1)')
     df = df.Define('Fatjet_sel','(abs(SelectedFatJet_eta) < 2.4 && SelectedFatJet_msoftdrop < 210 && SelectedFatJet_SubJet1_pt > 20  && SelectedFatJet_SubJet2_pt > 20 && abs(SelectedFatJet_SubJet1_eta) < 2.4 && abs(SelectedFatJet_SubJet2_pt) < 2.4 && Fatjet_tau21 <= 0.75 )')
     df = df.Define('Fatjet_sel_btagged','Fatjet_sel && ((SelectedFatJet_SubJet1_btagDeepB > 0.4184 && SelectedFatJet_SubJet1_pt > 30) || (SelectedFatJet_SubJet2_btagDeepB > 0.4184 && SelectedFatJet_SubJet2_pt > 30))')
     df = df.Define('NSelectedFatjet','SelectedFatJet_pt[Fatjet_sel].size()')
     df = df.Define('NSelectedFatjet_batgged','SelectedFatJet_pt[Fatjet_sel].size()')
     # #Resolved
     df = df.Define('Resolved','Nak4_sel >= 1 && Nak4_sel_batgged >= 1 && NSelectedFatjet_batgged == 0 ')
     df = df.Define('Boosted', 'NSelectedFatjet_batgged >= 1')
     #df = df.Filter('Boosted || Resolved') #5388.
     return df