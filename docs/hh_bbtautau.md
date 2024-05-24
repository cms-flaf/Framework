# Simple commands

## DeepTau 2p1

### AnaCache Production
```sh
year=2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run InputFileTask  --period Run2_${year} --version ${dir}
year=2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run AnaCacheTask  --period Run2_${year} --workflow htcondor --version ${dir} --transfer-logs
```

### AnaTuple Production (AFTER AnaCacheTask)
```sh
year=2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run InputFileTask  --period Run2_${year} --version ${dir}
year=2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run AnaTupleTask --period Run2_${year} --version ${dir} --workflow htcondor --transfer-logs
year=2016; dir=v8_deepTau2p1_onlyTauTau_HTT; mkdir /eos/user/v/vdamante/HH_bbtautau_resonant_Run2/${dir}/Run2_${year}/data
year=2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run DataMergeTask --period Run2_${year} --version ${dir} --workflow htcondor --transfer-logs
```

### AnaCacheTuple Production (AFTER AnaTupleTask)
```sh
year=2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run AnaCacheTupleTask --period Run2_${year} --version ${dir} --workflow htcondor --transfer-logs
```

### Histograms Production (AFTER AnaTupleTask but NOT NECESSAIRLY AnaCacheTupleTask)
```sh
year=2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run HistProducerFileTask --period Run2_${year} --version ${dir} --workflow htcondor --transfer-logs
year=2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run HistProducerSampleTask --period Run2_${year} --version ${dir} --workflow htcondor --transfer-logs
year=2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run  MergeTask --period Run2_${year}  --version ${dir}  --workflow htcondor --transfer-logs
```

**Work in progress**
```sh
year=2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run  HistRebinnerTask --period Run2_${year}  --version ${dir}  --workflow htcondor --transfer-logs #This does something only for KinFit_m, currently
year=2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run  HaddMergedTask --period Run2_${year}  --version ${dir}  --workflow htcondor --transfer-logs
```

## DeepTau 2p5

### AnaCache Production
```sh
year=2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run InputFileTask  --period Run2_${year} --version ${dir}
year=2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run AnaCacheTask  --period Run2_${year} --workflow htcondor --version ${dir} --transfer-logs --customisations deepTauVersion=2p5
```

### AnaTuple Production (AFTER AnaCacheTask)
```sh
year=2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run InputFileTask  --period Run2_${year} --version ${dir}
year=2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run AnaTupleTask --period Run2_${year} --version ${dir} --workflow htcondor --transfer-logs --customisations deepTauVersion=2p5
year=2016; dir=v8_deepTau2p5_onlyTauTau_HTT; mkdir /eos/user/v/vdamante/HH_bbtautau_resonant_Run2/${dir}/Run2_${year}/data
year=2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run DataMergeTask --period Run2_${year} --version ${dir} --workflow htcondor --transfer-logs --customisations deepTauVersion=2p5 # not sure it's needed in this step but I add it usually
```

### AnaCacheTuple Production (AFTER AnaTupleTask)
```sh
year=2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run AnaCacheTupleTask --period Run2_${year} --version ${dir} --workflow htcondor --transfer-logs --customisations deepTauVersion=2p5
year=2016; dir=v8_deepTau2p5_onlyTauTau_HTT; mkdir -p /eos/home-k/kandroso/cms-hh-bbtautau/anaCache/Run2_${year}/data/${dir}
year=2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run DataCacheMergeTask --period Run2_${year} --version ${dir} --workflow htcondor --transfer-logs --customisations deepTauVersion=2p5
```

### Histograms Production (AFTER AnaTupleTask but NOT NECESSAIRLY AnaCacheTupleTask)
```sh
year=2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run HistProducerFileTask --period Run2_${year} --version ${dir} --workflow htcondor --transfer-logs
year=2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run HistProducerSampleTask --period Run2_${year} --version ${dir} --workflow htcondor --transfer-logs
year=2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run  MergeTask --period Run2_${year}  --version ${dir}  --workflow htcondor --transfer-logs
```

**Work in progress**
```sh
year=2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run  HistRebinnerTask --period Run2_${year}  --version ${dir}  --workflow htcondor --transfer-logs #This does something only for KinFit_m, currently
year=2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run  HaddMergedTask --period Run2_${year}  --version ${dir}  --workflow htcondor --transfer-logs
```

## Tips
1. For local production switch from `--workflow htcondor` to `--workflow local`
1. To produce specific branches add `--branches X1,X2,...`, where `Xi` are the branch numbers