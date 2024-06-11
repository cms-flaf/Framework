# Simple commands

## DeepTau 2p1

### AnaCache Production
```sh
era=Run2_2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run InputFileTask  --period ${era} --version ${dir}
era=Run2_2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run AnaCacheTask  --period ${era} --workflow htcondor --version ${dir} --transfer-logs
```

### AnaTuple Production (AFTER AnaCacheTask)
```sh
era=Run2_2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run InputFileTask  --period ${era} --version ${dir}
era=Run2_2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run AnaTupleTask --period ${era} --version ${dir} --workflow htcondor --transfer-logs
era=Run2_2016; dir=v8_deepTau2p1_onlyTauTau_HTT; mkdir /eos/user/v/vdamante/HH_bbtautau_resonant_Run2/${dir}/Run2_${era}/data
era=Run2_2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run DataMergeTask --period ${era} --version ${dir} --workflow htcondor --transfer-logs
```

### AnaCacheTuple Production (AFTER AnaTupleTask)
```sh
era=Run2_2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run AnaCacheTupleTask --period Run2_${era} --version ${dir} --workflow htcondor --transfer-logs
```

### Histograms Production (AFTER AnaTupleTask but NOT NECESSAIRLY AnaCacheTupleTask)
```sh
era=Run2_2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run HistProducerFileTask --period Run2_${era} --version ${dir} --workflow htcondor --transfer-logs
era=Run2_2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run HistProducerSampleTask --period Run2_${era} --version ${dir} --workflow htcondor --transfer-logs
era=Run2_2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run  MergeTask --period Run2_${era}  --version ${dir}  --workflow htcondor --transfer-logs
```

**Work in progress**
```sh
era=Run2_2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run  HistRebinnerTask --period Run2_${era}  --version ${dir}  --workflow htcondor --transfer-logs #This does something only for KinFit_m, currently
era=Run2_2016; dir=v8_deepTau2p1_onlyTauTau_HTT; law run  HaddMergedTask --period Run2_${era}  --version ${dir}  --workflow htcondor --transfer-logs
```

## DeepTau 2p5

### AnaCache Production
```sh
era=Run2_2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run InputFileTask  --period Run2_${era} --version ${dir}
era=Run2_2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run AnaCacheTask  --period Run2_${era} --workflow htcondor --version ${dir} --transfer-logs --customisations deepTauVersion=2p5
```

### AnaTuple Production (AFTER AnaCacheTask)
```sh
era=Run2_2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run InputFileTask  --period Run2_${era} --version ${dir}
era=Run2_2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run AnaTupleTask --period Run2_${era} --version ${dir} --workflow htcondor --transfer-logs --customisations deepTauVersion=2p5
era=Run2_2016; dir=v8_deepTau2p5_onlyTauTau_HTT; mkdir /eos/user/v/vdamante/HH_bbtautau_resonant_Run2/${dir}/Run2_${era}/data
era=Run2_2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run DataMergeTask --period Run2_${era} --version ${dir} --workflow htcondor --transfer-logs --customisations deepTauVersion=2p5 # not sure it's needed in this step but I add it usually
```

### AnaCacheTuple Production (AFTER AnaTupleTask)
```sh
era=Run2_2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run AnaCacheTupleTask --period Run2_${era} --version ${dir} --workflow htcondor --transfer-logs --customisations deepTauVersion=2p5
era=Run2_2016; dir=v8_deepTau2p5_onlyTauTau_HTT; mkdir -p /eos/home-k/kandroso/cms-hh-bbtautau/anaCache/Run2_${era}/data/${dir}
era=Run2_2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run DataCacheMergeTask --period Run2_${era} --version ${dir} --workflow htcondor --transfer-logs --customisations deepTauVersion=2p5
```

### Histograms Production (AFTER AnaTupleTask but NOT NECESSAIRLY AnaCacheTupleTask)
```sh
era=Run2_2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run HistProducerFileTask --period Run2_${era} --version ${dir} --workflow htcondor --transfer-logs
era=Run2_2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run HistProducerSampleTask --period Run2_${era} --version ${dir} --workflow htcondor --transfer-logs
era=Run2_2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run  MergeTask --period Run2_${era}  --version ${dir}  --workflow htcondor --transfer-logs
```

**Work in progress**
```sh
era=Run2_2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run  HistRebinnerTask --period Run2_${era}  --version ${dir}  --workflow htcondor --transfer-logs #This does something only for KinFit_m, currently
era=Run2_2016; dir=v8_deepTau2p5_onlyTauTau_HTT; law run  HaddMergedTask --period Run2_${era}  --version ${dir}  --workflow htcondor --transfer-logs
```

## Tips
1. For local production switch from `--workflow htcondor` to `--workflow local`
1. To produce specific branches add `--branches X1,X2,...`, where `Xi` are the branch numbers

## How to run HHbtag training skim ntuple production
```sh
python Studies/HHBTag/CreateTrainingSkim.py --inFile $CENTRAL_STORAGE/prod_v1/nanoAOD/2018/GluGluToBulkGravitonToHHTo2B2Tau_M-350.root --outFile output/skim.root --mass 350 --sample GluGluToBulkGraviton --year 2018 >& EventInfo.txt
python Common/SaveHisto.txt --inFile $CENTRAL_STORAGE/prod_v1/nanoAOD/2018/GluGluToBulkGravitonToHHTo2B2Tau_M-350.root --outFile output/skim.root
```
