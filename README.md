# HH -> bbtautau Framework

## How to install
```sh
git clone git@github.com:cms-hh-bbtautau/Framework.git
```

## Loading environment
Following command activates the framework environment:
```sh
source env.sh
```
## Run nanoAOD production
```sh
law run CreateNanoSkims --version prod_v1 --periods 2016,2016APV,2017,2018 --ignore-missing-samples True
```
## Run training skim ntuple production
```sh
python Studies/HHBTag/CreateTrainingSkim.py --inFile $CENTRAL_STORAGE/prod_v1/nanoAOD/2018/GluGluToBulkGravitonToHHTo2B2Tau_M-350.root --outFile output/skim.root --mass 350 --sample GluGluToBulkGraviton --year 2018 >& EventInfo.txt
python Common/SaveHisto.txt --inFile $CENTRAL_STORAGE/prod_v1/nanoAOD/2018/GluGluToBulkGravitonToHHTo2B2Tau_M-350.root --outFile output/skim.root
```

