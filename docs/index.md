# FLAF

FLAF - Flexible LAW-based Analysis Framework.
Task workflow managed is done via [LAW](https://github.com/riga/law) (Luigi Analysis Framework).

## How to install
1. Setup ssh keys:
  - On GitHub [settings/keys](https://github.com/settings/keys)
  - On CERN GitLab [profile/keys](https://gitlab.cern.ch/-/profile/keys)

1. Clone the repository:
  ```sh
  git clone --recursive git@github.com:cms-flaf/Framework.git FLAF
  ```

## How to load environment
Following command activates the framework environment:
```sh
source env.sh
```

## How to run limits
1. As a temporary workaround, if you want to run multiplie commands, to avoid delays to load environment each time run:
  ```sh
  cmbEnv /bin/zsh # or /bin/bash
  ```
  Alternatively add `cmbEnv` in front of each command. E.g.
  ```sh
  cmbEnv python3 -c 'print("hello")'
  ```

1. Create datacards.
  ```sh
  python3 StatInference/dc_make/create_datacards.py --input PATH_TO_SHAPES  --output PATH_TO_CARDS --config PATH_TO_CONFIG
  ```
  Available configurations:
    - For X->HH>bbtautau Run 2: [StatInference/config/x_hh_bbtautau_run2.yaml](https://github.com/cms-flaf/StatInference/blob/main/config/x_hh_bbtautau_run2.yaml)
    - For X->HH->bbWW Run 3: [StatInference/config/x_hh_bbww_run3.yaml](https://github.com/cms-flaf/StatInference/blob/main/config/x_hh_bbww_run3.yaml)

1. Run limits.
  ```sh
  law run PlotResonantLimits --version dev --datacards 'PATH_TO_CARDS/*.txt' --xsec fb --y-log
  ```
  Hints:
    - use `--workflow htcondor` to submit on HTCondor (by default it runs locally)
    - add `--remove-output 4,a,y` to remove previous output files
    - add `--print-status 0` to get status of the workflow (where `0` is a depth). Useful to get the output file name.
    - for more details see [cms-hh inference documentation](https://cms-hh.web.cern.ch/tools/inference/)

## How to run nanoAOD->nanoAOD skims production
```sh
law run CreateNanoSkims --version prod_v1 --periods 2016,2016APV,2017,2018 --ignore-missing-samples True
```
## How to run HHbtag training skim ntuple production
```sh
python Studies/HHBTag/CreateTrainingSkim.py --inFile $CENTRAL_STORAGE/prod_v1/nanoAOD/2018/GluGluToBulkGravitonToHHTo2B2Tau_M-350.root --outFile output/skim.root --mass 350 --sample GluGluToBulkGraviton --year 2018 >& EventInfo.txt
python Common/SaveHisto.txt --inFile $CENTRAL_STORAGE/prod_v1/nanoAOD/2018/GluGluToBulkGravitonToHHTo2B2Tau_M-350.root --outFile output/skim.root
```
## How to run Histogram production
Please, see the file all_commands.txt (to be updated)