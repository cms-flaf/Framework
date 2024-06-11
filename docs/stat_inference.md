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
