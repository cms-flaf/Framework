#!/bin/bash

python3 ScrapeSkimDatasets.py --input-dir /eos/cms/store/group/phys_higgs/HLepRare/skim_2024_v1/Run3_2022 --output temp_Run3_2022.yaml

python3 ScrapeXSDBManual.py --input temp_Run3_2022.yaml --input-xsec temp_Run3_2022_xsec.yaml

