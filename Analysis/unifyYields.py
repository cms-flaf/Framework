import ROOT
import sys
import os
import math
import pandas as pd
import shutil
from RunKit.sh_tools import sh_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *
from Analysis.hh_bbtautau import *

if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--histDir', required=True, type=str)
    parser.add_argument('--hists', required=True, type=str)
    parser.add_argument('--sampleConfig', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--onlyCentral', required=False, type=bool, default=False)

    args = parser.parse_args()
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)

    all_vars = args.hists.split(',')
    #print(all_samples_list)
    categories = list(sample_cfg_dict['GLOBAL']['categories'])
    QCDregions = list(sample_cfg_dict['GLOBAL']['QCDRegions'])
    channels = list(sample_cfg_dict['GLOBAL']['channelSelection'])
    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    new_columns = []
    uncNameTypes = GetUncNameTypes(unc_cfg_dict)
    for var in all_vars:
        for channel in channels:
            for cat in categories:
                fileNameCentral = f'output/csv_files/{var}/{channel}/{cat}/{channel}_{cat}_Central_Central_{var}.csv'
                df1 = pd.read_csv(fileNameCentral, sep=';')
                for uncNameType in uncNameTypes:
                    #print(f".. and uncNameType {uncNameType}")
                    for scale in scales:
                        fileNameScaled = f'output/csv_files/{var}/{channel}/{cat}/{channel}_{cat}_{uncNameType}_{scale}_{var}.csv'
                        df2 = pd.read_csv(fileNameScaled, sep=';')
                        df2.rename(columns={'yield': f'yield_{uncNameType}{scale}'}, inplace=True)
                        df1 = pd.concat([df1, df2[f'yield_{uncNameType}{scale}']], axis=1)
                finalName = f'output/csv_files/{var}/{channel}/{cat}/{channel}_{cat}_Total.csv'
                df1.to_csv(finalName, index=False,encoding='utf-8-sig', decimal=',', sep=';')
    # remove files

    for var in all_vars:
        for channel in channels:
            for cat in categories:
                fileNameCentral = f'output/csv_files/{var}/{channel}/{cat}/{channel}_{cat}_Central_Central_{var}.csv'
                os.remove(fileNameCentral)
                for uncNameType in uncNameTypes:
                    #print(f".. and uncNameType {uncNameType}")
                    for scale in scales:
                        fileNameScaled = f'output/csv_files/{var}/{channel}/{cat}/{channel}_{cat}_{uncNameType}_{scale}_{var}.csv'
                        os.remove(fileNameScaled)
