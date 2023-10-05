import ROOT
import sys
import os
import math
import shutil
from RunKit.sh_tools import sh_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *

if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--histDir', required=True, type=str)
    parser.add_argument('--remove-files', required=False, type=bool, default=False)
    parser.add_argument('--hists', required=False, type=str, default='bbtautau_mass,dR_tautau,tautau_m_vis,tau1_pt')
    parser.add_argument('--file-name-pattern', required=False, type=str, default='all_histograms')
    parser.add_argument('--uncConfig', required=True, type=str)

    args = parser.parse_args()

    # 1 list files :

    all_vars = args.hists.split(',')
    uncNameTypes = GetUncNameTypes(unc_cfg_dict) + ['Central']
    all_files = {}
    for var in all_vars:
        all_files[var] = []
        for uncName in uncNameTypes:
            all_files[var].append(f"{args.histDir}/{args.file_name_pattern}_{var}_{uncName}.root")
        outFileNameFinal = f'{args.histDir}/{args.file_name_pattern}_{var}.root'
        hadd_str = f'hadd -f209 -j -O {outFileNameFinal} '
        hadd_str += ' '.join(f for f in all_files[var])
        if len(all_files[var]) > 1:
            sh_call([hadd_str], True)
        else:
            shutil.copy(all_files[var][0],outFileNameFinal)
        if os.path.exists(outFileNameFinal) and args.remove_files:
            for histFile in all_files[var]:
                if args.test : print(histFile)
                if histFile == outFileNameFinal: continue
                os.remove(histFile)