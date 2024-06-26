import ROOT
import sys
import os
import math
import shutil
from RunKit.run_tools import ps_call
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
    parser.add_argument('--suffix', required=False, type=str, default='')
    parser.add_argument('--period', required=False, type=str, default='Run2_2018')
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--wantBTag', required=False, type=bool, default=False)
    args = parser.parse_args()

    # 1 list files :

    btag_dir= "bTag_WP" if args.wantBTag else "bTag_shape"


    all_vars = args.hists.split(',')
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    uncNameTypes = GetUncNameTypes(unc_cfg_dict)
    uncNameTypes.extend(['Central'])
    all_files = {}
    for var in all_vars:
        inDir = os.path.join(args.histDir, 'all_histograms',var,btag_dir)
        all_files[var] = []
        for uncName in uncNameTypes:
            if uncName in uncs_to_exclude[args.period]: continue
            all_files[var].append(os.path.join(inDir, f"all_histograms_{var}_{uncName}{args.suffix}.root"))
            #inFileName=f'{args.inFileName}_{args.var}_{args.uncSource}{args.suffix}.root'
        outFileNameFinal = os.path.join(inDir, f'all_histograms_{var}{args.suffix}.root')
        hadd_str = f'hadd -f209 {outFileNameFinal} '
        hadd_str += ' '.join(f for f in all_files[var])
        print(hadd_str)
        if len(all_files[var]) > 1:
            ps_call([hadd_str], True)
        else:
            shutil.copy(all_files[var][0],outFileNameFinal)
        if os.path.exists(outFileNameFinal) and args.remove_files:
            for histFile in all_files[var]:
                if histFile == outFileNameFinal: continue
                os.remove(histFile)