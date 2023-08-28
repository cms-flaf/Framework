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
    parser.add_argument('--inputDir', required=True, type=str)
    parser.add_argument('--histDir', required=True, type=str)
    parser.add_argument('--test', required=False, type=bool, default=False)
    parser.add_argument('--deepTauVersion', required=False, type=str, default='v2p1')
    parser.add_argument('--dataset', required=True, type=str)
    parser.add_argument('--compute_unc_variations', type=bool, default=False)
    parser.add_argument('--compute_rel_weights', type=bool, default=False)
    parser.add_argument('--histConfig', required=True, type=str)
    parser.add_argument('--sampleConfig', required=True, type=str)
    args = parser.parse_args()

    sample_cfg_dict = {}
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)

    if not os.path.isdir(args.histDir):
        os.makedirs(args.histDir)
    hist_cfg_dict = {}
    with open(args.histConfig, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    vars_to_plot = list(hist_cfg_dict.keys())

    # 1 list files :

    samples_files = {}
    tmpDir = os.path.join(args.histDir, args.dataset,'tmp')

    sample_type = ""
    if(args.dataset=='data'): sample_type = 'data'
    else: sample_type = sample_cfg_dict[args.dataset]['sampleType']

    all_files = os.listdir(os.path.join(args.inputDir, args.dataset))
    for single_file in all_files:
        inputFile = os.path.join(args.inputDir, args.dataset, single_file)
        if not os.path.isdir(tmpDir):
            os.makedirs(tmpDir)
        print(inputFile)
        histProducerFile = os.path.join(os.environ['ANALYSIS_PATH'], "Analysis/HistProducerFile.py")
        cmd_list = ['python3', histProducerFile, '--inFile', inputFile, '--outDir', tmpDir,
                    '--dataset', args.dataset, '--histConfig', args.histConfig]
        if args.compute_unc_variations:
            cmd_list.extend(['--compute_unc_variations', 'True'])
        if args.compute_rel_weights:
            cmd_list.extend(['--compute_rel_weights', 'True'])
        print(cmd_list)
        if args.test:
            cmd_list.extend(['--test','True'])
        sh_call(cmd_list,verbose=1)

    for var in vars_to_plot:
        if var not in samples_files.keys():
            samples_files[var] = {}
        hist_produced_dir = os.path.join(tmpDir, var)
        all_out_files = []
        for hist in os.listdir(hist_produced_dir):
            if args.dataset in hist:
                all_out_files.append(os.path.join(hist_produced_dir, hist))
        hist_out_dir = os.path.join(args.histDir, var)
        if not os.path.isdir(hist_out_dir):
            os.makedirs(hist_out_dir)
        outFileName = f'{hist_out_dir}/{args.dataset}.root'
        hadd_str = f'hadd -f209 -j -O {outFileName} '
        hadd_str += ' '.join(f for f in all_out_files)
        if len(all_out_files) > 1:
            sh_call([hadd_str], True)
            if os.path.exists(outFileName):
                for histFile in all_out_files:
                    if args.test : print(histFile)
                    if histFile == outFileName: continue
                    os.remove(histFile)
        else:
            shutil.move(all_out_files[0],outFileName)
    shutil.rmtree(os.path.join(args.histDir, args.dataset))
