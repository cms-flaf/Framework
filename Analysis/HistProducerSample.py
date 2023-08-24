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
    parser.add_argument('--sample', required=True, type=str)
    args = parser.parse_args()

    sample_cfg_dict = {}
    sample_cfg = "config/samples_Run2_2018.yaml"
    with open(sample_cfg, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)

    if not os.path.isdir(args.histDir):
        os.makedirs(args.histDir)
    hist_cfg_dict = {}
    hist_cfg = "config/plot/histograms.yaml"
    with open(hist_cfg, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    vars_to_plot = list(hist_cfg_dict.keys())

    # 1 list files :

    samples_files = {}
    tmpDir = os.path.join(args.histDir, 'tmp')

    sample_type = sample_cfg_dict[args.sample]['sampleType']

    all_files = os.listdir(os.path.join(args.inputDir, args.sample))
    for single_file in all_files:
        inputFile = os.path.join(args.inputDir, args.sample, single_file)
        if not os.path.isdir(tmpDir):
            os.makedirs(tmpDir)
        print(inputFile)
        cmd_list = ['python3', 'Analysis/HistProducerFile.py', '--inFile', inputFile, '--outDir', tmpDir, '--dataset', args.sample, '--compute_unc_variations', 'True', '--compute_rel_weights', 'True']
        if args.test:
            cmd_list.extend(['--test','True'])
        sh_call(cmd_list,verbose=1)

    for var in vars_to_plot:
        if var not in samples_files.keys():
            samples_files[var] = {}
        hist_produced_dir = os.path.join(tmpDir, var)
        all_out_files = [ os.path.join(hist_produced_dir, hist) for hist in os.listdir(hist_produced_dir)]
        hist_out_dir = os.path.join(args.histDir, var)
        if not os.path.isdir(hist_out_dir):
            os.makedirs(hist_out_dir)
        outFileName = f'{hist_out_dir}/{args.sample}.root'
        hadd_str = f'hadd -f209 -j -O {outFileName} '
        hadd_str += ' '.join(f for f in all_out_files)
        if len(all_out_files) > 1:
            sh_call([hadd_str], True)
            if os.path.exists(outFileName):
                for histFile in all_out_files:# + [outFileCentralName]:
                    if args.test : print(histFile)
                    if histFile == outFileName: continue
                    os.remove(histFile)
        else:
            shutil.move(all_out_files[0],outFileName)
        os.rmdir(tmpDir)

'''
    for var in vars_to_plot:
        hist_out_dir = os.path.join(args.histDir, var)
        for hist_file in os.listdir(hist_out_dir):
            sample_name = hist_file.split('.')[0]
            sample_type = sample_cfg_dict[sample_name]['sampleType']
            if 'mass' in sample_cfg_dict[sample_name].keys():
                mass = sample_cfg_dict[sample_name]['mass']
                sample_type+=f'_M-{mass}'
            elif sample_type == 'QCD' : continue
            if sample_type not in samples_files[var].keys():
                samples_files[var][sample_type] = []
            samples_files[var][sample_type].append(hist_file)
    print(samples_files)
'''