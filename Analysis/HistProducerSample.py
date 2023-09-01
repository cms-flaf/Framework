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
    parser.add_argument('--test', required=False, type=bool, default=False)
    parser.add_argument('--remove-files', required=False, type=bool, default=False)
    parser.add_argument('--dataset', required=True, type=str)
    parser.add_argument('--sampleConfig', required=True, type=str)
    args = parser.parse_args()

    sample_cfg_dict = {}
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)

    if not os.path.isdir(args.histDir):
        os.makedirs(args.histDir)

    # 1 list files :

    samples_files = {}
    tmpDir = os.path.join(args.histDir, args.dataset,'tmp')

    sample_type = ""
    if(args.dataset=='data'): sample_type = 'data'
    else: sample_type = sample_cfg_dict[args.dataset]['sampleType']

    all_dir_vars = os.listdir(tmpDir)

    for dir_var in all_dir_vars:
        txtFile  = os.path.join(tmpDir, dir_var, "filesToHAdd.txt")
        input_files_to_write = []
        input_files_to_read = []
        for root, dirs, files in os.walk(os.path.join(tmpDir, dir_var)):
            for file in files:
                if file.endswith('.root') and not file.startswith('.'):
                    if os.path.join(root, file) not in input_files_to_read:
                        input_files_to_write.append(os.path.join(root, file))
        with open(txtFile, 'w') as inputFileTxt:
            for input_line in input_files_to_write:
                inputFileTxt.write(input_line+'\n')
        with open(txtFile, 'r') as inputtxtFile:
            input_files_to_read = inputtxtFile.read().splitlines()
            if len(input_files_to_read) == 0:
                raise RuntimeError(f"no input files found for {tmpDir}, {dir_var}")

        outFileName = f'{args.histDir}/{args.dataset}.root'
        hadd_str = f'hadd -f209 -j -O {outFileName} '
        hadd_str += ' '.join(f for f in input_files_to_read)
        if len(input_files_to_read) > 1:
            sh_call([hadd_str], True)
        else:
            shutil.copy(input_files_to_read[0],outFileName)
        if os.path.exists(outFileName) and args.remove_files:
            for histFile in input_files_to_read:
                if args.test : print(histFile)
                if histFile == outFileName: continue
                os.remove(histFile)
    if args.remove_files:
        shutil.rmtree(tmpDir)
