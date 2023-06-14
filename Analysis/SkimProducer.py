import os
import shutil
import ROOT
import sys
from RunKit.sh_tools import sh_call


if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])


if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument('--inputDir', required=True, type=str)
  parser.add_argument('--workingDir', required=True, type=str)
  parser.add_argument('--outputFile', required=True, type=str)
  parser.add_argument('--centralFile', required=False, type=str, default='Events.root')
  parser.add_argument('--recreateOutDir', required=False, type=bool, default=True)
  parser.add_argument('--test', required=False, type=bool, default=False)
  args = parser.parse_args()
  all_files = os.listdir(args.inputDir)
  central_idx = all_files.index(args.centralFile)
  other_files = all_files.pop(central_idx)
  if args.recreateOutDir:
    if os.path.exists(args.workingDir):
      shutil.rmtree(args.workingDir)
  if not os.path.isdir(args.workingDir):
    os.makedirs(args.workingDir)
  if os.path.exists(args.outputFile):
    os.remove(args.outputFile)
  syst_files_to_merge = []
  syst_trees = []
  k=0
  for systFile in all_files:
    if args.test and k>2 : continue
    k+=1
    inFileCentralName = os.path.join(args.inputDir, args.centralFile)
    if args.test: print(inFileCentralName)
    inFileShiftedName = os.path.join(args.inputDir, systFile)
    if args.test: print(inFileShiftedName)
    treeName = systFile.strip('.root')
    cmd = f"""python3 /afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/Analysis/SkimEvents.py --inFileCentral {inFileCentralName} --inFileShifted {inFileShiftedName} --outDir {args.workingDir} --treeName {treeName}"""
    if args.test : print(cmd)
    sh_call(cmd, True)
  for file_syst in os.listdir(args.workingDir):
    syst_files_to_merge.append(f'{args.workingDir}/{file_syst}')
  if args.test: print(f for f in syst_files_to_merge)
  hadd_str = f'hadd -f209 -j -O {args.outputFile} '
  hadd_str += ' '.join(f for f in syst_files_to_merge)
  if args.test: print(hadd_str)
  sh_call([hadd_str], True)
  shutil.rmtree(args.workingDir)