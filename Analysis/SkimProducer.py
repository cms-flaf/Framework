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
  parser.add_argument('--outputDir', required=True, type=str)
  parser.add_argument('--centralFile', required=False, type=str, default='Events.root')
  parser.add_argument('--recreateOutDir', required=False, type=bool, default=True)
  parser.add_argument('--test', required=False, type=bool, default=False)
  args = parser.parse_args()
  all_files = os.listdir(args.inputDir)
  central_idx = all_files.index(args.centralFile)
  other_files = all_files.pop(central_idx)
  if args.recreateOutDir:
    if os.path.isdir(args.outputDir):
      shutil.rmtree(args.outputDir)
  if not os.path.isdir(args.outputDir):
    os.makedirs(args.outputDir)
  syst_files_to_merge = []
  syst_trees = []
  k=0
  for systFile in all_files:
    if args.test and k>2 : continue
    k+=1
    outFileName = os.path.join(args.outputDir, systFile.strip('.root'))
    inFileCentralName = os.path.join(args.inputDir, args.centralFile)
    inFileShiftedName = os.path.join(args.inputDir, systFile)
    treeName = systFile.strip('.root')
    cmd = f"""python3 /afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/Analysis/SkimEvents.py --inFileCentral {inFileCentralName} --inFileShifted {inFileShiftedName} --outFile {outFileName} --treeName {treeName}"""
    sh_call(cmd, True)
  for file_syst in os.listdir(args.outputDir):
    syst_files_to_merge.append(f'{args.outputDir}/{file_syst}')
  finalFile = os.path.join(args.outputDir, args.centralFile)
  if os.path.exists(finalFile):
    os.remove(finalFile)
  hadd_str = f'hadd -k -fk {finalFile} '
  hadd_str += ' '.join(f for f in syst_files_to_merge)
  print(hadd_str)
  sh_call([hadd_str], True)
  for syst_file in syst_files_to_merge:
    os.remove(syst_file)