import os
import shutil
import ROOT
import sys
ROOT.EnableThreadSafety()
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

from RunKit.sh_tools import sh_call
import Common.ConvertUproot as ConvertUproot


def getTreeName(systFile):
  treeName_elements = systFile.split('.')[0].split('_')
  good_treeName = []
  for element in treeName_elements:
    if element == 'nano' or element == 'Events' or element in [str(k) for k in range(0,10000)]: continue
    good_treeName.append(element)
  if args.test : print(f"good_treename elements are {good_treeName}")
  treeName = 'Events_'
  if len(good_treeName):
    treeName += '_'.join(good_treeName)
  return treeName

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument('--inputDir', required=True, type=str)
  parser.add_argument('--workingDir', required=True, type=str)
  parser.add_argument('--outputFile', required=True, type=str)
  parser.add_argument('--centralFile', required=False, type=str, default='nano.root')
  parser.add_argument('--recreateOutDir', required=False, type=bool, default=False)
  parser.add_argument('--test', required=False, type=bool, default=False)
  args = parser.parse_args()
  all_files = os.listdir(args.inputDir)
  central_idx = all_files.index(args.centralFile)
  other_files = all_files.pop(central_idx)

  inFileCentralName = os.path.join(args.inputDir, args.centralFile)
  if args.test: print('CentralFile = ', inFileCentralName)
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
    if args.test and k>=10 :
      print(systFile)
      continue
    inFileShiftedName = os.path.join(args.inputDir, systFile)
    if args.test: print('shifted file = ', inFileShiftedName)
    if args.test: print('index = ', k)
    treeName = getTreeName(systFile)
    if args.test : print(f"final TreeName is {treeName}")
    skimEventsPython = os.path.join(os.environ['ANALYSIS_PATH'], "AnaProd/SkimEvents.py")
    cmd = f"""python3 {skimEventsPython} --inFileCentral {inFileCentralName} --inFileShifted {inFileShiftedName} --outDir {args.workingDir} --treeName_out {treeName}"""
    if args.test : print(cmd)
    sh_call(cmd, True)
    k+=1
  for file_syst in os.listdir(args.workingDir) + [inFileCentralName]:
    if args.test : print(f"file_syst name is {file_syst}")
    inFileUproot = os.path.join(args.workingDir, file_syst)
    outFileUproot = os.path.join(args.workingDir, f'uproot_{file_syst}')
    if file_syst == inFileCentralName:
      inFileUproot = inFileCentralName
      outFileUproot = os.path.join(args.workingDir, f'uproot_{args.centralFile}')
    ConvertUproot.toUproot(inFileUproot,outFileUproot)
    if args.test : print(f"UprootFileName name is {outFileUproot}")
    syst_files_to_merge.append(outFileUproot)
  if args.test:
    for f in syst_files_to_merge:
      print(f)
  outFileName = os.path.join(args.workingDir, args.outputFile)
  if args.test : print(f'outFileName is {outFileName}')
  hadd_str = f'hadd -f209 -j -O {outFileName} '
  hadd_str += ' '.join(f for f in syst_files_to_merge)

  if args.test : print(f'hadd_str is {hadd_str}')
  sh_call([hadd_str], True)
  if args.test : print(f"outFileName is {outFileName}")
  if os.path.exists(outFileName):
    for file_syst in syst_files_to_merge:# + [outFileCentralName]:
      if args.test : print(file_syst)
      if file_syst == outFileName: continue
      os.remove(file_syst)