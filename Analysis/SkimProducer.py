import os
import shutil
import ROOT
import sys



if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

def mergeRootFiles(root_files, outFileName, tree_names):#, snapshotOptions):
  snapshotOptions = ROOT.RDF.RSnapshotOptions()
  snapshotOptions.fOverwriteIfExists=True
  snapshotOptions.fLazy=True
  snapshotOptions.fMode="UPDATE"
  snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + 'LZ4')
  snapshotOptions.fCompressionLevel = 5
  snaps = []
  for singleFileName,systTreeName in zip(root_files, tree_names):
    singleFileDf = ROOT.RDataFrame("Events", singleFileName)
    snaps.append(singleFileDf.Snapshot(systTreeName, outFileName, singleFileDf.GetColumnNames(), snapshotOptions))
  ROOT.RDF.RunGraphs(snaps)

if __name__ == "__main__":
  import argparse
  '''
  parser = argparse.ArgumentParser()
  parser.add_argument('--inDir', required=True, type=str)
  parser.add_argument('--outDir', required=True, type=str)
  args = parser.parse_args()
  '''

  abs_path = os.path.join(os.environ['ANALYSIS_PATH'], 'output/')
  indir = os.path.join(abs_path, 'tmp_ggR_320')
  file_Central = 'Events.root'
  all_files = os.listdir(indir)
  central_idx = all_files.index(file_Central)
  other_files = all_files.pop(central_idx)
  outdir = os.path.join(abs_path,'outFiles')
  if os.path.isdir(outdir):
    shutil.rmtree(outdir)
  os.makedirs(outdir)

  for systFile in all_files:
    syst_files_to_merge = []
    syst_trees = []
    outFileName = os.path.join(outdir, systFile.strip('.root'))
    inFileCentralName = os.path.join(indir, file_Central)
    inFileShiftedName = os.path.join(indir, systFile)
    print(f'computing the skimEvents for {systFile}')
    cmd = f"""python3 /afs/cern.ch/work/v/vdamante/hhbbTauTauRes/prod/Framework/Analysis/SkimEvents.py --inFileCentral {inFileCentralName} --inFileShifted {inFileShiftedName} --outFile {outFileName} """
    os.system(cmd)
    syst_name = systFile.strip(".root").split("Events_")[1]
    for fileSyst in os.listdir(outdir) :
      if syst_name in fileSyst:
        syst_files_to_merge.append(f'{outdir}/{fileSyst}')
        syst_trees.append(fileSyst.strip('.root'))
    mergeRootFiles(syst_files_to_merge, f'{outdir}/Events_{syst_name}.root', syst_trees)
    print(f' merging {syst_files_to_merge}')
    for syst_file in syst_files_to_merge:
      os.remove(syst_file)
    print(f'finished to compute skimEvents for {systFile}')

  print(os.listdir(outdir))
