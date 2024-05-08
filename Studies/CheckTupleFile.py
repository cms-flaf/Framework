import ROOT
import sys
import os
import math
import shutil
from RunKit.run_tools import ps_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

from Analysis.HistMerger import *

def getKeyList(infile):
    keys_str = [str(key.GetName()) for key in infile.GetListOfKeys()]
    return keys_str

if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--inDir', required=False, type=str, default='')
    args = parser.parse_args()
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")

    for inFile in os.listdir(args.inDir):
        inFile_tot = os.path.join(args.inDir, inFile)
        print(f"scanning {inFile_tot}")
        inFileRoot = ROOT.TFile.Open(inFile_tot, "READ")
        if inFileRoot.IsZombie() or len(getKeyList(inFileRoot))==0:
            inFileRoot.Close()
            print(f"removing {inFile_tot}")
            #os.remove(inFile_tot)
        if 'Events' not in getKeyList(inFileRoot):
            inFileRoot.Close()
            print(f"Events not present in {inFile_tot}, removing it")
            #os.remove(inFile_tot)
        inFileRoot.Close()
