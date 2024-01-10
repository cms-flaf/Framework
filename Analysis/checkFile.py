import ROOT
import sys
import os
import math
import shutil
from RunKit.sh_tools import sh_call
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
    parser.add_argument('--inFile', required=False, type=str, default='') 
    parser.add_argument('--sampleConfig', required=True, type=str)
    args = parser.parse_args()
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")

    sample_cfg_dict = {}
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f) 

    categories = ['res2b', 'res1b', 'inclusive', 'btag_shape', 'baseline'] #list(sample_cfg_dict['GLOBAL']['categories'])
    QCDregions = list(sample_cfg_dict['GLOBAL']['QCDRegions'])
    channels = list(sample_cfg_dict['GLOBAL']['channelSelection'])  
        
         
    print(f"scanning {args.inFile}")
    inFileRoot = ROOT.TFile.Open(args.inFile, "READ")
    if inFileRoot.IsZombie() or len(getKeyList(inFileRoot))==0:
        inFileRoot.Close()
        print(f"removing {args.inFile}")
        os.remove(args.inFile)
    if os.path.exists(args.inFile):
        #print(getKeyList(inFileRoot))
        for channel in channels:
            #print(channel)
            dir_0 = inFileRoot.Get(channel)
            if (not type(dir_0) is ROOT.TDirectoryFile):
                inFileRoot.Close()
                print(f"removing {args.inFile}")
                os.remove(args.inFile)
                break
            #print(getKeyList(dir_0))
            if len(getKeyList(dir_0))==0:
                inFileRoot.Close()
                print(f"removing {args.inFile}")
                os.remove(args.inFile)
                break
            #print(getKeyList(dir_0))
            for qcdRegion in QCDregions:
                #print(qcdRegion)
                dir_1 = dir_0.Get(qcdRegion)
                if (not type(dir_1) is ROOT.TDirectoryFile):
                    inFileRoot.Close()
                    print(f"removing {args.inFile}")
                    os.remove(args.inFile)
                    break
                if len(getKeyList(dir_1))==0:
                    inFileRoot.Close()
                    print(f"removing {args.inFile}")
                    os.remove(args.inFile)
                    break
                #print(getKeyList(dir_1))
                for cat in categories:
                    #print(cat)
                    dir_2 = dir_1.Get(cat)
                    if (not type(dir_2) is ROOT.TDirectoryFile):
                        inFileRoot.Close()
                        print(f"removing {args.inFile}")
                        os.remove(args.inFile)
                        break
                    if len(getKeyList(dir_2))==0:
                        inFileRoot.Close()
                        print(f"removing {args.inFile}")
                        os.remove(args.inFile)
                        break
                    #print(getKeyList(dir_2))
                    for key in dir_2.GetListOfKeys():
                        obj = key.ReadObj()
                        if not obj.IsA().InheritsFrom(ROOT.TH1.Class()): 
                            inFileRoot.Close()
                            print(f"removing {args.inFile}")
                            os.remove(args.inFile)
                            break 
                        obj.SetDirectory(0)
                        
        inFileRoot.Close() 
    