import ROOT
import sys
import os
import math
import shutil
import json
import time
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

from Analysis.HistHelper import *
from Analysis.hh_bbtautau import *


if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--inFile', required=True)
    parser.add_argument('--outFile', required=True)
    parser.add_argument('--year', required=True)
    parser.add_argument('--var', required=False, type=str, default='tau1_pt')
    #parser.add_argument('--remove-files', required=False, type=bool, default=False)
    args = parser.parse_args()


all_histnames = {}

for process in processes:
    all_histnames[process] = process
    for unc_old in uncReNames.keys():
        new_unc = uncReNames[unc_old].format(args.year)
        #print(unc_old.split('_'))
        #print(args.year)
        for scale in ['Up','Down']:
            all_histnames[f"{process}_{unc_old}_{args.year}_{scale}"] = f"{process}_{new_unc}{scale}"
            all_histnames[f"{process}_{unc_old}_{scale}"] = f"{process}_{new_unc}{scale}"

#print(all_histnames.keys())
inFile = ROOT.TFile.Open(args.inFile, "READ")
outFile = ROOT.TFile.Open(args.outFile, "RECREATE")
channels =[str(key.GetName()) for key in inFile.GetListOfKeys()]
for channel in channels:
    dir_0 = inFile.Get(channel)
    dir_1 = dir_0.Get("OS_Iso")
    keys_categories = [str(key.GetName()) for key in dir_1.GetListOfKeys()]
    for cat in keys_categories:
        dir_2= dir_1.Get(cat)
        for key_hist in dir_2.GetListOfKeys():
            key_name = key_hist.GetName()
            if key_name not in all_histnames.keys():
                print(f"{key_name} not in all_histnames keys")
                continue
            #key_hist = dir_1.Get(key_name)
            obj = key_hist.ReadObj()
            obj.SetDirectory(0)
            dirStruct = (channel, cat)
            dir_name = '/'.join(dirStruct)
            dir_ptr = mkdir(outFile,dir_name)
            obj.SetTitle(all_histnames[key_name])
            obj.SetName(all_histnames[key_name])
            dir_ptr.WriteTObject(obj, all_histnames[key_name], "Overwrite")
outFile.Close()
inFile.Close()

