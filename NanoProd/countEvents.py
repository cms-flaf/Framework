import os
import numpy as np
import yaml

import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True
ROOT.gROOT.SetBatch(True)

import sys

def locate_files(folder, dataset_name=None):
    outputfiles = []
    for root, dirs, files in os.walk(folder):
        for file in files:
            if not dataset_name:
                outputfiles.append(os.path.join(root, file))
            else:
                subroot = root[len(folder):]
                if subroot.startswith("/"):
                    subroot = subroot[1:]
                if dataset_name not in subroot:
                    continue
                subroot = subroot.split("/")[0][len("crab_") + len(dataset_name):].split("_")
                if len(subroot) == 1:
                    outputfiles.append(os.path.join(root, file))
                elif subroot[1] == "recovery":
                    outputfiles.append(os.path.join(root, file))
    return outputfiles

def check_files(int_folder, final_folder, sample_file):
    with open(sample_file) as f:
        samples = yaml.load(f, yaml.Loader)

    dataset_names = os.listdir(final_folder)
    for dataset_name in dataset_names:
        miniaod_d = samples[dataset_name]['miniAOD'].split("/")[1]
        int_files = locate_files(os.path.join(int_folder + miniaod_d), dataset_name)
        final_files = locate_files(os.path.join(final_folder + dataset_name))

        df1 = ROOT.RDataFrame("Events", tuple(int_files))
        df2 = ROOT.RDataFrame("Events", tuple(final_files))

        sum_count = df2.Count()
        sampletype = getattr(samples[dataset_name], "sampleType", "None") 

        if sampletype != "data":
            columns1 = df1.AsNumpy(['event'])
            columns2 = df2.AsNumpy(['event'])
            sum1 = len(np.unique(columns1['event']))
            sum2 = len(np.unique(columns2['event']))
        else:
            columns1 = df1.AsNumpy(columns=['event', 'run', 'luminosityBlock'])
            columns1 = np.stack((columns1['event'], columns1['run'], columns1['luminosityBlock']), axis=-1)
            columns2 = df2.AsNumpy(columns=['event', 'run', 'luminosityBlock'])
            columns2 = np.stack((columns2['event'], columns2['run'], columns2['luminosityBlock']), axis=-1)
            sum1 = len(np.unique(columns1, axis=0))
            sum2 = len(np.unique(columns2, axis=0))

        print("{:>50} {:>10} {:>10} {:>10} {:>5} {:>5}".format(
            dataset_name, sum1, sum2, sum_count.GetValue(),
            sum1 - sum2, sum2 - sum_count.GetValue()))


if __name__ == "__main__":

    assert len(sys.argv) == 4

    int_folder = sys.argv[1]
    final_folder = sys.argv[2]
    sample_file = sys.argv[3]

    check_files(int_folder, final_folder, sample_file)
