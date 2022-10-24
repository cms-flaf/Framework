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
        if not dataset_name:
            for file in files:
                outputfiles.append(os.path.join(root, file))
        else:
            subroot = root[len(folder):]
            if subroot.startswith("/"):
                subroot = subroot[1:]
            if dataset_name not in subroot:
                continue
            subroot = subroot.split("/")[0][len("crab_") + len(dataset_name):].split("_")
            if len(subroot) == 1 or subroot[1] == "recovery":
                for file in files:
                    outputfiles.append(os.path.join(root, file))
    return outputfiles

def check_good_files(input_files):
    output_files = []
    bad_files = []
    for file in input_files:
        try:
            df = ROOT.RDataFrame("Events", file)
            histo = df.Histo1D("event")
            histo = histo.GetValue()
            output_files.append(file)
        except:
            bad_files.append(file)
    if len(bad_files) > 0:
        print("Corrupted files:")
        for file in bad_files:
            print("  - %s" % file)
    return output_files

def check_files(int_folder, final_folder, sample_file):
    print("\n{:<50} | {:>11} | {:>12} | {:>10} | {:>11} | {:>11} | {:>22}".format(
            "dataset", "crab unique", "final unique", "crab count", "final count",
            "dif uniques", "dif final count-unique"))
    print("-" * (53 + 14 + 15 + 13 + 14 + 14 + 22))
    with open(sample_file) as f:
        samples = yaml.load(f, yaml.Loader)

    dataset_names = os.listdir(final_folder)
    for dataset_name in dataset_names:
        miniaod_d = samples[dataset_name]['miniAOD'].split("/")[1]
        int_files = locate_files(os.path.join(int_folder, miniaod_d), dataset_name)
        final_files = locate_files(os.path.join(final_folder, dataset_name))

        int_files = check_good_files(int_files)
        final_files = check_good_files(final_files)

        df1 = ROOT.RDataFrame("Events", tuple(int_files))
        df2 = ROOT.RDataFrame("Events", tuple(final_files))

        sum_count = df1.Count()
        sum_count2 = df2.Count()

        columns1 = df1.AsNumpy(columns=['event', 'run', 'luminosityBlock'])
        columns1 = np.stack((columns1['event'], columns1['run'], columns1['luminosityBlock']), axis=-1)
        columns2 = df2.AsNumpy(columns=['event', 'run', 'luminosityBlock'])
        columns2 = np.stack((columns2['event'], columns2['run'], columns2['luminosityBlock']), axis=-1)
        sum1 = len(np.unique(columns1, axis=0))
        sum2 = len(np.unique(columns2, axis=0))

        print("{:<50} | {:>11} | {:>12} | {:>10} | {:>11} | {:>11} | {:>22}".format(
            dataset_name, sum1, sum2, sum_count.GetValue(), sum_count2.GetValue(),
            sum1 - sum2, sum_count2.GetValue() - sum2))


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: python countEvents.py int_folder final_folder sample_file")
        sys.exit(1)

    int_folder = sys.argv[1]
    final_folder = sys.argv[2]
    sample_file = sys.argv[3]

    check_files(int_folder, final_folder, sample_file)
