import ROOT
import sys
import os
import math
import shutil
import json
from RunKit.sh_tools import sh_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *
from Analysis.hh_bbtautau import *

#include <TH1F.h>
#include <TH2F.h>
#include <vector>
ROOT.gInterpreter.Declare(){
    """

    TH1F* computeRatioOfSliceIntegrals(const TH2F* hist1, const TH2F* hist2) {
        if (!hist1 || !hist2 || (hist1->GetNbinsX() != hist2->GetNbinsX()) || (hist1->GetNbinsY() != hist2->GetNbinsY())) {
            return nullptr; // Return nullptr in case of invalid input histograms or different binning
        }

        int numXBins = hist1->GetNbinsX();

        TH1F* outputHist = new TH1F("outputHist", "Ratio of Slice Integrals", numXBins, hist1->GetXaxis()->GetXmin(), hist1->GetXaxis()->GetXmax());

        for (int xBin = 1; xBin <= numXBins; ++xBin) {
            double integral1 = hist1->ProjectionY("", xBin, xBin)->Integral();
            double integral2 = hist2->ProjectionY("", xBin, xBin)->Integral();
            double ratio = (integral2 != 0.0) ? integral1 / integral2 : 0.0;

            outputHist->SetBinContent(xBin, ratio);
        }

        return outputHist;
    }

    """
    }



def CreateNamesDict(histNamesDict, sample_types, uncName, scales, sample_cfg_dict):
    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    for sample_key in sample_types.keys():
        if sample_key in sample_cfg_dict.keys():
            sample_type = sample_cfg_dict[sample_key]['sampleType']
        histNamesDict[sample_key] = (sample_key, 'Central','Central')
        if sample_key == 'data': continue
        if uncName == 'Central':continue
        for scale in scales:
            histName = f"{sample_key}_{uncName}{scale}"
            histKey = (sample_key, uncName, scale)
            histNamesDict[histName] = histKey



def fillHistDict(inFileRoot, all_histograms, unc_source,channels, QCDregions, categories, sample_type, histNamesDict,signals):
    for channel in channels:
        dir_0 = inFileRoot.Get(channel)
        for qcdRegion in QCDregions:
            dir_1 = dir_0.Get(qcdRegion)
            for cat in categories:
                dir_2 = dir_1.Get(cat)
                for key in dir_2.GetListOfKeys():
                    obj = key.ReadObj()
                    if not obj.IsA().InheritsFrom(ROOT.TH1.Class()): continue
                    obj.SetDirectory(0)
                    key_name = key.GetName()
                    key_name_split = key_name.split('_')
                    if key_name_split[0] in signals:
                        key_name = f"{sample_type}"
                        if len(key_name_split)>1:
                            key_name+="_"
                            key_name += '_'.join(ks for ks in key_name_split[1:])
                    if key_name not in histNamesDict.keys(): continue
                    sample,uncNameType,scale = histNamesDict[key_name]
                    if sample=='data' and uncNameType!='Central':continue
                    if sample!='data' and uncNameType!=unc_source:continue
                    key_total = ((channel, qcdRegion, cat), (uncNameType, scale))
                    if key_total not in all_histograms.keys():
                        all_histograms[key_total] = []
                    all_histograms[key_total].append(obj)

def MergeHistogramsPerType(all_histograms):
    for key_name,histlist in all_histograms.items():
        final_hist =  histlist[0]
        objsToMerge = ROOT.TList()
        for hist in histlist[1:]:
            objsToMerge.Add(hist)
        final_hist.Merge(objsToMerge)
        all_histograms[key_name] = final_hist

def GetBTagWeightDict(all_histograms, sample_name, final_json_dict)
    all_histograms_1D = {}
    for key_name,histogram in all_histograms.items():
        (key_1, key_2) = key_name
        ch, reg, cat = key_1
        uncName,scale = key_2
        key_tuple_num = ((ch, reg, 'btag_shape'), key_2)
        key_tuple_den = ((ch, reg, 'inclusive'), key_2)
        ratio_num_hist = all_histograms[key_tuple_num]
        ratio_den_hist = all_histograms[key_tuple_den]
        final_json_dict[sample_name]={}
        final_json_dict[sample_name][((ch, reg), key_2)] = {}
        for yBin in range(0, ratio_num_hist.GetNbinsY()):
            if (ratio_num_hist.GetYaxis().GetBinCenter(yBin) != ratio_den_hist.GetYaxis().GetBinCenter(yBin)):
                print(f"bin centers are different, for num it's: {ratio_num_hist.GetYaxis().GetBinCenter(yBin)} and for den it's {ratio_den_hist.GetYaxis().GetBinCenter(yBin)}")
            histName_num = ratio_num_hist.GetName()
            hist1DProjection_num = ratio_num_hist.ProfileX(f"{histName_num}_pfx", yBin, yBin)
            ratio = ratio_num_hist.Integral(0,ratio_num_hist.GetNbinsX()+1)/ratio_den_hist.Integral(0,ratio_den_hist.GetNbinsX()+1)
            histName_den = ratio_den_hist.GetName()
            hist1DProjection_den = ratio_den_hist.ProfileX(f"{histName_den}_pfx", yBin, yBin)
            final_json_dict[sample_name][((ch, reg), key_2)]['nJets'] = ratio_num_hist.GetYaxis().GetBinCenter(yBin)
            final_json_dict[sample_name][((ch, reg), key_2)]['num'] =  hist1DProjection_num.Integral(0,hist1DProjection_num.GetNbinsX()+1)
            final_json_dict[sample_name][((ch, reg), key_2)]['den'] = hist1DProjection_den.Integral(0,hist1DProjection_den.GetNbinsX()+1)
            final_json_dict[sample_name][((ch, reg), key_2)]['ratio'] =  hist1DProjection_num.Integral(0,hist1DProjection_num.GetNbinsX()+1)/ hist1DProjection_den.Integral(0,hist1DProjection_den.GetNbinsX()+1)
            histName = all_histograms[key_name].GetName()
            hist1D = all_histograms[key_name].ProfileX(f"{histName}_pfx", yBin, yBin).Scale(hist1DProjection_num.Integral(0,hist1DProjection_num.GetNbinsX()+1)/ hist1DProjection_den.Integral(0,hist1DProjection_den.GetNbinsX()+1))
            if yBin == 0 :
                all_histograms_1D[key_name] = hist1D
            else:
                all_histograms_1D[key_name].Add(all_histograms_1D[key_name], hist1D)
        return all_histograms_1D


def ApplyBTagWeight(all_histograms):
    for key_name,histogram in all_histograms.items():
        (key_1, key_2) = key_name
        ch, reg, cat = key_1
        uncName,scale = key_2
        key_tuple_num = ((ch, reg, 'btag_shape'), key_2)
        key_tuple_den = ((ch, reg, 'inclusive'), key_2)
        ratio_num_hist = all_histograms[key_tuple_num]
        ratio_den_hist = all_histograms[key_tuple_den]
        ratio = ratio_num_hist.Integral(0,ratio_num_hist.GetNbinsX()+1)/ratio_den_hist.Integral(0,ratio_den_hist.GetNbinsX()+1)
        histogram.Scale(ratio)
        all_histograms[key_name] = histogram

if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--histDir', required=True, type=str)
    parser.add_argument('--jsonDir', required=True, type=str)
    parser.add_argument('--hists', required=True, type=str)
    parser.add_argument('--sampleConfig', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--uncSource', required=False, type=str,default='Central')
    parser.add_argument('--wantBTag', required=False, type=bool, default=False)


    args = parser.parse_args()
    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)

    all_samples_list,all_samples_types = GetSamplesStuff(sample_cfg_dict,args.histDir, False)
    histNamesDict = {}
    uncNameTypes = GetUncNameTypes(unc_cfg_dict)
    btag_dir= "bTag_WP" if args.wantBTag else "bTag_shape"
    if args.uncSource != 'Central' and args.uncSource not in uncNameTypes:
        print("unknown unc source {args.uncSource}")
    CreateNamesDict(histNamesDict, all_samples_types, args.uncSource, scales, sample_cfg_dict)
    all_vars = args.hists.split(',')
    categories = list(sample_cfg_dict['GLOBAL']['categories'])
    QCDregions = list(sample_cfg_dict['GLOBAL']['QCDRegions'])
    channels = list(sample_cfg_dict['GLOBAL']['channelSelection'])
    signals = list(sample_cfg_dict['GLOBAL']['signal_types'])
    all_files = {}
    for var in all_vars:
        fileName =  f"{var}2D.root"
        if var not in all_files.keys():
            all_files[var] = {}
        for sample_type in all_samples_types.keys():
            samples = all_samples_types[sample_type]
            histDirs = [os.path.join(args.histDir, sample,btag_dir) for sample in samples]
            all_files[var][sample_type] = [os.path.join(hist_dir,fileName) for hist_dir in histDirs]
    # 1. get Histograms
    all_histograms ={}
    all_histograms_1D ={}
    for var in all_files.keys():
        final_json_dict = {}
        if var not in all_histograms.keys():
            all_histograms[var] = {}
            all_histograms_1D[var] = {}
        for sample_type in all_files[var].keys():
            if sample_type not in all_histograms[var].keys():
                all_histograms[var][sample_type] = {}
                all_histograms_1D[var][sample_type] = {}
            for inFileName in all_files[var][sample_type]:
                if not os.path.exists(inFileName): continue
                inFileRoot = ROOT.TFile.Open(inFileName, "READ")
                if inFileRoot.IsZombie():
                    raise RuntimeError(f"{inFile} is Zombie")
                fillHistDict(inFileRoot, all_histograms[var][sample_type],args.uncSource, channels, QCDregions, categories, sample_type, histNamesDict, signals)
                inFileRoot.Close()
            MergeHistogramsPerType(all_histograms[var][sample_type])
            all_histograms_1D[var][sample_type]=GetBTagWeightDict(all_histograms[var][sample_type],sample_type,final_json_dict)
        AddQCDInHistDict(all_histograms_1D[var], channels, categories, sample_type, args.uncSource, all_samples_list, scales)
        json_file = os.path.join(args.jsonDir, f'all_rations_{var}_{args.uncSource}.json')
        with open(f"{json_file}", "w") as write_file:
            json.dump(final_json_dict, write_file, indent=4)
    for var in all_histograms_1D.keys():
        outDir = os.path.join(args.histDir,'all_histograms',var, btag_dir)
        if not os.path.exists(outDir):
            os.makedirs(outDir)
        outFileName = os.path.join(outDir, f'all_histograms_{var}_{args.uncSource}.root')
        if os.path.exists(outFileName):
            os.remove(outFileName)
        outFile = ROOT.TFile(outFileName, "RECREATE")
        for sample_type in all_histograms_1D[var].keys():
            for key in all_histograms_1D[var][sample_type]:
                (channel, qcdRegion, cat), (uncNameType, uncScale) = key

                if qcdRegion != 'OS_Iso': continue
                dirStruct = (channel, cat)
                dir_name = '/'.join(dirStruct)
                dir_ptr = mkdir(outFile,dir_name)
                hist = all_histograms_1D[var][sample_type][key]

                hist_name =  sample_type
                if uncNameType!=args.uncSource: continue
                if uncNameType != 'Central':
                    if sample_type == 'data' : continue
                    if uncScale == 'Central': continue
                    hist_name+=f"_{uncNameType}{uncScale}"
                else:
                    if uncScale!='Central':continue
                hist.SetTitle(hist_name)
                hist.SetName(hist_name)
                dir_ptr.WriteTObject(hist, hist_name, "Overwrite")
        outFile.Close()