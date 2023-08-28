import ROOT
import sys
import os
import math
import shutil
from RunKit.sh_tools import sh_call
if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

import Common.Utilities as Utilities
from Analysis.HistHelper import *
#all_histograms_inputVar[sample_type][channel][QCDRegion][category][key_name]
# region A = OS_Iso
# region B = OS_ANTI ISO
# region C = SS_Iso
# region D = SS_AntiIso

def QCD_Estimation(histograms, all_samples_list, channel='tauTau', category='res2b', key_name = 'Central',data = 'data'):
    print(channel, category)
    hist_data = histograms[data][channel]
    hist_data_B = hist_data['OS_AntiIso'][category][key_name]
    #if channel != 'tauTau' and category != 'inclusive': return hist_data_B
    hist_data_C = hist_data['SS_Iso'][category][key_name]
    hist_data_D = hist_data['SS_AntiIso'][category][key_name]
    n_data_C = hist_data_C.Integral(0, hist_data_C.GetNbinsX()+1)
    n_data_D = hist_data_D.Integral(0, hist_data_D.GetNbinsX()+1)
    for sample in all_samples_list:
        if sample==data or "Radion" in sample or "Graviton" in sample or "node" in sample:
            #print(f"sample {sample} is not considered")
            continue
        #print(sample)
        # find kappa value
        hist_sample = histograms[sample][channel]
        hist_sample_B = hist_sample['OS_AntiIso'][category][key_name]
        hist_sample_C = hist_sample['SS_Iso'][category][key_name]
        hist_sample_D = hist_sample['SS_AntiIso'][category][key_name]
        n_sample_C = hist_sample_C.Integral(0, hist_sample_C.GetNbinsX()+1)
        n_data_C-=n_sample_C
        n_sample_D = hist_sample_D.Integral(0, hist_sample_D.GetNbinsX()+1)
        n_data_D-=n_sample_D
        hist_data_B.Add(hist_sample_B, -1)
    kappa = n_data_C/n_data_D
    if n_data_C <= 0 or n_data_D <= 0:
        print(f"n_data_C = {n_data_C}")
        print(f"n_data_D = {n_data_D}")
    if kappa<0:
        print(f"transfer factor <0")
        return ROOT.TH1D()
        #raise  RuntimeError(f"transfer factor <=0 ! {kappa}")
    hist_data_B.Scale(kappa)
    fix_negative_contributions,debug_info,negative_bins_info = FixNegativeContributions(hist_data_B)
    if not fix_negative_contributions:
        #return hist_data_B
        print(debug_info)
        print(negative_bins_info)
        print("Unable to estimate QCD")

        return ROOT.TH1D()
        #raise RuntimeError("Unable to estimate QCD")
    return hist_data_B


if __name__ == "__main__":
    import argparse
    import yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputDir', required=True, type=str)
    parser.add_argument('--test', required=False, type=bool, default=False)
    #parser.add_argument('--deepTauVersion', required=False, type=str, default='v2p1')
    #parser.add_argument('--compute_unc_variations', type=bool, default=False)
    #parser.add_argument('--compute_rel_weights', type=bool, default=False)
    parser.add_argument('--histConfig', required=True, type=str)
    parser.add_argument('--sampleConfig', required=True, type=str)

    args = parser.parse_args()

    headers_dir = os.path.dirname(os.path.abspath(__file__))
    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    inputVariables = []
    finalFileName = 'all_histograms.root'

    with open(args.histConfig, 'r') as f:
        hist_cfg_dict = yaml.safe_load(f)
    vars_to_plot = list(hist_cfg_dict.keys())
    for var in os.listdir(args.inputDir):
        if var not in vars_to_plot: continue
        inputVariables.append(var)
    print(f"input variables are {inputVariables}")
    all_inputFiles = {}
    all_histograms = {}
    sample_cfg_dict = {}
    all_samples_list = []
    with open(args.sampleConfig, 'r') as f:
        sample_cfg_dict = yaml.safe_load(f)
    for inputVar in inputVariables:
        all_histograms[inputVar] = {}
        inputDir_tot = os.path.join(args.inputDir, inputVar)
        outFileName = os.path.join(args.inputDir, inputVar, finalFileName)
        if os.path.exists(outFileName):
            os.remove(outFileName)
        #all_inputFiles[inputVar] = [os.path.join(inputDir_tot, f) for f in os.listdir(inputDir_tot)]
        all_inputFiles[inputVar] = os.listdir(inputDir_tot)
        k = 0
        for inFile in all_inputFiles[inputVar]:
            sample_name = inFile.split('.')[0]
            #if sample_name != 'GluGluToBulkGravitonToHHTo2B2Tau_M-1250' : continue
            #if sample_name == 'TTToSemiLeptonic' : continue
            if 'all_histograms' in sample_name: continue
            if "tmp" in sample_name:
                continue
            sample_type = sample_cfg_dict[sample_name]['sampleType'] if sample_name!='data' else 'data'
            if sample_name != 'data' and 'mass' in sample_cfg_dict[sample_name].keys():
                mass = sample_cfg_dict[sample_name]['mass']
                sample_type+=f'_M-{mass}'
            if sample_type not in all_histograms[inputVar].keys():
                all_histograms[inputVar][sample_type] = {}
            if sample_type == 'QCD' : continue
            if sample_type not in all_samples_list:
                all_samples_list.append(sample_type)
            all_inFile = os.path.join(inputDir_tot, inFile)
            inFile_root = ROOT.TFile.Open(all_inFile, "READ")
            if inFile_root.IsZombie():
                raise RuntimeError(f"{inFile} is Zombie")
            for channel in channels:
                #if channel == 'eTau':channel='muTau'
                if channel not in all_histograms[inputVar][sample_type].keys():
                     all_histograms[inputVar][sample_type][channel] = {}
                dir_0 = inFile_root.Get(channel)
                for qcdRegion in QCDregions:
                    if qcdRegion not in all_histograms[inputVar][sample_type][channel].keys():
                        all_histograms[inputVar][sample_type][channel][qcdRegion] = {}
                    dir_1 = dir_0.Get(qcdRegion)
                    for cat in categories:
                        if cat not in all_histograms[inputVar][sample_type][channel][qcdRegion].keys():
                            all_histograms[inputVar][sample_type][channel][qcdRegion][cat] = {}
                        dir_2 = dir_1.Get(cat)
                        for key in dir_2.GetListOfKeys():
                            key_name = key.GetName()
                            if key_name !='Central': continue
                            obj = key.ReadObj()
                            if obj.IsA().InheritsFrom(ROOT.TH1.Class()):
                                obj.SetDirectory(0)
                                if key_name not in all_histograms[inputVar][sample_type][channel][qcdRegion][cat].keys():
                                    all_histograms[inputVar][sample_type][channel][qcdRegion][cat][key_name] = []
                                all_histograms[inputVar][sample_type][channel][qcdRegion][cat][key_name].append(obj)
            inFile_root.Close()

    for inputVar in inputVariables:
        # 1 merge histograms per sample:
        # let's try to fix first the var
        #inputVar = 'bbtautau_mass'
        all_histograms_inputVar = all_histograms[inputVar]
        for sample_type in all_histograms_inputVar.keys():
            for channel in all_histograms_inputVar[sample_type].keys():
                for QCDRegion in all_histograms_inputVar[sample_type][channel].keys():
                    for category in all_histograms_inputVar[sample_type][channel][QCDRegion]:
                        #print(sample_type, channel, QCDRegion, category, all_histograms_inputVar[sample_type][channel][QCDRegion][category])
                        all_final_hists = []
                        for key_name,histlist in all_histograms_inputVar[sample_type][channel][QCDRegion][category].items():
                            if key_name !='Central': continue
                            final_hist =  histlist[0]
                            objsToMerge = ROOT.TList()
                            for hist in histlist[1:]:
                                objsToMerge.Add(hist)
                            final_hist.Merge(objsToMerge)
                            all_histograms_inputVar[sample_type][channel][QCDRegion][category][key_name] = final_hist

        if 'QCD' not in all_histograms_inputVar.keys():
                all_histograms_inputVar['QCD'] = {}
        # now the histograms are merged. We need to evaluate the QCD
        for channel in channels:
            #if channel == 'eTau': channel='muTau'
            if channel not in all_histograms_inputVar['QCD'].keys():
                all_histograms_inputVar['QCD'][channel]= {}
            for category in categories:
                if 'OS_Iso' not in all_histograms_inputVar['QCD'][channel].keys():
                    all_histograms_inputVar['QCD'][channel]['OS_Iso']= {}
                if category not in all_histograms_inputVar['QCD'][channel]['OS_Iso'].keys():
                    all_histograms_inputVar['QCD'][channel]['OS_Iso'][category]= {}
                for key_name in all_histograms_inputVar[sample_type][channel]['OS_Iso'][category].keys():
                    if key_name !='Central': continue
                    all_histograms_inputVar['QCD'][channel]['OS_Iso'][category][key_name] =  QCD_Estimation(all_histograms_inputVar, all_samples_list, channel, category,key_name,'data')


        outFileName = os.path.join(args.inputDir, inputVar, finalFileName)
        if os.path.exists(outFileName):
            os.remove(outFileName)
        print(inputVar)
        outFile = ROOT.TFile(outFileName, "RECREATE")
        for sample_type in all_histograms_inputVar.keys():
            for channel in all_histograms_inputVar[sample_type].keys():
                for QCDRegion in all_histograms_inputVar[sample_type][channel].keys():
                    for category in all_histograms_inputVar[sample_type][channel][QCDRegion]:
                        for key_name,hist in all_histograms_inputVar[sample_type][channel][QCDRegion][category].items():
                            if key_name !='Central': continue
                            new_histName = f'{sample_type}_{channel}_{QCDRegion}_{category}_{key_name}'
                            hist.SetTitle(new_histName)
                            hist.SetName(new_histName)
                            hist.Write()
        outFile.Close()
