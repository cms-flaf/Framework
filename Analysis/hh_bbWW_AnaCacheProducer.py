import os
import sys
import yaml
import ROOT
import datetime
import time
import shutil
import tensorflow as tf
import awkward as ak
import numpy as np
import uproot
import vector


if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])

from RunKit.run_tools import ps_call
ROOT.EnableThreadSafety()
#ROOT.EnableImplicitMT()
import Common.LegacyVariables as LegacyVariables
import Common.Utilities as Utilities
defaultColToSave = ["entryIndex","luminosityBlock", "run","event", "sample_type", "sample_name", "period", "X_mass", "X_spin", "isData"]
scales = ['Up','Down']
#from Analysis.HistHelper import *
#from Analysis.hh_bbtautau import *

def getKeyNames(root_file_name):
    #print(root_file_name)
    root_file = ROOT.TFile(root_file_name, "READ")
    #print(root_file.GetListOfKeys())
    key_names = [str(k.GetName()) for k in root_file.GetListOfKeys() ]
    root_file.Close()
    return key_names



def CreateHigherLevelVariables(branches, highlevelfeatures_names):
    #HT
    HT = ak.sum(branches['centralJet_pt'], axis=1)

    #dR_dilep
    lep1 = vector.arr({'pt': branches['lep1_pt'], 'phi': branches['lep1_phi'], 'eta': branches['lep1_eta'], 'mass': branches['lep1_mass']})
    lep2 = vector.arr({'pt': branches['lep2_pt'], 'phi': branches['lep2_phi'], 'eta': branches['lep2_eta'], 'mass': branches['lep2_mass']})
    dR_dilep = lep1.deltaR(lep2)

    #dR_diBjet
    jet_pt_branch = ak.pad_none(branches['centralJet_pt'], 4)
    jet_phi_branch = ak.pad_none(branches['centralJet_phi'], 4)
    jet_eta_branch = ak.pad_none(branches['centralJet_eta'], 4)
    jet_mass_branch = ak.pad_none(branches['centralJet_mass'], 4)
    jets = vector.arr({'pt': jet_pt_branch, 'phi': jet_phi_branch, 'eta': jet_eta_branch, 'mass': jet_mass_branch})
    dR_dibjet = ak.fill_none(jets[:,0].deltaR(jets[:,1]), 0.0)

    #dR_dilep_dijet
    dilep = lep1+lep2
    dijet = jets[:,2] + jets[:,3]
    dR_dilep_dijet = ak.fill_none(dilep.deltaR(dijet), 0.0)

    #dR_dilep_dibjet
    dibjet = jets[:,0] + jets[:,1]
    dR_dilep_dibjet = ak.fill_none(dilep.deltaR(dibjet), 0.0)

    #dPhi_MET_dilep
    met = vector.arr({'pt': branches['met_pt'], 'phi': branches['met_phi']})
    dPhi_MET_dilep = met.deltaphi(dilep)

    #dPhi_MET_dibjet
    dPhi_MET_dibjet = ak.fill_none(met.deltaphi(dibjet), 0.0)

    #min_dR_lep0_jets
    min_dR_lep0_jets = ak.fill_none(ak.min(lep1.deltaR(jets), axis=1), 0.0)

    #min_dR_lep1_jets
    min_dR_lep1_jets = ak.fill_none(ak.min(lep2.deltaR(jets), axis=1), 0.0)

    #mt2
    #MT2 code requires a event loop ):
    mt2_ll_lester_list = []
    mt2_bb_lester_list = []
    mt2_blbl_lester_list = []

    calculate_mt2 = ('mt2_ll_lester' in highlevelfeatures_names) or ('mt2_bb_lester' in highlevelfeatures_names) or ('mt2_blbl_lester' in highlevelfeatures_names)
    if calculate_mt2:
        print("Calculating mt2 (sadly this must be an event loop)")
        for idx in tqdm.tqdm(range(len(lep1))): #Can loop over the lep object since it is of length nEvent
            lep0_p4 = lep1[idx]
            lep1_p4 = lep2[idx]
            bjet0_p4 = jets[idx,0]
            bjet1_p4 = jets[idx,1]
            #Don't have subjets yet
            # if ev.Double_HbbFat:
            #     bjet0_p4 = vector.obj(pt = ev.ak8_jet0_subjet1_pt, eta = ev.ak8_jet0_subjet1_eta, phi = ev.ak8_jet0_subjet1_phi, energy = ev.ak8_jet0_subjet1_E)
            #     bjet1_p4 = vector.obj(pt = ev.ak8_jet0_subjet2_pt, eta = ev.ak8_jet0_subjet2_eta, phi = ev.ak8_jet0_subjet2_phi, energy = ev.ak8_jet0_subjet2_E)
            this_met = met[idx]

            if bjet0_p4.pt == None:
                bjet0_p4 = vector.obj(pt = 0.0, eta = 0.0, phi = 0.0, energy = 0.0)
            if bjet1_p4.pt == None:
                bjet1_p4 = vector.obj(pt = 0.0, eta = 0.0, phi = 0.0, energy = 0.0)
                
            mt2_ll_lester_list.append(mt2_lester.get_mT2(lep0_p4.M, lep0_p4.px, lep0_p4.py, lep1_p4.M, lep1_p4.px, lep1_p4.py, this_met.px+bjet0_p4.px+bjet1_p4.px, this_met.py+bjet0_p4.py+bjet1_p4.py, bjet0_p4.M, bjet1_p4.M, 0, True))
            wMass = 80.4
            mt2_bb_lester_list.append(mt2_lester.get_mT2(bjet0_p4.M, bjet0_p4.px, bjet0_p4.py, bjet1_p4.M, bjet1_p4.px, bjet1_p4.py, this_met.px+lep0_p4.px+lep1_p4.px, this_met.py+lep0_p4.py+lep1_p4.py, wMass, wMass, 0, True))

            lep0bjet0_p4 = lep0_p4 + bjet0_p4
            lep0bjet1_p4 = lep0_p4 + bjet1_p4
            lep1bjet0_p4 = lep1_p4 + bjet0_p4
            lep1bjet1_p4 = lep1_p4 + bjet1_p4
            if max(lep0bjet0_p4.M, lep1bjet1_p4.M) < max(lep0bjet1_p4.M, lep1bjet0_p4.M):
                mt2_blbl_lester_list.append(mt2_lester.get_mT2(lep0bjet0_p4.M, lep0bjet0_p4.px, lep0bjet0_p4.py, lep1bjet1_p4.M, lep1bjet1_p4.px, lep1bjet1_p4.py, this_met.px, this_met.py, 0, 0, 0, True))
            else:
                mt2_blbl_lester_list.append(mt2_lester.get_mT2(lep0bjet1_p4.M, lep0bjet1_p4.px, lep0bjet1_p4.py, lep1bjet0_p4.M, lep1bjet0_p4.px, lep1bjet0_p4.py, this_met.px, this_met.py, 0, 0, 0, True))
        mt2_ll_lester = np.array(mt2_ll_lester_list)
        mt2_bb_lester = np.array(mt2_bb_lester_list)
        mt2_blbl_lester = np.array(mt2_blbl_lester_list)



    hlv_list = []
    if 'HT' in highlevelfeatures_names: hlv_list.append(HT)
    if 'dR_dilep' in highlevelfeatures_names: hlv_list.append(dR_dilep)
    if 'dR_dibjet' in highlevelfeatures_names: hlv_list.append(dR_dibjet)

    if 'dR_dilep_dijet' in highlevelfeatures_names: hlv_list.append(dR_dilep_dijet)
    if 'dR_dilep_dibjet' in highlevelfeatures_names: hlv_list.append(dR_dilep_dibjet)

    if 'dPhi_MET_dilep' in highlevelfeatures_names: hlv_list.append(dPhi_MET_dilep)
    if 'dPhi_MET_dibjet' in highlevelfeatures_names: hlv_list.append(dPhi_MET_dibjet)

    if 'min_dR_lep0_jets' in highlevelfeatures_names: hlv_list.append(min_dR_lep0_jets)
    if 'min_dR_lep1_jets' in highlevelfeatures_names: hlv_list.append(min_dR_lep1_jets)

    if 'mt2_ll_lester' in highlevelfeatures_names: hlv_list.append(mt2_ll_lester)
    if 'mt2_bb_lester' in highlevelfeatures_names: hlv_list.append(mt2_bb_lester)
    if 'mt2_blbl_lester' in highlevelfeatures_names: hlv_list.append(mt2_blbl_lester)

    high_level_features = np.array(hlv_list)
    return high_level_features



def applyDNNScore(inFileName, global_cfg_dict, dnnFolder, param_mass_list, outFileName, varToSave, is_central=True):
    print("Going to load model")
    dnnName = os.path.join(dnnFolder, 'ResHH_Classifier.keras')
    dnnConfig_Name = os.path.join(dnnFolder, 'dnn_config.yaml')

    dnnConfig = None
    with open(dnnConfig_Name, 'r') as file:
        dnnConfig = yaml.safe_load(file)

    model = tf.keras.models.load_model(dnnName)


    print("We have dnnConfig")
    print(dnnConfig)


    #Features to use for DNN application (single vals)
    features = dnnConfig['features']
    #Features to use for DNN application (vectors and index)
    list_features = dnnConfig['listfeatures']
    #Features to use for DNN application (high level names to create)
    highlevel_features = dnnConfig['highlevelfeatures']

    #Features to load from df to awkward
    load_features = set()
    load_features.update(features)
    for feature in list_features:
        load_features.update([feature[0]])
    load_features.update(highlevel_features)




    events = uproot.open(inFileName)
    branches = events['Events'].arrays(list(load_features)+varToSave)

    #Get single value array
    array = np.array([getattr(branches, feature_name) for feature_name in features]).transpose()

    #Get vector value array
    default_value = 0.0
    if list_features != None:
        array_listfeatures = np.array([ak.fill_none(ak.pad_none(getattr(branches, feature_name), index+1), default_value)[:,index] for [feature_name,index] in list_features]).transpose()
        #Need to append the value features and the listfeatures together
        array = np.append(array, array_listfeatures, axis=1)

    #Need to append the high level features and the other features together
    if highlevel_features != None: 
        array_highlevelfeatures = CreateHigherLevelVariables(branches, highlevel_features).transpose()
        array = np.append(array, array_highlevelfeatures, axis=1)


    #Add parametric mass point to the array
    print("Starting param loop")
    for param_mass in param_mass_list:
        param_array = np.array([[param_mass for x in array]]).transpose()
        final_array = np.append(array, param_array, axis=1)

        prediction = model.predict(final_array)

        branches[f'dnn_M{param_mass}_Signal'] = prediction.transpose()[0]
        branches[f'dnn_M{param_mass}_TT'] = prediction.transpose()[1]
        branches[f'dnn_M{param_mass}_DY'] = prediction.transpose()[2]

    #But we want to drop the features from this outfile
    for feature in load_features:
        del branches[feature]

    #This will save the file
    print("Saving tree")
    outfile = uproot.recreate(outFileName)
    outfile['Events'] = branches

    return outfile


def createAnaCacheTuple(inFileName, outFileName, unc_cfg_dict, global_cfg_dict, snapshotOptions, compute_unc_variations, deepTauVersion, dnnFolder):
    start_time = datetime.datetime.now()
    verbosity = ROOT.Experimental.RLogScopedVerbosity(ROOT.Detail.RDF.RDFLogChannel(), ROOT.Experimental.ELogLevel.kInfo)
    snaps = []
    all_files = []
    file_keys = getKeyNames(inFileName)
    df = ROOT.RDataFrame('Events', inFileName)
    df_begin = df
    dfw = Utilities.DataFrameWrapper(df_begin,defaultColToSave)

    #param_mass_list = [250, 260, 270, 280, 300, 350, 450, 550, 600, 650, 700, 800, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000, 4000, 5000 ]
    param_mass_list = [250, 260, 270, 280, 300, 350, 450, 550, 600, 650, 700, 800, 1000 ]
    print("Starting add DNN")
    dnn_prediction = applyDNNScore(inFileName, global_cfg_dict, dnnFolder, param_mass_list, f'{outFileName}_Central.root', defaultColToSave)
    all_files.append(f'{outFileName}_Central.root')
    return all_files


if __name__ == "__main__":
    import argparse
    import os
    parser = argparse.ArgumentParser()
    parser.add_argument('--inFileName', required=True, type=str)
    parser.add_argument('--outFileName', required=True, type=str)
    parser.add_argument('--uncConfig', required=True, type=str)
    parser.add_argument('--globalConfig', required=True, type=str)
    parser.add_argument('--compute_unc_variations', type=bool, default=False)
    parser.add_argument('--compressionLevel', type=int, default=4)
    parser.add_argument('--compressionAlgo', type=str, default="ZLIB")
    parser.add_argument('--deepTauVersion', type=str, default="v2p1")
    parser.add_argument('--dnnFolder', type=str, default=None)
    #parser.add_argument('--modules', type=list, default=None)
    args = parser.parse_args()
    snapshotOptions = ROOT.RDF.RSnapshotOptions()
    snapshotOptions.fOverwriteIfExists=True
    snapshotOptions.fLazy = True
    snapshotOptions.fMode="RECREATE"
    snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + args.compressionAlgo)
    snapshotOptions.fCompressionLevel = args.compressionLevel
    unc_cfg_dict = {}

    with open(args.uncConfig, 'r') as f:
        unc_cfg_dict = yaml.safe_load(f)

    global_cfg_dict = {}
    with open(args.globalConfig, 'r') as f:
        global_cfg_dict = yaml.safe_load(f)

    startTime = time.time()

    outFileNameFinal = f'{args.outFileName}'

    all_files = createAnaCacheTuple(args.inFileName, args.outFileName.split('.')[0], unc_cfg_dict, global_cfg_dict, snapshotOptions, args.compute_unc_variations, args.deepTauVersion, args.dnnFolder)
    try:
        all_files = createAnaCacheTuple(args.inFileName, args.outFileName.split('.')[0], unc_cfg_dict, global_cfg_dict, snapshotOptions, args.compute_unc_variations, args.deepTauVersion, args.dnnFolder)
        print("Finished add DNN")
        hadd_str = f'hadd -f209 -n10 {outFileNameFinal} '
        hadd_str += ' '.join(f for f in all_files)
        if len(all_files) > 1:
            ps_call([hadd_str], True)
        else:
            shutil.copy(all_files[0],outFileNameFinal)
        if os.path.exists(outFileNameFinal):
                for histFile in all_files:
                    if histFile == outFileNameFinal: continue
                    os.remove(histFile)
    except:
        df = ROOT.RDataFrame(0)
        df=df.Define("test", "return true;")
        df.Snapshot("Events", outFileNameFinal, {"test"})
        #Utilities.create_file(outFileNameFinal)
    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))

