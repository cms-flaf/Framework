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

    #Features to load from df to awkward
    load_features = set()
    load_features.update(features)
    for feature in list_features:
        load_features.update([feature[0]])


    events = uproot.open(inFileName)
    branches = events['Events'].arrays(list(load_features)+varToSave)

    #Get single value array
    array = np.array([getattr(branches, feature_name) for feature_name in features]).transpose()

    #Get vector value array (Not done yet)
    default_value = 0.0
    if list_features != None:
        print(branches[0])
        array_listfeatures = np.array([ak.fill_none(ak.pad_none(getattr(branches, feature_name), index+1), default_value)[:,index] for [feature_name,index] in list_features]).transpose()
    print("Got the list features")

    #Need to append the value features and the listfeatures together
    if list_features != None: 
        print("We have list features!")
        array = np.append(array, array_listfeatures, axis=1)


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

