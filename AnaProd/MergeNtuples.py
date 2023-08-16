import ROOT
import os
import shutil
import sys
ROOT.gROOT.SetBatch(True)
ROOT.EnableThreadSafety()

if __name__ == "__main__":
    sys.path.append(os.environ['ANALYSIS_PATH'])


import Common.ConvertUproot as ConvertUproot
ROOT.gInterpreter.Declare(
    """
    using lumiEventMapType = std::map < unsigned int, std::set <unsigned long long > >;
    using eventMapType =  std::map < unsigned int ,  lumiEventMapType >;
    using RunLumiEventSet = std::set < std::tuple < unsigned int, unsigned int, unsigned long long > > ;

    bool saveEvent(unsigned int run, unsigned int lumi, unsigned long long event){
        static eventMapType eventMap;
        static std::mutex eventMap_mutex;
        const std::lock_guard<std::mutex> lock(eventMap_mutex);
        auto& events = eventMap[run][lumi];
        if(events.find(event) != events.end())
            return false;
        events.insert(event);
        return true;
        }
    """
)

def merge_ntuples(df):
    if 'run' not in df.GetColumnNames() or 'luminosityBlock'  not in df.GetColumnNames() or 'event'  not in df.GetColumnNames():
        return df
    df = df.Filter("saveEvent(run, luminosityBlock, event)")
    return df

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputDirFile', required=True, type=str)
    parser.add_argument('--outDir', required=True, type=str)
    parser.add_argument('--outputFileName', required=True, type=str)
    args = parser.parse_args()
    #outputFileName = 'test_dataMerging.root'
    headers_dir = os.path.dirname(os.path.abspath(__file__))

    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    snapshotOptions = ROOT.RDF.RSnapshotOptions()
    snapshotOptions.fOverwriteIfExists=False
    snapshotOptions.fLazy = True
    snapshotOptions.fMode="UPDATE"
    snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'kZLIB')
    snapshotOptions.fCompressionLevel = 4
    other_histograms = []
    #dir_list = args.inputDirList.split(',')
    with open(args.inputDirFile, 'r') as inputDirFileTxt:
            dir_list = inputDirFileTxt.read().splitlines()
    inFiles_list = []
    for folder in dir_list :
        inFiles_list.extend([ os.path.join(folder, filein) for filein in os.listdir(folder)])
    #print(inFiles_list)
    dfNames = []
    histNames = []
    histograms = {}
    inputs = ROOT.TList()
    for fileName in inFiles_list:
        file_in = ROOT.TFile(fileName, "READ")
        for k in file_in.GetListOfKeys():
            key_name = k.GetName()
            obj = k.ReadObj()
            isTree = obj.IsA().InheritsFrom(ROOT.TTree.Class())
            if isTree:
                if key_name not in dfNames:
                    dfNames.append(key_name)
            else:
                if obj.IsA().InheritsFrom(ROOT.TH1.Class()):
                    if key_name not in histNames:
                        histNames.append(k.GetName())
                        histograms[key_name] = ROOT.TH1D()
                    inputs.Add(obj)
                    histograms[key_name].Merge(inputs)
                    inputs.Clear()
                else:
                    print(f"Object {obj} is not a TH1.")
        file_in.Close()

    for fileName in inFiles_list:
        file_in = ROOT.TFile(fileName, "READ")
        for dfName in dfNames:
            if dfName not in file_in.GetListOfKeys():
                print(f"{dfName} not in keys of {fileName}")
                rdf = ROOT.RDataFrame(1)
                snapshotOptions.fLazy = False
                rdf.Define("entryIndex", "-1").Snapshot(dfName, fileName, rdf.GetColumnNames(), snapshotOptions)
        file_in.Close()

    snaps = []
    if not os.path.isdir(args.outDir):
        os.makedirs(args.outDir)
    tmpFile =  os.path.join(args.outDir, 'tmp.root')
    for dfName in dfNames:
        df_key = ROOT.RDataFrame(dfName,inFiles_list)
        final_df = merge_ntuples(df_key)
        snaps.append(final_df.Snapshot(dfName, tmpFile, final_df.GetColumnNames(), snapshotOptions))
    tmpFile_ = ROOT.TFile(tmpFile, "RECREATE")
    for histName in histograms.keys():
        histograms[histName].Write()
    tmpFile_.Close()
    snapshotOptions.fLazy = True
    if snapshotOptions.fLazy == True:
        ROOT.RDF.RunGraphs(snaps)
    finalOutFile = os.path.join(args.outDir, args.outputFileName)
    ConvertUproot.toUproot(tmpFile, finalOutFile)
    if os.path.exists(finalOutFile):
        os.remove(tmpFile)
