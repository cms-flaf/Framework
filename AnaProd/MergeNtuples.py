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
    parser.add_argument('--inputDir', required=True, type=str)
    parser.add_argument('--outputFile', required=True, type=str)
    args = parser.parse_args()
    outputFile = 'output/test_dataMerging.root'
    headers_dir = os.path.dirname(os.path.abspath(__file__))

    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    snapshotOptions = ROOT.RDF.RSnapshotOptions()
    snapshotOptions.fOverwriteIfExists=False
    snapshotOptions.fLazy = True
    snapshotOptions.fMode="UPDATE"
    snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'kZLIB')
    snapshotOptions.fCompressionLevel = 4
    other_histograms = []
    inFiles_list =[ os.path.join(args.inputDir, filein) for filein in os.listdir(args.inputDir)]
    if os.path.exists(args.outputFile):
        os.remove(args.outputFile)
    dfNames = []
    histNames = []
    histograms = {}
    for fileName in inFiles_list:
        print(fileName)
        file_in = ROOT.TFile(fileName, "READ")
        for k in file_in.GetListOfKeys():
            key_name = k.GetName()
            if k.GetClassName() =='TTree':
                if key_name not in dfNames:
                    dfNames.append(k.GetName())
            else:
                obj = k.ReadObj()
                if isinstance(obj, ROOT.TH1):
                    if key_name not in histNames:
                        histNames.append(k.GetName())
                        histograms[key_name] = ROOT.TH1D()
                    hist = histograms[key_name]
                    if hist.GetTitle!=obj.GetTitle():
                        hist.SetTitle(obj.GetTitle())
                    if hist.GetName!=obj.GetName():
                        hist.SetName(obj.GetName())
                    hist.Add(obj)
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
    for fileName in inFiles_list:
        file_in = ROOT.TFile(fileName, "READ")
        for dfName in dfNames:
            inDF = dfName in file_in.GetListOfKeys()
            print(f"now {dfName} in keys? {inDF}")
        file_in.Close()
    for hist in other_histograms:
        print(hist.GetName(), hist.GetTitle(), hist.GetNbinsX(), hist.GetBinLowEdge(1), hist.GetBinLowEdge(hist.GetNbinsX()+1))
    snaps = []

    tmpFile =  os.path.join(args.inputDir, 'tmp.root')
    for dfName in dfNames:
        print(dfName)
        df_key = ROOT.RDataFrame(dfName,inFiles_list)
        final_df = merge_ntuples(df_key)
        snaps.append(final_df.Snapshot(dfName, tmpFile, final_df.GetColumnNames(), snapshotOptions))
    #print(snaps)
    outfile_ = ROOT.TFile(tmpFile, "UPDATE")
    for histName in histograms.keys():
        histograms[histName].Write()
    outfile_.Close()
    snapshotOptions.fLazy = True
    if snapshotOptions.fLazy == True:
        ROOT.RDF.RunGraphs(snaps)
    tmpFile =  os.path.join(args.inputDir, 'tmp.root')
    ConvertUproot.toUproot(tmpFile, args.outputFile)
    #if os.path.exists(args.outputFile):
        #os.remove(tmpFile)