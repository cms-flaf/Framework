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
    namespace kin_fit{
        struct FitResults {
            double mass, chi2, probability;
            int convergence;
            bool HasValidMass() const { return convergence > 0; }
            FitResults() : convergence(std::numeric_limits<int>::lowest()) {}
            FitResults(double _mass, double _chi2, double _probability, int _convergence) :
                mass(_mass), chi2(_chi2), probability(_probability), convergence(_convergence) {}
        };
    }
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
class ObjDesc:
    def __init__(self, obj_type):
        self.obj_type = obj_type
        self.ref_obj = None
        self.objsToMerge = ROOT.TList()
        self.file_names = []


def merge_ntuples(df):
    if 'run' not in df.GetColumnNames() or 'luminosityBlock'  not in df.GetColumnNames() or 'event'  not in df.GetColumnNames():
        return df
    df = df.Filter("saveEvent(run, luminosityBlock, event)")
    return df

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('inputFile', nargs='+', type=str)
    parser.add_argument('--outFile', required=True, type=str)
    args = parser.parse_args()
    headers_dir = os.path.dirname(os.path.abspath(__file__))

    ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
    snapshotOptions = ROOT.RDF.RSnapshotOptions()
    snapshotOptions.fOverwriteIfExists=False
    snapshotOptions.fLazy = False
    snapshotOptions.fMode="UPDATE"
    snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'kZLIB')
    snapshotOptions.fCompressionLevel = 4

    inputFiles = [ (fileName, ROOT.TFile(fileName, "READ")) for fileName in args.inputFile ]
    objects = {}
    for fileName, file in inputFiles:
        for key in file.GetListOfKeys():
            key_name = key.GetName()
            obj = key.ReadObj()

            if obj.IsA().InheritsFrom(ROOT.TTree.Class()):
                objType = "TTree"
            elif obj.IsA().InheritsFrom(ROOT.TH1.Class()):
                objType = "TH1"
            else:
                raise RuntimeError(f"Object {obj} is not a TTree or TH1.")

            if key_name not in objects:
                objects[key_name] = ObjDesc(objType)
            obj_desc = objects[key_name]
            if objType != obj_desc.obj_type:
                raise RuntimeError(f"Object {key_name} is of type {objType} but was previously found to be"
                                f"of type {obj_desc.obj_type}.")
            if objType == "TTree":
                obj_desc.file_names.append(fileName)
            else:
                if obj_desc.ref_obj is None:
                    obj_desc.ref_obj = obj
                else:
                    obj_desc.objsToMerge.Add(obj)

    tmpFileName = args.outFile #args.outFile + '.tmp.root'
    compression = ROOT.CompressionSettings(snapshotOptions.fCompressionAlgorithm, snapshotOptions.fCompressionLevel)
    outputFile = ROOT.TFile(tmpFileName, "RECREATE", "", compression)
    for obj_name, obj_desc in objects.items():
        if obj_desc.obj_type != "TH1":
            continue
        obj_desc.ref_obj.Merge(obj_desc.objsToMerge)
        outputFile.WriteTObject(obj_desc.ref_obj, obj_name)
    outputFile.Close()
    for fileName, file in inputFiles:
        file.Close()

    for obj_name, obj_desc in objects.items():
        if obj_desc.obj_type != "TTree":
            continue
        df = ROOT.RDataFrame(obj_name, obj_desc.file_names)
        df = merge_ntuples(df)
        df.Snapshot(obj_name, tmpFileName, df.GetColumnNames(), snapshotOptions)
    #print(tmpFileName)
    #ConvertUproot.toUproot(tmpFileName, args.outFile)
    #if os.path.exists(args.outFile):
    #    os.remove(tmpFileName)
