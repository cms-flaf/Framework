import ROOT
import Common.Utilities as Utilities
import os
import shutil
ROOT.gROOT.SetBatch(True)
ROOT.EnableThreadSafety()

ROOT.gInterpreter.Declare(
  """
    ROOT::RDF::RNode processExample(ROOT::RDF::RNode df_in, const std::vector<std::string>& var_names)
    {
      std::cout << "TupleMaker::process: foreach started." << std::endl;
      ROOT::RDF::RNode df = df_in;
      df.Foreach([&](unsigned long long i){ std::cout << i << std::endl;}, var_names);
      return df_in;
    }
  """)

col_type_dict = {
  'ULong64_t':'unsigned long long',
  'Float_t':'float',
  'Bool_t':'bool',
  'Int_t' :'int',
  'UInt_t':'unsigned int',
  'ULong_t':'unsigned long',
  'Long_t':'long',
  'Double_t':'double'
  }

def make_df(inputFileCentral,inputFileShifted,outFile):
  if os.path.exists(outFile):
    os.remove(outFile)
  #os.mkfile(outFile, exist_ok=True)
  df_in = ROOT.RDataFrame('Events', inputFileCentral)
  df_out = ROOT.RDataFrame('Events', inputFileShifted)
  colNames = [str(c) for c in df_out.GetColumnNames()]
  entryIndexIdx = colNames.index("entryIndex")
  colNames[entryIndexIdx], colNames[0] = colNames[0], colNames[entryIndexIdx]
  col_types = [str(df_in.GetColumnType(c)) for c in colNames]

  #nEvents = 100
  #df_in = df_in.Range(nEvents)
  #print(','.join(f'"{c}"' for c in colNames))
  #df_out = df_out.Range(nEvents)
  #print(df_in.Describe())
  #print(df_out.Count().GetValue())
  colNames_v = Utilities.ListToVector(colNames)
  tuple_maker = ROOT.analysis.TupleMaker(*col_types)(4)
  print("tuplemaker created")
  df_out = tuple_maker.process(ROOT.RDF.AsRNode(df_in), ROOT.RDF.AsRNode(df_out), colNames_v)
  print("tuplemaker proceassed")
  df_out = df_out.Define("isValid", "_entryCentral.valid")
  print("defined isValid entry")
  #df_out.Display({"isValid"}).Print()
  df_unique = df_out.Filter("!isValid")
  print("defined df_unique")
  df_out_valid = df_out.Filter('isValid')
  print("defined df_valid")
  colToSave_diff= []
  for var_idx,var_name in enumerate(colNames):
    if colNames[var_idx] in df_in.GetColumnNames():
      #df_out_valid= df_out.Filter('isValid').Define(f"diff_{var_name}", f"""std::cout <<"isValid \t entryCentral_idx = " << _entryCentral.GetValue<{col_type_dict[col_types[var_idx]]}>({var_idx}) << "\t entryShifted = " << {var_name} << std::endl; return _entryCentral.GetValue<{col_type_dict[col_types[var_idx]]}>({var_idx})-{var_name}""")
      df_out_valid=df_out_valid.Define(f"diff_{var_name}", f"""return _entryCentral.GetValue<{col_type_dict[col_types[var_idx]]}>({var_idx})-{var_name}""")
      colToSave_diff.append(f"diff_{var_name}")
    colToSave_diff.append(var_name)


  snaps = []
  print("start making screenshot")
  snapshotOptions = ROOT.RDF.RSnapshotOptions()
  snapshotOptions.fOverwriteIfExists=False
  snapshotOptions.fLazy=True
  snapshotOptions.fMode="RECREATE"
  snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + 'LZ4')
  snapshotOptions.fCompressionLevel = 5
  snaps.append(df_out_valid.Snapshot(f"Events", outFile, Utilities.ListToVector(colToSave_diff), snapshotOptions))
  snaps.append(df_unique.Snapshot(f"Events_nonValid", f"output/prova_nonValid.root", {"entryIndex"}, snapshotOptions))

  ROOT.RDF.RunGraphs(snaps)
  print("finished screenshot")
  tuple_maker.join()




if __name__ == "__main__":
  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument('--inFileCentral', required=True, type=str)
  parser.add_argument('--inFileShifted', required=True, type=str)
  parser.add_argument('--outFile', required=True, type=str)
  args = parser.parse_args()
  headers_dir = os.path.dirname(os.path.abspath(__file__))
  ROOT.gROOT.ProcessLine(f".include {os.environ['ANALYSIS_PATH']}")
  header_path_Skimmer = os.path.join(headers_dir, "SystSkimmer.h")
  ROOT.gInterpreter.Declare(f'#include "{header_path_Skimmer}"')
  make_df(args.inFileCentral,args.inFileShifted,args.outFile)
  #df_in = ROOT.RDataFrame("Events", "output/tmp_ggR_320/Events.root")
  #df_out = ROOT.RDataFrame("Events", "output/tmp_ggR_320/Events_TotalUp.root")