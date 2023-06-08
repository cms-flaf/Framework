import ROOT
#import Common.Utilities as Utilities
import os
import shutil
ROOT.gROOT.SetBatch(True)
ROOT.EnableThreadSafety()

ROOT.gInterpreter.Declare(
  """

namespace detail {
  template<typename T>
  struct DeltaImpl {
    static T Delta(const T& shifted, const T& central) {
      return shifted - central;
    }
  };

  template<typename T>
  struct DeltaImpl<ROOT::VecOps::RVec<T>> {
    static ROOT::VecOps::RVec<T> Delta(const ROOT::VecOps::RVec<T>& shifted, const ROOT::VecOps::RVec<T>& central)
    {
      ROOT::VecOps::RVec<T> delta = shifted;
      size_t n_max = std::min(shifted.size(), central.size());
      for(size_t n = 0; n < n_max; ++n)
        delta[n] -= central[n];
      return delta;
    }
  };

  template<typename T>
  struct IsSameImpl {
    static bool IsSame(T shifted, T central) {
      return shifted == central;
    }
  };

  template<typename T>
  struct IsSameImpl<ROOT::VecOps::RVec<T>> {
    static bool IsSame(const ROOT::VecOps::RVec<T>& shifted, const ROOT::VecOps::RVec<T>& central)
    {
      const size_t n_shifted = shifted.size();
      if(n_shifted != central.size())
        return false;
      for(size_t n = 0; n < n_shifted; ++n)
        if(!IsSameImpl<T>::IsSame(shifted[n], central[n]))
          return false;
      return true;
    }
  };
}

template<typename T>
bool IsSame(const T& shifted, const T& central)
{
  return detail::IsSameImpl<T>::IsSame(shifted, central);
}
template<typename T>
T Delta(const T& shifted, const T& central)
{
  return detail::DeltaImpl<T>::Delta(shifted, central);
}



  template<typename T>
  T FromDelta(T delta, T central)
  {
    return central + delta;
  }
  /*
  template<>
  bool FromDelta<bool>(bool delta, bool central)
  {
    return delta ? !central : central;
  }

  template<typename T>
  ROOT::VecOps::RVec<T> FromDelta<ROOT::VecOps::RVec<T>>(const ROOT::VecOps::RVec<T>& delta, const ROOT::VecOps::RVec<T>& central)
  {
    ROOT::VecOps::RVec<T> shifted = delta;
    size_t n_max = std::min(shifted.size(), delta.size());
    for(size_t n = 0; n < n_max; ++n)
      shifted[n] += central[n];
    return shifted;
  }*/
  """)


def ListToVector(list, type="string"):
	vec = ROOT.std.vector(type)()
	for item in list:
		vec.push_back(item)
	return vec
col_type_dict = {
  'ULong64_t':'unsigned long long',
  'Float_t':'float',
  'Bool_t':'bool',
  'Int_t' :'int',
  'UInt_t':'unsigned int',
  'ULong_t':'unsigned long',
  'Long_t':'long',
  'Double_t':'double',
  'ROOT::VecOps::RVec<float>':'ROOT::VecOps::RVec<float>',
  'ROOT::VecOps::RVec<int>':'ROOT::VecOps::RVec<int>',
  'ROOT::VecOps::RVec<bool>':'ROOT::VecOps::RVec<bool>',
  'ROOT::VecOps::RVec<unsigned long>':'ROOT::VecOps::RVec<unsigned long>',
  }

def make_df(inputFileCentral,inputFileShifted,outFile):

  df_out = ROOT.RDataFrame('Events', inputFileShifted)
  colNames = [str(c) for c in df_out.GetColumnNames()]
  entryIndexIdx = colNames.index("entryIndex")
  colNames[entryIndexIdx], colNames[0] = colNames[0], colNames[entryIndexIdx]
  #print(colNames)
  col_types = [str(df_out.GetColumnType(c)) for c in colNames]
  #print(','.join(f'"{c}"' for c in colNames))
  #print(list(set(col_types)))
  #nEvents = 10
  #df_out = df_out.Range(nEvents)
  tuple_maker = ROOT.analysis.TupleMaker(*col_types)('Events', inputFileCentral, 4)
  print("tuplemaker created")
  df_out = tuple_maker.processOut(ROOT.RDF.AsRNode(df_out))
  print("tuplemaker processed")
  df_out = df_out.Define("isValid", "_entryCentral.use_count() > 0")
  print("defined isValid entry")
  df_unique = df_out.Filter("!isValid")
  print("defined df_unique")
  df_out_valid = df_out.Filter('isValid')

  print("defined df_valid")
  colToSave_diff= []
  condition_noDiff_list = []
  condition_Valid_list = []
  for var_idx,var_name in enumerate(colNames):
    # if colNames[var_idx] in df_in.GetColumnNames():
      #df_out_valid= df_out.Filter('isValid').Define(f"diff_{var_name}", f"""std::cout <<"isValid \t entryCentral_idx = " << _entryCentral.GetValue<{col_type_dict[col_types[var_idx]]}>({var_idx}) << "\t entryShifted = " << {var_name} << std::endl; return _entryCentral.GetValue<{col_type_dict[col_types[var_idx]]}>({var_idx})-{var_name}""")
    df_out_valid=df_out_valid.Define(f"diff_{var_name}", f"""Delta(_entryCentral->GetValue<{col_type_dict[col_types[var_idx]]}>({var_idx}),{var_name})""")
    colToSave_diff.append(f"diff_{var_name}")
    condition_noDiff_list.append(f"IsSame(_entryCentral->GetValue<{col_type_dict[col_types[var_idx]]}>({var_idx}),{var_name})")
    condition_Valid_list.append(f"! IsSame(_entryCentral->GetValue<{col_type_dict[col_types[var_idx]]}>({var_idx}),{var_name})")
  df_out_valid_noDiff = df_out_valid
  colToSave_noDiff= ["period","run", "sample_name", "sample_type", "channelId", "entryIndex", "event", "isData", "luminosityBlock"]
  condition_noDiff = ' && '.join(condition_noDiff_list)
  # print(condition_noDiff)
  df_out_valid_noDiff = df_out_valid_noDiff.Define("condition_noDiff", condition_noDiff).Filter("condition_noDiff")
  condition_Valid = ' || '.join(condition_Valid_list)
  #print(condition_Valid)
  df_out_valid = df_out_valid.Define("condition_Valid", condition_Valid).Filter("condition_Valid")

  snaps = []
  print("start making screenshot")
  snapshotOptions = ROOT.RDF.RSnapshotOptions()
  snapshotOptions.fOverwriteIfExists=False
  snapshotOptions.fLazy=True
  snapshotOptions.fMode="RECREATE"
  snapshotOptions.fCompressionAlgorithm = getattr(ROOT.ROOT, 'k' + 'LZ4')
  snapshotOptions.fCompressionLevel = 5
  colToSave_noDiff_v = ListToVector(colToSave_noDiff)
  colToSave_diff_v = ListToVector(colToSave_diff)
  colNames_v = ListToVector(colNames)
  outFile_Valid = f"{outFile}.root"
  outFile_nonValid = f"{outFile}_nonValid.root"
  outFile_Valid_noDiff = f"{outFile}_noDiff.root"
  if os.path.exists(outFile_Valid):
    os.remove(outFile_Valid)
  if os.path.exists(outFile_nonValid):
    os.remove(outFile_nonValid)
  if os.path.exists(outFile_Valid_noDiff):
    os.remove(outFile_Valid_noDiff)
  #df_out_valid.Snapshot(f"Events", outFile_Valid, colToSave_diff_v, snapshotOptions)
  snaps.append(df_out_valid_noDiff.Snapshot(f"Events", outFile_Valid_noDiff, colToSave_noDiff_v, snapshotOptions))
  snaps.append(df_out_valid.Snapshot(f"Events", outFile_Valid, colToSave_diff_v, snapshotOptions))
  snaps.append(df_unique.Snapshot(f"Events", outFile_nonValid, colNames_v, snapshotOptions))
  tuple_maker.processIn(colNames_v)
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