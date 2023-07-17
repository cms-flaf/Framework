import uproot
import awkward as ak
import os
import numpy as np
def saveFile(outFile, out_tree, treeName):
  if os.path.exists(outFile):
    os.remove(outFile)
    with uproot.recreate(outFile, compression=uproot.LZMA(9)) as out_file:
      out_file[treeName] = out_tree
      out_file.close()
    return outFile

def toUproot(workingDir, fileName):
  inFile = os.path.join(workingDir, fileName)
  treeName = fileName.strip(".root")
  input_file = uproot.open(f"{inFile}")
  input_tree = input_file[f"{treeName}"]
  keys = input_tree.keys()
  outFile=f"{os.path.join(workingDir, treeName)}.root"
  out_tree = {}
  if not keys:
    with uproot.recreate(outFile, compression=uproot.LZMA(9)) as out_file:
      out_file.close()
    return outFile
  df = input_tree.arrays()
  collections = {}
  other_columns = []
  for key in keys:
    parts = key.split("_", 1)
    if len(parts) == 1:
      other_columns.append(key)
    else:
      col_name, br_name = parts
      if not col_name in collections:
        collections[col_name] = []
      collections[col_name].append(br_name)

  for col_name, columns in collections.items():
    out_tree[col_name] = ak.zip({ column: df[col_name + "_" + column] for column in columns })

  for column in other_columns:
    out_tree[column] = df[column]
  return saveFile(outFile, out_tree, treeName)

