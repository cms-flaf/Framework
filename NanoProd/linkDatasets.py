import datetime
import json
import os
import pathlib
import select
import shutil
import sys
import yaml

def link_datasets(central_storage, other_locations):
  os.makedirs(central_storage, exist_ok=True)
  datasets = {}
  for location in [ central_storage ] + other_locations:
    location = os.path.abspath(location)
    for dataset in os.listdir(location):
      ds_path = os.path.join(location, dataset)
      if os.path.isdir(ds_path) and not os.path.islink(ds_path):
        path_resolved = pathlib.Path(ds_path).resolve()
        if dataset not in datasets:
          datasets[dataset] = set()
        datasets[dataset].add(path_resolved)
  all_ok = True
  to_link = []
  for dataset, paths in datasets.items():
    if len(paths) > 1:
      print(f'{dataset}: multiple locations: {" ".join(paths)}')
      all_ok = False
    else:
      orig_path = list(paths)[0]
      target_path = os.path.join(central_storage, dataset)
      if os.path.exists(target_path):
        if os.path.islink(target_path):
          if pathlib.Path(target_path).resolve() != orig_path:
            os.remove(target_path)
        else:
          if pathlib.Path(target_path).resolve() != orig_path:
            print(f'{dataset}: target file already exists and it is not a symlink.')
            all_ok = False
      if not os.path.exists(target_path):
        to_link.append((dataset, orig_path, target_path))

  if all_ok:
    for dataset, source, dest in to_link:
      print(f'{dataset} -> {source}')
      os.symlink(source, dest)
  else:
    print("ERROR: consistency check has failed.")

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description='Create symlinks for dataset directories')
  parser.add_argument('--central', required=True, type=str,
                      help="Central storage where symlinks will be created")
  parser.add_argument('location', type=str, nargs='*', help="storage location")
  args = parser.parse_args()

  link_datasets(args.central, args.location)
