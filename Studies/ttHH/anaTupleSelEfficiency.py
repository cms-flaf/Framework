import numpy as np
import ROOT
ROOT.EnableImplicitMT(4)

import os
import sys

if __name__ == "__main__":
  file_dir = os.path.dirname(os.path.abspath(__file__))
  base_dir = os.path.dirname(file_dir)
  if base_dir not in sys.path:
    sys.path.append(base_dir)
  __package__ = os.path.split(file_dir)[-1]

from Common.Utilities import *

def compute_eff(input_file):
  df = ROOT.RDataFrame("Events", input_file)
  channels = sorted(np.unique(df.AsNumpy(['channelId'])['channelId']))
  print(f'n_channels = {len(channels)}')
  total_cnt = df.Count()


  correct_branches = []
  for leg_idx in range(4):
    br_name = f"tau{leg_idx+1}_correct"
    df = df.Define(br_name, f"""
    if(tau{leg_idx+1}_legType == 0) return true;
    if(tau{leg_idx+1}_legType == 1) return tau{leg_idx+1}_gen_kind == 1 || tau{leg_idx+1}_gen_kind == 3;
    if(tau{leg_idx+1}_legType == 2) return tau{leg_idx+1}_gen_kind == 2 || tau{leg_idx+1}_gen_kind == 4;
    if(tau{leg_idx+1}_legType == 3) return tau{leg_idx+1}_gen_kind == 5;
    throw std::runtime_error("Invalid leg type");
    """)
    correct_branches.append(br_name)
  df = df.Define("gen_correct", ' && '.join(correct_branches))

  ch_cnt = {}
  ch_correct_cnt = {}
  for channel in channels:
      ch_cnt[channel] = df.Filter(f"channelId == {channel}").Count()
      ch_correct_cnt[channel] = df.Filter(f"channelId == {channel} && gen_correct").Count()

  total_cnt = total_cnt.GetValue()
  print(f'Total: cnt={total_cnt}')
  out = []
  for channel in channels:
    cnt = ch_cnt[channel].GetValue()
    correct_cnt = ch_correct_cnt[channel].GetValue()
    out.append((channel, cnt, correct_cnt))


  cum_cnt = 0
  for channel, cnt, correct_cnt,  in sorted(out, key=lambda x: -x[1]):
    cum_cnt += cnt
    print(f'{channel}: cnt={cnt} ({cnt/total_cnt * 100:.2f}, {cum_cnt/total_cnt * 100:.2f}) correct={correct_cnt} ({correct_cnt/cnt * 100:.2f})')
if __name__ == "__main__":

  import argparse
  parser = argparse.ArgumentParser(description='ttHH Selection efficiency.')
  parser.add_argument('--input', required=True, type=str, help="Input file")
  args = parser.parse_args()

  compute_eff(args.input)
