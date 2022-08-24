import Common.BaselineSelection as Baseline

def skim_baseline(df):
  Baseline.Initialize()
  df = Baseline.ApplyRecoBaseline0(df)
  df = Baseline.ApplyRecoBaseline1(df)
  return df

def skim_failed_baseline(df):
  Baseline.Initialize()
  df, b0_filter = Baseline.ApplyRecoBaseline0(df, apply_filter=False)
  df, b1_filter = Baseline.ApplyRecoBaseline1(df, apply_filter=False)
  df = df.Filter(f'!(({b0_filter}) && ({b1_filter}))')
  return df