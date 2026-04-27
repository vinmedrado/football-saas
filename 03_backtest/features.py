import pandas as pd

# ==============================
# FEATURES POR TIME
# ==============================
def add_team_features(df, window=5):
    if "Home" in df.columns:
        df["ma_home"] = (
            df.groupby("Home")["value"]
            .rolling(window, min_periods=1)
            .mean()
            .shift(1)
            .reset_index(level=0, drop=True)
        )
        df["zscore_Home"] = (df["value"] - df["ma_home"]) / (df.groupby("Home")["value"].rolling(window, min_periods=1).std().shift(1).reset_index(level=0, drop=True).replace(0,0.0001))
    if "Away" in df.columns:
        df["ma_away"] = (
            df.groupby("Away")["value"]
            .rolling(window, min_periods=1)
            .mean()
            .shift(1)
            .reset_index(level=0, drop=True)
        )
        df["zscore_Away"] = (df["value"] - df["ma_away"]) / (df.groupby("Away")["value"].rolling(window, min_periods=1).std().shift(1).reset_index(level=0, drop=True).replace(0,0.0001))
    return df

# ==============================
# FEATURES POR LIGA
# ==============================
def add_league_features(df):
    if "League_std" in df.columns:
        df["league_mean"] = df.groupby("League_std")["value"].transform("mean")
        df["league_std"] = df.groupby("League_std")["value"].transform("std").replace(0,0.0001)
        df["zscore_league"] = (df["value"] - df["league_mean"]) / df["league_std"]
    return df

# ==============================
# FEATURES DE FORÇA
# ==============================
def add_strength_features(df):
    if "PPG_H" in df.columns and "PPG_A" in df.columns:
        df["strength_diff"] = df["PPG_H"] - df["PPG_A"]
    if "XG_H_Pre" in df.columns and "XG_A_Pre" in df.columns:
        df["xg_diff"] = df["XG_H_Pre"] - df["XG_A_Pre"]
    return df

# ==============================
# DIFERENÇAS
# ==============================
def add_diff_features(df):
    if "league_mean" in df.columns:
        df["diff_league"] = df["value"] - df["league_mean"]
    if "ma" in df.columns:
        df["diff_ma"] = df["value"] - df["ma"]
    return df

# ==============================
# PIPELINE COMPLETO
# ==============================
def apply_all_features(df, window=5):
    df = add_team_features(df, window)
    df = add_league_features(df)
    df = add_strength_features(df)
    df = add_diff_features(df)
    return df