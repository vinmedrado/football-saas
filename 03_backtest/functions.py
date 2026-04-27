import pandas as pd

# ==============================
# MÉDIA MÓVEL GLOBAL (POR LIGA)
# shift(1) garante que só usa jogos ANTERIORES
# ==============================
def calcular_media_movel(df, window):
    df = df.sort_values("Date")
    df["ma"] = (
        df.groupby("League_std")["value"]
        .rolling(window, min_periods=1)
        .mean()
        .shift(1)
        .reset_index(level=0, drop=True)
    )
    return df

# ==============================
# DESVIO PADRÃO GLOBAL
# shift(1) garante que só usa jogos ANTERIORES
# ==============================
def calcular_desvio_padrao(df, window):
    df = df.sort_values("Date")
    df["std"] = (
        df.groupby("League_std")["value"]
        .rolling(window, min_periods=1)
        .std()
        .shift(1)
        .reset_index(level=0, drop=True)
    )
    df["std"] = df["std"].replace(0, 0.0001)
    return df

# ==============================
# Z-SCORE GLOBAL
# Compara value atual vs média/std ANTERIORES — correto para ML
# ==============================
def calcular_zscore(df):
    df["zscore"] = ((df["value"] - df["ma"]) / df["std"]).clip(-5, 5)
    return df

# ==============================
# MÉDIA POR TIME
# shift(1) garante que só usa jogos ANTERIORES do time
# ==============================
def calcular_media_movel_grupo(df, group_col, window):
    df = df.sort_values("Date")
    df[f"ma_{group_col}"] = (
        df.groupby(group_col)["value"]
        .rolling(window, min_periods=1)
        .mean()
        .shift(1)
        .reset_index(level=0, drop=True)
    )
    return df

# ==============================
# DESVIO POR TIME
# shift(1) garante que só usa jogos ANTERIORES do time
# ==============================
def calcular_std_grupo(df, group_col, window):
    df = df.sort_values("Date")
    df[f"std_{group_col}"] = (
        df.groupby(group_col)["value"]
        .rolling(window, min_periods=1)
        .std()
        .shift(1)
        .reset_index(level=0, drop=True)
    )
    df[f"std_{group_col}"] = df[f"std_{group_col}"].replace(0, 0.0001)
    return df

# ==============================
# Z-SCORE POR TIME
# Compara value atual vs média/std ANTERIORES do time
# ==============================
def calcular_zscore_grupo(df, group_col):
    df[f"zscore_{group_col}"] = (
        (df["value"] - df[f"ma_{group_col}"]) / df[f"std_{group_col}"]
    ).clip(-5, 5)
    return df

# ==============================
# MÉDIA EXPANSIVA LIGA
# shift(1) garante que só usa jogos ANTERIORES da liga
# ==============================
def calcular_media_liga(df):
    df = df.sort_values("Date")
    df["league_mean"] = (
        df.groupby("League_std")["value"]
        .expanding()
        .mean()
        .shift(1)
        .reset_index(level=0, drop=True)
    )
    return df

# ==============================
# DESVIO EXPANSIVO LIGA
# shift(1) garante que só usa jogos ANTERIORES da liga
# ==============================
def calcular_std_liga(df):
    df = df.sort_values("Date")
    df["league_std"] = (
        df.groupby("League_std")["value"]
        .expanding()
        .std()
        .shift(1)
        .reset_index(level=0, drop=True)
    )
    df["league_std"] = df["league_std"].replace(0, 0.0001)
    return df

# ==============================
# Z-SCORE LIGA
# Compara value atual vs média/std ANTERIORES da liga
# ==============================
def calcular_zscore_liga(df):
    df["zscore_league"] = (
        (df["value"] - df["league_mean"]) / df["league_std"]
    ).clip(-5, 5)
    return df

# ==============================
# SINAL INTELIGENTE
# FIX: usa shift(1) nos z-scores para garantir que o sinal
# é baseado em padrões ANTERIORES ao jogo — sem leakage
# ==============================
def gerar_sinal_inteligente(df, z_global=1.5, z_league=1.0, z_team=1.0):

    # shift(1) — usa zscore do jogo ANTERIOR para decidir apostar no jogo ATUAL
    z_global_prev  = df["zscore"].shift(1).fillna(0)
    z_league_prev  = df["zscore_league"].shift(1).fillna(0)
    z_home_prev    = df.get("zscore_Home", pd.Series(0, index=df.index)).shift(1).fillna(0)
    z_away_prev    = df.get("zscore_Away", pd.Series(0, index=df.index)).shift(1).fillna(0)

    z_team_combined = (z_home_prev + z_away_prev) / 2

    score = (
        (z_global_prev  > z_global).astype(int) +
        (z_league_prev  > z_league).astype(int) +
        (z_team_combined > z_team).astype(int)
    )

    df["signal"] = score >= 2

    return df
