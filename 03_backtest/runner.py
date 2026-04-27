import pandas as pd
import numpy as np
import json
import os
from datetime import datetime
from itertools import combinations
from functions import (
    calcular_media_movel,
    calcular_desvio_padrao,
    calcular_zscore,
    calcular_media_liga,
    calcular_std_liga,
    calcular_zscore_liga,
    calcular_media_movel_grupo,
    calcular_std_grupo,
    calcular_zscore_grupo,
    gerar_sinal_inteligente
)

# ==============================
# PATHS
# ==============================
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE  = os.path.join(BASE_DIR, "config.json")
MARKET_FILE  = os.path.join(os.path.dirname(BASE_DIR), "02_validation", "markets_map.json")
DATA_DIR     = os.path.join(os.path.dirname(BASE_DIR), "data", "eventos")
BASE_OFICIAL = os.path.join(os.path.dirname(BASE_DIR), "data", "base_oficial.csv")
OUTPUT_DIR   = os.path.join(BASE_DIR, "results")
os.makedirs(OUTPUT_DIR, exist_ok=True)

LOG_FILE = os.path.join(OUTPUT_DIR, "backtest_log.txt")
open(LOG_FILE, "w").close()

# ==============================
# Função de log colorido
# ==============================
def log(msg, status="info"):
    now = datetime.now().strftime("%H:%M:%S")
    color_reset = "\033[0m"
    colors = {
        "info": "\033[90m",     # cinza
        "ok"  : "\033[92m",     # verde
        "warn": "\033[93m",     # amarelo
        "erro": "\033[91m",     # vermelho
    }
    prefix = colors.get(status, colors["info"])
    formatted_msg = f"[{now}] {prefix}{msg}{color_reset}"
    print(formatted_msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{now}] {msg}\n")

# ==============================
# LOAD CONFIG
# ==============================
with open(CONFIG_FILE, "r", encoding="utf-8") as f:
    config = json.load(f)

with open(MARKET_FILE, "r", encoding="utf-8") as f:
    market_map = json.load(f)

events   = config["events"]
filters  = config.get("filters", {})
strategy = config.get("strategy", {})
windows  = [3, 5, 7, 10]

# ==============================
# LOAD BASE OFICIAL (odds reais)
# ==============================
log("Carregando base_oficial com odds reais...")
if os.path.exists(BASE_OFICIAL):
    df_odds = pd.read_csv(BASE_OFICIAL, encoding="utf-8-sig")
    df_odds = df_odds[["Game_ID"] + [c for c in df_odds.columns if c.startswith("O_") or c.startswith("U_")]]
    df_odds = df_odds.drop_duplicates("Game_ID")
    log(f"{len(df_odds)} jogos com odds carregados", "ok")
else:
    df_odds = None
    log("base_oficial.csv não encontrado — usando odd default 1.50", "warn")

# ==============================
# TARGET POR LIGA
# ==============================
def create_target(df, thresholds_liga, threshold_default, event_name):
    df = df.sort_values("Date").reset_index(drop=True)
    def get_threshold(liga):
        return thresholds_liga.get(liga, threshold_default)
    df[event_name] = df.apply(
        lambda row: int(row["value"] > get_threshold(row["League_std"])),
        axis=1
    )
    return df

# ==============================
# FEATURES COMBINADAS
# ==============================
def add_features(df, event_name, max_features=100):
    exclude  = {event_name, "value", "signal", "apostar", "retorno", "odd"}
    num_cols = [c for c in df.select_dtypes(include="number").columns if c not in exclude]
    new_cols = {}
    count    = 0
    for a, b in combinations(num_cols, 2):
        new_cols[f"{a}_minus_{b}"] = df[a] - df[b]
        new_cols[f"{a}_ratio_{b}"] = df[a] / (df[b] + 0.001)
        count += 2
        if count >= max_features:
            break
    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)
    return df

# ==============================
# LOOP PRINCIPAL
# ==============================
log("INÍCIO DO BACKTEST")
log("=" * 60)

resumo_final = []

for event in events:

    key               = event["source"]
    thresholds_liga   = event.get("thresholds_liga", {})
    threshold_default = event.get("threshold_default", 0.5)
    odd_col           = event.get("odd_col", None)
    odd_default       = event.get("odd_default", 1.50)

    if key not in market_map:
        log(f"Market não encontrado: {key}", "erro")
        continue

    market_info = market_map[key]
    event_name  = market_info["event"]
    market_type = market_info["type"]

    file_name = event_name + ("_long" if market_type == "evento_minuto" else "")
    file_path = os.path.join(DATA_DIR, f"{file_name}.csv")

    if not os.path.exists(file_path):
        log(f"Arquivo não encontrado: {file_path}", "erro")
        continue

    df = pd.read_csv(file_path, encoding="utf-8-sig")

    # ==============================
    # FILTROS
    # ==============================
    if filters.get("leagues") and "League_std" in df.columns:
        df = df[df["League_std"].isin(filters["leagues"])]
    if filters.get("teams") and "Home" in df.columns:
        df = df[(df["Home"].isin(filters["teams"])) | (df["Away"].isin(filters["teams"]))]
    if df.empty:
        log(f"Dataset vazio: {key}", "warn")
        continue

    # ==============================
    # AGRUPAMENTO
    # ==============================
    if market_type == "evento_minuto":
        df_grouped = df.groupby("Game_ID")["minute"].sum().reset_index()
        df_grouped.rename(columns={"minute": "value"}, inplace=True)
    else:
        df_grouped = df.groupby("Game_ID")["value"].sum().reset_index()

    meta_cols  = ["Game_ID", "Date", "League_std", "Home", "Away"]
    df_meta    = df[meta_cols].drop_duplicates("Game_ID")
    df_grouped = df_grouped.merge(df_meta, on="Game_ID", how="left")
    df_grouped["Date"] = pd.to_datetime(df_grouped["Date"])
    df_grouped = df_grouped.sort_values("Date").reset_index(drop=True)

    # ==============================
    # JOIN COM ODDS REAIS
    # ==============================
    if df_odds is not None and odd_col is not None:
        df_grouped = df_grouped.merge(
            df_odds[["Game_ID", odd_col]].rename(columns={odd_col: "odd"}),
            on="Game_ID",
            how="left"
        )
        df_grouped["odd"] = df_grouped["odd"].fillna(odd_default)
    else:
        df_grouped["odd"] = odd_default

    # ==============================
    # FEATURES TEMPORAIS
    # ==============================
    for w in windows:
        df_temp = df_grouped.copy()
        df_temp = calcular_media_movel(df_temp, w)
        df_temp = calcular_desvio_padrao(df_temp, w)
        df_temp = calcular_zscore(df_temp)
        df_grouped[f"ma_{w}"]  = df_temp["ma"]
        df_grouped[f"std_{w}"] = df_temp["std"]
        df_grouped[f"z_{w}"]   = df_temp["zscore"]

    # ==============================
    # CONTEXTO LIGA & TIME
    # ==============================
    df_grouped = calcular_media_liga(df_grouped)
    df_grouped = calcular_std_liga(df_grouped)
    df_grouped = calcular_zscore_liga(df_grouped)
    for w in windows:
        for team in ["Home", "Away"]:
            df_grouped = calcular_media_movel_grupo(df_grouped, team, w)
            df_grouped = calcular_std_grupo(df_grouped, team, w)
            df_grouped = calcular_zscore_grupo(df_grouped, team)

    # ==============================
    # ZSCORE GLOBAL
    # ==============================
    z_cols  = [f"z_{w}" for w in windows if f"z_{w}" in df_grouped.columns]
    weights = np.array([0.4, 0.3, 0.2, 0.1])[:len(z_cols)]
    weights = weights / weights.sum()
    df_grouped["zscore"] = sum(df_grouped[col] * w for col, w in zip(z_cols, weights))

    # ==============================
    # SINAL
    # ==============================
    df_grouped = gerar_sinal_inteligente(
        df_grouped,
        z_global=strategy.get("z_global", 1.5),
        z_league=strategy.get("z_league", 1.0),
        z_team=strategy.get("z_team", 1.0),
    )

    # ==============================
    # TARGET
    # ==============================
    df_grouped = create_target(df_grouped, thresholds_liga, threshold_default, event_name)
    df_grouped = df_grouped.dropna(subset=[event_name])
    df_grouped = add_features(df_grouped, event_name, max_features=100)
    df_grouped.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_grouped.fillna(0, inplace=True)

    # ==============================
    # BACKTEST
    # ==============================
    df_grouped["retorno"] = np.where(df_grouped[event_name] == 1, df_grouped["odd"] - 1, -1.0)
    df_grouped["apostar"] = df_grouped["signal"] == 1

    apostas       = df_grouped[df_grouped["apostar"]]
    total_apostas  = len(apostas)
    lucro_total    = apostas["retorno"].sum()
    winrate        = apostas[event_name].mean() if total_apostas > 0 else 0
    roi            = lucro_total / total_apostas if total_apostas > 0 else 0
    odd_media      = apostas["odd"].mean() if total_apostas > 0 else odd_default
    target_pct     = df_grouped[event_name].mean()

    output_path = os.path.join(OUTPUT_DIR, f"{key}_ml.csv")
    df_grouped.to_csv(output_path, index=False, encoding="utf-8-sig")

    resumo_final.append({
        "event"     : event_name,
        "market"    : key,
        "apostas"   : total_apostas,
        "lucro"     : lucro_total,
        "winrate"   : winrate,
        "roi"       : roi,
        "odd_media" : round(odd_media, 3),
        "target_pct": round(target_pct, 3),
        "odd_col"   : odd_col or "default",
    })

    log(
        f"{event_name:<25} | apostas: {total_apostas:>5} | "
        f"winrate: {winrate:.2%} | ROI: {roi:>+.3f} | "
        f"odd_media: {odd_media:.2f} | target%: {target_pct:.2%}", "ok"
    )

# ==============================
# RESUMO FINAL
# ==============================
resumo_df = pd.DataFrame(resumo_final)
resumo_df.to_csv(os.path.join(OUTPUT_DIR, "resumo.csv"), index=False)

log("=" * 60)
log("BACKTEST FINALIZADO", "ok")
log("\nRANKING POR ROI:\n" + resumo_df.sort_values("roi", ascending=False).to_string(index=False), "info")