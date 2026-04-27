#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PREDICT PRO 🎯
- Lê jogos + odds direto do JSONL do scraper (sem API Football)
- Padroniza ligas via dicionario_ligas_flash.csv
- Padroniza times via dicionario_times_flash.csv (exato) + fuzzy fallback
- Previsão por mercado com EV e threshold por liga
- Relatório final + CSV
"""

import os
import sys
import json
import pickle
import logging
import numpy as np
import pandas as pd
from datetime import date
from rapidfuzz import process, fuzz

# ==============================
# CONFIG
# ==============================
BASE_DIR          = os.path.dirname(os.path.abspath(__file__))
DATA_DIR          = os.path.join(os.path.dirname(BASE_DIR), "data", "eventos")
DICT_TIMES        = os.path.join(os.path.dirname(BASE_DIR), "data", "dicionario_times.csv")
DICT_TIMES_FLASH  = os.path.join(os.path.dirname(BASE_DIR), "data", "dicionario_times_flash.csv")
DICT_LIGAS        = os.path.join(os.path.dirname(BASE_DIR), "data", "dicionario_ligas_flash.csv")
BACKTEST_DIR      = os.path.join(os.path.dirname(BASE_DIR), "03_backtest")
MODEL_DIR         = os.path.join(BASE_DIR, "models")
DATASET_DIR       = os.path.join(BASE_DIR, "datasets")
ODDS_DIR          = os.path.join(os.path.dirname(BASE_DIR), "jogos_futuros")

sys.path.insert(0, BACKTEST_DIR)

CONFIDENCE_THRESHOLD = 0.62
WINDOWS              = [3, 5, 7, 10]
LIMITE_FUZZY_TIMES   = 70   # fallback quando não está no dicionario_times_flash
LIMITE_FUZZY_LIGAS   = 80
MIN_ROI_BT           = 0.0
MIN_EV               = 0.05

# Mapeamento market → (chave_scraper, linha_ou, campo)
MAPA_ODDS_SCRAPER = {
    "TG_HT" : ("Odds_OU_HT",  "OU_0.5", "Over"),
    "G_A_FT": ("Odds_OU_FT",  "OU_0.5", "Over"),
    "TG_FT" : ("Odds_OU_FT",  "OU_2.5", "Over"),
    "O_C_H" : ("Odds_1X2_FT", None,     None),
    "G_H_HT": ("Odds_1X2_HT", None,     None),
    "G_A_HT": ("Odds_1X2_HT", None,     None),
    "G_H_FT": ("Odds_1X2_FT", None,     None),
    "TC_FT" : ("Odds_OU_FT",  "OU_9.5", "Over"),
    "C_H_FT": ("Odds_OU_FT",  "OU_8.5", "Over"),
    "C_A_FT": ("Odds_OU_FT",  "OU_7.5", "Over"),
}

# ==============================
# LOG
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s - %(message)s",
    datefmt="%H:%M:%S"
)

# ==============================
# IMPORTS DO BACKTEST
# ==============================
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
)

# ==============================
# DICIONÁRIOS
# ==============================
def normalizar_nome(nome):
    if pd.isna(nome):
        return nome
    nome = str(nome).lower().strip()
    nome = nome.replace(".", "")
    nome = nome.replace("-", " ")
    nome = nome.replace("fc", "")
    nome = nome.replace("cf", "")
    nome = nome.replace("club", "")
    return " ".join(nome.split())

def carregar_dicionario_times(path):
    """Carrega dicionario_times.csv (Footystats) para fuzzy fallback."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Dicionário de times não encontrado: {path}")

    df = pd.read_csv(path)

    # Limpar nome das colunas
    df.columns = df.columns.astype(str).str.strip()

    # Limpar valores string com segurança
    for col in df.columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

    logging.info(f"Dicionário de times (Footystats): {len(df)} registros")
    return df


def carregar_dicionario_times_flash(path):
    """
    Carrega dicionario_times_flash.csv com colunas Time_flash, Time_padronizado.
    Retorna dict: {time_flash: time_padronizado}
    """
    if not os.path.exists(path):
        logging.warning("dicionario_times_flash.csv não encontrado — usando só fuzzy")
        return {}
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].str.strip()
    mapa = dict(zip(df["Time_flash"], df["Time_padronizado"]))
    logging.info(f"Dicionário de times (Flash): {len(mapa)} mapeamentos")
    return mapa


def carregar_dicionario_ligas(path):
    """
    Carrega dicionario_ligas_flash.csv com colunas Flash_nome, Footystats_nome.
    Retorna dict: {flash_nome: footystats_nome}
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Dicionário de ligas não encontrado: {path}")
    df   = pd.read_csv(path)
    mapa = dict(zip(df["Flash_nome"].str.strip(), df["Footystats_nome"].str.strip()))
    logging.info(f"Dicionário de ligas (Flash): {len(mapa)} mapeamentos")
    return mapa


def padronizar_liga(liga_flash, mapa_ligas, limite=LIMITE_FUZZY_LIGAS):
    """Mapeia nome da liga do Flashscore para nome do Footystats."""

    if pd.isna(liga_flash):
        return liga_flash, False

    # normaliza entrada
    liga_flash_norm = normalizar_nome(liga_flash)

    #cria mapa normalizado (uma vez por chamada)
    mapa_norm = {normalizar_nome(k): k for k in mapa_ligas.keys()}

    # 1. match direto (normalizado)
    if liga_flash_norm in mapa_norm:
        liga_original = mapa_norm[liga_flash_norm]
        return mapa_ligas[liga_original], True

    # 2. fuzzy com nomes normalizados
    resultado = process.extractOne(
        liga_flash_norm,
        list(mapa_norm.keys()),
        scorer=fuzz.ratio
    )

    if resultado:
        score = resultado[1]
        liga_match_norm = resultado[0]
        liga_original = mapa_norm[liga_match_norm]

        if score >= limite:
            logging.debug(
                f"Fuzzy liga: '{liga_flash}' → '{liga_original}' ({score:.0f})"
            )
            return mapa_ligas[liga_original], True
        else:
            logging.warning(
                f"Sem match liga: '{liga_flash}' — melhor '{liga_original}' ({score:.0f})"
            )

    return liga_flash, False


def padronizar_time(time_flash, liga_std, df_dic, mapa_flash, limite=LIMITE_FUZZY_TIMES):
    """
    Mapeia nome do time do Flashscore para nome padronizado do Footystats.
    Estratégia em 3 camadas:
    1. Dicionário direto (dicionario_times_flash.csv) — match exato
    2. Fuzzy contra os times da liga no dicionario_times.csv
    3. Rejeita se score muito baixo
    """
    if pd.isna(time_flash):
        return time_flash, True

    # 1. Dicionário direto
    if time_flash in mapa_flash:
        return mapa_flash[time_flash], False

    # 2. Fuzzy contra times da liga
    df_filtrado = df_dic[df_dic["League_padronizada"] == liga_std]
    times_ref   = df_filtrado["Time_padronizado"].tolist()

    if not times_ref:
        logging.warning(f"Liga sem times no dicionário: '{liga_std}'")
        return time_flash, True

    resultado = process.extractOne(time_flash, times_ref, scorer=fuzz.ratio)
    if resultado:
        score = resultado[1]
        nome  = resultado[0]

        if score >= 85:
            return nome, False
        elif score >= limite:
            logging.warning(
                f"Fuzzy baixo: '{time_flash}' → '{nome}' ({score:.0f}) [{liga_std}]"
                f" — adicione ao dicionario_times_flash.csv"
            )
            return nome, False
        else:
            logging.warning(
                f"Sem match: '{time_flash}' ({liga_std}) — melhor foi '{nome}' ({score:.0f})"
            )
            return time_flash, True

    return time_flash, True


# ==============================
# CARREGA JOGOS DO SCRAPER (JSONL)
# ==============================
def carregar_jogos_scraper(data_str, mapa_ligas, df_dic_times, mapa_times_flash):
    """
    Lê o JSONL do scraper e já padroniza ligas e times.
    Retorna:
      - jogos_validos:      lista de dicts com info padronizada
      - odds_por_par:       {(home_flash, away_flash): match_data}
      - jogos_ignorados:    lista de jogos não mapeados
      - ligas_nao_mapeadas: set de ligas sem mapeamento
    """
    odds_file = os.path.join(ODDS_DIR, f"{data_str}.jsonl")
    if not os.path.exists(odds_file):
        logging.warning(f"Arquivo JSONL não encontrado: {odds_file}")
        return [], {}, [], set()

    jogos_validos      = []
    jogos_ignorados    = []
    ligas_nao_mapeadas = set()
    odds_por_par       = {}

    with open(odds_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                jogo = json.loads(line)
            except json.JSONDecodeError:
                continue

            home_flash = jogo.get("Home", "").strip()
            away_flash = jogo.get("Away", "").strip()
            liga_flash = jogo.get("League", "").strip()
            date_str   = jogo.get("Date", "").strip()
            time_str   = jogo.get("Time", "00:00").strip()

            if not home_flash or not away_flash or not liga_flash:
                continue

            # Indexa odds pelo par de nomes do Flashscore
            odds_por_par[(home_flash, away_flash)] = jogo

            # Padroniza liga
            liga_std, liga_ok = padronizar_liga(liga_flash, mapa_ligas)
            if not liga_ok:
                ligas_nao_mapeadas.add(liga_flash)
                jogos_ignorados.append((home_flash, away_flash, liga_flash, "liga não mapeada"))
                continue

            # Padroniza times
            home_std, home_novo = padronizar_time(home_flash, liga_std, df_dic_times, mapa_times_flash)
            away_std, away_novo = padronizar_time(away_flash, liga_std, df_dic_times, mapa_times_flash)

            if home_novo or away_novo:
                motivos = []
                if home_novo: motivos.append(f"'{home_flash}' não mapeado")
                if away_novo: motivos.append(f"'{away_flash}' não mapeado")
                jogos_ignorados.append((home_flash, away_flash, liga_flash, " | ".join(motivos)))
                continue

            # Converte data "DD/MM/YYYY HH:MM" → datetime
            try:
                data_jogo = pd.to_datetime(f"{date_str} {time_str}", format="%d/%m/%Y %H:%M")
            except Exception:
                data_jogo = pd.Timestamp(date.today())

            jogos_validos.append({
                "Date"       : data_jogo,
                "Home"       : home_std,
                "Away"       : away_std,
                "League_std" : liga_std,
                "Home_flash" : home_flash,
                "Away_flash" : away_flash,
            })

    logging.info(f"Jogos no JSONL    : {len(odds_por_par)}")
    logging.info(f"Jogos válidos     : {len(jogos_validos)}")
    logging.info(f"Ignorados         : {len(jogos_ignorados)}")
    return jogos_validos, odds_por_par, jogos_ignorados, ligas_nao_mapeadas


# ==============================
# EXTRAI ODD DO MERCADO
# ==============================
def extrair_odd_mercado(match_data, market):
    if market not in MAPA_ODDS_SCRAPER:
        return None

    tipo_odds, linha, campo = MAPA_ODDS_SCRAPER[market]
    odds_section = match_data.get(tipo_odds)

    if not odds_section:
        return None

    try:
        if linha is not None:
            if not isinstance(odds_section, dict):
                return None
            casas = odds_section.get(linha, [])
            if not casas:
                return None
            valores = [c.get(campo, 0) for c in casas if c.get(campo)]
            return max(valores) if valores else None
        else:
            if not isinstance(odds_section, list) or not odds_section:
                return None
            if market in ("G_H_HT", "G_H_FT", "O_C_H"):
                campo_1x2 = "Odd_1"
            elif market in ("G_A_HT", "G_A_FT"):
                campo_1x2 = "Odd_2"
            else:
                campo_1x2 = "Odd_1"
            valores = [c.get(campo_1x2, 0) for c in odds_section if c.get(campo_1x2)]
            return max(valores) if valores else None

    except Exception as e:
        logging.debug(f"Erro ao extrair odd [{market}]: {e}")
        return None


# ==============================
# LOAD MODELOS + ARTEFATOS
# ==============================
logging.info("=" * 55)
logging.info("📂 Carregando modelos e artefatos...")

resumo_path   = os.path.join(MODEL_DIR, "resumo_modelos.pkl")
encoders_path = os.path.join(DATASET_DIR, "label_encoders.pkl")
config_path   = os.path.join(BACKTEST_DIR, "config.json")

for path in [resumo_path, encoders_path, config_path]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Não encontrado: {path}")

with open(resumo_path,   "rb") as f: resumo_modelos = pickle.load(f)
with open(encoders_path, "rb") as f: encoders       = pickle.load(f)
with open(config_path,   "r",  encoding="utf-8") as f: config_bt = json.load(f)

mapa_threshold = {
    e["source"]: {
        "thresholds_liga"  : e.get("thresholds_liga", {}),
        "threshold_default": e.get("threshold_default", 0.5),
    }
    for e in config_bt["events"]
}

resumo_modelos = [m for m in resumo_modelos if m["roi_bt"] >= MIN_ROI_BT]

modelos_carregados = {}
for meta in resumo_modelos:
    market     = meta["market"]
    model_path = os.path.join(MODEL_DIR, market, "model.pkl")
    feat_path  = os.path.join(DATASET_DIR, market, "feature_columns.pkl")

    if not os.path.exists(model_path) or not os.path.exists(feat_path):
        logging.warning(f"[{market}] arquivos não encontrados — pulando")
        continue

    with open(model_path, "rb") as f: model           = pickle.load(f)
    with open(feat_path,  "rb") as f: feature_columns = pickle.load(f)

    thresh_info = mapa_threshold.get(market, {})
    modelos_carregados[market] = {
        "model"            : model,
        "feature_columns"  : feature_columns,
        "meta"             : meta,
        "thresholds_liga"  : thresh_info.get("thresholds_liga", {}),
        "threshold_default": thresh_info.get("threshold_default", 0.5),
    }

logging.info(f"{len(modelos_carregados)} modelos carregados")
for market, m in modelos_carregados.items():
    meta = m["meta"]
    logging.info(
        f"  [{market}] {meta['event']:<25} "
        f"threshold={m['threshold_default']} | "
        f"AUC={meta['auc']:.4f} | ROI_bt={meta['roi_bt']:+.3f}"
    )

# ==============================
# LOAD DICIONÁRIOS
# ==============================
logging.info("=" * 55)
logging.info("📖 Carregando dicionários...")
df_dic_times     = carregar_dicionario_times(DICT_TIMES)
mapa_times_flash = carregar_dicionario_times_flash(DICT_TIMES_FLASH)
mapa_ligas       = carregar_dicionario_ligas(DICT_LIGAS)

# ==============================
# CARREGA JOGOS + ODDS DO SCRAPER
# ==============================
logging.info("=" * 55)
logging.info(f"📊 Carregando jogos do scraper ({date.today()})...")

jogos_validos, odds_por_par, jogos_ignorados, ligas_nao_mapeadas = carregar_jogos_scraper(
    str(date.today()), mapa_ligas, df_dic_times, mapa_times_flash
)

if ligas_nao_mapeadas:
    logging.warning(f"Ligas sem mapeamento ({len(ligas_nao_mapeadas)}):")
    for liga in sorted(ligas_nao_mapeadas):
        logging.warning(f"  → {liga}")

if not jogos_validos:
    logging.warning("Nenhum jogo válido para prever hoje.")
    sys.exit()

df_hoje = pd.DataFrame(jogos_validos)

# ==============================
# LOAD HISTÓRICO LOCAL
# ==============================
logging.info("=" * 55)
logging.info("📂 Carregando histórico local...")

historico_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
if not historico_files:
    raise Exception(f"❌ Nenhum arquivo histórico em {DATA_DIR}")

dfs = []
for fname in historico_files:
    try:
        dfs.append(pd.read_csv(os.path.join(DATA_DIR, fname), encoding="utf-8-sig"))
    except Exception as e:
        logging.warning(f"Erro ao carregar {fname}: {e}")

df_historico = pd.concat(dfs, ignore_index=True)
df_historico["Date"] = pd.to_datetime(df_historico["Date"], errors="coerce").dt.tz_localize(None)
df_historico = df_historico.dropna(subset=["Date"])
logging.info(f"Histórico carregado: {df_historico.shape}")

# ==============================
# CALCULA FEATURES
# ==============================
def calcular_features_jogo(home, away, liga, data_jogo, df_historico):
    df_liga = df_historico[
        (df_historico["League_std"] == liga) &
        (df_historico["Date"] < data_jogo)
    ].copy()

    if df_liga.empty:
        return None

    if "Game_ID" in df_liga.columns:
        df_agg  = df_liga.groupby("Game_ID")["value"].sum().reset_index()
        mc      = [c for c in ["Game_ID", "Date", "League_std", "Home", "Away"] if c in df_liga.columns]
        df_meta = df_liga[mc].drop_duplicates("Game_ID")
        df_agg  = df_agg.merge(df_meta, on="Game_ID", how="left")
    else:
        df_agg = df_liga.copy()

    df_agg["Date"] = pd.to_datetime(df_agg["Date"])
    df_agg = df_agg.sort_values("Date")

    jogo_row = pd.DataFrame([{
        "Game_ID"   : 0,
        "Date"      : data_jogo,
        "Home"      : home,
        "Away"      : away,
        "League_std": liga,
        "value"     : df_agg["value"].mean(),
    }])

    df_calc = pd.concat([df_agg, jogo_row], ignore_index=True).sort_values("Date").reset_index(drop=True)

    for w in WINDOWS:
        df_temp = df_calc.copy()
        df_temp = calcular_media_movel(df_temp, w)
        df_temp = calcular_desvio_padrao(df_temp, w)
        df_calc[f"ma_{w}"]  = df_temp["ma"]
        df_calc[f"std_{w}"] = df_temp["std"]

    df_calc = calcular_media_liga(df_calc)
    df_calc = calcular_std_liga(df_calc)

    for w in WINDOWS:
        for team in ["Home", "Away"]:
            df_calc = calcular_media_movel_grupo(df_calc, team, w)
            df_calc = calcular_std_grupo(df_calc, team, w)

    return df_calc.iloc[[-1]].copy()


# ==============================
# PREDIÇÃO
# ==============================
logging.info("=" * 55)
logging.info("⚙️  Gerando previsões...")

resultados_jogos     = {}
odds_nao_encontradas = []

for _, jogo in df_hoje.iterrows():
    home       = jogo["Home"]
    away       = jogo["Away"]
    liga       = jogo["League_std"]
    data_jogo  = jogo["Date"]
    home_flash = jogo["Home_flash"]
    away_flash = jogo["Away_flash"]
    jogo_key   = f"{home} vs {away}"

    row_features = calcular_features_jogo(home, away, liga, data_jogo, df_historico)
    if row_features is None:
        logging.warning(f"Sem histórico para '{liga}' — pulando {jogo_key}")
        continue

    # Label encoding
    for col, le in encoders.items():
        if col in row_features.columns:
            val = str(row_features[col].values[0])
            row_features[col] = le.transform([val])[0] if val in le.classes_ else -1

    row_features.replace([np.inf, -np.inf], 0, inplace=True)
    row_features.fillna(0, inplace=True)

    # Odds — busca direta pelo par de nomes do Flashscore
    match_data = odds_por_par.get((home_flash, away_flash))
    if match_data is None:
        odds_nao_encontradas.append(f"{home_flash} vs {away_flash} ({liga})")

    mercados_jogo = []

    for market, m in modelos_carregados.items():
        model           = m["model"]
        feature_columns = m["feature_columns"]
        meta            = m["meta"]
        threshold       = m["thresholds_liga"].get(liga, m["threshold_default"])

        row_pred = row_features.copy()
        for col in feature_columns:
            if col not in row_pred.columns:
                row_pred[col] = 0

        X_pred = row_pred[feature_columns]

        # Probabilidade do modelo
        prob = model.predict_proba(X_pred)[0][1]

        # 🔥 Define lado automaticamente
        if prob >= 0.5:
            prob_evento = prob
            lado = "original"
        else:
            prob_evento = 1 - prob
            lado = "contrario"

        # Confiança (mantém sua lógica)
        prob_display = prob_evento
        apostar_conf = prob >= CONFIDENCE_THRESHOLD or prob <= (1 - CONFIDENCE_THRESHOLD)

        # Odds
        odd_real = extrair_odd_mercado(match_data, market) if match_data else None
        odd_usar = odd_real if odd_real else 1.50

        # 🔥 EV correto
        ev = (prob_evento * odd_usar) - 1

        # Decisão
        apostar = apostar_conf and ev >= MIN_EV

        mercados_jogo.append({
            "market"      : market,
            "event"       : meta["event"],
            "threshold"   : threshold,
            "prob"        : round(prob, 4),
            "confianca"   : round(prob_display, 4),
            "odd_real"    : round(odd_usar, 2),
            "ev"          : round(ev, 4),
            "apostar"     : apostar,
            "roi_bt"      : meta["roi_bt"],
            "winrate_bt"  : meta["winrate_bt"],
            "auc"         : meta["auc"],
            "tem_odd_real": odd_real is not None,
        })

    if mercados_jogo:
        resultados_jogos[jogo_key] = {
            "home"    : home,
            "away"    : away,
            "liga"    : liga,
            "mercados": sorted(mercados_jogo, key=lambda x: x["ev"], reverse=True),
        }

# ==============================
# RELATÓRIO FINAL
# ==============================
print("\n" + "=" * 60)
print(f"🎯 PREVISÕES DO DIA — {date.today()}")
print("=" * 60)

if not resultados_jogos:
    print("⚠️  Nenhum jogo com histórico suficiente para prever.")
else:
    todas_apostas = []

    for jogo_key, jogo in resultados_jogos.items():
        mercados_apostar = [m for m in jogo["mercados"] if m["apostar"]]
        if not mercados_apostar:
            continue

        print(f"\n  ⚽ {jogo['home']} vs {jogo['away']}")
        print(f"     Liga: {jogo['liga']}")
        print(f"     {'Mercado':<12} {'Evento':<22} {'Thresh':>8} {'Conf':>7} {'Odd':>7} {'EV':>7} {'ROI_bt':>7}")
        print(f"     {'-'*76}")

        for m in mercados_apostar:
            odd_str = f"{m['odd_real']:.2f}{'*' if m['tem_odd_real'] else ' '}"
            print(
                f"     {m['market']:<12} {m['event']:<22} "
                f">{m['threshold']:>6.1f} "
                f"{m['confianca']:>6.1%} "
                f"{odd_str:>7} "
                f"{m['ev']:>+6.1%} "
                f"{m['roi_bt']:>+6.1%}  ✅"
            )
            todas_apostas.append({
                "jogo"        : jogo_key,
                "liga"        : jogo["liga"],
                "market"      : m["market"],
                "event"       : m["event"],
                "threshold"   : m["threshold"],
                "confianca"   : m["confianca"],
                "prob_sim"    : m["prob"],
                "odd_real"    : m["odd_real"],
                "tem_odd_real": m["tem_odd_real"],
                "ev"          : m["ev"],
                "roi_bt"      : m["roi_bt"],
                "winrate_bt"  : m["winrate_bt"],
                "auc"         : m["auc"],
            })

    print(f"\n{'='*60}")
    print(f"  📊 Jogos no JSONL       : {len(odds_por_par)}")
    print(f"  📊 Jogos analisados     : {len(resultados_jogos)}")
    print(f"  ✅ Apostas recomendadas : {len(todas_apostas)}")
    print(f"  🎚️  Threshold confiança  : {int(CONFIDENCE_THRESHOLD*100)}%")
    print(f"  📈 EV mínimo            : {MIN_EV:+.0%}")
    print(f"  * = odd real do scraper")

    if jogos_ignorados:
        print(f"\n  ⚠️  Jogos ignorados ({len(jogos_ignorados)}):")
        for h, a, l, motivo in jogos_ignorados[:10]:
            print(f"     {h} vs {a} ({l}) — {motivo}")
        if len(jogos_ignorados) > 10:
            print(f"     ... e mais {len(jogos_ignorados) - 10}")

    if odds_nao_encontradas:
        print(f"\n  ⚠️  Sem odds ({len(odds_nao_encontradas)} jogos):")
        for j in odds_nao_encontradas:
            print(f"     {j}")

    if todas_apostas:
        output_path = os.path.join(BASE_DIR, f"previsoes_{date.today()}.csv")
        pd.DataFrame(todas_apostas).to_csv(output_path, index=False)
        print(f"\n  💾 Previsões salvas em: {output_path}")

print("=" * 60)