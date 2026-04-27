import pandas as pd
import numpy as np
import os
import pickle
from sklearn.preprocessing import LabelEncoder

# ==============================
# CONFIG
# ==============================
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
BACKTEST_DIR = os.path.join(os.path.dirname(BASE_DIR), "03_backtest", "results")
OUTPUT_DIR   = os.path.join(BASE_DIR, "datasets")

os.makedirs(OUTPUT_DIR, exist_ok=True)

RESUMO_FILE = os.path.join(BACKTEST_DIR, "resumo.csv")

MIN_ROI     = 0
MIN_APOSTAS = 1000

TRAIN_SPLIT_PCT = 0.8

# Colunas categóricas encodadas
CATEGORICAL_COLS = ["Home", "Away", "League_std"]

# Colunas de leakage — nunca entram como feature
DROP_COLS = {
    "signal", "apostar", "retorno", "odd", "value", "Game_ID",
    "zscore_league", "zscore_Home", "zscore_Away", "zscore",
    "z_3", "z_5", "z_7", "z_10"  
}

INF_NAN_WARN_THRESHOLD = 0.05

# ==============================
# LOAD RESUMO
# ==============================
print("=" * 55)
print("📂 Carregando resumo...")

if not os.path.exists(RESUMO_FILE):
    raise FileNotFoundError(f"❌ Resumo não encontrado: {RESUMO_FILE}")

resumo = pd.read_csv(RESUMO_FILE)
resumo_original = len(resumo)
resumo = resumo[
    (resumo["roi"] > MIN_ROI) &
    (resumo["apostas"] > MIN_APOSTAS)
].reset_index(drop=True)

print(f"🔍 Mercados no resumo     : {resumo_original}")
print(f"✅ Após filtro            : {len(resumo)} (ROI > {MIN_ROI}, apostas > {MIN_APOSTAS})")
print(f"🚫 Descartados pelo filtro: {resumo_original - len(resumo)}")

# ==============================
# LOOP MERCADOS — salva um dataset por mercado
# ==============================
print("\n" + "=" * 55)
print("📥 Processando mercados...\n")

rejected      = []
mercados_ok   = []

# Encoders globais — fitados em todos os dados combinados para consistência
print("🏷️  Preparando encoders globais...")
all_homes, all_aways, all_leagues = [], [], []

for _, row in resumo.iterrows():
    market    = row["market"]
    file_path = os.path.join(BACKTEST_DIR, f"{market}_ml.csv")
    if not os.path.exists(file_path):
        continue
    df = pd.read_csv(file_path)
    df = df[[c for c in df.columns if "Game_ID" not in c]]
    if "Home"       in df.columns: all_homes.extend(df["Home"].dropna().astype(str).tolist())
    if "Away"       in df.columns: all_aways.extend(df["Away"].dropna().astype(str).tolist())
    if "League_std" in df.columns: all_leagues.extend(df["League_std"].dropna().astype(str).tolist())

encoders = {}
if all_homes:
    le = LabelEncoder()
    le.fit(all_homes)
    encoders["Home"] = le
if all_aways:
    le = LabelEncoder()
    le.fit(all_aways)
    encoders["Away"] = le
if all_leagues:
    le = LabelEncoder()
    le.fit(all_leagues)
    encoders["League_std"] = le

for col, le in encoders.items():
    print(f"  ✅ [{col}] {len(le.classes_)} categorias únicas")

encoders_path = os.path.join(OUTPUT_DIR, "label_encoders.pkl")
with open(encoders_path, "wb") as f:
    pickle.dump(encoders, f)
print(f"  💾 Encoders salvos em: {encoders_path}\n")

# ==============================
# PROCESSA CADA MERCADO
# ==============================
print("=" * 55)
print("⚙️  Gerando datasets por mercado...\n")

for _, row in resumo.iterrows():

    market     = row["market"]
    event_name = row["event"]
    roi_bt     = row["roi"]
    winrate_bt = row["winrate"]

    file_path = os.path.join(BACKTEST_DIR, f"{market}_ml.csv")
    if not os.path.exists(file_path):
        rejected.append((market, "arquivo não encontrado"))
        continue

    df = pd.read_csv(file_path)
    df = df[[c for c in df.columns if "Game_ID" not in c]]

    # --- Date ---
    if "Date" not in df.columns:
        rejected.append((market, "coluna Date ausente"))
        continue

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])

    if df.empty:
        rejected.append((market, "sem linhas válidas após limpeza de Date"))
        continue

    # --- Target ---
    if event_name not in df.columns:
        rejected.append((market, f"target '{event_name}' não encontrado"))
        continue

    if df[event_name].nunique() < 2:
        rejected.append((market, f"target sem variação"))
        continue

    target = df[event_name].values

    # --- Drop leakage + target ---
    cols_to_drop = DROP_COLS | {event_name}
    cols_to_keep = [c for c in df.columns if c not in cols_to_drop]

    df_model         = df[cols_to_keep].copy()
    df_model["target"] = target

    # --- Label Encoding ---
    for col, le in encoders.items():
        if col in df_model.columns:
            vals = df_model[col].astype(str).fillna("UNKNOWN")
            # Valores desconhecidos recebem -1
            df_model[col] = vals.apply(
                lambda v: le.transform([v])[0] if v in le.classes_ else -1
            )

    # --- Limpeza ---
    cols_num = [c for c in df_model.columns if c not in ["Date", "target"]]
    inf_count = df_model[cols_num].isin([float("inf"), float("-inf")]).sum().sum()
    nan_count = df_model[cols_num].isna().sum().sum()
    total_cells = df_model.shape[0] * len(cols_num)

    if inf_count / total_cells > INF_NAN_WARN_THRESHOLD:
        print(f"  🚨 [{market}] Infs: {inf_count/total_cells:.1%}")
    if nan_count / total_cells > INF_NAN_WARN_THRESHOLD:
        print(f"  🚨 [{market}] NaNs: {nan_count/total_cells:.1%}")

    df_model.replace([float("inf"), float("-inf")], 0, inplace=True)
    df_model.fillna(0, inplace=True)

     # --- Remove features combinadas ---
    df_model = df_model[[c for c in df_model.columns
                          if "minus" not in c and "ratio" not in c]]   

    # --- Split temporal ---
    df_model = df_model.sort_values("Date").reset_index(drop=True)
    split_idx  = int(len(df_model) * TRAIN_SPLIT_PCT)
    split_date = df_model.loc[split_idx, "Date"]

    train = df_model.iloc[:split_idx].copy()
    test  = df_model.iloc[split_idx:].copy()

    if train.empty or test.empty:
        rejected.append((market, "split resultou em conjunto vazio"))
        continue

    # --- X / Y ---
    drop_xy  = ["target", "Date"]
    X_train  = train.drop(columns=drop_xy)
    y_train  = train["target"]
    X_test   = test.drop(columns=drop_xy)
    y_test   = test["target"]

      
    # --- Salva ---
    market_dir = os.path.join(OUTPUT_DIR, market)
    os.makedirs(market_dir, exist_ok=True)

    X_train.to_csv(os.path.join(market_dir, "X_train.csv"), index=False)
    y_train.to_csv(os.path.join(market_dir, "y_train.csv"), index=False)
    X_test.to_csv(os.path.join(market_dir,  "X_test.csv"),  index=False)
    y_test.to_csv(os.path.join(market_dir,  "y_test.csv"),  index=False)

    # Salva lista de features do mercado
    with open(os.path.join(market_dir, "feature_columns.pkl"), "wb") as f:
        pickle.dump(list(X_train.columns), f)

    target_dist = dict(pd.Series(target).value_counts(normalize=True).round(3).to_dict())
    print(f"  ✅ [{market}] shape={X_train.shape} | split={split_date.date()} | ROI_bt={roi_bt:+.3f} | target={target_dist}")

    mercados_ok.append({
        "market"    : market,
        "event"     : event_name,
        "roi_bt"    : roi_bt,
        "winrate_bt": winrate_bt,
        "n_features": X_train.shape[1],
        "n_train"   : len(X_train),
        "n_test"    : len(X_test),
    })

# ==============================
# RELATÓRIO
# ==============================
print()
if rejected:
    print(f"⚠️  Mercados rejeitados ({len(rejected)}):")
    for market, reason in rejected:
        print(f"    ❌ {market}: {reason}")

# Salva meta dos mercados para uso no train e predict
meta_mercados_path = os.path.join(OUTPUT_DIR, "mercados_meta.pkl")
with open(meta_mercados_path, "wb") as f:
    pickle.dump(mercados_ok, f)

print(f"\n✅ {len(mercados_ok)} mercados processados com sucesso!")
print(f"💾 Meta salva em: {meta_mercados_path}")
print("=" * 55)

# ------------------------------
# FRIENDLY SUMMARY
# ------------------------------
print("\n📊 Resumo final de mercados")

summary = pd.DataFrame(mercados_ok)
if not summary.empty:
    summary = summary[["market", "event", "roi_bt", "n_features", "n_train", "n_test"]]
    summary["roi_bt"] = summary["roi_bt"].apply(lambda x: f"{x:+.2%}")
    summary.rename(columns={
        "market": "Mercado",
        "event": "Evento",
        "roi_bt": "ROI Backtest",
        "n_features": "Nº Features",
        "n_train": "Treino",
        "n_test": "Teste"
    }, inplace=True)

    top_markets = summary.sort_values("ROI Backtest", ascending=False).head(5)
    print(top_markets.to_string(index=False))
else:
    print("❌ Nenhum mercado processado para exibir.")

print("=" * 55)