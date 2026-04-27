import pandas as pd
import json
import os

# --- Configurações ---
BASE_DIR = os.path.dirname(__file__)
INPUT_FILE = os.path.join(BASE_DIR, "..", "data", "base_oficial.csv")
MARKET_FILE = os.path.join(BASE_DIR, "markets_map.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "data", "eventos")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Lê dados ---
df = pd.read_csv(
    INPUT_FILE,
    encoding="utf-8-sig",
    low_memory=False,
    dtype={"Game_ID": "string"}
)
df.columns = df.columns.str.strip()

# --- Lê market.json ---
with open(MARKET_FILE, "r", encoding="utf-8") as f:
    market_map = json.load(f)

# --- Colunas de metadata ---
metadata_cols = [
    "League_std", "Country", "Num", "Game_ID", "Season", "Date", "Round",
    "Home", "Away", "PPG_H_Pre", "PPG_A_Pre", "PPG_H", "PPG_A",
    "XG_H_Pre", "XG_A_Pre", "XG_T_Pre", "File_Origin", "League_File",
    "Home_std", "Away_std", "Home_new", "Away_new"
]

# --- Inicializa logs ---
colunas_nao_encontradas = []
colunas_processadas = []

# --- Processa cada mercado ---
for i, (col, info) in enumerate(market_map.items(), start=1):
    tipo = info.get("type")
    event_name = info.get("event", col)

    if tipo != "quantidade":
        continue

    if col not in df.columns:
        colunas_nao_encontradas.append(col)
        print(f"[AVISO] Coluna não encontrada: {col}")
        continue

    try:
        df_out = df[metadata_cols + [col]].copy()
        df_out.rename(columns={col: "value"}, inplace=True)
        df_out["value"] = pd.to_numeric(df_out["value"], errors="coerce").astype("float64")
        df_out["event"] = event_name

        output_path = os.path.join(OUTPUT_DIR, f"{event_name}.csv")
        df_out.to_csv(output_path, index=False, encoding="utf-8-sig")

        colunas_processadas.append(col)
        print(f" Mercado salvo: {output_path} ({len(df_out)} linhas) [{i}/{len(market_map)}]")

    except Exception as e:
        colunas_nao_encontradas.append(col)
        print(f"[ERRO] Falha ao processar {col}: {e}")

print("\n Transformação long concluída!")
print(f"Colunas processadas com sucesso: {len(colunas_processadas)}")
print(f"Colunas que deram erro ou não foram encontradas: {len(colunas_nao_encontradas)}")
if colunas_nao_encontradas:
    print("Lista de colunas não processadas:", colunas_nao_encontradas)