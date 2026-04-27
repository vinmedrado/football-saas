# -*- coding: utf-8 -*-

from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from utils.paths import FINAL_OUT, OUTPUT_DIR

from datetime import datetime
from pathlib import Path


ARQUIVO  = FINAL_OUT / "historico_flashscore.csv"
OUT_DIR  = OUTPUT_DIR / "analise"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "analise.txt"


def carregar_base(caminho: Path) -> pd.DataFrame:
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")
    return pd.read_csv(caminho, encoding="utf-8-sig", sep=";")


def parse_data_coluna(df: pd.DataFrame) -> pd.Series:
    def _parse(valor):
        if pd.isna(valor):
            return pd.NaT
        texto = str(valor).strip()
        if not texto:
            return pd.NaT
        parte = texto.split()[0].strip().strip(".")
        partes = parte.split(".")
        try:
            if len(partes) == 3:
                dia, mes, ano = int(partes[0]), int(partes[1]), int(partes[2])
                return pd.Timestamp(year=ano, month=mes, day=dia)
        except Exception:
            return pd.NaT
        return pd.NaT

    return df["Date"].apply(_parse)


def pct_nulo(serie: pd.Series) -> float:
    if len(serie) == 0:
        return 0.0
    return round((serie.isna().sum() / len(serie)) * 100, 2)


def imprimir_secao(titulo: str):
    print("\n" + "=" * 90)
    print(titulo)
    print("=" * 90)


def main():
    print("\n[INFO] Iniciando análise do histórico...\n")

    df = carregar_base(ARQUIVO)

    eventos_sem_odds = df[
        df["Odd_H_FT"].isna()
    ][["Id_Jogo", "League", "Date", "Home", "Away"]]

    if df.empty:
        print("[ERRO] O arquivo existe, mas está vazio.")
        return

    hoje = pd.Timestamp(datetime.now().date())
    datas_parseadas = parse_data_coluna(df)

    imprimir_secao("RESUMO GERAL")
    print(f"Arquivo analisado         : {ARQUIVO}")
    print(f"Total de linhas           : {len(df):,}".replace(",", "."))
    print(f"Total de colunas          : {len(df.columns)}")

    if "League" in df.columns:
        ligas_unicas = df["League"].dropna().astype(str).str.strip().nunique()
        print(f"Total de ligas únicas     : {ligas_unicas}")

    if datas_parseadas.notna().any():
        print(f"Menor data encontrada     : {datas_parseadas.min().date()}")
        print(f"Maior data encontrada     : {datas_parseadas.max().date()}")
    else:
        print("Menor data encontrada     : N/A")
        print("Maior data encontrada     : N/A")

    imprimir_secao("INTEGRIDADE")
    if "Id_Jogo" in df.columns:
        duplicados = df["Id_Jogo"].duplicated().sum()
        ids_unicos = df["Id_Jogo"].nunique(dropna=True)
        print(f"Ids únicos                : {ids_unicos:,}".replace(",", "."))
        print(f"Linhas duplicadas         : {duplicados:,}".replace(",", "."))

        if duplicados > 0:
            print("\n[ALERTA] Exemplos de IDs duplicados:")
            exemplos_dup = df[df["Id_Jogo"].duplicated(keep=False)]["Id_Jogo"].dropna().astype(str).unique()[:10]
            for eid in exemplos_dup:
                print(f" - {eid}")

    futuras = df[datas_parseadas > hoje]
    print(f"Jogos com data futura     : {len(futuras):,}".replace(",", "."))

    if len(futuras) > 0:
        print("\n[ALERTA] Amostra de jogos futuros:")
        cols = [c for c in ["Id_Jogo", "League", "Date", "Home", "Away"] if c in futuras.columns]
        print(futuras[cols].head(10).to_string(index=False))

    imprimir_secao("QUALIDADE DAS COLUNAS PRINCIPAIS")
    colunas_chave = [
        "League", "Season", "Date", "Home", "Away",
        "Goals_H_FT", "Goals_A_FT", "TotalGoals_FT",
        "Odd_H_FT", "Odd_D_FT", "Odd_A_FT",
        "Shots_H", "Shots_A",
        "Corners_H_FT", "Corners_A_FT",
    ]

    for col in colunas_chave:
        if col in df.columns:
            print(f"{col:<22} -> nulos: {pct_nulo(df[col]):>6}%")
        else:
            print(f"{col:<22} -> coluna não encontrada")

    imprimir_secao("TOP LIGAS POR VOLUME")
    if "League" in df.columns:
        top_ligas = (
            df["League"].fillna("UNKNOWN").astype(str).value_counts().head(15)
        )
        for liga, qtd in top_ligas.items():
            print(f"{liga:<35} {qtd:>8}")

    imprimir_secao("TOP ANOS / TEMPORADAS")
    if "Season" in df.columns:
        top_seasons = (
            df["Season"].fillna("UNKNOWN").astype(str).value_counts().head(10)
        )
        for season, qtd in top_seasons.items():
            print(f"{season:<20} {qtd:>8}")

    imprimir_secao("LINHAS COM POSSÍVEIS PROBLEMAS")
    problemas = pd.Series(False, index=df.index)

    for col in ["League", "Date", "Home", "Away"]:
        if col in df.columns:
            problemas |= df[col].isna()

    if "Id_Jogo" in df.columns:
        problemas |= df["Id_Jogo"].isna()

    if len(futuras) > 0:
        problemas |= datas_parseadas > hoje

    df_problemas = df[problemas]
    print(f"Total de linhas suspeitas : {len(df_problemas):,}".replace(",", "."))

    if len(df_problemas) > 0:
        cols = [c for c in ["Id_Jogo", "League", "Date", "Home", "Away"] if c in df_problemas.columns]
        print("\nAmostra:")
        print(df_problemas[cols].head(15).to_string(index=False))

    imprimir_secao("STATUS FINAL")
    status_ok = True

    if "Id_Jogo" in df.columns and df["Id_Jogo"].duplicated().sum() > 0:
        status_ok = False

    if len(futuras) > 0:
        status_ok = False

    if status_ok:
        print("[OK] Base aparentemente consistente para seguir.")
    else:
        print("[ALERTA] Base possui pontos que precisam de revisão.")

    print("\n[INFO] Análise concluída.\n")

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        f.write("=== RESUMO GERAL ===\n")
        f.write(f"Total jogos: {len(df)}\n")

        if datas_parseadas.notna().any():
            f.write(f"Data mínima: {datas_parseadas.min().date()}\n")
            f.write(f"Data máxima: {datas_parseadas.max().date()}\n")

        f.write(f"Sem odds FT: {len(eventos_sem_odds)}\n\n")

        f.write("=== QUALIDADE ODDS ===\n")
        f.write(f"Odd_H_FT nulos: {pct_nulo(df['Odd_H_FT'])}%\n")
        f.write(f"Odd_D_FT nulos: {pct_nulo(df['Odd_D_FT'])}%\n")
        f.write(f"Odd_A_FT nulos: {pct_nulo(df['Odd_A_FT'])}%\n\n")

        f.write("=== EVENTOS SEM ODDS FT ===\n")
        for _, row in eventos_sem_odds.iterrows():
            f.write(
                f"{row['Id_Jogo']} | {row['League']} | "
                f"{row['Date']} | {row['Home']} vs {row['Away']}\n"
            )


if __name__ == "__main__":
    main()