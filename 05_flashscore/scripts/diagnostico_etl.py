# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from utils.paths import FINAL_OUT, OUTPUT_DIR


ARQUIVO = FINAL_OUT / "historico_flashscore.csv"
OUT_DIR = OUTPUT_DIR / "analise"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def carregar_base(caminho: Path) -> pd.DataFrame:
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")
    return pd.read_csv(caminho, encoding="utf-8-sig", sep=";")


def pct_nulo(serie: pd.Series) -> float:
    if len(serie) == 0:
        return 0.0
    return round((serie.isna().sum() / len(serie)) * 100, 2)


def main():
    print("\n[INFO] Iniciando diagnóstico ETL...\n")

    df = carregar_base(ARQUIVO)

    if df.empty:
        print("[ERRO] Base vazia.")
        return

    # ── Resumo geral por coluna ─────────────────────────────
    resumo = []
    for col in df.columns:
        nulos = int(df[col].isna().sum())
        preenchidos = int(df[col].notna().sum())
        resumo.append({
            "coluna": col,
            "nulos": nulos,
            "preenchidos": preenchidos,
            "pct_nulos": pct_nulo(df[col]),
            "dtype": str(df[col].dtype),
        })

    resumo_df = pd.DataFrame(resumo).sort_values(
        by=["pct_nulos", "coluna"],
        ascending=[False, True],
    )

    resumo_df.to_csv(OUT_DIR / "resumo_nulos.csv", index=False, encoding="utf-8-sig")

    # ── Colunas 100% vazias / parcialmente vazias ──────────
    colunas_100_vazias = resumo_df[resumo_df["pct_nulos"] == 100]["coluna"].tolist()
    colunas_parciais = resumo_df[
        (resumo_df["pct_nulos"] > 0) & (resumo_df["pct_nulos"] < 100)
    ]["coluna"].tolist()

    with open(OUT_DIR / "colunas_100_vazias.txt", "w", encoding="utf-8") as f:
        for c in colunas_100_vazias:
            f.write(c + "\n")

    with open(OUT_DIR / "colunas_parcialmente_vazias.txt", "w", encoding="utf-8") as f:
        for c in colunas_parciais:
            f.write(c + "\n")

    # ── Nulos por liga ─────────────────────────────────────
    if "League" in df.columns:
        ligas = []
        colunas_alvo = [c for c in df.columns if c != "Nº"]

        for liga, grupo in df.groupby("League", dropna=False):
            linha = {"League": liga, "qtd_jogos": len(grupo)}
            for col in colunas_alvo:
                linha[f"{col}__pct_nulos"] = pct_nulo(grupo[col])
            ligas.append(linha)

        pd.DataFrame(ligas).to_csv(
            OUT_DIR / "nulos_por_liga.csv", index=False, encoding="utf-8-sig"
        )

    # ── Nulos por temporada ────────────────────────────────
    if "Season" in df.columns:
        temporadas = []
        colunas_alvo = [c for c in df.columns if c != "Nº"]

        for season, grupo in df.groupby("Season", dropna=False):
            linha = {"Season": season, "qtd_jogos": len(grupo)}
            for col in colunas_alvo:
                linha[f"{col}__pct_nulos"] = pct_nulo(grupo[col])
            temporadas.append(linha)

        pd.DataFrame(temporadas).to_csv(
            OUT_DIR / "nulos_por_temporada.csv", index=False, encoding="utf-8-sig"
        )

    # ── Resumo no terminal ─────────────────────────────────
    print("=" * 90)
    print("RESUMO ETL")
    print("=" * 90)
    print(f"Arquivo analisado          : {ARQUIVO}")
    print(f"Total de linhas            : {len(df):,}".replace(",", "."))
    print(f"Total de colunas           : {len(df.columns)}")
    print(f"Colunas 100% vazias        : {len(colunas_100_vazias)}")
    print(f"Colunas parcialmente vazias: {len(colunas_parciais)}")

    print("\nTOP 20 COLUNAS COM MAIS NULOS")
    print(resumo_df.head(20).to_string(index=False))

    print("\nCOLUNAS 100% VAZIAS")
    if colunas_100_vazias:
        for c in colunas_100_vazias:
            print(f" - {c}")
    else:
        print("Nenhuma")

    print("\nCOLUNAS PARCIALMENTE VAZIAS")
    if colunas_parciais:
        for c in colunas_parciais[:30]:
            pct = resumo_df.loc[resumo_df["coluna"] == c, "pct_nulos"].iloc[0]
            print(f" - {c}: {pct}%")
    else:
        print("Nenhuma")

    print("\nARQUIVOS GERADOS")
    print(f" - {OUT_DIR / 'resumo_nulos.csv'}")
    print(f" - {OUT_DIR / 'nulos_por_liga.csv'}")
    print(f" - {OUT_DIR / 'nulos_por_temporada.csv'}")
    print(f" - {OUT_DIR / 'colunas_100_vazias.txt'}")
    print(f" - {OUT_DIR / 'colunas_parcialmente_vazias.txt'}")

    print("\n[OK] Diagnóstico ETL concluído.\n")


if __name__ == "__main__":
    main()