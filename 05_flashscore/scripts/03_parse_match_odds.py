# -*- coding: utf-8 -*-
"""
03_parse_match_odds.py
Parser de odds no formato JSON GraphQL (FlashScore API).

Mercados suportados:
- 1x2 FT
- 1x2 HT (se existir na fonte)
- OU FT
- OU HT (se existir na fonte)
- BTTS FT
- DC FT
- Corners OU
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import csv
import json
from typing import Any

from utils.paths import ODDS_OUT as ODDS_DIR, PARSED_OUT as OUT_DIR

OUT_FILE = OUT_DIR / "match_odds_parsed.csv"

FIELDNAMES = [
    "event_id",
    "Odd_H_HT", "Odd_D_HT", "Odd_A_HT",
    "Odd_Over05_HT", "Odd_Under05_HT",
    "Odd_Over15_HT", "Odd_Under15_HT",
    "Odd_Over25_HT", "Odd_Under25_HT",
    "Odd_H_FT", "Odd_D_FT", "Odd_A_FT",
    "Odd_Over05_FT", "Odd_Under05_FT",
    "Odd_Over15_FT", "Odd_Under15_FT",
    "Odd_Over25_FT", "Odd_Under25_FT",
    "Odd_BTTS_Yes", "Odd_BTTS_No",
    "Odd_DC_1X", "Odd_DC_12", "Odd_DC_X2",
    "Odd_Corners_Over75",  "Odd_Corners_Under75",
    "Odd_Corners_Over85",  "Odd_Corners_Under85",
    "Odd_Corners_Over95",  "Odd_Corners_Under95",
    "Odd_Corners_Over105", "Odd_Corners_Under105",
    "Odd_Corners_Over115", "Odd_Corners_Under115",
]

OU_GOALS_MAP = {
    "0.5": ("Odd_Over05", "Odd_Under05"),
    "1.5": ("Odd_Over15", "Odd_Under15"),
    "2.5": ("Odd_Over25", "Odd_Under25"),
}

OU_CORNERS_MAP = {
    "7.5": ("Odd_Corners_Over75", "Odd_Corners_Under75"),
    "8.5": ("Odd_Corners_Over85", "Odd_Corners_Under85"),
    "9.5": ("Odd_Corners_Over95", "Odd_Corners_Under95"),
    "10.5": ("Odd_Corners_Over105", "Odd_Corners_Under105"),
    "11.5": ("Odd_Corners_Over115", "Odd_Corners_Under115"),
}


def safe_float(v: Any) -> float | None:
    try:
        if v in (None, "", "None"):
            return None
        val = float(str(v).replace(",", "."))
        if val <= 1.0:
            return None
        return val
    except Exception:
        return None


def ler_json(path: Path) -> dict | None:
    try:
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            return None
        return json.loads(text)
    except Exception:
        return None


def odds_data(parsed: dict) -> dict | None:
    try:
        data = parsed.get("data", {})
        if "findPrematchOddsForBookmaker" in data:
            return data["findPrematchOddsForBookmaker"]
        return None
    except Exception:
        return None


def escolher_melhor(valor_atual: float | None, novo_valor: float | None) -> float | None:
    if novo_valor is None:
        return valor_atual
    if valor_atual is None:
        return novo_valor
    return max(valor_atual, novo_valor)


def atualizar_tripla(
    row: dict,
    cols: tuple[str, str, str],
    valores: tuple[float | None, float | None, float | None],
) -> None:
    for col, val in zip(cols, valores):
        row[col] = escolher_melhor(row.get(col), val)


def atualizar_par(
    row: dict,
    cols: tuple[str, str],
    valores: tuple[float | None, float | None],
) -> None:
    for col, val in zip(cols, valores):
        row[col] = escolher_melhor(row.get(col), val)


def extrair_1x2(data: dict) -> tuple[float | None, float | None, float | None]:
    try:
        return (
            safe_float(data.get("home", {}).get("value")),
            safe_float(data.get("draw", {}).get("value")),
            safe_float(data.get("away", {}).get("value")),
        )
    except Exception:
        return None, None, None


def extrair_btts(data: dict) -> tuple[float | None, float | None]:
    try:
        return (
            safe_float(data.get("yes", {}).get("value")),
            safe_float(data.get("no", {}).get("value")),
        )
    except Exception:
        return None, None


def extrair_dc(data: dict) -> tuple[float | None, float | None, float | None]:
    try:
        v1x = safe_float(data.get("homeOrDraw", {}).get("value"))
        vx2 = safe_float(data.get("awayOrDraw", {}).get("value"))
        v12 = safe_float(data.get("noDraw", {}).get("value"))
        return v1x, v12, vx2
    except Exception:
        return None, None, None


def extrair_ou(data: dict, mapa: dict[str, tuple[str, str]], sufixo: str, row: dict) -> None:
    try:
        opportunities = data.get("opportunities", [])
        if not opportunities:
            return

        for opp in opportunities:
            handicap = opp.get("handicap", {})
            hcap = str(handicap.get("value", "")).strip()

            if hcap not in mapa:
                continue

            over_col_base, under_col_base = mapa[hcap]

            if sufixo:
                over_col = f"{over_col_base}{sufixo}"
                under_col = f"{under_col_base}{sufixo}"
            else:
                over_col = over_col_base
                under_col = under_col_base

            over_val = safe_float(opp.get("over", {}).get("value"))
            under_val = safe_float(opp.get("under", {}).get("value"))

            row[over_col] = escolher_melhor(row.get(over_col), over_val)
            row[under_col] = escolher_melhor(row.get(under_col), under_val)

    except Exception:
        pass


def inferir_tipo(data: dict) -> str:
    tipo = str(data.get("type", "")).upper().strip()
    if tipo:
        return tipo

    # fallbacks por estrutura do payload
    if isinstance(data.get("home"), dict) and isinstance(data.get("draw"), dict) and isinstance(data.get("away"), dict):
        return "HOME_DRAW_AWAY"

    if isinstance(data.get("yes"), dict) and isinstance(data.get("no"), dict):
        return "BOTH_TEAMS_TO_SCORE"

    if isinstance(data.get("homeOrDraw"), dict) and isinstance(data.get("awayOrDraw"), dict):
        return "DOUBLE_CHANCE"

    if isinstance(data.get("opportunities"), list):
        return "OVER_UNDER"

    return ""


def processar_evento(event_id: str) -> dict:
    pasta = ODDS_DIR / event_id
    row = {k: None for k in FIELDNAMES}
    row["event_id"] = event_id

    if not pasta.exists():
        return row

    for arq in sorted(pasta.glob("*.txt")):
        nome = arq.stem.lower()
        parsed = ler_json(arq)
        if not parsed:
            continue

        data = odds_data(parsed)
        if not data:
            continue

        tipo = inferir_tipo(data)

        is_ft = "ft" in nome
        is_ht = "ht" in nome
        is_corners = "corner" in nome

        # 1x2 FT
        if tipo == "HOME_DRAW_AWAY" and is_ft:
            atualizar_tripla(
                row,
                ("Odd_H_FT", "Odd_D_FT", "Odd_A_FT"),
                extrair_1x2(data),
            )

        # 1x2 HT
        elif tipo == "HOME_DRAW_AWAY" and is_ht:
            atualizar_tripla(
                row,
                ("Odd_H_HT", "Odd_D_HT", "Odd_A_HT"),
                extrair_1x2(data),
            )

        # BTTS FT
        elif tipo == "BOTH_TEAMS_TO_SCORE":
            atualizar_par(
                row,
                ("Odd_BTTS_Yes", "Odd_BTTS_No"),
                extrair_btts(data),
            )

        # DC FT
        elif tipo == "DOUBLE_CHANCE":
            atualizar_tripla(
                row,
                ("Odd_DC_1X", "Odd_DC_12", "Odd_DC_X2"),
                extrair_dc(data),
            )

        # OU FT
        elif tipo == "OVER_UNDER" and is_ft and not is_corners:
            extrair_ou(data, OU_GOALS_MAP, "_FT", row)

        # OU HT
        elif tipo == "OVER_UNDER" and is_ht and not is_corners:
            extrair_ou(data, OU_GOALS_MAP, "_HT", row)

        # Corners OU
        elif tipo == "OVER_UNDER" and is_corners:
            extrair_ou(data, OU_CORNERS_MAP, "", row)

    return row


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not ODDS_DIR.exists():
        print(f"[ERRO] Pasta não encontrada: {ODDS_DIR}")
        return

    event_ids = sorted([p.name for p in ODDS_DIR.iterdir() if p.is_dir()])
    print(f"[INFO] {len(event_ids)} eventos encontrados")

    with open(OUT_FILE, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, delimiter=";")
        writer.writeheader()

        ok = 0
        erros = 0

        for idx, eid in enumerate(event_ids, start=1):
            try:
                row = processar_evento(eid)
                writer.writerow(row)
                ok += 1
            except Exception as e:
                print(f"[ERRO] {eid} | {e}")
                erros += 1

            if idx % 500 == 0:
                print(f"[INFO] {idx}/{len(event_ids)} processados...")

    print(f"\n[OK] {OUT_FILE} ({ok} linhas, {erros} erros)")


if __name__ == "__main__":
    main()