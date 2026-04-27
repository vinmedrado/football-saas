# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from utils.paths import PARSED_OUT, LEAGUES_OUT as LEAGUES_DIR, FINAL_OUT as OUT_DIR
MATCHES_FILE = PARSED_OUT / "matches_parsed.csv"
ODDS_FILE    = PARSED_OUT / "match_odds_parsed.csv"
OUT_FILE     = OUT_DIR / "historico_flashscore.csv"

FIELDNAMES = [
    "Nº", "Id_Jogo", "League", "Season", "Date", "Rodada", "Home", "Away",
    "Goals_H_HT", "Goals_A_HT", "TotalGoals_HT",
    "Goals_H_FT", "Goals_A_FT", "TotalGoals_FT",
    "Goals_H_Minutes", "Goals_A_Minutes",
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
    "PPG_Home_Pre", "PPG_Away_Pre", "PPG_Home", "PPG_Away",
    "XG_Home_Pre", "XG_Away_Pre", "XG_Total_Pre",
    "ShotsOnTarget_H", "ShotsOnTarget_A",
    "ShotsOffTarget_H", "ShotsOffTarget_A",
    "Shots_H", "Shots_A",
    "Corners_H_FT", "Corners_A_FT", "TotalCorners_FT",
    "Odd_Corners_H", "Odd_Corners_D", "Odd_Corners_A",
    "Odd_Corners_Over75",  "Odd_Corners_Under75",
    "Odd_Corners_Over85",  "Odd_Corners_Under85",
    "Odd_Corners_Over95",  "Odd_Corners_Under95",
    "Odd_Corners_Over105", "Odd_Corners_Under105",
    "Odd_Corners_Over115", "Odd_Corners_Under115",
    "arquivo_origem", "liga_arquivo",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def extrair_event_id(link: str):
    try:
        parsed = urlparse(str(link))

        query = parse_qs(parsed.query)
        if "mid" in query and query["mid"]:
            return query["mid"][0]

        parts = parsed.path.strip("/").split("/")
        for part in reversed(parts):
            if part and part != "match":
                return part
    except Exception:
        return None

    return None

def garantir_pasta() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def ler_csv(caminho: Path, sep=";") -> list[dict]:
    if not caminho.exists():
        print(f"[AVISO] Não encontrado: {caminho}")
        return []
    with open(caminho, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f, delimiter=sep))


def idx_por_event_id(linhas: list[dict]) -> dict[str, dict]:
    return {r["event_id"]: r for r in linhas if r.get("event_id")}


def to_int(v):
    if v in (None, "", "None"):
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def to_float(v):
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except Exception:
        return None


def minuto_ht(minuto: str) -> bool:
    try:
        return int(minuto.split("+")[0]) <= 45
    except Exception:
        return False


def gols_ht(minutos_texto: str | None) -> int:
    if not minutos_texto:
        return 0
    return sum(1 for m in minutos_texto.split("|") if m.strip() and minuto_ht(m.strip()))


def calcular_dc_from_1x2(h, d, a):
    """Calcula DC a partir das odds 1X2 (fallback quando API não retorna DC)."""
    try:
        h = float(h); d = float(d); a = float(a)
        return (
            round(1 / (1/h + 1/d), 2),
            round(1 / (1/h + 1/a), 2),
            round(1 / (1/d + 1/a), 2),
        )
    except Exception:
        return None, None, None


def calcular_season(date_text: str | None) -> str | None:
    """
    Infere a temporada pelo mês:
    Meses >= 7 → temporada começa nesse ano (ex: 2025/2026)
    Meses <  7 → temporada começou no ano anterior (ex: 2024/2025)
    """
    if not date_text:
        return None
    try:
        parte = date_text.split()[0]            # "24.01." ou "24.01.2025"
        partes = parte.strip(".").split(".")
        mes  = int(partes[1])
        # Se tiver o ano na data, usa ele; senão infere pelo mês
        if len(partes) >= 3 and partes[2]:
            ano_ref = int(partes[2])
        else:
            from datetime import date
            ano_ref = date.today().year

        ano_inicio = ano_ref if mes >= 7 else ano_ref - 1
        return f"{ano_inicio}/{ano_inicio + 1}"
    except Exception:
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    garantir_pasta()

    matches = ler_csv(MATCHES_FILE)
    odds    = idx_por_event_id(ler_csv(ODDS_FILE))
    meta = {}

    for pasta in LEAGUES_DIR.iterdir():
        if not pasta.is_dir():
            continue

        arquivo = pasta / "events.json"
        if not arquivo.exists():
            continue


        import json
        dados = json.loads(arquivo.read_text(encoding="utf-8"))

        for row in dados:
            eid = extrair_event_id(row.get("event_id"))
            if eid:
                meta[eid] = row

    print(f"[INFO] matches={len(matches)}  odds={len(odds)}  meta={len(meta)}")

    with open(OUT_FILE, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, delimiter=";")
        writer.writeheader()

        ids_vistos = set()
        count = 0

        for idx, row in enumerate(matches, start=1):
            eid = extrair_event_id(row.get("event_id")) or extrair_event_id(row.get("url", ""))

            if eid in ids_vistos:
                continue

            ids_vistos.add(eid)
            o   = odds.get(eid, {})
            m   = meta.get(eid, {})

            # ── Stats ──────────────────────────────────────────────────
            shots_h   = to_int(row.get("shots_h"))
            shots_a   = to_int(row.get("shots_a"))
            sot_h     = to_int(row.get("shots_on_target_h"))
            sot_a     = to_int(row.get("shots_on_target_a"))
            corners_h = to_int(row.get("corners_h"))
            corners_a = to_int(row.get("corners_a"))
            g_h_ft    = to_int(row.get("goals_home"))
            g_a_ft    = to_int(row.get("goals_away"))
            total_ft = (
                to_int(row.get("goals_total"))
                or (
                    (g_h_ft or 0) + (g_a_ft or 0)
                    if g_h_ft is not None and g_a_ft is not None
                    else None
                )
            )

            gh_min = row.get("goals_home_minutes", "")
            ga_min = row.get("goals_away_minutes", "")

            g_h_ht = gols_ht(gh_min)
            g_a_ht = gols_ht(ga_min)

            sof_h         = (shots_h - sot_h) if shots_h is not None and sot_h is not None else None
            sof_a         = (shots_a - sot_a) if shots_a is not None and sot_a is not None else None
            total_corners = (corners_h + corners_a) if corners_h is not None and corners_a is not None else None

            # ── Double Chance ──────────────────────────────────────────
            # Usa DC direto da API se disponível; senão calcula de 1X2
            dc_1x = to_float(o.get("Odd_DC_1X"))
            dc_12 = to_float(o.get("Odd_DC_12"))
            dc_x2 = to_float(o.get("Odd_DC_X2"))
            if dc_1x is None or dc_12 is None or dc_x2 is None:
                dc_1x, dc_12, dc_x2 = calcular_dc_from_1x2(
                    o.get("Odd_H_FT"), o.get("Odd_D_FT"), o.get("Odd_A_FT")
                )

            # ── Season ────────────────────────────────────────────────
            # Prioridade: Season do meta (01_get_league_events) → calcula pela data
            season = m.get("Season") or calcular_season(m.get("Date"))

            nova_linha = {
                "Nº"                   : count +1,
                "Id_Jogo"              : eid,
                "League": (
                    m.get("League")
                    or m.get("league")
                    or m.get("league_name")
                    or "UNKNOWN"
                ),
                "Season"               : season,
                "Date": m.get("Date") or m.get("date"),
                "Rodada"               : m.get("Rodada"),
                "Home": m.get("Home") or m.get("home"),
                "Away": m.get("Away") or m.get("away"),
                # Gols
                "Goals_H_HT"           : g_h_ht,
                "Goals_A_HT"           : g_a_ht,
                "TotalGoals_HT"        : g_h_ht + g_a_ht,
                "Goals_H_FT"           : g_h_ft,
                "Goals_A_FT"           : g_a_ft,
                "TotalGoals_FT"        : total_ft,
                "Goals_H_Minutes"      : gh_min,
                "Goals_A_Minutes"      : ga_min,
                # Odds HT
                "Odd_H_HT"             : to_float(o.get("Odd_H_HT")),
                "Odd_D_HT"             : to_float(o.get("Odd_D_HT")),
                "Odd_A_HT"             : to_float(o.get("Odd_A_HT")),
                "Odd_Over05_HT"        : to_float(o.get("Odd_Over05_HT")),
                "Odd_Under05_HT"       : to_float(o.get("Odd_Under05_HT")),
                "Odd_Over15_HT"        : to_float(o.get("Odd_Over15_HT")),
                "Odd_Under15_HT"       : to_float(o.get("Odd_Under15_HT")),
                "Odd_Over25_HT"        : to_float(o.get("Odd_Over25_HT")),
                "Odd_Under25_HT"       : to_float(o.get("Odd_Under25_HT")),
                # Odds FT
                "Odd_H_FT"             : to_float(o.get("Odd_H_FT")),
                "Odd_D_FT"             : to_float(o.get("Odd_D_FT")),
                "Odd_A_FT"             : to_float(o.get("Odd_A_FT")),
                "Odd_Over05_FT"        : to_float(o.get("Odd_Over05_FT")),
                "Odd_Under05_FT"       : to_float(o.get("Odd_Under05_FT")),
                "Odd_Over15_FT"        : to_float(o.get("Odd_Over15_FT")),
                "Odd_Under15_FT"       : to_float(o.get("Odd_Under15_FT")),
                "Odd_Over25_FT"        : to_float(o.get("Odd_Over25_FT")),
                "Odd_Under25_FT"       : to_float(o.get("Odd_Under25_FT")),
                "Odd_BTTS_Yes"         : to_float(o.get("Odd_BTTS_Yes")),
                "Odd_BTTS_No"          : to_float(o.get("Odd_BTTS_No")),
                "Odd_DC_1X"            : dc_1x,
                "Odd_DC_12"            : dc_12,
                "Odd_DC_X2"            : dc_x2,
                # Stats
                "PPG_Home_Pre"         : None,
                "PPG_Away_Pre"         : None,
                "PPG_Home"             : None,
                "PPG_Away"             : None,
                "XG_Home_Pre"          : None,
                "XG_Away_Pre"          : None,
                "XG_Total_Pre"         : None,
                "ShotsOnTarget_H"      : sot_h,
                "ShotsOnTarget_A"      : sot_a,
                "ShotsOffTarget_H"     : sof_h,
                "ShotsOffTarget_A"     : sof_a,
                "Shots_H"              : shots_h,
                "Shots_A"              : shots_a,
                "Corners_H_FT"         : corners_h,
                "Corners_A_FT"         : corners_a,
                "TotalCorners_FT"      : total_corners,
                # Corners odds (H/D/A não existem na API)
                "Odd_Corners_H"        : None,
                "Odd_Corners_D"        : None,
                "Odd_Corners_A"        : None,
                "Odd_Corners_Over75"   : to_float(o.get("Odd_Corners_Over75")),
                "Odd_Corners_Under75"  : to_float(o.get("Odd_Corners_Under75")),
                "Odd_Corners_Over85"   : to_float(o.get("Odd_Corners_Over85")),
                "Odd_Corners_Under85"  : to_float(o.get("Odd_Corners_Under85")),
                "Odd_Corners_Over95"   : to_float(o.get("Odd_Corners_Over95")),
                "Odd_Corners_Under95"  : to_float(o.get("Odd_Corners_Under95")),
                "Odd_Corners_Over105"  : to_float(o.get("Odd_Corners_Over105")),
                "Odd_Corners_Under105" : to_float(o.get("Odd_Corners_Under105")),
                "Odd_Corners_Over115"  : to_float(o.get("Odd_Corners_Over115")),
                "Odd_Corners_Under115" : to_float(o.get("Odd_Corners_Under115")),
                "arquivo_origem"       : "flashscore_api",
                "liga_arquivo"         : m.get("League"),
            }

            writer.writerow(nova_linha)
            count += 1

    print(f"[OK] {OUT_FILE}  ({count} linhas)")


if __name__ == "__main__":
    main()
