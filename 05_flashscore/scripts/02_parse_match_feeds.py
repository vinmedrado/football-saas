# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import json
import re
from pathlib import Path

from utils.paths import MATCHES_OUT as MATCHES_DIR, LEAGUES_OUT as LEAGUES_DIR, PARSED_OUT as OUT_DIR
OUT_FILE = OUT_DIR / "matches_parsed.csv"


def garantir_pasta() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def ler_feed(event_id: str, nome: str) -> str:
    caminho = MATCHES_DIR / event_id / f"{nome}.txt"
    if caminho.exists():
        return caminho.read_text(encoding="utf-8")
    return ""


def carregar_eventos_info() -> dict[str, dict]:
    mapa = {}

    arquivos = list(LEAGUES_DIR.glob("*/events.json"))

    for arq in arquivos:
        try:
            data = json.loads(arq.read_text(encoding="utf-8"))
        except Exception:
            continue

        for item in data:
            eid = item.get("event_id")
            if not eid:
                continue

            mapa[eid] = {
                "League": item.get("League"),
                "Season": item.get("Season"),
                "Date": item.get("Date"),
                "Rodada": item.get("Rodada"),
                "Home": item.get("Home"),
                "Away": item.get("Away"),
            }

    return mapa


def extrair_blocos_eventos(texto: str) -> list[str]:
    if not texto:
        return []
    return texto.split("~III÷")


def extrair_gols_por_time(texto: str):
    blocos = extrair_blocos_eventos(texto)

    gols_home = []
    gols_away = []

    for bloco in blocos:
        if "¬IK÷Goal" in bloco:
            try:
                match_time = re.search(r"IA÷(\d+)", bloco)
                match_min = re.search(r"IB÷(\d+(?:\+\d+)?)", bloco)

                if match_time and match_min:
                    team = match_time.group(1)
                    minuto = match_min.group(1)

                    if team == "1":
                        gols_home.append(minuto)
                    elif team == "2":
                        gols_away.append(minuto)

            except Exception:
                continue

    return gols_home, gols_away


def extrair_cartoes_amarelos(texto: str) -> int:
    blocos = extrair_blocos_eventos(texto)
    total = 0

    for bloco in blocos:
        if "Yellow Card" in bloco:
            total += 1

    return total


def extrair_stats(texto: str):
    stats = {}

    if not texto:
        return stats

    blocos = texto.split("~")
    secao_atual = None

    for bloco in blocos:
        bloco = bloco.strip()

        match_secao = re.search(r"SE÷([^¬]+)", bloco)
        if match_secao:
            secao_atual = match_secao.group(1).strip()
            continue

        if secao_atual != "Match":
            continue

        if "SG÷" in bloco and "SH÷" in bloco and "SI÷" in bloco:
            try:
                nome = re.search(r"SG÷([^¬]+)", bloco)
                home = re.search(r"SH÷([^¬]+)", bloco)
                away = re.search(r"SI÷([^¬]+)", bloco)

                if nome and home and away:
                    key = nome.group(1).strip().lower()

                    if key not in stats:
                        stats[key] = {
                            "home": home.group(1).strip(),
                            "away": away.group(1).strip(),
                        }

            except Exception:
                continue

    return stats


def safe_int(v):
    try:
        if v is None or v == "":
            return None
        v = str(v).replace("%", "").strip()
        return int(float(v))
    except Exception:
        return None


def processar_evento(event_id: str, eventos_info: dict):
    sui = ler_feed(event_id, "sui")
    stats_txt = ler_feed(event_id, "stats")

    info = eventos_info.get(event_id, {})

    gols_home, gols_away = extrair_gols_por_time(sui)
    gols_total = len(gols_home) + len(gols_away)
    yellow_cards = extrair_cartoes_amarelos(sui)
    stats = extrair_stats(stats_txt)

    def get_stat(nome):
        if nome in stats:
            return stats[nome]["home"], stats[nome]["away"]
        return None, None

    shots_h, shots_a = get_stat("total shots")
    sot_h, sot_a = get_stat("shots on target")
    corners_h, corners_a = get_stat("corner kicks")

    shots_h = safe_int(shots_h)
    shots_a = safe_int(shots_a)
    sot_h = safe_int(sot_h)
    sot_a = safe_int(sot_a)
    corners_h = safe_int(corners_h)
    corners_a = safe_int(corners_a)

    return {
        "event_id": event_id,
        "League": info.get("League"),
        "Season": info.get("Season"),
        "Date": info.get("Date"),
        "Rodada": info.get("Rodada"),
        "Home": info.get("Home"),
        "Away": info.get("Away"),
        "goals_total": gols_total,
        "goals_home": len(gols_home),
        "goals_away": len(gols_away),
        "goals_home_minutes": "|".join(sorted(gols_home, key=lambda x: int(x.split("+")[0]))),
        "goals_away_minutes": "|".join(sorted(gols_away, key=lambda x: int(x.split("+")[0]))),
        "yellow_cards": yellow_cards,
        "shots_h": shots_h,
        "shots_a": shots_a,
        "shots_on_target_h": sot_h,
        "shots_on_target_a": sot_a,
        "corners_h": corners_h,
        "corners_a": corners_a,
    }


def main() -> None:
    garantir_pasta()

    if not MATCHES_DIR.exists():
        print(f"[ERRO] Pasta não encontrada: {MATCHES_DIR}")
        return

    eventos_info = carregar_eventos_info()
    event_ids = sorted([p.name for p in MATCHES_DIR.iterdir() if p.is_dir()])

    with open(OUT_FILE, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "event_id",
                "League",
                "Season",
                "Date",
                "Rodada",
                "Home",
                "Away",
                "goals_total",
                "goals_home",
                "goals_away",
                "goals_home_minutes",
                "goals_away_minutes",
                "yellow_cards",
                "shots_h",
                "shots_a",
                "shots_on_target_h",
                "shots_on_target_a",
                "corners_h",
                "corners_a",
            ],
            delimiter=";",
        )
        writer.writeheader()

        for idx, event_id in enumerate(event_ids, start=1):
            print(f"[INFO] Processando {idx}/{len(event_ids)} | {event_id}")

            try:
                row = processar_evento(event_id, eventos_info)
                writer.writerow(row)
            except Exception as e:
                print(f"[ERRO] {event_id} | {e}")

    print(f"\n[OK] Arquivo salvo em: {OUT_FILE}")


if __name__ == "__main__":
    main()