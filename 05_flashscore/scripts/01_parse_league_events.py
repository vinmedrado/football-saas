# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import json
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from utils.paths import LEAGUES_OUT as LEAGUES_DIR, PARSED_OUT as OUT_DIR
OUT_FILE = OUT_DIR / "league_events_parsed.csv"


def garantir_pasta():
    OUT_DIR.mkdir(parents=True, exist_ok=True)


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


def carregar_todas_ligas():
    arquivos = list(LEAGUES_DIR.glob("*/events.json"))
    dados = []

    for arq in arquivos:
        try:
            with open(arq, "r", encoding="utf-8") as f:
                conteudo = json.load(f)
                dados.extend(conteudo)
        except Exception as e:
            print(f"[ERRO] Falha ao ler {arq} | {e}")

    return dados


def main():
    garantir_pasta()
    eventos = carregar_todas_ligas()

    with open(OUT_FILE, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["event_id", "league"])

        total = 0
        ignorados = 0

        for item in eventos:
            eid_raw = item.get("event_id")
            eid = extrair_event_id(eid_raw)

            if not eid:
                ignorados += 1
                continue

            league = (
                item.get("League")
                or item.get("league")
                or item.get("league_name")
                or "UNKNOWN"
            )

            writer.writerow([eid, league])
            total += 1

    print(f"[OK] CSV salvo em: {OUT_FILE}")
    print(f"[OK] Total de event_ids válidos: {total}")
    print(f"[OK] Ignorados sem event_id válido: {ignorados}")


if __name__ == "__main__":
    main()