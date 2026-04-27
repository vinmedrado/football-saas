# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
import requests
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from typing import Any

from utils.paths import LEAGUES_OUT, ODDS_OUT, BOOKMAKERS, STATE_DIR, RUN_CONFIG

STATE_FILE = STATE_DIR / "match_odds_state.json"

HEADERS = {
    "referer": "https://www.flashscore.com/",
    "user-agent": "Mozilla/5.0",
    "x-fsign": "SW9D1eZo",
}

ODDS_URL = "https://global.ds.lsapp.eu/odds/pq_graphql"

# nome_arquivo, betType, betScope, bookmaker_group
MARKETS = [
    ("1x2_ft",     "HOME_DRAW_AWAY",      "FULL_TIME",  "home_draw_away"),
    ("1x2_ht",     "HOME_DRAW_AWAY",      "FIRST_HALF", "home_draw_away"),
    ("ou_ft",      "OVER_UNDER",          "FULL_TIME",  "over_under"),
    ("ou_ht",      "OVER_UNDER",          "FIRST_HALF", "over_under"),
    ("btts_ft",    "BOTH_TEAMS_TO_SCORE", "FULL_TIME",  "both_teams_to_score"),
    ("dc_ft",      "DOUBLE_CHANCE",       "FULL_TIME",  "double_chance"),
    ("corners_ou", "OVER_UNDER",          "FULL_TIME",  "corners_over_under"),
]

MAX_EMPTY_ATTEMPTS = 3
MAX_ERROR_ATTEMPTS = 3


def load_config() -> dict[str, Any]:
    if RUN_CONFIG.exists():
        try:
            return json.loads(RUN_CONFIG.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


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


def load_state() -> dict[str, Any]:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def carregar_eventos() -> list[str]:
    arquivos = list(LEAGUES_OUT.glob("*/events.json"))
    eventos = []

    for arq in arquivos:
        try:
            data = json.loads(arq.read_text(encoding="utf-8"))
        except Exception:
            continue

        for item in data:
            eid_raw = item.get("event_id")
            eid = extrair_event_id(eid_raw)
            if eid:
                eventos.append(eid)

    vistos = set()
    final = []
    for eid in eventos:
        if eid not in vistos:
            vistos.add(eid)
            final.append(eid)

    return final


def carregar_bookmakers() -> dict[str, list[int]]:
    raw = json.loads(BOOKMAKERS.read_text(encoding="utf-8"))

    return {
        "home_draw_away": raw.get("home_draw_away", []),
        "over_under": raw.get("over_under", raw.get("home_draw_away", [])),
        "both_teams_to_score": raw.get("both_teams_to_score", raw.get("home_draw_away", [])),
        "double_chance": raw.get("double_chance", raw.get("home_draw_away", [])),
        "corners_over_under": raw.get("corners_over_under", raw.get("over_under", raw.get("home_draw_away", []))),
    }


def salvar(eid: str, nome: str, conteudo: str) -> None:
    pasta = ODDS_OUT / eid
    pasta.mkdir(parents=True, exist_ok=True)
    (pasta / f"{nome}.txt").write_text(conteudo, encoding="utf-8")


def salvar_debug(eid: str, nome: str, payload: dict[str, Any]) -> None:
    pasta = ODDS_OUT / eid / "_debug"
    pasta.mkdir(parents=True, exist_ok=True)
    (pasta / f"{nome}.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def market_key(nome: str) -> str:
    return f"market::{nome}"


def resposta_util(texto: str) -> bool:
    if not texto or not texto.strip():
        return False

    try:
        parsed = json.loads(texto)
    except Exception:
        return False

    if not isinstance(parsed, dict):
        return False

    data = parsed.get("data")
    if not data:
        return False

    return bool(data.get("findPrematchOddsForBookmaker"))


def arquivo_ja_ok(eid: str, nome: str) -> bool:
    pasta = ODDS_OUT / eid
    arquivos = list(pasta.glob(f"*_{nome}.txt"))
    if not arquivos:
        return False

    for arq in arquivos:
        try:
            if arq.exists() and arq.stat().st_size > 100:
                texto = arq.read_text(encoding="utf-8").strip()
                if resposta_util(texto):
                    return True
        except Exception:
            continue

    return False


def garantir_estado_mercado(markets_processados: dict[str, Any], eid: str, chave_market: str) -> dict[str, Any]:
    if eid not in markets_processados:
        markets_processados[eid] = {}

    atual = markets_processados[eid].get(chave_market)

    if isinstance(atual, str):
        atual = {
            "status": atual,
            "empty_attempts": 0,
            "error_attempts": 0,
        }

    if not isinstance(atual, dict):
        atual = {
            "status": "pending",
            "empty_attempts": 0,
            "error_attempts": 0,
        }

    markets_processados[eid][chave_market] = atual
    return atual


def deve_pular_por_limite(info: dict[str, Any]) -> bool:
    status = info.get("status")
    empty_attempts = int(info.get("empty_attempts", 0) or 0)
    error_attempts = int(info.get("error_attempts", 0) or 0)

    if status == "ok":
        return True

    if status == "empty" and empty_attempts >= MAX_EMPTY_ATTEMPTS:
        return True

    if status == "error" and error_attempts >= MAX_ERROR_ATTEMPTS:
        return True

    return False


def main():
    config = load_config()
    sleep_seconds = float(config.get("sleep_seconds", 0.35) or 0.35)

    event_ids = carregar_eventos()
    bookmakers_map = carregar_bookmakers()

    state = load_state()
    markets_processados = state.get("markets_processados", {})

    session = requests.Session()
    session.headers.update(HEADERS)

    for idx, eid in enumerate(event_ids, 1):
        print(f"[INFO] {idx}/{len(event_ids)} | {eid}")

        for nome, bet_type, scope, bookmaker_group in MARKETS:
            chave_market = market_key(nome)
            info = garantir_estado_mercado(markets_processados, eid, chave_market)

            if deve_pular_por_limite(info):
                print(
                    f"  [SKIP LIMIT] {nome} | status={info.get('status')} | "
                    f"empty={info.get('empty_attempts', 0)} | error={info.get('error_attempts', 0)}"
                )
                continue

            if arquivo_ja_ok(eid, nome):
                info["status"] = "ok"
                markets_processados[eid][chave_market] = info
                state["markets_processados"] = markets_processados
                save_state(state)
                print(f"  [SKIP FILE] {nome}")
                continue

            bookmakers = bookmakers_map.get(bookmaker_group, [])
            if not bookmakers:
                info["status"] = "error"
                info["error_attempts"] = int(info.get("error_attempts", 0)) + 1
                markets_processados[eid][chave_market] = info
                state["markets_processados"] = markets_processados
                save_state(state)
                print(f"  [SEM BOOKMAKERS] {nome} | grupo={bookmaker_group}")
                continue

            salvou_algum = False
            tentativas = 0
            teve_erro = False

            for bm in bookmakers:
                tentativas += 1
                params = {
                    "_hash": "ope2",
                    "eventId": eid,
                    "bookmakerId": str(bm),
                    "betType": bet_type,
                    "betScope": scope,
                }

                try:
                    r = session.get(ODDS_URL, params=params, timeout=20)
                    texto = r.text

                    if r.status_code == 200 and resposta_util(texto):
                        salvar(eid, f"{bm}_{nome}", texto)
                        salvou_algum = True
                    else:
                        salvar_debug(
                            eid,
                            f"{bm}_{nome}",
                            {
                                "status_code": r.status_code,
                                "url": r.url,
                                "params": params,
                                "body_preview": texto[:2000] if texto else "",
                            },
                        )

                except Exception as err:
                    teve_erro = True
                    salvar_debug(
                        eid,
                        f"{bm}_{nome}",
                        {
                            "error": str(err),
                            "params": params,
                        },
                    )
                    print(f"  [ERRO] {nome} | bm={bm} | {err}")

                time.sleep(sleep_seconds)

            if salvou_algum:
                info["status"] = "ok"
                info["empty_attempts"] = 0
                info["error_attempts"] = 0
                print(f"  [OK] {nome} | tentativas={tentativas}")

            elif teve_erro:
                info["status"] = "error"
                info["error_attempts"] = int(info.get("error_attempts", 0)) + 1
                print(
                    f"  [ERROR] {nome} | tentativas={tentativas} | "
                    f"error_attempts={info['error_attempts']}"
                )

            else:
                info["status"] = "empty"
                info["empty_attempts"] = int(info.get("empty_attempts", 0)) + 1
                print(
                    f"  [VAZIO] {nome} | tentativas={tentativas} | "
                    f"empty_attempts={info['empty_attempts']}"
                )

            markets_processados[eid][chave_market] = info
            state["markets_processados"] = markets_processados
            save_state(state)

    print("\n[OK] odds atualizadas")


if __name__ == "__main__":
    main()