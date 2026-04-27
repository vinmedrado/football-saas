# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
import requests
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from utils.paths import LEAGUES_OUT as LEAGUES_DIR, MATCHES_OUT as OUT_DIR, STATE_DIR

STATE_FILE = STATE_DIR / "match_feeds_state.json"

HEADERS = {
    "referer": "https://www.flashscore.com/",
    "user-agent": "Mozilla/5.0",
    "x-fsign": "SW9D1eZo",
}

BASE_URL = "https://global.flashscore.ninja/2/x/feed"

FEEDS = {
    "dc": "dc_1_{event_id}",
    "sui": "df_sui_1_{event_id}",
    "mr": "df_mr_1_{event_id}",
    "dos": "df_dos_1_{event_id}_",
    "hi": "df_hi_1_{event_id}",
    "stats": "df_st_1_{event_id}",
}


# ── Utils ───────────────────────────────────────────────
def extrair_event_id(link: str):
    try:
        parsed = urlparse(str(link))
        query = parse_qs(parsed.query)

        if "mid" in query:
            return query["mid"][0]

        parts = parsed.path.strip("/").split("/")
        for part in reversed(parts):
            if part and part != "match":
                return part

    except Exception:
        return None

    return None


def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def carregar_eventos():
    arquivos = list(LEAGUES_DIR.glob("*/events.json"))
    eventos = []

    for arq in arquivos:
        try:
            data = json.loads(arq.read_text(encoding="utf-8"))
        except:
            continue

        for item in data:
            eid = extrair_event_id(item.get("event_id"))
            if eid:
                eventos.append(eid)

    return list(set(eventos))


def salvar(event_id, nome, conteudo):
    pasta = OUT_DIR / event_id
    pasta.mkdir(parents=True, exist_ok=True)
    (pasta / f"{nome}.txt").write_text(conteudo, encoding="utf-8")


def feed_key(nome):
    return f"feed::{nome}"


# ── Main ────────────────────────────────────────────────
def main():
    eventos = carregar_eventos()
    state = load_state()

    feeds_state = state.get("feeds_processados", {})

    session = requests.Session()
    session.headers.update(HEADERS)

    for idx, eid in enumerate(eventos, 1):
        print(f"[INFO] {idx}/{len(eventos)} | {eid}")

        if eid not in feeds_state:
            feeds_state[eid] = {}

        for nome, template in FEEDS.items():
            key = feed_key(nome)

            # ── SKIP se já OK ─────────────────────────────
            if feeds_state[eid].get(key) == "ok":
                print(f"  [SKIP] {nome}")
                continue

            # ── SKIP se arquivo já existe ────────────────
            arquivo = OUT_DIR / eid / f"{nome}.txt"
            if arquivo.exists() and arquivo.stat().st_size > 100:
                feeds_state[eid][key] = "ok"
                print(f"  [SKIP FILE] {nome}")
                continue

            try:
                url = f"{BASE_URL}/{template.format(event_id=eid)}"
                r = session.get(url, timeout=20)

                if r.status_code == 200 and r.text.strip():
                    salvar(eid, nome, r.text)
                    feeds_state[eid][key] = "ok"
                    print(f"  [OK] {nome}")
                else:
                    feeds_state[eid][key] = "empty"
                    print(f"  [VAZIO] {nome}")

            except Exception as err:
                feeds_state[eid][key] = "error"
                print(f"  [ERRO] {nome} | {err}")

            time.sleep(0.4)

        state["feeds_processados"] = feeds_state
        save_state(state)

    print("\n[OK] feeds atualizados")


if __name__ == "__main__":
    main()