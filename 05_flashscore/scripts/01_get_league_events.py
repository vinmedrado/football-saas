import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import time
import json
from pathlib import Path
from datetime import datetime

from playwright.sync_api import sync_playwright

from utils.paths import LEAGUES_FILE, RUN_CONFIG, LEAGUES_OUT
from utils.io_utils import load_json, save_json
from utils.date_utils import resolver_data

STATE_FILE = Path("05_flashscore/state/league_events_state.json")


# ── STATE ────────────────────────────────────────────────
def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


# ── FILTRO ───────────────────────────────────────────────
def filtrar_ligas(ligas, config):
    if config["test_mode"]:
        slugs = set(config["league_slugs"])
        return [l for l in ligas if l.get("league_slug") in slugs][:config["max_leagues"]]
    return ligas


# ── DATA ─────────────────────────────────────────────────
def extrair_data_valida(date_text):
    if not date_text:
        return None

    try:
        parte = date_text.split()[0]
        partes = parte.strip(".").split(".")

        dia = int(partes[0])
        mes = int(partes[1])

        hoje = datetime.now()

        ano = hoje.year
        if mes > hoje.month:
            ano -= 1

        return datetime(ano, mes, dia)

    except:
        return None


# ── MAIN ────────────────────────────────────────────────
def main():
    ligas = load_json(LEAGUES_FILE)
    config = load_json(RUN_CONFIG)

    start_date = datetime.strptime(resolver_data(config["start_date"]), "%Y-%m-%d")
    end_date   = datetime.strptime(resolver_data(config["end_date"]), "%Y-%m-%d")

    state = load_state()

    ligas = filtrar_ligas(ligas, config)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=config["headless"])
        page = browser.new_page()

        for liga in ligas:
            nome = liga["league_name"]
            url  = liga["league_url"]

            print(f"\n[INFO] {nome}")

            ultima_data_str = state.get(nome)
            ultima_data = None

            if ultima_data_str:
                ultima_data = datetime.strptime(ultima_data_str, "%Y-%m-%d")
                print(f"[STATE] Última data: {ultima_data_str}")

            page.goto(url, timeout=120000)
            page.wait_for_timeout(4000)

            try:
                page.locator("#onetrust-accept-btn-handler").click(timeout=2000)
            except:
                pass

            for _ in range(30):
                try:
                    btn = page.locator("a.event__more, span.event__more").first
                    if btn.is_visible():
                        btn.click()
                        page.wait_for_timeout(1000)
                    else:
                        break
                except:
                    break

            elementos = page.locator("div.event__match")
            total = elementos.count()

            eventos = []
            vistos = set()
            max_data_liga = None

            for i in range(total):
                el = elementos.nth(i)

                try:
                    home = el.locator(".event__homeParticipant").inner_text()
                    away = el.locator(".event__awayParticipant").inner_text()
                    date = el.locator(".event__time").inner_text()

                    link = el.locator('a[href*="/match/"]').first.get_attribute("href")
                    if not link:
                        continue

                    event_id = link.split("/")[-2]

                    if event_id in vistos:
                        continue

                    dt = extrair_data_valida(date)
                    if not dt:
                        continue

                    #  filtro histórico
                    if dt > end_date:
                        continue

                    if dt < start_date:
                        continue

                    # incremental
                    if ultima_data and dt <= ultima_data:
                        continue

                    vistos.add(event_id)

                    eventos.append({
                        "event_id": event_id,
                        "League": nome,
                        "Date": date,
                        "Home": home,
                        "Away": away
                    })

                    if not max_data_liga or dt > max_data_liga:
                        max_data_liga = dt

                except:
                    continue

            pasta = LEAGUES_OUT / nome.replace(" ", "_")

            #  append inteligente
            arquivo = pasta / "events.json"
            existentes = []

            if arquivo.exists():
                existentes = load_json(arquivo)

            eventos_final = existentes + eventos

            save_json(arquivo, eventos_final)

            #  atualiza state
            if max_data_liga:
                state[nome] = max_data_liga.strftime("%Y-%m-%d")

            print(f"[OK] novos jogos: {len(eventos)}")

        save_state(state)

        browser.close()


if __name__ == "__main__":
    main()