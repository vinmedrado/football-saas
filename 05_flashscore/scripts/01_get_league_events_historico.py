# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs

from playwright.sync_api import sync_playwright

from utils.paths import LEAGUES_FILE, RUN_CONFIG, LEAGUES_OUT, STATE_DIR
from utils.io_utils import load_json, save_json
from utils.date_utils import resolver_data

STATE_FILE = STATE_DIR / "league_events_state.json"
TIMEOUT_MS = 120000


# ── Utils ────────────────────────────────────────────────────────────────
def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def filtrar_ligas(ligas, config):
    if config.get("test_mode"):
        slugs = config.get("league_slugs", [])

        if slugs:
            slugs = set(slugs)
            filtradas = [l for l in ligas if l.get("league_slug") in slugs]
            return filtradas[:config.get("max_leagues", len(filtradas))]

        return ligas[:config.get("max_leagues", len(ligas))]

    return ligas


def aceitar_cookies(page):
    seletores = [
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept')",
        "button:has-text('Aceitar')",
        "button:has-text('I agree')",
    ]
    for seletor in seletores:
        try:
            page.locator(seletor).first.click(timeout=2000)
            page.wait_for_timeout(1000)
            return
        except Exception:
            continue


def limpar_rodada(texto: str | None) -> str | None:
    if not texto:
        return None
    texto = " ".join(str(texto).split()).strip()
    return texto if texto else None


def extrair_event_id(link: str | None):
    if not link:
        return None

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


def extrair_temporada_da_url(url: str) -> tuple[int | None, int | None]:
    m = re.search(r"-(\d{4})-(\d{4})(?:/|$)", url)
    if m:
        return int(m.group(1)), int(m.group(2))

    m2 = re.search(r"-(\d{4})(?:/|$)", url)
    if m2:
        ano = int(m2.group(1))
        return ano, ano

    return None, None


def formatar_season(season_start: int | None, season_end: int | None) -> str | None:
    if not season_start or not season_end:
        return None
    return f"{season_start}/{season_end}"


def parse_data_evento(
    date_text: str | None,
    season_start: int | None,
    season_end: int | None,
) -> datetime | None:
    if not date_text:
        return None

    try:
        parte = str(date_text).split()[0].strip()
        partes = parte.strip(".").split(".")

        if len(partes) < 2:
            return None

        dia = int(partes[0])
        mes = int(partes[1])

        if len(partes) >= 3 and partes[2]:
            ano = int(partes[2])
            return datetime(ano, mes, dia)

        if season_start and season_end and season_start == season_end:
            return datetime(season_start, mes, dia)

        if season_start and season_end:
            ano = season_start if mes >= 7 else season_end
            return datetime(ano, mes, dia)

        hoje = datetime.now()
        ano = hoje.year
        if mes > hoje.month:
            ano -= 1

        return datetime(ano, mes, dia)

    except Exception:
        return None


# ── Archive / temporadas ────────────────────────────────────────────────
def obter_urls_temporadas(page, archive_url: str, league_url: str, start_date: datetime) -> list[str]:
    urls = set()

    base_comp = league_url.replace("/results/", "").rstrip("/")

    try:
        page.goto(archive_url, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
        page.wait_for_timeout(4000)
    except Exception:
        return [league_url]

    for _ in range(4):
        try:
            page.mouse.wheel(0, 2500)
            page.wait_for_timeout(500)
        except Exception:
            pass

    try:
        hrefs = page.eval_on_selector_all(
            "a",
            """
            els => els
                .map(a => a.href || a.getAttribute('href'))
                .filter(Boolean)
            """
        )
    except Exception:
        hrefs = []

    for href in hrefs:
        try:
            href_abs = urljoin("https://www.flashscore.com", href)
            href_limpo = href_abs.split("?")[0].split("#")[0].rstrip("/")

            if "/football/" not in href_limpo:
                continue
            if "/archive" in href_limpo:
                continue

            ultimo_slug_base = base_comp.split("/")[-1]
            ultimo_slug_href = href_limpo.split("/")[-1]

            if not ultimo_slug_href.startswith(ultimo_slug_base):
                continue

            href_results = href_limpo + "/results/"
            href_results = href_results.replace("//results/", "/results/")
            urls.add(href_results)

        except Exception:
            continue

    if not urls:
        urls.add(league_url)

    urls_validas = []
    for url in sorted(urls):
        if any(str(ano) in url for ano in range(start_date.year, datetime.now().year + 2)):
            urls_validas.append(url)

    if league_url not in urls_validas:
        urls_validas.append(league_url)

    final = []
    vistos = set()
    for u in urls_validas:
        if u not in vistos:
            vistos.add(u)
            final.append(u)

    print(f"[INFO] Temporadas encontradas: {len(final)}")
    for u in final:
        print(f"   - {u}")

    return final


# ── Extração de eventos ─────────────────────────────────────────────────
def extrair_eventos_da_pagina(
    page,
    league_name: str,
    season_hint: str | None,
    season_start: int | None,
    season_end: int | None,
    start_date: datetime,
    end_date: datetime,
    ultima_data: datetime | None,
) -> tuple[list[dict], datetime | None]:
    eventos = []
    vistos = set()
    max_data_liga = None
    rodada_atual = None

    for _ in range(8):
        try:
            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(700)
        except Exception:
            pass

    for _ in range(80):
        try:
            btn = page.locator("a.event__more, span.event__more").first
            if btn.is_visible(timeout=1000):
                btn.click()
                page.wait_for_timeout(1000)
            else:
                break
        except Exception:
            break

    elementos = page.locator("div.event__round, div.event__match")
    total = elementos.count()

    for i in range(total):
        try:
            el = elementos.nth(i)
            classes = el.get_attribute("class") or ""

            if "event__round" in classes:
                try:
                    rodada_atual = limpar_rodada(el.inner_text().strip())
                except Exception:
                    rodada_atual = None
                continue

            if "event__match" not in classes:
                continue

            home = None
            away = None
            data_texto = None
            match_url = None
            event_id = None

            try:
                home = el.locator(".event__homeParticipant").inner_text(timeout=500).strip()
            except Exception:
                pass

            try:
                away = el.locator(".event__awayParticipant").inner_text(timeout=500).strip()
            except Exception:
                pass

            try:
                data_texto = el.locator(".event__time").inner_text(timeout=500).strip()
            except Exception:
                pass

            home_score = None
            away_score = None

            try:
                home_score = el.locator(".event__score--home").inner_text(timeout=300).strip()
            except Exception:
                pass

            try:
                away_score = el.locator(".event__score--away").inner_text(timeout=300).strip()
            except Exception:
                pass

            try:
                href = el.locator('a[href*="/match/"]').first.get_attribute("href", timeout=500)
                if href:
                    match_url = urljoin("https://www.flashscore.com", href)
                    event_id = extrair_event_id(match_url)
            except Exception:
                pass

            if not event_id or event_id in vistos:
                continue

            dt = parse_data_evento(data_texto, season_start, season_end)
            if not dt:
                continue

            if dt > end_date:
                continue
            if dt < start_date:
                continue
            if ultima_data and dt <= ultima_data:
                continue

            if (not home_score and not away_score) and dt.date() >= datetime.now().date():
                continue

            vistos.add(event_id)

            eventos.append({
                "event_id": event_id,
                "League": league_name,
                "Season": season_hint,
                "Date_raw": data_texto,
                "Date": dt.strftime("%Y-%m-%d"),
                "Rodada": rodada_atual,
                "Home": home,
                "Away": away,
                "match_url": match_url,
            })

            if not max_data_liga or dt > max_data_liga:
                max_data_liga = dt

        except Exception:
            continue

    return eventos, max_data_liga


# ── Main ────────────────────────────────────────────────────────────────
def main():
    ligas = load_json(LEAGUES_FILE) or []
    config = load_json(RUN_CONFIG) or {}

    start_date_raw = config.get("start_date", "2023-01-01")
    end_date_raw = config.get("end_date", "yesterday")

    start_date = datetime.strptime(resolver_data(start_date_raw), "%Y-%m-%d")
    end_date = datetime.strptime(resolver_data(end_date_raw), "%Y-%m-%d")

    state = load_state()
    ligas = filtrar_ligas(ligas, config)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=config.get("headless", True))
        context = browser.new_context()
        page = context.new_page()

        for liga in ligas:
            nome = liga["league_name"]
            url = liga["league_url"]
            archive_url = url.replace("/results/", "/archive/")

            print(f"\n[INFO] {nome}")

            ultima_data = None
            ultima_data_str = state.get(nome)
            if ultima_data_str:
                try:
                    ultima_data = datetime.strptime(ultima_data_str, "%Y-%m-%d")
                    print(f"[STATE] Última data: {ultima_data_str}")
                except Exception:
                    ultima_data = None

            pasta = LEAGUES_OUT / nome.replace(" ", "_")
            arquivo = pasta / "events.json"
            existentes = load_json(arquivo) or []

            existentes_limpos = []
            existentes_ids = set()

            for item in existentes:
                eid = extrair_event_id(item.get("event_id"))
                if not eid or eid in existentes_ids:
                    continue

                item["event_id"] = eid
                existentes_ids.add(eid)
                existentes_limpos.append(item)

            aceitar_cookies(page)
            urls_temporadas = obter_urls_temporadas(page, archive_url, url, start_date)

            novos_total = []
            max_data_global = ultima_data

            for temporada_url in urls_temporadas:
                print(f"[TEMP] {temporada_url}")

                try:
                    page.goto(temporada_url, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
                    page.wait_for_timeout(2500)
                except Exception:
                    continue

                season_start, season_end = extrair_temporada_da_url(temporada_url)
                season_hint = formatar_season(season_start, season_end)

                eventos, max_data_liga = extrair_eventos_da_pagina(
                    page=page,
                    league_name=nome,
                    season_hint=season_hint,
                    season_start=season_start,
                    season_end=season_end,
                    start_date=start_date,
                    end_date=end_date,
                    ultima_data=ultima_data,
                )

                for ev in eventos:
                    eid = ev.get("event_id")
                    if eid and eid not in existentes_ids:
                        existentes_ids.add(eid)
                        novos_total.append(ev)

                if max_data_liga and (not max_data_global or max_data_liga > max_data_global):
                    max_data_global = max_data_liga

            eventos_finais = existentes_limpos + novos_total
            save_json(arquivo, eventos_finais)

            if max_data_global:
                state[nome] = max_data_global.strftime("%Y-%m-%d")

            print(f"[OK] novos jogos: {len(novos_total)} | total acumulado: {len(eventos_finais)}")

        save_state(state)
        browser.close()


if __name__ == "__main__":
    main()