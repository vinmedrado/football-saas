# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright


URL = "https://www.flashscore.com/football/"
OUT_FILE = Path("05_flashscore/config/leagues.json")
OUT_PARTIAL = Path("05_flashscore/config/leagues_parcial.json")


def slug_para_nome(slug: str) -> str:
    return slug.replace("-", " ").title()


def extrair_partes(url: str) -> list[str]:
    try:
        path = urlparse(url).path.strip("/")
        return path.split("/")
    except Exception:
        return []


def extrair_slug(url: str):
    partes = extrair_partes(url)

    if len(partes) < 3:
        return None

    if partes[0] != "football":
        return None

    return partes[1], partes[2]


def normalizar(url: str) -> str:
    url = url.split("?")[0].split("#")[0].rstrip("/")
    finais = {"fixtures", "results", "standings", "draw", "archive", "table"}

    partes = url.split("/")
    if partes and partes[-1].lower() in finais:
        url = "/".join(partes[:-1])

    return url.rstrip("/") + "/results/"


def salvar_json(path: Path, data: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )


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


def carregar_checkpoint():
    resultados = []
    vistos = set()
    paises_processados = set()

    if OUT_PARTIAL.exists():
        print("[INFO] Carregando checkpoint...")
        resultados = json.loads(OUT_PARTIAL.read_text(encoding="utf-8"))

        for item in resultados:
            slug = item.get("league_slug")
            if slug:
                vistos.add(slug)
                pais = slug.split("/")[0]
                paises_processados.add(pais)

        print(f"[INFO] {len(resultados)} ligas carregadas do parcial")

    return resultados, vistos, paises_processados


def pegar_paises(page) -> set[str]:
    paises = set()

    try:
        hrefs = page.locator("a[href*='/football/']").evaluate_all(
            "(els) => els.map(e => e.href).filter(Boolean)"
        )
    except Exception:
        return paises

    for href in hrefs:
        partes = extrair_partes(href)
        if len(partes) >= 2 and partes[0] == "football":
            paises.add(partes[1])

    return paises


def extrair_ligas_do_pais(page, pais: str, resultados: list, vistos: set) -> int:
    url_pais = f"https://www.flashscore.com/football/{pais}/"
    print(f"\n[INFO] Entrando em {pais}")

    try:
        page.goto(url_pais, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2500)
    except Exception:
        print(f"[ERRO] Falha ao abrir {pais}")
        return 0

    try:
        hrefs = page.locator("a[href*='/football/']").evaluate_all(
            "(els) => els.map(e => e.href).filter(Boolean)"
        )
    except Exception:
        print(f"[ERRO] Falha ao ler links em {pais}")
        return 0

    adicionadas = 0

    for href in hrefs:
        try:
            slugs = extrair_slug(href)
            if not slugs:
                continue

            country, league = slugs
            chave = f"{country}/{league}"

            if chave in vistos:
                continue

            vistos.add(chave)

            resultados.append({
                "league_name": f"{slug_para_nome(country)} {slug_para_nome(league)}",
                "league_slug": chave,
                "league_url": normalizar(href)
            })
            adicionadas += 1

        except Exception:
            continue

    return adicionadas


def main():
    resultados, vistos, paises_processados = carregar_checkpoint()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            slow_mo=0
        )
        context = browser.new_context()
        page = context.new_page()

        page.goto(URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(4000)

        aceitar_cookies(page)

        print("[INFO] Expandindo blocos...")
        try:
            headers = page.locator(".lmc__block .lmc__blockHeader")
            total_headers = headers.count()

            for i in range(total_headers):
                try:
                    headers.nth(i).click(timeout=1000)
                    page.wait_for_timeout(100)
                except Exception:
                    continue
        except Exception:
            pass

        print("[INFO] Coletando países/categorias...")
        paises = pegar_paises(page)
        print(f"[INFO] Total encontrado: {len(paises)}")

        for pais in sorted(paises):
            if pais in paises_processados:
                print(f"[SKIP] {pais} já processado")
                continue

            qtd = extrair_ligas_do_pais(page, pais, resultados, vistos)
            paises_processados.add(pais)

            resultados.sort(key=lambda x: x["league_name"])
            salvar_json(OUT_PARTIAL, resultados)

            print(f"[OK] {pais}: +{qtd} ligas")

        browser.close()

    resultados.sort(key=lambda x: x["league_name"])
    salvar_json(OUT_FILE, resultados)

    print(f"\n[OK] TOTAL FINAL: {len(resultados)} ligas")
    print(f"[OK] Final: {OUT_FILE.resolve()}")
    print(f"[OK] Parcial: {OUT_PARTIAL.resolve()}")


if __name__ == "__main__":
    main()