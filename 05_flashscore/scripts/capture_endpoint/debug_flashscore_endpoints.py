# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import (
    BrowserContext,
    Page,
    Response,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)

# ==============================================================================
# CONFIG
# ==============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent

FLASHSCORE_URL = os.getenv(
    "FLASHSCORE_DEBUG_URL",
    "https://www.flashscore.com/match/027ZSHvp/#/match-summary",
)

OUT_DIR = Path(os.getenv("FLASHSCORE_DEBUG_OUT", str(ROOT / "debug_endpoints")))
HEADLESS = os.getenv("FLASHSCORE_DEBUG_HEADLESS", "false").lower() == "true"
SLOW_MO_MS = int(os.getenv("FLASHSCORE_DEBUG_SLOW_MO_MS", "0"))
TIMEOUT_MS = int(os.getenv("FLASHSCORE_DEBUG_TIMEOUT_MS", "120000"))
MAX_TEXT = int(os.getenv("FLASHSCORE_DEBUG_MAX_TEXT", "12000"))

SAVE_HTML = os.getenv("FLASHSCORE_DEBUG_SAVE_HTML", "false").lower() == "true"
SAVE_STORAGE = os.getenv("FLASHSCORE_DEBUG_SAVE_STORAGE", "true").lower() == "true"

USE_PROXY = os.getenv("FLASHSCORE_DEBUG_PROXY", "").strip()

JSONL_FILE = OUT_DIR / "captura.jsonl"
CSV_FILE = OUT_DIR / "captura.csv"
UNIQUE_FILE = OUT_DIR / "endpoints_unicos.txt"
HTML_FILE = OUT_DIR / "pagina_final.html"
STORAGE_FILE = OUT_DIR / "storage_state.json"

# Apenas pistas úteis para o Flashscore
INTERESTING_URL_PARTS = [
    "graphql",
    "feed",
    "odds",
    "event",
    "archive",
    "detail",
    "match",
]

INTERESTING_CONTENT_TYPES = [
    "application/json",
    "application/graphql-response+json",
    "text/plain",
    "javascript",
    "text/html",
]

IGNORE_EXTENSIONS = (
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico",
    ".css", ".woff", ".woff2", ".ttf", ".eot", ".map",
    ".mp4", ".webm", ".mp3", ".wav", ".zip",
)

BLOCK_IF_URL_CONTAINS = [
    "doubleclick",
    "googleads",
    "googlesyndication",
    "analytics",
    "pixel",
    "beacon",
    "clarity.ms",
    "facebook.net",
]

# ==============================================================================
# HELPERS
# ==============================================================================

def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def ensure_output() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def truncate_text(value: str | None, limit: int = MAX_TEXT) -> str | None:
    if value is None:
        return None
    return value[:limit]


def get_host(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


def ignored_extension(url: str) -> bool:
    base = url.split("?", 1)[0].lower()
    return base.endswith(IGNORE_EXTENSIONS)


def sanitize_headers(headers: dict[str, str]) -> dict[str, str]:
    sensitive = {
        "cookie",
        "authorization",
        "proxy-authorization",
        "x-csrf-token",
        "x-auth-token",
        "set-cookie",
        "token",
    }
    result: dict[str, str] = {}
    for k, v in headers.items():
        result[k] = "***" if k.lower() in sensitive else v
    return result


def parse_post_data(raw: str | None) -> Any:
    if not raw:
        return None

    raw = truncate_text(raw, MAX_TEXT)

    try:
        return json.loads(raw)
    except Exception:
        pass

    try:
        return parse_qs(raw, keep_blank_values=True)
    except Exception:
        return raw


def response_preview(response: Response, content_type: str | None) -> str | None:
    ct = (content_type or "").lower()

    if not any(x in ct for x in INTERESTING_CONTENT_TYPES):
        return None

    try:
        return truncate_text(response.text(), MAX_TEXT)
    except Exception:
        return None


def is_interesting(url: str, resource_type: str, content_type: str | None) -> bool:
    url_l = url.lower()
    ct = (content_type or "").lower()

    if resource_type in {"font", "image", "media"}:
        return False

    if ignored_extension(url):
        return False

    if any(part in url_l for part in BLOCK_IF_URL_CONTAINS):
        return False

    if resource_type in {"xhr", "fetch"}:
        return True

    if any(part in url_l for part in INTERESTING_URL_PARTS):
        return True

    if any(part in ct for part in INTERESTING_CONTENT_TYPES):
        return True

    return False


def dedup_key(method: str, url: str, status: int | None, post_data: str | None) -> str:
    return f"{method.upper()}|{url}|{status}|{post_data or ''}"


def auto_accept_cookies(page: Page) -> None:
    selectors = [
        "#onetrust-accept-btn-handler",
        "button:has-text('Aceitar')",
        "button:has-text('Accept')",
        "button:has-text('Concordo')",
        "button:has-text('I agree')",
    ]
    for sel in selectors:
        try:
            locator = page.locator(sel).first
            if locator.is_visible(timeout=1500):
                locator.click(timeout=1500)
                page.wait_for_timeout(800)
                print(f"[INFO] Cookies aceitos com seletor: {sel}")
                return
        except Exception:
            continue


# ==============================================================================
# CAPTURADOR
# ==============================================================================

class FlashscoreEndpointDebugger:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self.seen: set[str] = set()
        self.unique_endpoints: set[str] = set()

    def on_response(self, response: Response) -> None:
        try:
            request = response.request
            method = request.method.upper()
            url = request.url
            status = response.status
            resource_type = request.resource_type
            content_type = response.headers.get("content-type", "")

            if not is_interesting(url, resource_type, content_type):
                return

            try:
                raw_post_data = request.post_data
            except Exception:
                raw_post_data = None

            key = dedup_key(method, url, status, raw_post_data)
            if key in self.seen:
                return
            self.seen.add(key)

            item = {
                "timestamp": now_str(),
                "host": get_host(url),
                "method": method,
                "url": url,
                "status": status,
                "resource_type": resource_type,
                "content_type": content_type,
                "request_headers": sanitize_headers(request.headers),
                "response_headers": sanitize_headers(response.headers),
                "request_post_data": parse_post_data(raw_post_data),
                "response_preview": response_preview(response, content_type),
            }

            self.records.append(item)
            self.unique_endpoints.add(f"{method} {url}")

            print(f"[{status}] {method:<6} {resource_type:<8} {url}")

        except Exception as exc:
            print(f"[ERRO] Falha ao registrar response: {exc}")

    def save(self) -> None:
        with JSONL_FILE.open("w", encoding="utf-8") as f:
            for item in self.records:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

        fields = [
            "timestamp",
            "host",
            "method",
            "status",
            "resource_type",
            "content_type",
            "url",
            "request_post_data",
            "response_preview",
        ]

        with CSV_FILE.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields, delimiter=";")
            writer.writeheader()
            for item in self.records:
                row = {k: item.get(k) for k in fields}
                for key in ("request_post_data", "response_preview"):
                    value = row.get(key)
                    if isinstance(value, (dict, list)):
                        row[key] = json.dumps(value, ensure_ascii=False)
                writer.writerow(row)

        with UNIQUE_FILE.open("w", encoding="utf-8") as f:
            for ep in sorted(self.unique_endpoints):
                f.write(ep + "\n")


# ==============================================================================
# EXECUÇÃO
# ==============================================================================

def create_context() -> tuple[Any, BrowserContext, Page]:
    playwright = sync_playwright().start()

    launch_args: dict[str, Any] = {
        "headless": HEADLESS,
        "slow_mo": SLOW_MO_MS,
    }

    if USE_PROXY:
        launch_args["proxy"] = {"server": USE_PROXY}

    browser = playwright.chromium.launch(**launch_args)
    context = browser.new_context(ignore_https_errors=True)
    page = context.new_page()

    return playwright, context, page


def run() -> None:
    ensure_output()
    debugger = FlashscoreEndpointDebugger()
    playwright, context, page = create_context()
    page.on("response", debugger.on_response)

    try:
        print("=" * 90)
        print("FLASHSCORE DEBUG DE ENDPOINTS")
        print("=" * 90)
        print(f"URL inicial : {FLASHSCORE_URL}")
        print(f"Saída       : {OUT_DIR.resolve()}")
        print(f"Headless    : {HEADLESS}")
        print(f"Proxy       : {USE_PROXY or 'não configurado'}")
        print("=" * 90)
        print("Navegue manualmente e clique nestes mercados:")
        print("- Odds")
        print("- Full Time > 1X2")
        print("- Full Time > Over/Under")
        print("- Full Time > Both Teams to Score")
        print("- Full Time > Double Chance")
        print("- Corners > Over/Under")
        print("- 1st Half > 1X2")
        print("- 1st Half > Over/Under")
        print("\nQuando terminar, volte ao terminal e pressione ENTER.\n")

        page.goto(FLASHSCORE_URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
        page.wait_for_timeout(2500)
        auto_accept_cookies(page)

        input()

    except PlaywrightTimeoutError:
        print("[AVISO] Timeout no carregamento inicial.")
    except Exception as exc:
        print(f"[ERRO] Falha geral: {exc}")
    finally:
        try:
            if SAVE_STORAGE:
                context.storage_state(path=str(STORAGE_FILE))
            if SAVE_HTML:
                HTML_FILE.write_text(page.content(), encoding="utf-8")
        except Exception as exc:
            print(f"[AVISO] Não foi possível salvar estado/HTML: {exc}")

        page.wait_for_timeout(1200)
        debugger.save()
        context.close()
        playwright.stop()

    print("\n===== CAPTURA FINALIZADA =====")
    print(f"JSONL   : {JSONL_FILE.resolve()}")
    print(f"CSV     : {CSV_FILE.resolve()}")
    print(f"RESUMO  : {UNIQUE_FILE.resolve()}")
    if SAVE_STORAGE:
        print(f"STORAGE : {STORAGE_FILE.resolve()}")
    if SAVE_HTML:
        print(f"HTML    : {HTML_FILE.resolve()}")
    print(f"TOTAL   : {len(debugger.records)} responses capturadas")


if __name__ == "__main__":
    run()