# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from playwright.sync_api import (
    Browser,
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

INPUT_MATCHES = ROOT / "input" / "matches.csv"
OUTPUT_DIR = ROOT / "output" / "odds"
LOG_DIR = ROOT / "logs"
AUDIT_FILE = LOG_DIR / "capture_odds_audit.csv"
SUMMARY_FILE = LOG_DIR / "capture_odds_summary.json"

HEADLESS = True
SLOW_MO_MS = 0
PAGE_TIMEOUT_MS = 45000

WAIT_AFTER_PAGE_OPEN_MS = 2500
WAIT_AFTER_TAB_CLICK_MS = 1500
WAIT_AFTER_MARKET_CLICK_MS = 2200
WAIT_BETWEEN_RETRIES_SEC = 2

MAX_RETRIES_PER_MARKET = 3
MAX_RETRIES_PER_MATCH = 2

USE_PROXY = False
PROXY_SERVER = "http://127.0.0.1:8080"

# ==============================================================================
# LOGGING
# ==============================================================================

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "capture_odds_robusto.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("capture_odds")

# ==============================================================================
# DATA MODELS
# ==============================================================================

@dataclass
class MarketConfig:
    slug: str
    filename_suffix: str
    section_candidates: List[str]
    tab_candidates: List[str]
    enabled: bool = True


MARKETS: List[MarketConfig] = [
    MarketConfig(
        slug="1x2_ft",
        filename_suffix="1x2_ft",
        section_candidates=["Full Time"],
        tab_candidates=["1X2"],
    ),
    MarketConfig(
        slug="ou_ft",
        filename_suffix="ou_ft",
        section_candidates=["Full Time"],
        tab_candidates=["Over/Under"],
    ),
    MarketConfig(
        slug="btts_ft",
        filename_suffix="btts_ft",
        section_candidates=["Full Time"],
        tab_candidates=["Both Teams to Score", "BTTS"],
    ),
    MarketConfig(
        slug="dc_ft",
        filename_suffix="dc_ft",
        section_candidates=["Full Time"],
        tab_candidates=["Double Chance"],
    ),
    MarketConfig(
        slug="corners_ou",
        filename_suffix="corners_ou",
        section_candidates=["Corners"],
        tab_candidates=["Over/Under"],
    ),
    MarketConfig(
        slug="1x2_ht",
        filename_suffix="1x2_ht",
        section_candidates=["1st Half"],
        tab_candidates=["1X2"],
    ),
    MarketConfig(
        slug="ou_ht",
        filename_suffix="ou_ht",
        section_candidates=["1st Half"],
        tab_candidates=["Over/Under"],
    ),
]

# ==============================================================================
# UTILS
# ==============================================================================

def safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_matches_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {path}")

    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            match_id = str(row.get("match_id", "")).strip()
            url = str(row.get("url", "")).strip()
            if match_id and url:
                rows.append({"match_id": match_id, "url": url})
    return rows


def write_audit_header_if_needed(path: Path) -> None:
    if path.exists():
        return
    safe_mkdir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "match_id",
            "market",
            "attempt",
            "status",
            "reason",
            "saved_file",
            "response_url",
        ])


def append_audit(
    match_id: str,
    market: str,
    attempt: int,
    status: str,
    reason: str,
    saved_file: str = "",
    response_url: str = "",
) -> None:
    with AUDIT_FILE.open("a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            match_id,
            market,
            attempt,
            status,
            reason,
            saved_file,
            response_url,
        ])


def try_parse_json(text: str) -> Optional[Any]:
    try:
        return json.loads(text)
    except Exception:
        return None


def save_payload(match_id: str, market: MarketConfig, attempt: int, text: str) -> Path:
    out_dir = OUTPUT_DIR / match_id
    safe_mkdir(out_dir)
    file_path = out_dir / f"{attempt}_{market.filename_suffix}.txt"
    file_path.write_text(text, encoding="utf-8")
    return file_path


def save_debug_payload(match_id: str, market: MarketConfig, attempt: int, text: str) -> Path:
    debug_dir = OUTPUT_DIR / match_id / "_debug"
    safe_mkdir(debug_dir)
    file_path = debug_dir / f"{attempt}_{market.slug}_debug.txt"
    file_path.write_text(text, encoding="utf-8")
    return file_path

# ==============================================================================
# RESPONSE FILTER / VALIDATION
# ==============================================================================

def is_relevant_response_url(url: str) -> bool:
    url_l = (url or "").lower()

    if "ds.lsapp.eu/odds/pq_graphql" in url_l:
        return True

    if "/odds/pq_graphql" in url_l:
        return True

    return False


def is_valid_payload_for_market(payload: str, market: MarketConfig, url: str) -> Tuple[bool, str]:
    if not payload:
        return False, "payload_vazio"

    if try_parse_json(payload) is None:
        return False, "json_invalido"

    url_l = (url or "").lower()
    payload_l = payload.lower()

    if market.slug == "1x2_ft":
        if "bettype=home_draw_away" in url_l and "betscope=full_time" in url_l:
            return True, "ok"
        return False, "url_nao_bate_1x2_ft"

    if market.slug == "ou_ft":
        if "over" in payload_l and "under" in payload_l:
            return True, "ok_payload_detectado"
        # fallback leve
        if ("over" in payload_l and "under" in payload_l and "full" in payload_l):
            return True, "ok_fallback_payload_ou_ft"
        return False, "url_nao_bate_ou_ft"

    if market.slug == "btts_ft":
        if "bettype=both_teams_to_score" in url_l:
            return True, "ok"
        if "both" in payload_l and "score" in payload_l:
            return True, "ok_fallback_payload_btts_ft"
        return False, "url_nao_bate_btts_ft"

    if market.slug == "dc_ft":
        if "bettype=double_chance" in url_l:
            return True, "ok"
        if "double" in payload_l and "chance" in payload_l:
            return True, "ok_fallback_payload_dc_ft"
        return False, "url_nao_bate_dc_ft"

    if market.slug == "corners_ou":
        if "bettype=over_under" in url_l and ("corner" in payload_l or "corners" in payload_l):
            return True, "ok"
        return False, "url_ou_payload_nao_bate_corners_ou"

    if market.slug == "1x2_ht":
        if "bettype=home_draw_away" in url_l and (
            "betscope=half_time" in url_l or "betscope=first_half" in url_l
        ):
            return True, "ok"
        return False, "url_nao_bate_1x2_ht"

    if market.slug == "ou_ht":
        if "bettype=over_under" in url_l and (
            "betscope=half_time" in url_l or "betscope=first_half" in url_l
        ):
            return True, "ok"
        return False, "url_nao_bate_ou_ht"

    return False, "mercado_nao_mapeado"

# ==============================================================================
# RESPONSE COLLECTOR
# ==============================================================================

class ResponseCollector:
    def __init__(self) -> None:
        self.responses: List[Tuple[str, str]] = []

    def clear(self) -> None:
        self.responses.clear()

    def handler(self, response: Response) -> None:
        try:
            url = response.url
            if not is_relevant_response_url(url):
                return

            content_type = response.headers.get("content-type", "").lower()

            try:
                text = response.text()
            except Exception:
                text = ""

            self.responses.append((url, text))
            logger.info(f"[CAPTURED] {url} | content-type={content_type}")

        except Exception as e:
            logger.debug(f"Falha ao registrar response: {e}")

    def best_payload_for_market(self, market: MarketConfig) -> Tuple[Optional[str], Optional[str], str]:
        if not self.responses:
            return None, None, "nenhuma_resposta_capturada"

        logger.info(f"[DEBUG] {market.slug} -> total responses: {len(self.responses)}")

        for url, text in reversed(self.responses):
            ok, reason = is_valid_payload_for_market(text, market, url)
            logger.info(f"[DEBUG] mercado={market.slug} | ok={ok} | reason={reason} | url={url[:180]}")
            if ok:
                return text, url, reason

        url, text = self.responses[-1]
        _, reason = is_valid_payload_for_market(text, market, url)
        return None, url, reason

# ==============================================================================
# PAGE HELPERS
# ==============================================================================

def dismiss_cookie_banner(page: Page) -> None:
    candidates = [
        "#onetrust-accept-btn-handler",
        "button:has-text('Accept')",
        "button:has-text('I Accept')",
        "button:has-text('Aceitar')",
        "button:has-text('Concordo')",
        "[id*='onetrust-accept']",
        "[aria-label*='accept']",
    ]
    for sel in candidates:
        try:
            locator = page.locator(sel).first
            if locator.is_visible(timeout=1500):
                locator.click(timeout=1500)
                logger.info("Banner de cookies fechado.")
                page.wait_for_timeout(1000)
                return
        except Exception:
            continue


def open_match_page(page: Page, url: str) -> None:
    page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
    page.wait_for_timeout(WAIT_AFTER_PAGE_OPEN_MS)
    dismiss_cookie_banner(page)


def open_odds_tab(page: Page) -> bool:
    selectors = [
        "button[data-testid='wcl-tab']:has-text('Odds')",
        "role=tab[name='Odds']",
        "text=Odds",
    ]

    for sel in selectors:
        try:
            locator = page.locator(sel).first
            if locator.is_visible(timeout=4000):
                locator.click(timeout=4000)
                page.wait_for_timeout(WAIT_AFTER_TAB_CLICK_MS)
                logger.info("Aba Odds aberta.")
                return True
        except Exception:
            continue

    logger.warning("Não conseguiu abrir a aba Odds.")
    return False


def click_single_tab_contains(page: Page, text: str, timeout: int = 4000) -> bool:
    try:
        locator = page.locator("button[data-testid='wcl-tab']", has_text=text).first
        if locator.is_visible(timeout=timeout):
            locator.click(timeout=timeout)
            page.wait_for_timeout(WAIT_AFTER_TAB_CLICK_MS)
            logger.info(f"Clicou na aba: {text}")
            return True
    except Exception:
        pass
    return False


def click_any_tab_contains(page: Page, texts: List[str], timeout: int = 4000) -> bool:
    for text in texts:
        if click_single_tab_contains(page, text, timeout=timeout):
            return True
    return False


def force_market_request(page: Page) -> None:
    try:
        page.wait_for_load_state("networkidle", timeout=5000)
    except Exception:
        pass

    page.wait_for_timeout(800)

    try:
        page.mouse.wheel(0, 700)
        page.wait_for_timeout(700)
        page.mouse.wheel(0, -700)
        page.wait_for_timeout(1000)
    except Exception:
        pass


def activate_market_on_page(page: Page, market: MarketConfig) -> bool:
    opened_anything = False

    if not open_odds_tab(page):
        return False

    if market.section_candidates:
        if click_any_tab_contains(page, market.section_candidates):
            opened_anything = True

    if market.tab_candidates:
        if click_any_tab_contains(page, market.tab_candidates):
            opened_anything = True

    force_market_request(page)
    page.wait_for_timeout(WAIT_AFTER_MARKET_CLICK_MS)

    return opened_anything

# ==============================================================================
# CAPTURE
# ==============================================================================

def capture_market_payload(
    page: Page,
    collector: ResponseCollector,
    market: MarketConfig,
    match_id: str,
) -> Tuple[bool, str, Optional[Path], Optional[str]]:
    for attempt in range(1, MAX_RETRIES_PER_MARKET + 1):
        logger.info(f"[{match_id}] Mercado={market.slug} tentativa={attempt}")

        try:
            activated = activate_market_on_page(page, market)

            payload_text, response_url, reason = collector.best_payload_for_market(market)

            if payload_text:
                file_path = save_payload(match_id, market, attempt, payload_text)
                append_audit(
                    match_id=match_id,
                    market=market.slug,
                    attempt=attempt,
                    status="ok",
                    reason=reason,
                    saved_file=str(file_path),
                    response_url=response_url or "",
                )
                logger.info(f"[{match_id}] OK {market.slug} -> {file_path.name}")
                return True, reason, file_path, response_url

            if collector.responses:
                save_debug_payload(match_id, market, attempt, collector.responses[-1][1])

            if not activated:
                reason = "aba_ou_secao_nao_encontrada"

            append_audit(
                match_id=match_id,
                market=market.slug,
                attempt=attempt,
                status="fail",
                reason=reason,
                saved_file="",
                response_url=response_url or "",
            )
            logger.warning(f"[{match_id}] FAIL {market.slug}: {reason}")

            if attempt < MAX_RETRIES_PER_MARKET:
                time.sleep(WAIT_BETWEEN_RETRIES_SEC)

        except PlaywrightTimeoutError:
            append_audit(
                match_id=match_id,
                market=market.slug,
                attempt=attempt,
                status="fail",
                reason="timeout",
            )
            logger.warning(f"[{match_id}] TIMEOUT em {market.slug}")

        except Exception as e:
            append_audit(
                match_id=match_id,
                market=market.slug,
                attempt=attempt,
                status="fail",
                reason=f"erro:{type(e).__name__}",
            )
            logger.exception(f"[{match_id}] Erro em {market.slug}: {e}")

    return False, "max_retries_excedido", None, None

# ==============================================================================
# PIPELINE
# ==============================================================================

def process_match(context: BrowserContext, match_id: str, url: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "match_id": match_id,
        "url": url,
        "ok_markets": [],
        "failed_markets": [],
    }

    for match_attempt in range(1, MAX_RETRIES_PER_MATCH + 1):
        logger.info("=" * 90)
        logger.info(f"[{match_id}] Abrindo jogo | tentativa página={match_attempt}")
        logger.info(url)

        page = context.new_page()
        page.set_default_timeout(PAGE_TIMEOUT_MS)

        collector = ResponseCollector()
        page.on("response", collector.handler)

        try:
            open_match_page(page, url)
            open_odds_tab(page)
            page.wait_for_timeout(1500)

            for market in MARKETS:
                if not market.enabled:
                    continue

                ok, reason, _, _ = capture_market_payload(
                    page=page,
                    collector=collector,
                    market=market,
                    match_id=match_id,
                )

                if ok:
                    result["ok_markets"].append(market.slug)
                else:
                    result["failed_markets"].append({
                        "market": market.slug,
                        "reason": reason,
                    })

            failed_market_slugs = {x["market"] for x in result["failed_markets"]}
            if match_attempt < MAX_RETRIES_PER_MATCH and (
                "1x2_ht" in failed_market_slugs or "ou_ht" in failed_market_slugs
            ):
                logger.info(f"[{match_id}] Reabrindo página por falha em HT...")
                page.close()
                time.sleep(2)
                continue

            page.close()
            break

        except Exception as e:
            logger.exception(f"[{match_id}] Erro geral no jogo: {e}")
            try:
                page.close()
            except Exception:
                pass

            if match_attempt >= MAX_RETRIES_PER_MATCH:
                break

            time.sleep(2)

    return result


def launch_browser(p) -> Browser:
    launch_args: Dict[str, Any] = {
        "headless": HEADLESS,
        "slow_mo": SLOW_MO_MS,
    }
    if USE_PROXY:
        launch_args["proxy"] = {"server": PROXY_SERVER}
    return p.chromium.launch(**launch_args)


def main() -> None:
    logger.info("=" * 90)
    logger.info("CAPTURA ROBUSTA DE ODDS - INÍCIO")
    logger.info("=" * 90)

    safe_mkdir(OUTPUT_DIR)
    write_audit_header_if_needed(AUDIT_FILE)

    matches = read_matches_csv(INPUT_MATCHES)
    logger.info(f"Total de jogos para processar: {len(matches)}")

    all_results: List[Dict[str, Any]] = []

    with sync_playwright() as p:
        browser = launch_browser(p)
        context = browser.new_context(
            ignore_https_errors=True,
            service_workers="block",
        )

        for idx, row in enumerate(matches, start=1):
            match_id = row["match_id"]
            url = row["url"]

            logger.info(f"[{idx}/{len(matches)}] Processando {match_id}")
            result = process_match(context, match_id, url)
            all_results.append(result)

        browser.close()

    SUMMARY_FILE.write_text(
        json.dumps(all_results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    logger.info("=" * 90)
    logger.info("CAPTURA ROBUSTA DE ODDS - FIM")
    logger.info(f"Resumo salvo em: {SUMMARY_FILE}")
    logger.info(f"Auditoria salva em: {AUDIT_FILE}")
    logger.info("=" * 90)


if __name__ == "__main__":
    main()