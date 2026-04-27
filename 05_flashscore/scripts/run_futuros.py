# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import shutil
import subprocess
import time
from pathlib import Path
from datetime import datetime


# ── Paths ─────────────────────────────────────────────────
ROOT = Path("05_flashscore")
CONFIG_FILE = ROOT / "config" / "run_config.json"
OUTPUT_DIR  = ROOT / "output"
LOG_FILE    = ROOT / "logs" / "pipeline.log"

# ── Pipeline ──────────────────────────────────────────────
STEPS = [
    "01_get_league_events.py",
    "02_get_match_feeds.py",
    "03_get_match_odds.py",
    "04_parse_league_events.py",
    "05_parse_match_feeds.py",
    "06_parse_match_odds.py",
    "07_build_base_unificada.py",
]


# ── Utils ─────────────────────────────────────────────────
def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}] {msg}"
    print(linha)

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(linha + "\n")


def load_config():
    if not CONFIG_FILE.exists():
        return {}
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def limpar_output(config: dict):
    if config.get("test_mode"):
        log("Modo TESTE → limpando output")

        if OUTPUT_DIR.exists():
            shutil.rmtree(OUTPUT_DIR)

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def run_step(script_name: str):
    script_path = ROOT / "scripts" / script_name

    if not script_path.exists():
        raise FileNotFoundError(f"Script não encontrado: {script_path}")

    log(f"INICIANDO → {script_name}")

    start = time.time()

    result = subprocess.run(
        ["python", str(script_path)],
        capture_output=True,
        text=True
    )

    duration = time.time() - start

    if result.returncode != 0:
        log(f"ERRO em {script_name}")
        log(result.stderr)
        raise RuntimeError(f"Falha na etapa: {script_name}")

    log(f"FINALIZADO → {script_name} ({duration:.2f}s)")

    if result.stdout:
        print(result.stdout)


# ── Main ──────────────────────────────────────────────────
def main():
    log("===== PIPELINE START =====")

    config = load_config()

    limpar_output(config)

    for step in STEPS:
        run_step(step)

    log("===== PIPELINE END =====")


if __name__ == "__main__":
    main()