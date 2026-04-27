# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime


# ── Paths ─────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
CONFIG_FILE = ROOT / "config" / "run_config.json"
OUTPUT_DIR  = ROOT / "output"
LOG_FILE    = ROOT / "logs" / "pipeline.log"

# ── Pipeline ──────────────────────────────────────────────
STEPS = [
    "01_get_league_events_historico.py",
    "02_get_match_feeds.py",
    "03_get_match_odds.py",
    "01_parse_league_events.py",
    "02_parse_match_feeds.py",
    "03_parse_match_odds.py",
    "04_build_base_unificada.py",
]


# ── Utils ─────────────────────────────────────────────────
def log(msg: str, level: str = "INFO") -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}] [{level}] {msg}"
    print(linha, flush=True)

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(linha + "\n")


def load_config() -> dict:
    if not CONFIG_FILE.exists():
        log(f"Config não encontrada: {CONFIG_FILE}", "WARNING")
        return {}

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)

    log(f"Config carregada: {CONFIG_FILE}")
    return config


def limpar_output(config: dict) -> None:
    if config.get("test_mode"):
        log("Modo TESTE ativado -> limpando output", "WARNING")

        if OUTPUT_DIR.exists():
            shutil.rmtree(OUTPUT_DIR)
            log(f"Output removido: {OUTPUT_DIR}")

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        log(f"Output recriado: {OUTPUT_DIR}")
    else:
        log("Modo TESTE desativado -> output preservado")


def build_env() -> dict:
    """
    Retorna uma cópia do ambiente com ROOT adicionado ao PYTHONPATH,
    garantindo que todos os subprocessos enxerguem o pacote utils/.
    """
    env = os.environ.copy()
    current = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT) + (os.pathsep + current if current else "")
    return env


def run_step(script_name: str, step_idx: int, total_steps: int) -> None:
    script_path = SCRIPTS_DIR / script_name

    if not script_path.exists():
        raise FileNotFoundError(f"Script não encontrado: {script_path}")

    log("=" * 80)
    log(f"ETAPA {step_idx}/{total_steps} -> {script_name}")
    log(f"Executando: {script_path}")

    start = time.time()

    with subprocess.Popen(
        [sys.executable, "-u", str(script_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=build_env(),          # <── PYTHONPATH injetado aqui
    ) as proc:
        assert proc.stdout is not None

        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue

            print(line, flush=True)

            LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")

        proc.wait()

    duration = time.time() - start

    if proc.returncode != 0:
        log(
            f"FALHA NA ETAPA {step_idx}/{total_steps} -> {script_name} | "
            f"return_code={proc.returncode} | duração={duration:.2f}s",
            "ERROR",
        )
        raise RuntimeError(f"Falha na etapa: {script_name}")

    log(
        f"ETAPA CONCLUÍDA -> {script_name} | duração={duration:.2f}s",
        "SUCCESS",
    )


# ── Main ──────────────────────────────────────────────────
def main() -> None:
    pipeline_start = time.time()

    log("=" * 80)
    log("PIPELINE START")
    log("=" * 80)

    config = load_config()
    limpar_output(config)

    total_steps = len(STEPS)

    try:
        for idx, step in enumerate(STEPS, start=1):
            run_step(step, idx, total_steps)

    except Exception as e:
        total = time.time() - pipeline_start
        log("=" * 80, "ERROR")
        log(f"PIPELINE FALHOU -> {e}", "ERROR")
        log(f"Tempo total até falha: {total:.2f}s", "ERROR")
        log("=" * 80, "ERROR")
        raise

    total = time.time() - pipeline_start

    log("=" * 80)
    log(f"PIPELINE END | duração total={total:.2f}s", "SUCCESS")
    log("=" * 80)


if __name__ == "__main__":
    main()
