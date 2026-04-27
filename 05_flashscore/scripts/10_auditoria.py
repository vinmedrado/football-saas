# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import json
import csv
from collections import Counter, defaultdict

from utils.paths import ODDS_OUT

OUT_DIR = Path("05_flashscore/output/analise")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_TXT = OUT_DIR / "auditoria_odds.txt"
OUT_CSV = OUT_DIR / "auditoria_odds_resumo.csv"


MERCADOS_ALVO = [
    "1x2_ft",
    "1x2_ht",
    "ou_ft",
    "ou_ht",
    "btts_ft",
    "dc_ft",
    "corners_ou",
]


def ler_json(path: Path):
    try:
        texto = path.read_text(encoding="utf-8").strip()
        if not texto:
            return None
        return json.loads(texto)
    except Exception:
        return None


def json_valido_com_data(path: Path) -> tuple[bool, str | None, str | None]:
    parsed = ler_json(path)
    if not parsed:
        return False, None, None

    try:
        data = parsed["data"]["findPrematchOddsForBookmaker"]
        tipo = data.get("type")
        typename = data.get("__typename")
        return True, tipo, typename
    except Exception:
        return False, None, None


def classificar_arquivo(nome_arquivo: str) -> str | None:
    nome = nome_arquivo.lower()
    for mercado in MERCADOS_ALVO:
        if mercado in nome:
            return mercado
    return None


def main():
    if not ODDS_OUT.exists():
        print(f"[ERRO] Pasta não encontrada: {ODDS_OUT}")
        return

    eventos = [p for p in ODDS_OUT.iterdir() if p.is_dir()]
    total_eventos = len(eventos)

    resumo = {m: {
        "arquivos_total": 0,
        "json_valido": 0,
        "json_invalido": 0,
        "eventos_com_mercado": 0,
    } for m in MERCADOS_ALVO}

    eventos_por_mercado = {m: set() for m in MERCADOS_ALVO}
    exemplos_validos = defaultdict(list)
    exemplos_invalidos = defaultdict(list)
    tipos_encontrados = defaultdict(Counter)

    for pasta_evento in eventos:
        arquivos = list(pasta_evento.glob("*.txt"))

        mercados_vistos_no_evento = set()

        for arq in arquivos:
            mercado = classificar_arquivo(arq.name)
            if not mercado:
                continue

            resumo[mercado]["arquivos_total"] += 1

            valido, tipo, typename = json_valido_com_data(arq)
            if valido:
                resumo[mercado]["json_valido"] += 1
                mercados_vistos_no_evento.add(mercado)

                if len(exemplos_validos[mercado]) < 3:
                    exemplos_validos[mercado].append(str(arq))

                if tipo:
                    tipos_encontrados[mercado][tipo] += 1
                if typename:
                    tipos_encontrados[mercado][f"__typename::{typename}"] += 1
            else:
                resumo[mercado]["json_invalido"] += 1
                if len(exemplos_invalidos[mercado]) < 3:
                    exemplos_invalidos[mercado].append(str(arq))

        for mercado in mercados_vistos_no_evento:
            eventos_por_mercado[mercado].add(pasta_evento.name)

    for mercado in MERCADOS_ALVO:
        resumo[mercado]["eventos_com_mercado"] = len(eventos_por_mercado[mercado])

    with open(OUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow([
            "mercado",
            "eventos_com_mercado",
            "eventos_total",
            "pct_eventos_com_mercado",
            "arquivos_total",
            "json_valido",
            "json_invalido",
        ])

        for mercado in MERCADOS_ALVO:
            eventos_com = resumo[mercado]["eventos_com_mercado"]
            pct = round((eventos_com / total_eventos) * 100, 2) if total_eventos else 0.0

            writer.writerow([
                mercado,
                eventos_com,
                total_eventos,
                pct,
                resumo[mercado]["arquivos_total"],
                resumo[mercado]["json_valido"],
                resumo[mercado]["json_invalido"],
            ])

    with open(OUT_TXT, "w", encoding="utf-8") as f:
        f.write("=== AUDITORIA DE ODDS ===\n")
        f.write(f"Total de eventos auditados: {total_eventos}\n\n")

        for mercado in MERCADOS_ALVO:
            eventos_com = resumo[mercado]["eventos_com_mercado"]
            pct = round((eventos_com / total_eventos) * 100, 2) if total_eventos else 0.0

            f.write(f"--- {mercado} ---\n")
            f.write(f"Eventos com mercado : {eventos_com}/{total_eventos} ({pct}%)\n")
            f.write(f"Arquivos totais     : {resumo[mercado]['arquivos_total']}\n")
            f.write(f"JSON válidos        : {resumo[mercado]['json_valido']}\n")
            f.write(f"JSON inválidos      : {resumo[mercado]['json_invalido']}\n")

            if tipos_encontrados[mercado]:
                f.write("Tipos encontrados   :\n")
                for tipo, qtd in tipos_encontrados[mercado].most_common():
                    f.write(f"  - {tipo}: {qtd}\n")

            if exemplos_validos[mercado]:
                f.write("Exemplos válidos    :\n")
                for ex in exemplos_validos[mercado]:
                    f.write(f"  - {ex}\n")

            if exemplos_invalidos[mercado]:
                f.write("Exemplos inválidos  :\n")
                for ex in exemplos_invalidos[mercado]:
                    f.write(f"  - {ex}\n")

            f.write("\n")

    print("[OK] Auditoria concluída.")
    print(f"[OK] TXT: {OUT_TXT}")
    print(f"[OK] CSV: {OUT_CSV}")


if __name__ == "__main__":
    main()