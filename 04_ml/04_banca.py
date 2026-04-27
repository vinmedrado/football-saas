#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os
import json
import time
import platform
from datetime import date, datetime
from rapidfuzz import fuzz, process

# ==============================
# CONFIG
# ==============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BANCA_DIR = os.path.join(BASE_DIR, "banca")
BANCA_FILE = os.path.join(BANCA_DIR, "banca_estado.json")
HISTORICO_FILE = os.path.join(BANCA_DIR, "historico_apostas.csv")

os.makedirs(BANCA_DIR, exist_ok=True)

KELLY_FRACAO = 0.25
ODD_PADRAO = 1.50
APOSTA_MIN_PCT = 0.005
APOSTA_MAX_PCT = 0.05
ALERTA_DRAWDOWN = 0.20


# ==============================
# MAPA DE MERCADOS → LÓGICA DE RESULTADO
# ==============================
# Cada entrada define como avaliar se a aposta ganhou
# a partir do placar final (home_score, away_score)
# e do placar do 1º tempo (ht_home, ht_away) quando disponível.

MERCADOS_SUPORTADOS = {
    # --- Gols Home Full Time ---
    "G_H_FT": lambda hs, as_, hh, ha: hs >= 1,
    # --- Gols Away Full Time ---
    "G_A_FT": lambda hs, as_, hh, ha: as_ >= 1,
    # --- Gols Home 1º Tempo ---
    "G_H_HT": lambda hs, as_, hh, ha: hh >= 1,
    # --- Gols Away 1º Tempo ---
    "G_A_HT": lambda hs, as_, hh, ha: ha >= 1,
    # --- Over/Under Total FT ---
    "TG_FT": lambda hs, as_, hh, ha: (hs + as_),        # retorna total, avalia no resolver
    # --- Over/Under Total HT ---
    "TG_HT": lambda hs, as_, hh, ha: (hh + ha),
    # --- 1X2 FT ---
    "1X2_FT": lambda hs, as_, hh, ha: (
        "1" if hs > as_ else ("X" if hs == as_ else "2")
    ),
    # --- 1X2 HT ---
    "1X2_HT": lambda hs, as_, hh, ha: (
        "1" if hh > ha else ("X" if hh == ha else "2")
    ),
    # --- BTTS FT ---
    "BTTS_FT": lambda hs, as_, hh, ha: (hs >= 1 and as_ >= 1),
    # --- BTTS HT ---
    "BTTS_HT": lambda hs, as_, hh, ha: (hh >= 1 and ha >= 1),
}


def resolver_resultado_mercado(market, event, home_score, away_score, ht_home=0, ht_away=0):
    """
    Dado o mercado, o evento apostado e os placares,
    retorna True (ganhou), False (perdeu) ou None (não suportado).
    """
    market = str(market).strip().upper()
    event = str(event).strip()

    fn = MERCADOS_SUPORTADOS.get(market)
    if fn is None:
        return None  # mercado não mapeado

    resultado = fn(home_score, away_score, ht_home, ht_away)

    # Mercados que retornam valor numérico (TG_FT, TG_HT)
    if market in ("TG_FT", "TG_HT"):
        # event deve ser algo como "Over_2.5" ou "Under_1.5"
        try:
            partes = event.lower().split("_")
            direcao = partes[0]   # "over" ou "under"
            linha = float(partes[-1])
            if direcao == "over":
                return resultado > linha
            elif direcao == "under":
                return resultado < linha
        except Exception:
            return None

    # Mercados 1X2
    if market in ("1X2_FT", "1X2_HT"):
        # event deve ser "1", "X" ou "2"
        return resultado == str(event).strip()

    # Mercados booleanos diretos
    return bool(resultado)


# ==============================
# FUNÇÕES UTILITÁRIAS
# ==============================
def valor_ou_padrao(valor, padrao=0.0):
    if valor is None:
        return padrao
    if isinstance(valor, str):
        valor = valor.strip()
        if valor == "":
            return padrao
    try:
        if pd.isna(valor):
            return padrao
    except Exception:
        pass
    try:
        return float(valor)
    except Exception:
        return padrao


def texto_ou_padrao(valor, padrao=""):
    if valor is None:
        return padrao
    try:
        if pd.isna(valor):
            return padrao
    except Exception:
        pass
    valor = str(valor).strip()
    return valor if valor else padrao


def caminho_previsoes_do_dia():
    return os.path.join(BASE_DIR, f"previsoes_{date.today()}.csv")


def aposta_ja_registrada(jogo, market, event, data_ref=None):
    historico = carregar_historico()
    if historico.empty:
        return False
    data_ref = data_ref or str(date.today())
    filtro = (
        historico["data"].astype(str).eq(str(data_ref))
        & historico["jogo"].astype(str).eq(str(jogo))
        & historico["market"].astype(str).eq(str(market))
        & historico["event"].astype(str).eq(str(event))
        & historico["resultado"].astype(str).eq("pendente")
    )
    return filtro.any()


# ==============================
# FUNÇÕES DE BANCA
# ==============================
def carregar_estado():
    if os.path.exists(BANCA_FILE):
        with open(BANCA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def salvar_estado(estado):
    with open(BANCA_FILE, "w", encoding="utf-8") as f:
        json.dump(estado, f, indent=2, ensure_ascii=False)


def inicializar_banca(banca_inicial):
    estado = {
        "banca_inicial": banca_inicial,
        "banca_atual": banca_inicial,
        "banca_pico": banca_inicial,
        "total_apostas": 0,
        "total_ganhos": 0,
        "total_perdas": 0,
        "lucro_total": 0.0,
        "roi_total": 0.0,
        "data_inicio": str(date.today()),
        "ultima_atualizacao": str(date.today()),
    }
    salvar_estado(estado)
    return estado


def calcular_kelly(prob, odd, fracao=KELLY_FRACAO):
    if odd <= 1:
        return 0.0
    kelly = (prob * odd - 1) / (odd - 1)
    if kelly <= 0:
        return 0.0
    return kelly * fracao


def calcular_aposta(banca_atual, prob, odd, fracao=KELLY_FRACAO):
    kelly_pct = calcular_kelly(prob, odd, fracao)
    if kelly_pct <= 0:
        return 0.0, 0.0
    kelly_pct = max(APOSTA_MIN_PCT, min(APOSTA_MAX_PCT, kelly_pct))
    valor = banca_atual * kelly_pct
    return round(valor, 2), round(kelly_pct * 100, 2)


def carregar_historico():
    """Carrega o histórico de apostas com tratamento de CSV corrompido."""
    if os.path.exists(HISTORICO_FILE):
        # Detecta separador automaticamente
        with open(HISTORICO_FILE, 'r', encoding='utf-8') as f:
            primeira_linha = f.readline()
        sep = ';' if ';' in primeira_linha else ','

        df = pd.read_csv(HISTORICO_FILE, sep=sep)

        # Remove linhas onde 'data' é literalmente o header duplicado
        df = df[df['data'] != 'data'].reset_index(drop=True)

        # Remove colunas duplicadas
        df = df.loc[:, ~df.columns.duplicated()]

        # Remove linhas completamente vazias
        df = df.dropna(how='all').reset_index(drop=True)

        for col in [
            "data", "jogo", "liga", "market", "event",
            "prob_modelo", "confianca", "odd", "valor_apostado",
            "kelly_pct", "roi_bt", "resultado", "lucro", "banca_apos"
        ]:
            if col not in df.columns:
                df[col] = np.nan

        return df

    return pd.DataFrame(columns=[
        "data", "jogo", "liga", "market", "event",
        "prob_modelo", "confianca", "odd", "valor_apostado",
        "kelly_pct", "roi_bt", "resultado", "lucro", "banca_apos"
    ])


def salvar_historico(df):
    """Salva o histórico sempre limpo, sem duplicatas de header."""
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.dropna(how='all').reset_index(drop=True)
    df.to_csv(HISTORICO_FILE, index=False, sep=',', encoding='utf-8')


def registrar_aposta(jogo, liga, market, event, prob, confianca, odd, valor, kelly_pct, roi_bt):
    historico = carregar_historico()
    estado = carregar_estado()

    if aposta_ja_registrada(jogo, market, event):
        print(f"  ⚠️  Aposta já registrada hoje: {jogo} | {market} | {event}")
        return False

    nova = {
        "data": str(date.today()),
        "jogo": jogo,
        "liga": liga,
        "market": market,
        "event": event,
        "prob_modelo": prob,
        "confianca": confianca,
        "odd": odd,
        "valor_apostado": valor,
        "kelly_pct": kelly_pct,
        "roi_bt": roi_bt,
        "resultado": "pendente",
        "lucro": 0.0,
        "banca_apos": estado["banca_atual"],
    }

    historico = pd.concat([historico, pd.DataFrame([nova])], ignore_index=True)
    salvar_historico(historico)
    print(f"  ✅ Aposta registrada: {jogo} | {market} | R$ {valor:.2f}")
    return True


def atualizar_resultado(idx, ganhou):
    historico = carregar_historico()
    estado = carregar_estado()

    if idx >= len(historico):
        print(f"❌ Índice {idx} não encontrado no histórico.")
        return

    aposta = historico.iloc[idx]

    if aposta["resultado"] != "pendente":
        print(f"⚠️  Aposta {idx} já foi atualizada: {aposta['resultado']}")
        return

    try:
        valor = float(aposta["valor_apostado"])
        odd = float(aposta["odd"])
    except Exception:
        print(f"❌ Erro nos dados da aposta {idx}")
        return

    if ganhou:
        lucro = round(valor * (odd - 1), 2)
        resultado = "ganhou"
        estado["total_ganhos"] += 1
    else:
        lucro = round(-valor, 2)
        resultado = "perdeu"
        estado["total_perdas"] += 1

    estado["banca_atual"] = round(estado["banca_atual"] + lucro, 2)
    estado["lucro_total"] = round(estado["lucro_total"] + lucro, 2)
    estado["total_apostas"] += 1
    estado["ultima_atualizacao"] = str(date.today())

    if estado["banca_atual"] > estado["banca_pico"]:
        estado["banca_pico"] = estado["banca_atual"]

    historico.at[idx, "resultado"] = resultado
    historico.at[idx, "lucro"] = lucro
    historico.at[idx, "banca_apos"] = estado["banca_atual"]

    salvar_historico(historico)

    historico_finalizado = historico[historico["resultado"] != "pendente"]
    total_stake = historico_finalizado["valor_apostado"].sum()
    estado["roi_total"] = round(
        (estado["lucro_total"] / total_stake) * 100, 2
    ) if total_stake > 0 else 0

    salvar_estado(estado)

    print(
        f"  {'✅' if ganhou else '❌'} "
        f"[{aposta['market']}] {aposta['jogo']} → {resultado.upper()} | "
        f"Lucro: R$ {lucro:+.2f} | Banca: R$ {estado['banca_atual']:.2f}"
    )

    drawdown = (
        (estado["banca_pico"] - estado["banca_atual"]) / estado["banca_pico"]
    ) if estado["banca_pico"] > 0 else 0

    if drawdown >= ALERTA_DRAWDOWN:
        print(f"\n  🚨 ALERTA: Drawdown de {drawdown:.1%} — considere reduzir o risco!")


# ==============================
# FLASHSCORE — BUSCA DE RESULTADO
# ==============================

def _setup_driver_resultado():
    """Sobe um driver Chrome headless para buscar resultados."""
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options

    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    sistema = platform.system()
    try:
        service = Service('./chromedriver.exe' if sistema == 'Windows' else './chromedriver')
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except Exception:
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            print(f"  ❌ Não foi possível iniciar o Chrome: {e}")
            return None

    for attempt in range(3):
        try:
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            break
        except Exception:
            time.sleep(1)

    return driver


def _aceitar_cookies(driver):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    try:
        btn = WebDriverWait(driver, 4).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        btn.click()
        time.sleep(0.5)
    except Exception:
        pass


def buscar_resultado_flashscore(nome_jogo, data_jogo_str, driver=None):
    """
    Busca o resultado de um jogo no FlashScore pelo nome e data.
    Retorna dict com home_score, away_score, ht_home, ht_away
    ou None se não encontrar.

    data_jogo_str: formato "DD/MM/YYYY" ou "YYYY-MM-DD"
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from bs4 import BeautifulSoup

    fechar_driver = False

    if driver is None:
        driver = _setup_driver_resultado()
        fechar_driver = True
        if driver is None:
            return None

    # Normaliza data para DD.MM.YYYY (formato do FlashScore)
    try:
        if "/" in str(data_jogo_str):
            partes = str(data_jogo_str).split("/")
            if len(partes[2]) == 4:  # DD/MM/YYYY
                data_fs = f"{partes[0]}.{partes[1]}.{partes[2]}"
            else:  # YYYY/MM/DD improvável mas cobre
                data_fs = f"{partes[2]}.{partes[1]}.{partes[0]}"
        elif "-" in str(data_jogo_str):
            partes = str(data_jogo_str).split("-")
            data_fs = f"{partes[2]}.{partes[1]}.{partes[0]}"
        else:
            data_fs = str(data_jogo_str)
    except Exception:
        data_fs = str(data_jogo_str)

    # Extrai os dois times do nome do jogo
    try:
        if " vs " in nome_jogo:
            home_name, away_name = nome_jogo.split(" vs ", 1)
        elif " x " in nome_jogo.lower():
            home_name, away_name = nome_jogo.lower().split(" x ", 1)
        else:
            home_name = nome_jogo
            away_name = ""
    except Exception:
        home_name = nome_jogo
        away_name = ""

    home_name = home_name.strip()
    away_name = away_name.strip()

    # Monta URL de busca do FlashScore
    query = home_name.replace(" ", "+")
    url = f"https://www.flashscore.com/search/?q={query}"

    try:
        driver.get(url)
        _aceitar_cookies(driver)
        time.sleep(2)

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Busca nos resultados de jogo
        resultados = soup.select("div.sportName.soccer div.event__match")

        melhor_match = None
        melhor_score = 0

        for el in resultados:
            try:
                # Data do jogo no elemento
                data_el = el.select_one("div.event__time")
                if data_el:
                    data_texto = data_el.text.strip()
                    # Verifica se bate com a data buscada (DD.MM. ou DD.MM.YYYY)
                    data_curta = ".".join(data_fs.split(".")[:2]) + "."
                    if data_curta not in data_texto and data_fs not in data_texto:
                        continue

                # Times
                home_el = el.select_one("div.event__homeParticipant span.wcl-overline")
                away_el = el.select_one("div.event__awayParticipant span.wcl-overline")

                if not home_el:
                    home_el = el.select_one("div.event__participant--home")
                if not away_el:
                    away_el = el.select_one("div.event__participant--away")

                if not home_el or not away_el:
                    continue

                home_texto = home_el.text.strip()
                away_texto = away_el.text.strip()

                # Similaridade com os nomes buscados
                score_home = fuzz.partial_ratio(home_name.lower(), home_texto.lower())
                score_away = fuzz.partial_ratio(away_name.lower(), away_texto.lower()) if away_name else 50
                score_total = (score_home + score_away) / 2

                if score_total > melhor_score and score_total >= 60:
                    melhor_score = score_total

                    # Tenta pegar o ID do jogo para ir à página de detalhes
                    mid = el.get_attribute("id") if hasattr(el, 'get_attribute') else ""
                    if not mid:
                        link = el.select_one("a")
                        mid = link.get("href", "") if link else ""

                    melhor_match = {
                        "home": home_texto,
                        "away": away_texto,
                        "score_similaridade": score_total,
                        "elemento": el,
                    }
            except Exception:
                continue

        if melhor_match is None:
            # Tenta URL de resultados por data
            return _buscar_por_data_flashscore(driver, home_name, away_name, data_fs)

        # Extrai placar do elemento encontrado
        el = melhor_match["elemento"]
        return _extrair_placar_do_elemento(driver, el, home_name, away_name)

    except Exception as e:
        print(f"  ⚠️  Erro na busca FlashScore: {e}")
        return None
    finally:
        if fechar_driver and driver:
            driver.quit()


def _buscar_por_data_flashscore(driver, home_name, away_name, data_fs):
    """Busca por resultados do dia na página de resultados do FlashScore."""
    from bs4 import BeautifulSoup

    try:
        # data_fs no formato DD.MM.YYYY → converte para YYYYMMDD para URL
        partes = data_fs.split(".")
        data_url = f"{partes[2]}{partes[1]}{partes[0]}"
        url = f"https://www.flashscore.com/football/?d={data_url}"
        driver.get(url)
        time.sleep(2)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        elementos = soup.select("div.event__match")

        melhor = None
        melhor_score = 0

        for el in elementos:
            try:
                home_el = el.select_one("div.event__participant--home")
                away_el = el.select_one("div.event__participant--away")
                if not home_el or not away_el:
                    continue

                home_texto = home_el.text.strip()
                away_texto = away_el.text.strip()

                score_h = fuzz.partial_ratio(home_name.lower(), home_texto.lower())
                score_a = fuzz.partial_ratio(away_name.lower(), away_texto.lower()) if away_name else 50
                score = (score_h + score_a) / 2

                if score > melhor_score and score >= 55:
                    melhor_score = score
                    melhor = el
            except Exception:
                continue

        if melhor is None:
            return None

        return _extrair_placar_do_elemento(driver, melhor, home_name, away_name)

    except Exception as e:
        print(f"  ⚠️  Erro busca por data: {e}")
        return None


def _extrair_placar_do_elemento(driver, el, home_name, away_name):
    """Extrai placar FT e HT de um elemento de jogo do FlashScore."""
    try:
        # Placar FT
        score_home_el = el.select_one("div.event__score--home")
        score_away_el = el.select_one("div.event__score--away")

        if not score_home_el or not score_away_el:
            return None

        home_score = int(score_home_el.text.strip())
        away_score = int(score_away_el.text.strip())

        # Placar HT (nem sempre disponível no elemento de lista)
        ht_home = 0
        ht_away = 0

        ht_el = el.select_one("div.event__part")
        if ht_el:
            ht_texto = ht_el.text.strip()  # ex: "(1:0)"
            ht_texto = ht_texto.replace("(", "").replace(")", "")
            if ":" in ht_texto:
                partes = ht_texto.split(":")
                ht_home = int(partes[0].strip())
                ht_away = int(partes[1].strip())

        return {
            "home_score": home_score,
            "away_score": away_score,
            "ht_home": ht_home,
            "ht_away": ht_away,
            "home_nome": home_name,
            "away_nome": away_name,
        }

    except Exception as e:
        print(f"  ⚠️  Erro ao extrair placar: {e}")
        return None


# ==============================
# AUTO-RESULTADO
# ==============================

def menu_auto_resultado():
    """
    Busca automaticamente resultados de apostas pendentes no FlashScore.
    Mostra o placar encontrado e pede confirmação antes de salvar.
    """
    historico = carregar_historico()
    pendentes = historico[historico["resultado"] == "pendente"].copy()

    if len(pendentes) == 0:
        print("\n  ✅ Nenhuma aposta pendente encontrada.")
        return

    print("\n" + "=" * 60)
    print("🔍 AUTO-RESULTADO VIA FLASHSCORE")
    print("=" * 60)
    print(f"  {len(pendentes)} aposta(s) pendente(s) encontrada(s).\n")

    # Filtra apenas apostas de jogos que já aconteceram
    hoje = date.today()
    apostas_para_buscar = []

    for idx, row in pendentes.iterrows():
        data_aposta_str = texto_ou_padrao(row["data"])
        try:
            if "/" in data_aposta_str:
                partes = data_aposta_str.split("/")
                data_aposta = date(int(partes[2]), int(partes[1]), int(partes[0]))
            elif "-" in data_aposta_str:
                data_aposta = date.fromisoformat(data_aposta_str)
            else:
                data_aposta = hoje
        except Exception:
            data_aposta = hoje

        if data_aposta <= hoje:
            apostas_para_buscar.append((idx, row, data_aposta_str))

    if not apostas_para_buscar:
        print("  ⏳ Todas as apostas pendentes são de jogos futuros.")
        return

    print(f"  Buscando resultados de {len(apostas_para_buscar)} aposta(s)...\n")

    driver = _setup_driver_resultado()
    if driver is None:
        print("  ❌ Não foi possível iniciar o navegador.")
        return

    try:
        _aceitar_cookies(driver)
        resultados_encontrados = []

        for idx, row, data_str in apostas_para_buscar:
            jogo = texto_ou_padrao(row["jogo"])
            market = texto_ou_padrao(row["market"])
            event = texto_ou_padrao(row["event"])

            print(f"  🔎 Buscando: {jogo} ({data_str}) | {market} {event}")

            resultado_fs = buscar_resultado_flashscore(jogo, data_str, driver=driver)

            if resultado_fs is None:
                print(f"     ⚠️  Não encontrado no FlashScore.\n")
                resultados_encontrados.append((idx, row, None))
                continue

            hs = resultado_fs["home_score"]
            as_ = resultado_fs["away_score"]
            hh = resultado_fs["ht_home"]
            ha = resultado_fs["ht_away"]

            ganhou = resolver_resultado_mercado(market, event, hs, as_, hh, ha)

            print(f"     ✅ Placar encontrado: {hs}x{as_} (HT: {hh}x{ha})")

            if ganhou is None:
                print(f"     ⚠️  Mercado '{market}' não reconhecido automaticamente.\n")
                resultados_encontrados.append((idx, row, resultado_fs))
            else:
                status = "🟢 GANHOU" if ganhou else "🔴 PERDEU"
                print(f"     {status}\n")
                resultados_encontrados.append((idx, row, resultado_fs, ganhou))

        driver.quit()

        # Resumo e confirmação
        if not resultados_encontrados:
            print("  ⚠️  Nenhum resultado encontrado.")
            return

        print("\n" + "=" * 60)
        print("📋 RESUMO — Confirme os resultados antes de salvar")
        print("=" * 60)

        para_confirmar = []

        for item in resultados_encontrados:
            idx = item[0]
            row = item[1]
            fs = item[2]

            if fs is None:
                print(f"  [{idx}] {row['jogo']} | {row['market']} → ❓ Não encontrado (pular)")
                continue

            if len(item) == 3:
                # Mercado não reconhecido — pergunta manualmente
                print(f"  [{idx}] {row['jogo']} | {row['market']} {row['event']}")
                print(f"       Placar: {fs['home_score']}x{fs['away_score']} (HT: {fs['ht_home']}x{fs['ht_away']})")
                resp = input("       Ganhou? (g=sim / p=não / s=pular): ").strip().lower()
                if resp == "g":
                    para_confirmar.append((idx, True))
                elif resp == "p":
                    para_confirmar.append((idx, False))
                continue

            ganhou = item[3]
            status = "🟢 GANHOU" if ganhou else "🔴 PERDEU"
            print(f"  [{idx}] {row['jogo']} | {row['market']} {row['event']} → {status}")
            print(f"       Placar: {fs['home_score']}x{fs['away_score']} (HT: {fs['ht_home']}x{fs['ht_away']})")
            para_confirmar.append((idx, ganhou))

        if not para_confirmar:
            print("\n  ⚠️  Nada para confirmar.")
            return

        print(f"\n  Total a atualizar: {len(para_confirmar)} aposta(s)")
        confirmar = input("  Confirmar e salvar todos? (s=sim / n=cancelar / m=um a um): ").strip().lower()

        if confirmar == "n":
            print("  ↩️  Cancelado.")
            return

        if confirmar == "s":
            for idx, ganhou in para_confirmar:
                atualizar_resultado(idx, ganhou)

        elif confirmar == "m":
            for idx, ganhou in para_confirmar:
                historico = carregar_historico()
                aposta = historico.iloc[idx]
                status = "🟢 GANHOU" if ganhou else "🔴 PERDEU"
                print(f"\n  [{idx}] {aposta['jogo']} | {aposta['market']} → {status}")
                resp = input("  Confirmar? (s=sim / n=não / i=inverter): ").strip().lower()
                if resp == "s":
                    atualizar_resultado(idx, ganhou)
                elif resp == "i":
                    atualizar_resultado(idx, not ganhou)
                else:
                    print("  ⏭️  Pulado.")

        print("\n  ✅ Auto-resultado concluído!")

    except Exception as e:
        print(f"\n  ❌ Erro durante auto-resultado: {e}")
        if driver:
            driver.quit()


# ==============================
# DASHBOARD
# ==============================
def mostrar_dashboard():
    estado = carregar_estado()
    historico = carregar_historico()

    print("\n" + "=" * 55)
    print("💰 PAINEL DA BANCA")
    print("=" * 55)

    print(f"\n  📅 Início          : {estado['data_inicio']}")
    print(f"  📅 Última atualiz. : {estado['ultima_atualizacao']}")
    print(f"\n  💵 Banca inicial   : R$ {estado['banca_inicial']:.2f}")
    print(f"  💵 Banca atual     : R$ {estado['banca_atual']:.2f}")
    print(f"  📈 Banca no pico   : R$ {estado['banca_pico']:.2f}")

    variacao = ((estado["banca_atual"] / estado["banca_inicial"]) - 1) * 100
    print(f"  📊 Variação total  : {variacao:+.2f}%")

    drawdown = (estado["banca_pico"] - estado["banca_atual"]) / estado["banca_pico"] * 100
    print(f"  📉 Drawdown atual  : {drawdown:.2f}%")

    print(f"\n  🎯 Total apostas   : {estado['total_apostas']}")
    print(f"  ✅ Ganhos          : {estado['total_ganhos']}")
    print(f"  ❌ Perdas          : {estado['total_perdas']}")

    if estado["total_apostas"] > 0:
        winrate = estado["total_ganhos"] / estado["total_apostas"] * 100
        print(f"  🎯 Winrate         : {winrate:.1f}%")
        print(f"  📊 ROI geral       : {estado['roi_total']:+.2f}%")
        print(f"  💰 Lucro total     : R$ {estado['lucro_total']:+.2f}")

    if len(historico) > 0:
        pendentes = historico[historico["resultado"] == "pendente"]
        if len(pendentes) > 0:
            print(f"\n  ⏳ Apostas pendentes ({len(pendentes)}):")
            for idx, row in pendentes.iterrows():
                print(
                    f"     [{idx}] {row['jogo']} | {row['market']} | "
                    f"R$ {float(row['valor_apostado']):.2f} | {row['data']}"
                )

    if len(historico) > 0:
        finalizadas = historico[historico["resultado"] != "pendente"]
        if len(finalizadas) > 0:
            print(f"\n  📊 Performance por mercado:")
            for market, grp in finalizadas.groupby("market"):
                ganhos = (grp["resultado"] == "ganhou").sum()
                lucro = grp["lucro"].sum()
                wr = ganhos / len(grp) * 100
                print(f"     {market:<12} | {len(grp):>3} apostas | WR={wr:.0f}% | Lucro=R$ {lucro:+.2f}")

    print("=" * 55)


# ==============================
# PREVISÕES DO DIA
# ==============================
def preparar_previsoes(previsoes_path):
    estado = carregar_estado()

    if not os.path.exists(previsoes_path):
        print(f"❌ Arquivo de previsões não encontrado: {previsoes_path}")
        return None

    df = pd.read_csv(previsoes_path).copy()

    if df.empty:
        print("⚠️  Arquivo de previsões está vazio.")
        return None

    for col in ["jogo", "liga", "market", "event"]:
        if col not in df.columns:
            df[col] = ""

    for col in ["prob_sim", "confianca", "roi_bt", "odd_real"]:
        if col not in df.columns:
            df[col] = np.nan

    df["prob_sim"] = df["prob_sim"].apply(lambda x: valor_ou_padrao(x, 0.0))
    df["confianca"] = df["confianca"].apply(lambda x: valor_ou_padrao(x, 0.0))
    df["roi_bt"] = df["roi_bt"].apply(lambda x: valor_ou_padrao(x, 0.0))
    df["odd_usada"] = df["odd_real"].apply(lambda x: valor_ou_padrao(x, ODD_PADRAO))
    df.loc[df["odd_usada"] <= 0, "odd_usada"] = ODD_PADRAO

    df["valor_aposta"] = 0.0
    df["kelly_pct"] = 0.0
    df["status"] = "sem_valor"

    valores, percentuais, status = [], [], []

    for _, row in df.iterrows():
        valor, kelly_pct = calcular_aposta(
            estado["banca_atual"],
            row["confianca"],
            row["odd_usada"]
        )
        valores.append(valor)
        percentuais.append(kelly_pct)

        if valor <= 0:
            status.append("sem_valor")
        elif aposta_ja_registrada(
            texto_ou_padrao(row["jogo"]),
            texto_ou_padrao(row["market"]),
            texto_ou_padrao(row["event"])
        ):
            status.append("ja_registrada")
        else:
            status.append("disponivel")

    df["valor_aposta"] = valores
    df["kelly_pct"] = percentuais
    df["status"] = status

    return df


def calcular_apostas_do_dia(previsoes_path, mostrar_tela=True):
    estado = carregar_estado()
    df = preparar_previsoes(previsoes_path)

    if df is None:
        return None

    if mostrar_tela:
        print("\n" + "=" * 55)
        print(f"💡 APOSTAS RECOMENDADAS — {date.today()}")
        print(f"   Banca atual: R$ {estado['banca_atual']:.2f} | Kelly: {int(KELLY_FRACAO*100)}%")
        print("=" * 55)

        total_valor = 0.0

        for idx, row in df.iterrows():
            prob = valor_ou_padrao(row.get("prob_sim"), 0.0)
            confianca = valor_ou_padrao(row.get("confianca"), 0.0)
            market = texto_ou_padrao(row.get("market"))
            event = texto_ou_padrao(row.get("event"))
            jogo = texto_ou_padrao(row.get("jogo"))
            liga = texto_ou_padrao(row.get("liga"))
            roi_bt = valor_ou_padrao(row.get("roi_bt"), 0.0)
            odd = valor_ou_padrao(row.get("odd_usada"), ODD_PADRAO)
            valor = valor_ou_padrao(row.get("valor_aposta"), 0.0)
            kelly_pct = valor_ou_padrao(row.get("kelly_pct"), 0.0)
            status = texto_ou_padrao(row.get("status"))

            print(f"\n  [{idx}] ⚽ {jogo}")
            print(f"      Liga      : {liga}")
            print(f"      Mercado   : {market} ({event})")
            print(f"      Confiança : {confianca:.1%} | Prob: {prob:.1%} | ROI backtest: {roi_bt:+.1%}")
            print(f"      Odd usada : {odd:.2f}")

            if status == "sem_valor":
                print("      Status    : sem valor esperado positivo")
                continue
            if status == "ja_registrada":
                print("      Status    : já registrada no histórico")
                continue

            total_valor += valor
            print(f"      Kelly     : {kelly_pct:.2f}% da banca")
            print(f"      💰 APOSTAR: R$ {valor:.2f}")

        print(f"\n  {'=' * 50}")
        print(f"  💰 Total a apostar hoje : R$ {total_valor:.2f}")
        print(f"  📊 % da banca           : {(total_valor / estado['banca_atual'] * 100) if estado['banca_atual'] > 0 else 0:.1f}%")

        if estado["banca_atual"] > 0 and total_valor / estado["banca_atual"] > 0.20:
            print("  🚨 ATENÇÃO: Apostas representam mais de 20% da banca — considere reduzir!")

        print("=" * 55)

    return df


def registrar_apostas_automaticas(previsoes_path, selecao="todas"):
    df = calcular_apostas_do_dia(previsoes_path, mostrar_tela=False)

    if df is None or df.empty:
        return

    registradas = 0
    ignoradas = 0
    indices = df.index.tolist() if selecao == "todas" else selecao

    print("\n" + "=" * 55)
    print("🤖 PREENCHIMENTO AUTOMÁTICO DAS APOSTAS")
    print("=" * 55)

    for idx in indices:
        if idx not in df.index:
            print(f"  ⚠️  Índice inválido: {idx}")
            ignoradas += 1
            continue

        row = df.loc[idx]

        if texto_ou_padrao(row["status"]) != "disponivel":
            print(f"  ⏭️  [{idx}] Ignorada — status: {row['status']}")
            ignoradas += 1
            continue

        sucesso = registrar_aposta(
            jogo=texto_ou_padrao(row["jogo"]),
            liga=texto_ou_padrao(row["liga"]),
            market=texto_ou_padrao(row["market"]),
            event=texto_ou_padrao(row["event"]),
            prob=valor_ou_padrao(row["prob_sim"], 0.0),
            confianca=valor_ou_padrao(row["confianca"], 0.0),
            odd=valor_ou_padrao(row["odd_usada"], ODD_PADRAO),
            valor=valor_ou_padrao(row["valor_aposta"], 0.0),
            kelly_pct=valor_ou_padrao(row["kelly_pct"], 0.0),
            roi_bt=valor_ou_padrao(row["roi_bt"], 0.0),
        )

        if sucesso:
            registradas += 1
        else:
            ignoradas += 1

    print("\n" + "=" * 55)
    print(f"✅ Registradas : {registradas}")
    print(f"⏭️  Ignoradas   : {ignoradas}")
    print("=" * 55)


def menu_apostas_do_dia(estado):
    previsoes_path = caminho_previsoes_do_dia()
    df = calcular_apostas_do_dia(previsoes_path, mostrar_tela=True)

    if df is None or df.empty:
        return

    print("\n  O que deseja fazer?")
    print("  1. Só visualizar")
    print("  2. Preencher automaticamente todas as disponíveis")
    print("  3. Preencher automaticamente escolhendo os índices")
    print("  4. Voltar")

    escolha = input("  Escolha: ").strip()

    if escolha == "1":
        return

    if escolha == "2":
        confirmar = input("  Confirmar registro automático de todas as disponíveis? (s/n): ").strip().lower()
        if confirmar == "s":
            registrar_apostas_automaticas(previsoes_path, selecao="todas")
        return

    if escolha == "3":
        entrada = input("  Digite os índices separados por vírgula (ex: 0,2,5): ").strip()
        if not entrada:
            print("  ⚠️  Nenhum índice informado.")
            return
        try:
            indices = [int(x.strip()) for x in entrada.split(",") if x.strip() != ""]
        except ValueError:
            print("  ❌ Índices inválidos.")
            return
        confirmar = input("  Confirmar registro automático dos índices informados? (s/n): ").strip().lower()
        if confirmar == "s":
            registrar_apostas_automaticas(previsoes_path, selecao=indices)
        return

    if escolha == "4":
        return

    print("  ⚠️  Opção inválida.")


# ==============================
# PREVISÕES ANTERIORES
# ==============================
def listar_previsoes_anteriores():
    arquivos = []
    for f in os.listdir(BASE_DIR):
        if f.startswith("previsoes_") and f.endswith(".csv"):
            data_str = f.replace("previsoes_", "").replace(".csv", "")
            if data_str != str(date.today()):
                arquivos.append((data_str, os.path.join(BASE_DIR, f)))
    return sorted(arquivos, reverse=True)


def registrar_apostas_dia_anterior(path, data_str):
    """
    Registra apostas de um arquivo de previsões anterior no histórico,
    usando a data do arquivo (não a data de hoje).
    """
    estado = carregar_estado()

    if not os.path.exists(path):
        print(f"❌ Arquivo não encontrado: {path}")
        return 0

    df = pd.read_csv(path).copy()

    if df.empty:
        print("⚠️  Arquivo vazio.")
        return 0

    for col in ["jogo", "liga", "market", "event"]:
        if col not in df.columns:
            df[col] = ""
    for col in ["prob_sim", "confianca", "roi_bt", "odd_real"]:
        if col not in df.columns:
            df[col] = np.nan

    df["prob_sim"]  = df["prob_sim"].apply(lambda x: valor_ou_padrao(x, 0.0))
    df["confianca"] = df["confianca"].apply(lambda x: valor_ou_padrao(x, 0.0))
    df["roi_bt"]    = df["roi_bt"].apply(lambda x: valor_ou_padrao(x, 0.0))
    df["odd_usada"] = df["odd_real"].apply(lambda x: valor_ou_padrao(x, ODD_PADRAO))
    df.loc[df["odd_usada"] <= 0, "odd_usada"] = ODD_PADRAO

    # Converte data do arquivo "YYYY-MM-DD" → "DD/MM/YYYY"
    try:
        partes = data_str.split("-")
        data_registro = f"{partes[2]}/{partes[1]}/{partes[0]}"
    except Exception:
        data_registro = data_str

    historico = carregar_historico()
    registradas = 0
    ignoradas = 0

    print(f"\n  Registrando apostas de {data_str}...\n")

    for _, row in df.iterrows():
        jogo      = texto_ou_padrao(row["jogo"])
        liga      = texto_ou_padrao(row["liga"])
        market    = texto_ou_padrao(row["market"])
        event     = texto_ou_padrao(row["event"])
        prob      = valor_ou_padrao(row["prob_sim"], 0.0)
        confianca = valor_ou_padrao(row["confianca"], 0.0)
        odd       = valor_ou_padrao(row["odd_usada"], ODD_PADRAO)
        roi_bt    = valor_ou_padrao(row["roi_bt"], 0.0)

        valor, kelly_pct = calcular_aposta(estado["banca_atual"], confianca, odd)

        if valor <= 0:
            ignoradas += 1
            continue

        # Verifica duplicata usando a data real do arquivo
        filtro = (
            historico["data"].astype(str).eq(data_registro)
            & historico["jogo"].astype(str).eq(jogo)
            & historico["market"].astype(str).eq(market)
            & historico["event"].astype(str).eq(event)
        )
        if filtro.any():
            print(f"  ⚠️  Já registrada: {jogo} | {market}")
            ignoradas += 1
            continue

        nova = {
            "data": data_registro,
            "jogo": jogo,
            "liga": liga,
            "market": market,
            "event": event,
            "prob_modelo": prob,
            "confianca": confianca,
            "odd": odd,
            "valor_apostado": valor,
            "kelly_pct": kelly_pct,
            "roi_bt": roi_bt,
            "resultado": "pendente",
            "lucro": 0.0,
            "banca_apos": estado["banca_atual"],
        }

        historico = pd.concat([historico, pd.DataFrame([nova])], ignore_index=True)
        print(f"  ✅ {jogo} | {market} | R$ {valor:.2f}")
        registradas += 1

    salvar_historico(historico)

    print(f"\n  ✅ Registradas : {registradas}")
    print(f"  ⏭️  Ignoradas   : {ignoradas}")
    return registradas


def menu_apostas_anteriores(estado):
    arquivos = listar_previsoes_anteriores()

    if not arquivos:
        print("\n  ⚠️  Nenhum arquivo de previsões anteriores encontrado.")
        return

    print("\n" + "=" * 55)
    print("📂 PREVISÕES ANTERIORES DISPONÍVEIS")
    print("=" * 55)
    for i, (data_str, _) in enumerate(arquivos):
        print(f"  [{i}] {data_str}")
    print("=" * 55)

    try:
        escolha = int(input("  Escolha o índice do dia: ").strip())
        if escolha < 0 or escolha >= len(arquivos):
            print("  ❌ Índice inválido.")
            return
    except ValueError:
        print("  ❌ Entrada inválida.")
        return

    data_str, path = arquivos[escolha]
    df = calcular_apostas_do_dia(path, mostrar_tela=True)

    if df is None or df.empty:
        return

    # Verifica quantas já estão registradas
    historico = carregar_historico()
    try:
        partes = data_str.split("-")
        data_registro = f"{partes[2]}/{partes[1]}/{partes[0]}"
    except Exception:
        data_registro = data_str

    ja_registradas = historico[historico["data"].astype(str).eq(data_registro)]

    print("\n  O que deseja fazer?")
    print("  1. Só visualizar")
    print("  2. Registrar apostas deste dia no histórico")
    print("  3. Atualizar resultado de uma aposta deste dia")
    print("  4. Voltar")

    acao = input("  Escolha: ").strip()

    if acao == "1":
        return

    if acao == "2":
        if len(ja_registradas) > 0:
            print(f"\n  ⚠️  Já existem {len(ja_registradas)} apostas registradas para {data_str}.")
            confirmar = input("  Continuar mesmo assim (registra apenas as que faltam)? (s/n): ").strip().lower()
            if confirmar != "s":
                return

        registradas = registrar_apostas_dia_anterior(path, data_str)

        if registradas > 0:
            print(f"\n  ✅ {registradas} apostas registradas!")
            atualizar = input("  Deseja atualizar os resultados agora? (s/n): ").strip().lower()
            if atualizar == "s":
                _menu_atualizar_pendentes_do_dia(data_str, data_registro)
        return

    if acao == "3":
        _menu_atualizar_pendentes_do_dia(data_str, data_registro)
        return

    if acao == "4":
        return

    print("  ⚠️  Opção inválida.")


def _menu_atualizar_pendentes_do_dia(data_str, data_registro):
    """Submenu para atualizar resultados pendentes de um dia específico."""
    historico = carregar_historico()
    pendentes = historico[
        (historico["resultado"] == "pendente") &
        (historico["data"].astype(str).eq(data_registro))
    ]

    if len(pendentes) == 0:
        print(f"\n  ⚠️  Nenhuma aposta pendente para {data_str}.")
        todas = historico[historico["data"].astype(str).eq(data_registro)]
        if len(todas) > 0:
            print(f"\n  📊 Apostas de {data_str}:")
            for idx, row in todas.iterrows():
                print(
                    f"     [{idx}] {row['jogo']} | {row['market']} | "
                    f"R$ {float(row['valor_apostado']):.2f} | {row['resultado']}"
                )
        return

    print(f"\n  ⏳ Apostas pendentes de {data_str}:")
    for idx, row in pendentes.iterrows():
        print(
            f"     [{idx}] {row['jogo']} | {row['market']} | "
            f"R$ {float(row['valor_apostado']):.2f}"
        )

    print("\n  Como deseja atualizar?")
    print("  1. Um a um manualmente")
    print("  2. Buscar automaticamente no FlashScore")
    print("  3. Voltar")

    modo = input("  Escolha: ").strip()

    if modo == "1":
        try:
            idx = int(input("\n  Índice da aposta: "))
            resultado = input("  Resultado (g=ganhou / p=perdeu): ").strip().lower()
            atualizar_resultado(idx, ganhou=(resultado == "g"))
        except ValueError:
            print("  ❌ Entrada inválida.")

    elif modo == "2":
        menu_auto_resultado()

    elif modo == "3":
        return


# ==============================
# UTILITÁRIO — LIMPAR CSV CORROMPIDO
# ==============================
def limpar_historico_corrompido():
    """Executa uma vez para corrigir histórico com headers duplicados."""
    df = carregar_historico()
    salvar_historico(df)
    print(f"✅ Histórico limpo: {len(df)} apostas mantidas.")


# ==============================
# MENU INTERATIVO
# ==============================
def menu():
    estado = carregar_estado()

    if estado is None:
        print("=" * 55)
        print("🏦 INICIALIZAR BANCA")
        print("=" * 55)
        banca_inicial = float(input("  💵 Qual o valor inicial da sua banca? R$ "))
        estado = inicializar_banca(banca_inicial)
        print(f"  ✅ Banca inicializada com R$ {banca_inicial:.2f}")

    while True:
        estado = carregar_estado()

        print("\n" + "=" * 55)
        print(f"💰 GESTÃO DE BANCA | Banca: R$ {estado['banca_atual']:.2f}")
        print("=" * 55)
        print("  1. Ver previsões do dia / preencher automaticamente")
        print("  2. Registrar aposta manualmente")
        print("  3. Atualizar resultado de aposta")
        print("  4. 🔍 Buscar resultados automaticamente (FlashScore)")
        print("  5. 📂 Ver previsões anteriores")
        print("  6. Ver painel completo")
        print("  7. Sair")
        print("=" * 55)

        opcao = input("  Escolha: ").strip()

        if opcao == "1":
            menu_apostas_do_dia(estado)

        elif opcao == "2":
            print("\n  📝 Registrar aposta manual:")
            jogo = input("  Jogo (ex: Flamengo vs Palmeiras): ").strip()
            liga = input("  Liga: ").strip()
            market = input("  Mercado (ex: G_H_HT): ").strip()
            event = input("  Evento (ex: Gols_H_HT): ").strip()
            prob = float(input("  Probabilidade do modelo (0-1, ex: 0.72): ") or 0)
            confianca = float(input("  Confiança do modelo (0-1, ex: 0.75): ") or prob)
            odd = float(input(f"  Odd da casa (enter para {ODD_PADRAO}): ") or ODD_PADRAO)
            roi_bt = float(input("  ROI do backtest (ex: 0.035): ") or 0)

            valor, kelly_pct = calcular_aposta(estado["banca_atual"], confianca, odd)

            if valor <= 0:
                print("\n  ⚠️  Essa aposta ficou sem valor esperado positivo pelo Kelly.")
                continue

            print(f"\n  💰 Kelly recomenda: R$ {valor:.2f} ({kelly_pct:.2f}% da banca)")
            confirmar = input("  Confirmar aposta? (s/n): ").strip().lower()

            if confirmar == "s":
                registrar_aposta(jogo, liga, market, event, prob, confianca, odd, valor, kelly_pct, roi_bt)

        elif opcao == "3":
            historico = carregar_historico()
            pendentes = historico[historico["resultado"] == "pendente"]

            if len(pendentes) == 0:
                print("  ⚠️  Nenhuma aposta pendente.")
                continue

            print("\n  ⏳ Apostas pendentes:")
            for idx, row in pendentes.iterrows():
                print(
                    f"     [{idx}] {row['jogo']} | {row['market']} | "
                    f"R$ {float(row['valor_apostado']):.2f} | {row['data']}"
                )

            idx = int(input("\n  Índice da aposta: "))
            resultado = input("  Resultado (g=ganhou / p=perdeu): ").strip().lower()
            atualizar_resultado(idx, ganhou=(resultado == "g"))

        elif opcao == "4":
            menu_auto_resultado()

        elif opcao == "5":
            menu_apostas_anteriores(estado)

        elif opcao == "6":
            mostrar_dashboard()

        elif opcao == "7":
            print("\n  👋 Até logo!")
            break

        else:
            print("  ⚠️  Opção inválida.")


# ==============================
# EXECUÇÃO
# ==============================
if __name__ == "__main__":
    menu()