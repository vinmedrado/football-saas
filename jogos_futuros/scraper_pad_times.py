#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import logging
import unicodedata
from urllib.parse import urljoin

import pandas as pd
from rapidfuzz import process, fuzz
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ==========================================================
# CONFIG
# ==========================================================
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

DICT_TIMES = os.path.join(ROOT_DIR, "data", "dicionario_times.csv")
DICT_TIMES_FLASH = os.path.join(ROOT_DIR, "data", "dicionario_times_flash.csv")
DICT_LIGAS_FLASH = os.path.join(ROOT_DIR, "data", "dicionario_ligas_flash.csv")

OUTPUT_NOVOS = os.path.join(ROOT_DIR, "data", "dicionario_times_flash_novos.csv")
OUTPUT_REVISAO = os.path.join(ROOT_DIR, "data", "times_flash_revisao.csv")

BASE_FLASH_URL = "https://www.flashscore.com/football/"

# Regras de score
LIMITE_ADICIONAR_AUTO = 85
LIMITE_REVISAO = 70

HEADLESS = True
TEMPO_ESPERA = 12

# ==========================================================
# LOG
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s - %(message)s",
    datefmt="%H:%M:%S"
)

# ==========================================================
# UTIL
# ==========================================================
def strip_accents(texto: str) -> str:
    texto = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in texto if not unicodedata.combining(c))


def limpar_slug(texto: str) -> str:
    """
    Converte para slug de URL do Flashscore.
    Ex:
      'South Korea' -> 'south-korea'
      'A-League' -> 'a-league'
      'Liga Portugal 2' -> 'liga-portugal-2'
    """
    texto = str(texto).strip().lower()
    texto = strip_accents(texto)
    texto = texto.replace("&", " and ")
    texto = re.sub(r"[^\w\s-]", " ", texto)
    texto = re.sub(r"[_\s]+", "-", texto)
    texto = re.sub(r"-+", "-", texto)
    return texto.strip("-")


def normalizar_nome_time(nome: str) -> str:
    """
    Normalização para comparação de nomes de times.
    """
    if pd.isna(nome):
        return ""

    nome = str(nome).strip().lower()
    nome = strip_accents(nome)

    # remove conteúdo entre parênteses
    nome = re.sub(r"\(.*?\)", "", nome)

    # remove pontuação
    nome = nome.replace(".", " ")
    nome = nome.replace("-", " ")
    nome = nome.replace("/", " ")

    # remove tokens comuns que atrapalham
    tokens_ruins = {
        "fc", "cf", "club", "sc", "ac", "cd", "ud",
        "rj", "sp", "mg", "rs"
    }

    partes = [p for p in nome.split() if p not in tokens_ruins]
    nome = " ".join(partes)

    nome = re.sub(r"\s+", " ", nome).strip()
    return nome


def limpar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.astype(str).str.strip()
    for col in df.columns:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df


def garantir_pasta_data():
    pasta = os.path.join(ROOT_DIR, "data")
    os.makedirs(pasta, exist_ok=True)


# ==========================================================
# CARREGAMENTO
# ==========================================================
def carregar_dicionario_times() -> pd.DataFrame:
    if not os.path.exists(DICT_TIMES):
        raise FileNotFoundError(f"Arquivo não encontrado: {DICT_TIMES}")

    df = pd.read_csv(DICT_TIMES, encoding="utf-8-sig")
    df = limpar_colunas(df)

    obrigatorias = {"League_padronizada", "Time_padronizado"}
    faltando = obrigatorias - set(df.columns)
    if faltando:
        raise ValueError(f"dicionario_times.csv sem colunas obrigatórias: {faltando}")

    return df


def carregar_dicionario_times_flash() -> pd.DataFrame:
    if not os.path.exists(DICT_TIMES_FLASH):
        logging.warning("dicionario_times_flash.csv não encontrado. Será criado do zero.")
        return pd.DataFrame(columns=["Time_flash", "Time_padronizado"])

    df = pd.read_csv(DICT_TIMES_FLASH, encoding="utf-8-sig")
    df = limpar_colunas(df)

    obrigatorias = {"Time_flash", "Time_padronizado"}
    faltando = obrigatorias - set(df.columns)
    if faltando:
        raise ValueError(f"dicionario_times_flash.csv sem colunas obrigatórias: {faltando}")

    return df


def carregar_dicionario_ligas_flash() -> pd.DataFrame:
    if not os.path.exists(DICT_LIGAS_FLASH):
        raise FileNotFoundError(f"Arquivo não encontrado: {DICT_LIGAS_FLASH}")

    df = pd.read_csv(DICT_LIGAS_FLASH, encoding="utf-8-sig")
    df = limpar_colunas(df)

    obrigatorias = {"Flash_nome", "Footystats_nome"}
    faltando = obrigatorias - set(df.columns)
    if faltando:
        raise ValueError(f"dicionario_ligas_flash.csv sem colunas obrigatórias: {faltando}")

    return df


# ==========================================================
# URL A PARTIR DO CSV
# ==========================================================
def flash_nome_para_url(flash_nome: str) -> str | None:
    """
    Espera algo como:
      'Brazil - Serie A Betano'
      'Australia - A-League'
      'South America - Copa Libertadores'

    Gera:
      https://www.flashscore.com/football/brazil/serie-a-betano/fixtures/
      https://www.flashscore.com/football/australia/a-league/fixtures/
      https://www.flashscore.com/football/south-america/copa-libertadores/fixtures/

    Observação:
    se a URL gerada não existir exatamente, o script vai marcar erro dessa liga no relatório.
    """
    if " - " not in flash_nome:
        return None

    pais, liga = flash_nome.split(" - ", 1)
    pais_slug = limpar_slug(pais)
    liga_slug = limpar_slug(liga)

    return urljoin(BASE_FLASH_URL, f"{pais_slug}/{liga_slug}/fixtures/")


# ==========================================================
# SELENIUM
# ==========================================================
def iniciar_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-infobars")

    if HEADLESS:
        options.add_argument("--headless=new")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    return driver


def aceitar_cookies(driver):
    seletores = [
        (By.ID, "onetrust-accept-btn-handler"),
        (By.CSS_SELECTOR, "button#onetrust-accept-btn-handler"),
    ]

    for by, value in seletores:
        try:
            botao = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((by, value))
            )
            botao.click()
            time.sleep(1)
            return
        except Exception:
            pass


def expandir_partidas_se_houver(driver):
    """
    Em algumas páginas pode existir botão tipo 'Show more matches' / 'Load more'.
    Tenta clicar algumas vezes, sem quebrar o fluxo se não existir.
    """
    seletores = [
        "a.event__more",
        "button.event__more",
        "div.event__more",
        "[class*='showMore']",
        "[class*='loadMore']",
    ]

    for _ in range(4):
        clicou = False
        for seletor in seletores:
            try:
                botoes = driver.find_elements(By.CSS_SELECTOR, seletor)
                for botao in botoes:
                    if botao.is_displayed() and botao.is_enabled():
                        driver.execute_script("arguments[0].click();", botao)
                        time.sleep(1.5)
                        clicou = True
                        break
                if clicou:
                    break
            except Exception:
                continue

        if not clicou:
            break


def coletar_times_da_url(driver, url: str, flash_nome: str) -> list[dict]:
    registros = []

    logging.info(f"Abrindo: {url}")
    driver.get(url)
    time.sleep(3)
    aceitar_cookies(driver)
    time.sleep(2)

    try:
        WebDriverWait(driver, TEMPO_ESPERA).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='event__match']"))
        )
    except Exception:
        logging.warning(f"Nenhuma partida encontrada ou página não carregou: {flash_nome}")
        return registros

    expandir_partidas_se_houver(driver)

    partidas = driver.find_elements(By.CSS_SELECTOR, "[class*='event__match']")
    if not partidas:
        logging.warning(f"Sem partidas na página: {flash_nome}")
        return registros

    for partida in partidas:
        try:
            home = partida.find_element(
                By.CSS_SELECTOR, "[class*='event__homeParticipant']"
            ).text.strip()

            away = partida.find_element(
                By.CSS_SELECTOR, "[class*='event__awayParticipant']"
            ).text.strip()

            if home:
                registros.append({
                    "League_flash": flash_nome,
                    "Time_flash": home,
                    "Source_URL": url
                })

            if away:
                registros.append({
                    "League_flash": flash_nome,
                    "Time_flash": away,
                    "Source_URL": url
                })

        except Exception:
            continue

    logging.info(f"Times coletados em '{flash_nome}': {len(registros)}")
    return registros


# ==========================================================
# MATCH
# ==========================================================
def sugerir_time_padronizado(time_flash: str, liga_padronizada: str, df_times: pd.DataFrame):
    """
    Procura candidatos apenas dentro da liga padronizada.
    Retorna:
      melhor_nome, score
    """
    df_liga = df_times[df_times["League_padronizada"] == liga_padronizada].copy()
    if df_liga.empty:
        return None, 0

    candidatos = (
        df_liga["Time_padronizado"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    if not candidatos:
        return None, 0

    mapa_norm = {}
    for cand in candidatos:
        cand_norm = normalizar_nome_time(cand)
        if cand_norm and cand_norm not in mapa_norm:
            mapa_norm[cand_norm] = cand

    alvo = normalizar_nome_time(time_flash)
    if not alvo:
        return None, 0

    # match exato já normalizado
    if alvo in mapa_norm:
        return mapa_norm[alvo], 100

    resultado = process.extractOne(
        alvo,
        list(mapa_norm.keys()),
        scorer=fuzz.ratio
    )

    if not resultado:
        return None, 0

    melhor_norm = resultado[0]
    score = int(resultado[1])
    melhor_real = mapa_norm[melhor_norm]
    return melhor_real, score


# ==========================================================
# MAIN
# ==========================================================
def main():
    garantir_pasta_data()

    logging.info("=" * 60)
    logging.info("CARREGANDO DICIONÁRIOS")

    df_times = carregar_dicionario_times()
    df_times_flash_existente = carregar_dicionario_times_flash()
    df_ligas = carregar_dicionario_ligas_flash()

    logging.info(f"Dicionário de times base: {len(df_times)} registros")
    logging.info(f"Dicionário de times flash existente: {len(df_times_flash_existente)} registros")
    logging.info(f"Dicionário de ligas flash: {len(df_ligas)} registros")

    times_flash_existentes = set(
        df_times_flash_existente["Time_flash"]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )

    registros = []
    erros_ligas = []

    driver = iniciar_driver()

    try:
        total = len(df_ligas)

        for i, row in enumerate(df_ligas.itertuples(index=False), start=1):
            flash_nome = str(row.Flash_nome).strip()
            footystats_nome = str(row.Footystats_nome).strip()

            url = flash_nome_para_url(flash_nome)
            if not url:
                logging.warning(f"[{i}/{total}] Não foi possível gerar URL para: {flash_nome}")
                erros_ligas.append({
                    "League_flash": flash_nome,
                    "League_padronizada": footystats_nome,
                    "Time_flash": "",
                    "Time_padronizado_sugerido": "",
                    "Score": 0,
                    "Motivo": "não foi possível gerar URL",
                    "Source_URL": ""
                })
                continue

            logging.info(f"[{i}/{total}] Coletando times | {flash_nome}")

            try:
                registros_liga = coletar_times_da_url(driver, url, flash_nome)

                # já salva também a liga padronizada na coleta
                for reg in registros_liga:
                    reg["League_padronizada"] = footystats_nome

                registros.extend(registros_liga)

            except Exception as e:
                logging.warning(f"Erro na liga '{flash_nome}': {e}")
                erros_ligas.append({
                    "League_flash": flash_nome,
                    "League_padronizada": footystats_nome,
                    "Time_flash": "",
                    "Time_padronizado_sugerido": "",
                    "Score": 0,
                    "Motivo": f"erro na coleta: {e}",
                    "Source_URL": url
                })

    finally:
        driver.quit()

    if not registros and not erros_ligas:
        logging.warning("Nenhum dado coletado.")
        return

    df_coletado = pd.DataFrame(registros) if registros else pd.DataFrame(
        columns=["League_flash", "League_padronizada", "Time_flash", "Source_URL"]
    )

    if not df_coletado.empty:
        df_coletado["League_flash"] = df_coletado["League_flash"].astype(str).str.strip()
        df_coletado["League_padronizada"] = df_coletado["League_padronizada"].astype(str).str.strip()
        df_coletado["Time_flash"] = df_coletado["Time_flash"].astype(str).str.strip()
        df_coletado["Source_URL"] = df_coletado["Source_URL"].astype(str).str.strip()

        df_coletado = df_coletado.drop_duplicates(subset=["League_flash", "Time_flash"])

    logging.info(f"Times coletados únicos: {len(df_coletado)}")

    novos_auto = []
    revisao = []

    for _, row in df_coletado.iterrows():
        league_flash = row["League_flash"]
        league_padronizada = row["League_padronizada"]
        time_flash = row["Time_flash"]
        source_url = row["Source_URL"]

        if not time_flash:
            continue

        # se já existe no dicionário flash, ignora
        if time_flash in times_flash_existentes:
            continue

        sugestao, score = sugerir_time_padronizado(
            time_flash=time_flash,
            liga_padronizada=league_padronizada,
            df_times=df_times
        )

        if sugestao and score >= LIMITE_ADICIONAR_AUTO:
            novos_auto.append({
                "League_flash": league_flash,
                "League_padronizada": league_padronizada,
                "Time_flash": time_flash,
                "Time_padronizado": sugestao,
                "Score": score,
                "Source_URL": source_url
            })
        else:
            revisao.append({
                "League_flash": league_flash,
                "League_padronizada": league_padronizada,
                "Time_flash": time_flash,
                "Time_padronizado_sugerido": sugestao if sugestao else "",
                "Score": score,
                "Motivo": "score baixo ou sem sugestão",
                "Source_URL": source_url
            })

    # adiciona erros de liga no relatório de revisão
    revisao.extend(erros_ligas)

    df_novos = pd.DataFrame(novos_auto)
    df_revisao = pd.DataFrame(revisao)

    # salva relatório dos novos
    if not df_novos.empty:
        df_novos = df_novos.sort_values(
            by=["League_padronizada", "Score", "Time_flash"],
            ascending=[True, False, True]
        )
        df_novos.to_csv(OUTPUT_NOVOS, index=False, encoding="utf-8-sig")

        # atualiza o dicionário final usado no sistema
        df_adicionar = df_novos[["Time_flash", "Time_padronizado"]].drop_duplicates()

        df_final = pd.concat(
            [
                df_times_flash_existente[["Time_flash", "Time_padronizado"]],
                df_adicionar
            ],
            ignore_index=True
        )

        df_final = (
            df_final.drop_duplicates(subset=["Time_flash"], keep="first")
            .sort_values(by=["Time_flash"])
            .reset_index(drop=True)
        )

        df_final.to_csv(DICT_TIMES_FLASH, index=False, encoding="utf-8-sig")

        logging.info(f"Novos mapeamentos automáticos: {len(df_novos)}")
        logging.info(f"Dicionário Flash atualizado: {DICT_TIMES_FLASH}")
        logging.info(f"Relatório de novos: {OUTPUT_NOVOS}")
    else:
        logging.info("Nenhum novo mapeamento automático encontrado.")

    # salva revisão
    if not df_revisao.empty:
        cols_revisao = [
            "League_flash",
            "League_padronizada",
            "Time_flash",
            "Time_padronizado_sugerido",
            "Score",
            "Motivo",
            "Source_URL"
        ]
        for col in cols_revisao:
            if col not in df_revisao.columns:
                df_revisao[col] = ""

        df_revisao = df_revisao[cols_revisao].sort_values(
            by=["League_padronizada", "Score", "Time_flash"],
            ascending=[True, False, True]
        )
        df_revisao.to_csv(OUTPUT_REVISAO, index=False, encoding="utf-8-sig")
        logging.info(f"Arquivo para revisão manual: {OUTPUT_REVISAO}")
    else:
        logging.info("Nenhum caso para revisão manual.")

    logging.info("=" * 60)
    logging.info("PROCESSO FINALIZADO")


if __name__ == "__main__":
    main()