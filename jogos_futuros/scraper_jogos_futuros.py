#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FLASHSCORE SCRAPER PRO - VERSÃO REFATORADA 🚀
✅ Sistema robusto com gerenciamento de memória
✅ Prevenção de timeout e crash do driver
✅ Processamento por lote com reinício inteligente
✅ Verificação de saúde do driver
✅ Logs detalhados de performance
✅ Pronto para produção
"""

import sys
import os
import json
import time
import logging
import gc
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set, Tuple
from tqdm import tqdm

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from jogos_futuros.flashscore_scraper import FlashScoreScraper

# ========================= CONFIGURAÇÕES =========================
LEAGUE_URLS = [
    'https://www.flashscore.com/football/australia/a-league/fixtures/',
    'https://www.flashscore.com/football/austria/bundesliga/fixtures/',
    'https://www.flashscore.com/football/belgium/jupiler-pro-league/fixtures/',
    'https://www.flashscore.com/football/brazil/serie-a/fixtures/',
    'https://www.flashscore.com/football/brazil/serie-b/fixtures/',
    'https://www.flashscore.com/football/brazil/serie-c/fixtures/',
    'https://www.flashscore.com/football/chile/liga-de-primera/fixtures/',
    'https://www.flashscore.com/football/china/super-league/fixtures/',
    'https://www.flashscore.com/football/croatia/prva-nl/fixtures/',
    'https://www.flashscore.com/football/czech-republic/chance-liga/fixtures/',
    'https://www.flashscore.com/football/denmark/superliga/fixtures/',
    'https://www.flashscore.com/football/egypt/premier-league/fixtures/',
    'https://www.flashscore.com/football/england/championship/fixtures/',
    'https://www.flashscore.com/football/england/league-one/fixtures/',
    'https://www.flashscore.com/football/england/league-two/fixtures/',
    'https://www.flashscore.com/football/england/premier-league/fixtures/',
    'https://www.flashscore.com/football/europe/champions-league/fixtures/',
    'https://www.flashscore.com/football/france/ligue-1/fixtures/',
    'https://www.flashscore.com/football/france/ligue-2/fixtures/',
    'https://www.flashscore.com/football/france/national/fixtures/',
    'https://www.flashscore.com/football/germany/2-bundesliga/fixtures/',
    'https://www.flashscore.com/football/germany/3-liga/fixtures/',
    'https://www.flashscore.com/football/germany/bundesliga/fixtures/',
    'https://www.flashscore.com/football/greece/super-league/fixtures/',
    'https://www.flashscore.com/football/israel/ligat-ha-al/fixtures/',
    'https://www.flashscore.com/football/italy/serie-a/fixtures/',
    'https://www.flashscore.com/football/italy/serie-b/fixtures/',
    'https://www.flashscore.com/football/japan/j1-league/fixtures/',
    'https://www.flashscore.com/football/japan/j2-league/fixtures/',
    'https://www.flashscore.com/football/netherlands/eerste-divisie/fixtures/',
    'https://www.flashscore.com/football/netherlands/eredivisie/fixtures/',
    'https://www.flashscore.com/football/norway/eliteserien/fixtures/',
    'https://www.flashscore.com/football/poland/ekstraklasa/fixtures/',
    'https://www.flashscore.com/football/portugal/liga-portugal/fixtures/',
    'https://www.flashscore.com/football/portugal/liga-portugal-2/fixtures/',
    'https://www.flashscore.com/football/romania/superliga/fixtures/',
    'https://www.flashscore.com/football/saudi-arabia/saudi-professional-league/fixtures/',
    'https://www.flashscore.com/football/scotland/premiership/fixtures/',
    'https://www.flashscore.com/football/serbia/mozzart-bet-super-liga/fixtures/',
    'https://www.flashscore.com/football/slovenia/prva-liga/fixtures/',
    'https://www.flashscore.com/football/south-africa/betway-premiership/fixtures/',
    'https://www.flashscore.com/football/south-korea/k-league-1/fixtures/',
    'https://www.flashscore.com/football/south-korea/k-league-2/fixtures/',
    'https://www.flashscore.com/football/spain/laliga/fixtures/',
    'https://www.flashscore.com/football/spain/laliga2/fixtures/',
    'https://www.flashscore.com/football/sweden/allsvenskan/fixtures/',
    'https://www.flashscore.com/football/switzerland/super-league/fixtures/',
    'https://www.flashscore.com/football/usa/mls/fixtures/',
    'https://www.flashscore.com/football/slovakia/nike-liga/fixtures/',
    'https://www.flashscore.com/football/south-america/copa-libertadores/fixtures/'
]

OUTPUT_DIR = "./jogos_futuros"
DAYS_AHEAD = [0, 1, 2]

# Configurações de robustez
RETRY_LIMIT = 3
SLEEP_BETWEEN_MATCHES = 0.15
REINICIAR_A_CADA_JOGOS = 30  # Mais frequente (antes 50)
REINICIAR_A_CADA_LIGAS = 10  # Novo: reinicia a cada N ligas
TIMEOUT_PAGINA = 25  # Timeout mais curto para detectar problemas rápido
MAX_MEMORY_MB = 1500  # Limite de memória antes de reinício forçado

# ========================= CORES =========================
RESET = "\033[0m"
GREEN = "\033[92m"
BLUE = "\033[94m"
WHITE = "\033[97m"
YELLOW = "\033[93m"
RED = "\033[91m"

ICON_NEW = "🟢"
ICON_UPDATE = "🔵"
ICON_EXIST = "⚪"
ICON_ERROR = "❌"
ICON_WARNING = "⚠️"
ICON_MEMORY = "💾"

# ========================= LOGGING =========================
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)

# ========================= CLASSE PRINCIPAL =========================

class FlashScoreScraperPro:
    """Versão profissional do scraper com gerenciamento avançado"""
    
    def __init__(self):
        self.scraper = None
        self.jogos_processados = 0
        self.ligas_processadas = 0
        self.stats = {
            'novos': 0,
            'atualizados': 0,
            'existentes': 0,
            'erros': 0,
            'timeouts': 0
        }
        
    def iniciar_driver(self):
        """Inicializa o driver com configurações otimizadas"""
        if self.scraper:
            self.fechar_driver()
        
        self.scraper = FlashScoreScraper(headless=True)
        
        # Aplica configurações adicionais de memória (se o método existir)
        if hasattr(self.scraper, 'setup_driver'):
            try:
                # Tentar acessar e modificar as options
                driver = self.scraper.driver
                if driver:
                    # Configura timeouts
                    driver.set_page_load_timeout(TIMEOUT_PAGINA)
                    driver.set_script_timeout(TIMEOUT_PAGINA)
            except Exception as e:
                logging.debug(f"Não foi possível configurar timeouts: {e}")
        
        logging.info("🚀 Driver iniciado com configurações otimizadas")
        
    def fechar_driver(self):
        """Fecha o driver e limpa memória"""
        if self.scraper:
            try:
                self.scraper.driver.quit()
            except:
                pass
            self.scraper = None
            gc.collect()
            logging.info("🔒 Driver fechado e memória liberada")
            
    def verificar_saude_driver(self) -> bool:
        """Verifica se o driver está respondendo"""
        if not self.scraper or not self.scraper.driver:
            return False
        
        try:
            # Comando simples para verificar se o driver responde
            self.scraper.driver.current_url
            return True
        except (WebDriverException, TimeoutException) as e:
            logging.warning(f"{ICON_WARNING} Driver não responde: {str(e)[:50]}")
            return False
        except Exception:
            return False
            
    def verificar_memoria(self) -> bool:
        """Verifica uso de memória e retorna True se estiver alta"""
        try:
            import psutil
            process = psutil.Process()
            mem_mb = process.memory_info().rss / 1024 / 1024
            
            if mem_mb > MAX_MEMORY_MB:
                logging.warning(f"{ICON_MEMORY} Memória alta: {mem_mb:.0f}MB > {MAX_MEMORY_MB}MB")
                return True
            
            if self.jogos_processados % 20 == 0:  # Log a cada 20 jogos
                logging.info(f"{ICON_MEMORY} Memória atual: {mem_mb:.0f}MB")
            
            return False
        except ImportError:
            # psutil não instalado, ignora verificação
            return False
        except Exception:
            return False
            
    def reiniciar_driver_preventivo(self, motivo: str = "preventivo"):
        """Reinicia o driver de forma controlada"""
        logging.info(f"{ICON_WARNING} Reinício preventivo do driver ({motivo})")
        
        # Salva estatísticas atuais
        self.fechar_driver()
        time.sleep(2)
        self.iniciar_driver()
        self.aceitar_cookies()
        
    def aceitar_cookies(self):
        """Aceita cookies se o botão existir"""
        if not self.scraper:
            return
            
        try:
            wait = WebDriverWait(self.scraper.driver, 5)
            btn = wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
            self.scraper.driver.execute_script("arguments[0].click();", btn)
            logging.info("🍪 Cookies aceitos")
        except Exception:
            pass  # Botão não encontrado ou já aceito
            
    def navegar_com_retry(self, url: str) -> bool:
        """Navega para URL com retry automático"""
        for tentativa in range(RETRY_LIMIT):
            try:
                if not self.verificar_saude_driver():
                    self.reiniciar_driver_preventivo("driver inoperante")
                    
                self.scraper.driver.set_page_load_timeout(TIMEOUT_PAGINA)
                self.scraper.driver.get(url)
                time.sleep(1)
                return True
                
            except TimeoutException:
                logging.warning(f"Timeout ao carregar {url[:50]}... (tentativa {tentativa + 1})")
                self.reiniciar_driver_preventivo("timeout")
                
            except Exception as e:
                logging.warning(f"Erro ao navegar: {str(e)[:50]} (tentativa {tentativa + 1})")
                if tentativa < RETRY_LIMIT - 1:
                    time.sleep(2)
                    self.reiniciar_driver_preventivo("erro navegação")
                    
        logging.error(f"{ICON_ERROR} Falha ao carregar {url}")
        return False
        
    def expandir_secoes(self):
        """Expande seções da página"""
        if not self.scraper:
            return
            
        try:
            botoes = self.scraper.driver.find_elements(By.CSS_SELECTOR, 'div[role="button"]')
            for btn in botoes[:10]:  # Limita para não sobrecarregar
                try:
                    if btn.is_displayed():
                        self.scraper.driver.execute_script("arguments[0].click();", btn)
                        time.sleep(0.1)
                except:
                    pass
        except Exception:
            pass
            
    def extrair_ids_jogos(self, date_filter: str) -> List[str]:
        """Extrai IDs dos jogos para uma data específica"""
        if not self.scraper:
            return []
            
        ids_jogos = []
        hoje = datetime.today()
        
        try:
            elementos = self.scraper.driver.find_elements(
                By.CSS_SELECTOR, 'div.event__match'
            )
            
            for el in elementos:
                try:
                    match_id = el.get_attribute("id")
                    if not match_id or not match_id.startswith("g_1_"):
                        continue
                        
                    # Extrai horário/data
                    time_el = el.find_element(By.CSS_SELECTOR, "div.event__time")
                    time_text = time_el.text.strip()
                    
                    # Determina a data do jogo
                    if "." not in time_text:
                        # Jogo hoje (só horário)
                        data_jogo = hoje.strftime("%d.%m.%Y")
                    else:
                        # Jogo em outro dia
                        partes = time_text.split(" ")[0].rstrip(".").split(".")
                        if len(partes) >= 2:
                            dia, mes = int(partes[0]), int(partes[1])
                            ano = hoje.year
                            data_candidata = datetime(ano, mes, dia)
                            if data_candidata < hoje.replace(hour=0, minute=0, second=0):
                                data_candidata = datetime(ano + 1, mes, dia)
                            data_jogo = data_candidata.strftime("%d.%m.%Y")
                        else:
                            continue
                            
                    if data_jogo == date_filter:
                        ids_jogos.append(match_id.split("_")[-1])
                        
                except Exception as e:
                    logging.debug(f"Erro ao processar elemento: {e}")
                    continue
                    
        except Exception as e:
            logging.error(f"Erro ao extrair IDs: {e}")
            
        return ids_jogos
        
    def extrair_dados_jogo(self, match_id: str) -> Optional[Dict]:
        """Extrai todos os dados de um jogo com proteção"""
        if not self.scraper:
            return None
            
        for tentativa in range(RETRY_LIMIT):
            try:
                # Verifica saúde do driver antes de cada extração
                if not self.verificar_saude_driver():
                    self.reiniciar_driver_preventivo("driver morto")
                    
                # Extrai informações básicas
                dados = self.scraper.get_match_basic_info(match_id)
                if not dados:
                    return None
                    
                dados['Match_ID'] = dados.pop('Id', match_id)
                
                # Extrai odds com tolerância a falhas
                try:
                    dados = self.scraper.extract_odds_1x2_ft(match_id, dados)
                except Exception as e:
                    logging.debug(f"Erro odds 1x2 FT: {e}")
                    
                try:
                    dados = self.scraper.extract_odds_1x2_ht(match_id, dados)
                except Exception as e:
                    logging.debug(f"Erro odds 1x2 HT: {e}")
                    
                try:
                    dados = self.scraper.extract_odds_ou_ft(match_id, dados)
                except Exception as e:
                    logging.debug(f"Erro odds OU FT: {e}")
                    
                try:
                    dados = self.scraper.extract_odds_ou_ht(match_id, dados)
                except Exception as e:
                    logging.debug(f"Erro odds OU HT: {e}")
                    
                try:
                    dados = self.scraper.extract_odds_btts_ft(match_id, dados)
                except Exception as e:
                    logging.debug(f"Erro odds BTTS: {e}")
                    
                try:
                    dados = self.scraper.extract_odds_dc_ft(match_id, dados)
                except Exception as e:
                    logging.debug(f"Erro odds DC: {e}")
                    
                try:
                    dados = self.scraper.extract_special_markets(match_id, dados)
                except Exception as e:
                    logging.debug(f"Erro mercados especiais: {e}")
                    
                dados["last_update"] = datetime.now().isoformat()
                return dados
                
            except TimeoutException as e:
                self.stats['timeouts'] += 1
                logging.warning(f"Timeout no jogo {match_id} (tentativa {tentativa + 1})")
                if tentativa < RETRY_LIMIT - 1:
                    self.reiniciar_driver_preventivo("timeout extração")
                    time.sleep(1)
                    
            except Exception as e:
                logging.warning(f"Erro no jogo {match_id}: {str(e)[:80]}")
                if tentativa < RETRY_LIMIT - 1:
                    time.sleep(0.5)
                    
        self.stats['erros'] += 1
        logging.error(f"{ICON_ERROR} Falha ao extrair jogo {match_id}")
        return None
        
    def processar_dia(self, days_ahead: int):
        """Processa todos os jogos de um dia específico"""
        data_obj = datetime.today() + timedelta(days=days_ahead)
        data_arquivo = data_obj.strftime('%Y-%m-%d')
        data_url = data_obj.strftime('%Y%m%d')
        data_filtro = data_obj.strftime('%d.%m.%Y')
        
        arquivo_saida = f"{OUTPUT_DIR}/{data_arquivo}.jsonl"
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Carrega jogos já processados
        ids_processados = self.carregar_ids_processados(arquivo_saida)
        logging.info(f"\n{'='*60}")
        logging.info(f"📅 Processando dia: {data_arquivo}")
        logging.info(f"📊 Jogos existentes: {len(ids_processados)}")
        logging.info(f"{'='*60}\n")
        
        stats_dia = {'novos': 0, 'atualizados': 0, 'existentes': 0}
        
        for idx_liga, base_url in enumerate(LEAGUE_URLS):
            # Reinício preventivo a cada N ligas
            if idx_liga > 0 and idx_liga % REINICIAR_A_CADA_LIGAS == 0:
                self.reiniciar_driver_preventivo(f"{idx_liga} ligas processadas")
                self.aceitar_cookies()
                
            # Extrai nome da liga
            nome_liga = base_url.split('/')[-3].replace("-", " ").title()
            logging.info(f"🏆 Liga {idx_liga + 1}/{len(LEAGUE_URLS)}: {nome_liga}")
            
            # Navega para página da liga
            url = f"{base_url}?d={data_url}"
            if not self.navegar_com_retry(url):
                logging.warning(f"Pulando liga: {nome_liga}")
                continue
                
            self.aceitar_cookies()
            time.sleep(0.5)
            self.expandir_secoes()
            
            # Extrai IDs dos jogos
            ids_jogos = self.extrair_ids_jogos(data_filtro)
            if not ids_jogos:
                logging.info(f"  Sem jogos para esta data\n")
                continue
                
            logging.info(f"  🎮 Jogos encontrados: {len(ids_jogos)}")
            
            # Processa cada jogo
            for mid in tqdm(ids_jogos, desc="  Jogos", leave=False):
                self.jogos_processados += 1
                
                # Verificação periódica de memória
                if self.jogos_processados % 10 == 0 and self.verificar_memoria():
                    self.reiniciar_driver_preventivo("memória alta")
                    self.aceitar_cookies()
                    
                # Reinício preventivo por número de jogos
                if self.jogos_processados % REINICIAR_A_CADA_JOGOS == 0:
                    self.reiniciar_driver_preventivo(f"{self.jogos_processados} jogos processados")
                    self.aceitar_cookies()
                    
                # Verifica se jogo já existe
                if mid in ids_processados:
                    dados_existentes = self.carregar_jogo_por_id(arquivo_saida, mid)
                    if self.deve_atualizar(dados_existentes):
                        novos_dados = self.extrair_dados_jogo(mid)
                        if novos_dados:
                            self.atualizar_jogo(arquivo_saida, mid, novos_dados)
                            stats_dia['atualizados'] += 1
                            self.stats['atualizados'] += 1
                            log_game(ICON_UPDATE, f"Atualizado: {mid}", BLUE)
                        else:
                            stats_dia['existentes'] += 1
                            log_game(ICON_EXIST, f"Erro ao atualizar: {mid}", WHITE)
                    else:
                        stats_dia['existentes'] += 1
                        log_game(ICON_EXIST, f"Existente (ok): {mid}", WHITE)
                    continue
                    
                # Novo jogo
                dados = self.extrair_dados_jogo(mid)
                if dados:
                    self.salvar_jogo(arquivo_saida, dados)
                    ids_processados.add(mid)
                    stats_dia['novos'] += 1
                    self.stats['novos'] += 1
                    log_game(ICON_NEW, f"Novo jogo: {mid}", GREEN)
                else:
                    stats_dia['existentes'] += 1
                    
                time.sleep(SLEEP_BETWEEN_MATCHES)
                
            logging.info("")  # Linha em branco entre ligas
            
        # Resumo do dia
        logging.info(f"{'='*60}")
        logging.info(f"✅ Dia {data_arquivo} finalizado")
        logging.info(f"  🟢 Novos: {stats_dia['novos']}")
        logging.info(f"  🔵 Atualizados: {stats_dia['atualizados']}")
        logging.info(f"  ⚪ Existentes: {stats_dia['existentes']}")
        logging.info(f"{'='*60}\n")
        
    # ==================== MÉTODOS DE ARQUIVO ====================
    
    def carregar_ids_processados(self, file_path: str) -> Set[str]:
        """Carrega IDs dos jogos já processados"""
        ids = set()
        if not os.path.exists(file_path):
            return ids
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        dados = json.loads(line.strip())
                        if dados.get('Match_ID'):
                            ids.add(dados['Match_ID'])
                    except:
                        continue
        except Exception as e:
            logging.error(f"Erro ao carregar IDs: {e}")
            
        return ids
        
    def carregar_jogo_por_id(self, file_path: str, match_id: str) -> Optional[Dict]:
        """Carrega um jogo específico pelo ID"""
        if not os.path.exists(file_path):
            return None
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        dados = json.loads(line.strip())
                        if dados.get('Match_ID') == match_id:
                            return dados
                    except:
                        continue
        except Exception:
            pass
            
        return None
        
    def salvar_jogo(self, file_path: str, dados: Dict):
        """Salva um novo jogo"""
        try:
            with open(file_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(dados, ensure_ascii=False) + '\n')
        except Exception as e:
            logging.error(f"Erro ao salvar jogo: {e}")
            
    def atualizar_jogo(self, file_path: str, match_id: str, novos_dados: Dict):
        """Atualiza um jogo existente"""
        linhas = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        dados = json.loads(line.strip())
                        if dados.get('Match_ID') == match_id:
                            linhas.append(json.dumps(novos_dados, ensure_ascii=False))
                        else:
                            linhas.append(line.strip())
                    except:
                        linhas.append(line.strip())
                        
            with open(file_path, 'w', encoding='utf-8') as f:
                for linha in linhas:
                    f.write(linha + '\n')
                    
        except Exception as e:
            logging.error(f"Erro ao atualizar jogo: {e}")
            
    def deve_atualizar(self, dados: Optional[Dict]) -> bool:
        """Determina se um jogo deve ser atualizado"""
        if not dados:
            return True
            
        if 'last_update' not in dados:
            return True
            
        try:
            ultima_atualizacao = datetime.fromisoformat(dados['last_update'])
            delta = datetime.now() - ultima_atualizacao
            
            # Tenta extrair data/hora do jogo
            try:
                data_str = dados.get('Date', '')
                hora_str = dados.get('Time', '00:00')
                data_jogo = datetime.strptime(f"{data_str} {hora_str}", "%d/%m/%Y %H:%M")
                tempo_para_jogo = data_jogo - datetime.now()
                
                if tempo_para_jogo.total_seconds() < 0:
                    return False  # Jogo já passou
                elif tempo_para_jogo.total_seconds() < 6 * 3600:  # < 6h
                    return delta.total_seconds() > 900  # 15 min
                elif tempo_para_jogo.total_seconds() < 24 * 3600:  # < 24h
                    return delta.total_seconds() > 3600  # 1h
                else:
                    return delta.total_seconds() > 10800  # 3h
            except:
                return delta.total_seconds() > 3600  # Padrão: 1h
                
        except Exception:
            return True
            
    def exibir_resumo_final(self):
        """Exibe estatísticas finais da execução"""
        logging.info(f"\n{'='*60}")
        logging.info(f"📊 RESUMO FINAL")
        logging.info(f"{'='*60}")
        logging.info(f"  🟢 Novos jogos: {self.stats['novos']}")
        logging.info(f"  🔵 Jogos atualizados: {self.stats['atualizados']}")
        logging.info(f"  ⚪ Jogos existentes: {self.stats['existentes']}")
        logging.info(f"  ❌ Erros: {self.stats['erros']}")
        logging.info(f"  ⏱️  Timeouts: {self.stats['timeouts']}")
        logging.info(f"  🎮 Total processado: {self.jogos_processados}")
        logging.info(f"{'='*60}\n")
        
    def executar(self):
        """Executa o scraper completo"""
        logging.info(f"\n{'='*60}")
        logging.info(f"🚀 FLASHSCORE SCRAPER PRO INICIADO")
        logging.info(f"{'='*60}")
        logging.info(f"📅 Dias a processar: {DAYS_AHEAD}")
        logging.info(f"🔄 Reinício a cada {REINICIAR_A_CADA_JOGOS} jogos")
        logging.info(f"💾 Limite memória: {MAX_MEMORY_MB}MB")
        logging.info(f"{'='*60}\n")
        
        try:
            self.iniciar_driver()
            self.aceitar_cookies()
            
            for day in DAYS_AHEAD:
                self.processar_dia(day)
                
            self.exibir_resumo_final()
            
        except KeyboardInterrupt:
            logging.info(f"\n{ICON_WARNING} Interrompido pelo usuário")
            
        except Exception as e:
            logging.error(f"{ICON_ERROR} Erro fatal: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            self.fechar_driver()
            logging.info("✅ Scraper finalizado")


# ========================= FUNÇÕES AUXILIARES =========================

def log_game(icon: str, msg: str, color: str = RESET):
    """Log formatado para jogos"""
    logging.info(f"{color}{icon} {msg}{RESET}")


# ========================= MAIN =========================

def main():
    """Função principal"""
    scraper_pro = FlashScoreScraperPro()
    scraper_pro.executar()


if __name__ == "__main__":
    main()