# ==============================
# SCRIPT MESTRE 
# ==============================
import subprocess
from pathlib import Path
from datetime import datetime

# ==============================
# Configurações
# ==============================
GITHUB_PATH = Path("Youtube")  # pasta do git clone
SCRIPTS = [
    ("01_validar_arquivos.py", "1- Validação de Arquivos"),
    ("02_validar_colunas.py", "2- Validação de Colunas"),
    ("03_unificar_dados.py", "3- Unificação de Dados"),
    ("04_dicionario_ligas.py", "4- Dicionário de Ligas"),
    ("05_dicionario_times.py", "5- Dicionário de Times"),
    ("06_padronizar_ligas.py", "6- Padronização de Ligas"),
    ("07_padronizar_times.py", "7- Padronização de Times")
]

OUTPUT_LOG = []
LOG_FILE = Path("pipeline.log")

# ==============================
# Cores ANSI para terminal
# ==============================
COLOR_RESET = "\033[0m"
COLOR_RED = "\033[91m"
COLOR_GREEN = "\033[92m"
COLOR_YELLOW = "\033[93m"

# ==============================
# Funções de Log
# ==============================
def log(msg, ok=True, nome_etapa=None):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = "OK" if ok else "ERRO"
    cor = COLOR_GREEN if ok else COLOR_RED
    linha = f"[{timestamp}] [{status}] {nome_etapa or 'Geral'} -> {msg}"
    print(f"{cor}{linha}{COLOR_RESET}")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(linha + "\n")
    OUTPUT_LOG.append((nome_etapa or "Geral", ok, msg))

def log_info(msg, nome_etapa=None):
    cor_msg = f"{COLOR_YELLOW}{msg}{COLOR_RESET}"
    log(cor_msg, ok=True, nome_etapa=nome_etapa)

def log_error(msg, nome_etapa=None):
    log(msg, ok=False, nome_etapa=nome_etapa)

# ==============================
# Funções do Pipeline
# ==============================
def atualizar_github():
    if not GITHUB_PATH.exists():
        log_error(f"Pasta {GITHUB_PATH} não encontrada.", "GitHub")
        return False

    log_info("Atualizando repositório do GitHub...", "GitHub")
    comandos = [
        ["git", "-C", str(GITHUB_PATH), "fetch"],
        ["git", "-C", str(GITHUB_PATH), "status"],
        ["git", "-C", str(GITHUB_PATH), "pull"]
    ]

    for cmd in comandos:
        resultado = subprocess.run(cmd, capture_output=True, text=True)
        if resultado.returncode != 0:
            log_error(f"Erro no comando: {' '.join(cmd)}\nSTDOUT:{resultado.stdout}\nSTDERR:{resultado.stderr}", "GitHub")
            return False
        else:
            log_info(f"Comando executado com sucesso: {' '.join(cmd)}", "GitHub")

    log_info("Repositório atualizado com sucesso.", "GitHub")
    return True

def rodar_script(script_file: str, nome_exibicao: str):
    path = Path("01_scripts") / script_file
    if not path.exists():
        log_error(f"Script não encontrado: {path}", nome_exibicao)
        return False

    log_info(f"Iniciando etapa...", nome_exibicao)
    start_time = datetime.now()
    resultado = subprocess.run(["python", str(path)], capture_output=True, text=True)
    end_time = datetime.now()
    duracao = (end_time - start_time).total_seconds()

    if resultado.returncode != 0:
        log_error(f"Erro ao executar {nome_exibicao} (duração: {duracao:.2f}s)\nSTDOUT:{resultado.stdout}\nSTDERR:{resultado.stderr}", nome_exibicao)
        return False

    log_info(f"{nome_exibicao} concluído com sucesso (duração: {duracao:.2f}s)", nome_exibicao)
    return True

# ==============================
# Função Principal
# ==============================
def main():
    sucesso = atualizar_github()
    if not sucesso:
        log_error("Pipeline interrompido devido a falha na atualização do GitHub.", "Pipeline")
        return

    for script_file, nome_exibicao in SCRIPTS:
        sucesso = rodar_script(script_file, nome_exibicao)
        if not sucesso:
            log_error("Pipeline interrompido devido a erro.", "Pipeline")
            break

    print("\nPipeline concluído.")

# ==============================
# Execução
# ==============================
if __name__ == "__main__":
    main()