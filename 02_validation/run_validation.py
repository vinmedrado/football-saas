import subprocess
from pathlib import Path
from datetime import datetime

# ==============================
# Configurações
# ==============================
BASE_DIR = Path(__file__).parent
VALIDATION_SCRIPTS = [
    ("1- Validação de Planilhas", "01_validar_preprocesso_planilhas.py"),
    ("2- Transformação Long", "02_transformar_long.py")
]

# ==============================
# Cores para terminal
# ==============================
COLOR_GREEN = "\033[92m"
COLOR_RED   = "\033[91m"
COLOR_RESET = "\033[0m"

# ==============================
# Funções de log
# ==============================
def log_info(msg, etapa=None):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}] [OK] {etapa or 'INFO'} -> {msg}"
    print(f"{COLOR_GREEN}{linha}{COLOR_RESET}")

def log_error(msg, etapa=None):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}] [ERRO] {etapa or 'ERROR'} -> {msg}"
    print(f"{COLOR_RED}{linha}{COLOR_RESET}")

# ==============================
# Rodar scripts
# ==============================
def rodar_script(nome, caminho):
    if not caminho.exists():
        log_error(f"Script não encontrado: {caminho}", nome)
        return False

    log_info(f"Iniciando etapa...", nome)
    start = datetime.now()
    result = subprocess.run(["python", str(caminho)], capture_output=True, text=True)
    duracao = (datetime.now() - start).total_seconds()

    if result.returncode != 0:
        log_error(f"Etapa '{nome}' falhou (duração: {duracao:.2f}s)\nSTDOUT:{result.stdout}\nSTDERR:{result.stderr}", nome)
        return False
    else:
        log_info(f"Etapa '{nome}' concluída com sucesso (duração: {duracao:.2f}s)", nome)
        return True

# ==============================
# Execução do pipeline
# ==============================
def main():
    for nome, script in VALIDATION_SCRIPTS:
        caminho = BASE_DIR / script
        sucesso = rodar_script(nome, caminho)
        if not sucesso:
            log_error("Pipeline interrompido devido a erro.", "Pipeline")
            break

    log_info("Pipeline de validação concluído.", "Pipeline")

# ==============================
# Ponto de entrada
# ==============================
if __name__ == "__main__":
    main()