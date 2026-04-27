import pandas as pd
from pathlib import Path
from rapidfuzz import process, fuzz

# ==============================
# Configurações
# ==============================
CAMINHO_BASE = Path("data/base_ligas.csv")          # Base de jogos já com ligas padronizadas
CAMINHO_DICIONARIO = Path("data/dicionario_times.csv")  # Dicionário de times
CAMINHO_SAIDA = Path("data/base_times_padronizados.csv") # Base final
LIMITE_FUZZY = 90

# ==============================
# Funções
# ==============================
def carregar_dados():
    df_base = pd.read_csv(CAMINHO_BASE)

    if CAMINHO_DICIONARIO.exists():
        df_dic = pd.read_csv(CAMINHO_DICIONARIO)
    else:
        # Se não existir, cria vazio
        df_dic = pd.DataFrame(columns=["League_padronizada", "country", "Time_padronizado"])

    return df_base, df_dic

def padronizar_time(time, liga, pais, df_dic, limite=LIMITE_FUZZY):
    if pd.isna(time):
        return time, True

    df_filtrado = df_dic[(df_dic["League_padronizada"] == liga) & (df_dic["country"] == pais)]
    times_ref = df_filtrado["Time_padronizado"].tolist()

    if not times_ref:
        return time, True

    match, score, _ = process.extractOne(time, times_ref, scorer=fuzz.ratio)
    if score >= limite:
        return match, False
    else:
        return time, True

def aplicar_padronizacao(df_base, df_dic):
    # Função para cada linha
    def processar_linha(row):
        liga = row["League_padronizada"]
        pais = row["country"]

        home, novo_home = padronizar_time(row["Home"], liga, pais, df_dic)
        away, novo_away = padronizar_time(row["Away"], liga, pais, df_dic)

        row["Home_padronizado"] = home
        row["Away_padronizado"] = away
        row["Home_novo"] = novo_home
        row["Away_novo"] = novo_away
        return row

    df_base[["Home_padronizado", "Away_padronizado", "Home_novo", "Away_novo"]] = None
    df_base = df_base.apply(processar_linha, axis=1)
    return df_base

def atualizar_dicionario(df_base, df_dic):
    # Adiciona times novos ao dicionário
    novos_home = df_base[df_base["Home_novo"]][["League_padronizada", "country", "Home_padronizado"]]
    novos_away = df_base[df_base["Away_novo"]][["League_padronizada", "country", "Away_padronizado"]]

    if not novos_home.empty:
        novos_home = novos_home.rename(columns={"Home_padronizado": "Time_padronizado"})
        df_dic = pd.concat([df_dic, novos_home], ignore_index=True)

    if not novos_away.empty:
        novos_away = novos_away.rename(columns={"Away_padronizado": "Time_padronizado"})
        df_dic = pd.concat([df_dic, novos_away], ignore_index=True)

    # Remove duplicados
    df_dic = df_dic.drop_duplicates(subset=["League_padronizada", "country", "Time_padronizado"])
    return df_dic

# ==============================
# Execução principal
# ==============================
def main():
    print("Carregando dados...")
    df_base, df_dic = carregar_dados()

    print("Padronizando times...")
    df_base = aplicar_padronizacao(df_base, df_dic)

    print("Atualizando dicionário de times...")
    df_dic = atualizar_dicionario(df_base, df_dic)

    # Cria pasta de saída se não existir
    CAMINHO_SAIDA.parent.mkdir(parents=True, exist_ok=True)
    df_base.to_csv(CAMINHO_SAIDA, index=False)
    df_dic.to_csv(CAMINHO_DICIONARIO, index=False)

    print(f"Padronização concluída. Arquivo salvo em: {CAMINHO_SAIDA}")
    print(f"Times novos Home: {df_base['Home_novo'].sum()}, Away: {df_base['Away_novo'].sum()}")
    print(f"Dicionário de times atualizado: {len(df_dic)} registros")

if __name__ == "__main__":
    main()