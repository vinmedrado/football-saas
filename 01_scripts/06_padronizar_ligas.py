import pandas as pd
from pathlib import Path
from rapidfuzz import process, fuzz

# ==============================
# Configurações
# ==============================
CAMINHO_BASE = Path("data/base_unificada.csv")       # Base de jogos
CAMINHO_DIC_LIGAS = Path("data/dicionario_ligas.csv") 
CAMINHO_SAIDA = Path("data/base_ligas.csv")         # Base final com ligas padronizadas

LIMITE_FUZZY = 90  # Limite mínimo para considerar match do fuzzy

# ==============================
# Funções
# ==============================
def carregar_dados():
    df = pd.read_csv(CAMINHO_BASE)
    df_dic_ligas = pd.read_csv(CAMINHO_DIC_LIGAS)
    return df, df_dic_ligas

def padronizar_liga(valor, ligas_oficiais, limite=LIMITE_FUZZY):
    if pd.isna(valor):
        return valor
    match, score, _ = process.extractOne(valor, ligas_oficiais, scorer=fuzz.ratio)
    if score >= limite:
        return match
    return valor

def aplicar_padronizacao(df, ligas_oficiais):
    df["League_padronizada"] = df["League"].apply(
        lambda x: padronizar_liga(x, ligas_oficiais)
    )
    return df

def adicionar_metadados(df, df_dic_ligas):
    # Faz merge pelo nome da liga padronizada
    df = df.merge(
        df_dic_ligas[["name", "country"]],
        left_on="League_padronizada",
        right_on="name",
        how="left"
    )
    # Remove coluna redundante
    df = df.drop(columns=["name"])
    return df

# ==============================
# Execução principal
# ==============================
def main():
    df, df_dic_ligas = carregar_dados()

    # Lista de ligas do dicionário (apenas nomes)
    ligas_oficiais = df_dic_ligas["name"].dropna().unique()

    print("Padronizando ligas...")
    df = aplicar_padronizacao(df, ligas_oficiais)

    print("Adicionando metadados do dicionário de ligas...")
    df = adicionar_metadados(df, df_dic_ligas)

    # Reorganiza colunas principais (opcional)
    colunas_inicio = ["League", "League_padronizada", "country"]
    outras_colunas = [c for c in df.columns if c not in colunas_inicio]
    df = df[colunas_inicio + outras_colunas]

    CAMINHO_SAIDA.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CAMINHO_SAIDA, index=False)

    print("Processo concluído.")
    print(f"Arquivo salvo em: {CAMINHO_SAIDA}")

if __name__ == "__main__":
    main()