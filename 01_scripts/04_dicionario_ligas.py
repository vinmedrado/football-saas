import pandas as pd
from pathlib import Path

# ==============================
# Configurações
# ==============================
CAMINHO_LIGAS = Path("YouTube/ligas_footystats.xlsx")
CAMINHO_SAIDA = Path("data/dicionario_ligas.csv")

# ==============================
# Função principal
# ==============================
def gerar_dicionario_ligas():
    # Carrega planilha de ligas
    df_ligas = pd.read_excel(CAMINHO_LIGAS)

    # Seleciona colunas essenciais (apenas nome e country)
    df_dic = df_ligas[["name", "country"]].copy()

    # Remove duplicados
    df_dic = df_dic.drop_duplicates(subset=["name"]).reset_index(drop=True)

    # Cria pasta de saída se não existir
    CAMINHO_SAIDA.parent.mkdir(parents=True, exist_ok=True)

    # Salva CSV
    df_dic.to_csv(CAMINHO_SAIDA, index=False)
    print(f"Dicionário de ligas salvo em: {CAMINHO_SAIDA}")
    print(f"Total de ligas: {len(df_dic)}")

# ==============================
# Executar script
# ==============================
if __name__ == "__main__":
    gerar_dicionario_ligas()