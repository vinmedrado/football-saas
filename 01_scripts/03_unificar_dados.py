import pandas as pd
from pathlib import Path


CAMINHO_DADOS = Path("YouTube/Bases_de_Dados/FootyStats")
CAMINHO_SAIDA = Path("data/base_unificada.csv")


def carregar_dados(caminho_base: Path):
    dataframes = []

    for arquivo in caminho_base.rglob("*"):
        if arquivo.suffix.lower() in [".xlsx", ".xls"]:
            try:
                df = pd.read_excel(arquivo)

                # metadados úteis
                df["arquivo_origem"] = arquivo.name
                df["liga_arquivo"] = arquivo.stem

                dataframes.append(df)

            except Exception as erro:
                print(f"Erro ao processar {arquivo.name}: {erro}")

    return dataframes


def unificar_dataframes(lista_dfs):
    if not lista_dfs:
        return pd.DataFrame()

    return pd.concat(lista_dfs, ignore_index=True)


def salvar_dataframe(df: pd.DataFrame, caminho_saida: Path):
    caminho_saida.parent.mkdir(parents=True, exist_ok=True)
    
    # Se já existe, appenda só os jogos novos
    if caminho_saida.exists():
        df_existente = pd.read_csv(caminho_saida)
        ids_existentes = set(df_existente["Id_Jogo"].astype(str))
        df_novos = df[~df["Id_Jogo"].astype(str).isin(ids_existentes)]
        df_final = pd.concat([df_existente, df_novos], ignore_index=True)
        print(f"Jogos novos adicionados: {len(df_novos)}")
    else:
        df_final = df
    
    df_final.to_csv(caminho_saida, index=False)


def main():
    dataframes = carregar_dados(CAMINHO_DADOS)

    print(f"Arquivos processados: {len(dataframes)}")

    df_final = unificar_dataframes(dataframes)

    print(f"Linhas totais: {df_final.shape[0]}")
    print(f"Colunas totais: {df_final.shape[1]}")

    salvar_dataframe(df_final, CAMINHO_SAIDA)

    print(f"\nArquivo salvo em: {CAMINHO_SAIDA}")


if __name__ == "__main__":
    main()