import pandas as pd
from pathlib import Path


CAMINHO_DADOS = Path("YouTube/Bases_de_Dados/FootyStats")


def validar_arquivos_excel(caminho_base: Path):
    total_encontrados = 0
    total_lidos = 0

    arquivos_com_erro = []

    for arquivo in caminho_base.rglob("*"):
        if arquivo.suffix.lower() in [".xlsx", ".xls"]:
            total_encontrados += 1

            try:
                pd.read_excel(arquivo, nrows=5)
                total_lidos += 1

            except Exception as erro:
                arquivos_com_erro.append((arquivo.name, str(erro)))

    return total_encontrados, total_lidos, arquivos_com_erro


def main():
    total_encontrados, total_lidos, arquivos_com_erro = validar_arquivos_excel(CAMINHO_DADOS)

    print("\nResumo da validação")
    print("-------------------")
    print(f"Arquivos encontrados: {total_encontrados}")
    print(f"Arquivos lidos com sucesso: {total_lidos}")
    print(f"Arquivos com erro: {len(arquivos_com_erro)}")

    if arquivos_com_erro:
        print("\nArquivos com erro:")
        for nome, erro in arquivos_com_erro:
            print(f"- {nome}: {erro}")

    if total_encontrados == total_lidos:
        print("\nTodos os arquivos foram lidos com sucesso.")
    else:
        print("\nArquivos com erro.")


if __name__ == "__main__":
    main()