import pandas as pd
from pathlib import Path


CAMINHO_DADOS = Path("YouTube/Bases_de_Dados/FootyStats")


def coletar_colunas(caminho_base: Path):
    estruturas = {}
    arquivos_com_erro = []

    for arquivo in caminho_base.rglob("*"):
        if arquivo.suffix.lower() in [".xlsx", ".xls"]:
            try:
                df = pd.read_excel(arquivo, nrows=5)
                estruturas[arquivo.name] = set(df.columns)

            except Exception as erro:
                arquivos_com_erro.append((arquivo.name, str(erro)))

    return estruturas, arquivos_com_erro


def analisar_estruturas(estruturas: dict):
    todas = list(estruturas.values())

    colunas_unicas = set.union(*todas)
    colunas_comuns = set.intersection(*todas)

    inconsistencias = {}

    for nome, colunas in estruturas.items():
        faltando = colunas_unicas - colunas
        extras = colunas - colunas_comuns

        if faltando or extras:
            inconsistencias[nome] = {
                "faltando": faltando,
                "extras": extras
            }

    return colunas_unicas, colunas_comuns, inconsistencias


def main():
    estruturas, arquivos_com_erro = coletar_colunas(CAMINHO_DADOS)

    print("\nResumo da análise de colunas")
    print("----------------------------")
    print(f"Arquivos analisados: {len(estruturas)}")
    print(f"Arquivos com erro: {len(arquivos_com_erro)}")

    if arquivos_com_erro:
        print("\nArquivos com erro:")
        for nome, erro in arquivos_com_erro:
            print(f"- {nome}: {erro}")

    if not estruturas:
        print("\nNenhuma estrutura encontrada.")
        return

    colunas_unicas, colunas_comuns, inconsistencias = analisar_estruturas(estruturas)

    print(f"\nTotal de colunas únicas: {len(colunas_unicas)}")
    print(f"Colunas presentes em todos os arquivos: {len(colunas_comuns)}")

    if not inconsistencias:
        print("\nTodas as planilhas possuem a mesma estrutura de colunas.")
    else:
        print("\nInconsistências encontradas:")

        for nome, problema in inconsistencias.items():
            print(f"\nArquivo: {nome}")

            if problema["faltando"]:
                print(f"  Colunas ausentes: {sorted(problema['faltando'])}")

            if problema["extras"]:
                print(f"  Colunas extras: {sorted(problema['extras'])}")


if __name__ == "__main__":
    main()