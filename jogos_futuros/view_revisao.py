#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import questionary

# ==========================================================
# CONFIG
# ==========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ARQ_REVISAO = os.path.join(BASE_DIR, "data", "times_flash_revisao.csv")
ARQ_FLASH = os.path.join(BASE_DIR, "data", "dicionario_times_flash.csv")

COLS_REVISAO = [
    "League_flash",
    "League_padronizada",
    "Time_flash",
    "Time_padronizado_sugerido",
    "Score",
    "Motivo",
    "Source_URL"
]

COLS_FLASH = [
    "Time_flash",
    "Time_padronizado"
]

# ==========================================================
# UTIL CSV
# ==========================================================
def carregar_csv(caminho, colunas_padrao=None):
    if not os.path.exists(caminho):
        if colunas_padrao is None:
            return pd.DataFrame()
        return pd.DataFrame(columns=colunas_padrao)

    df = pd.read_csv(caminho, encoding="utf-8-sig")
    df.columns = df.columns.astype(str).str.strip()
    return df


def salvar_csv(df, caminho):
    df.to_csv(caminho, index=False, encoding="utf-8-sig")


def garantir_arquivos():
    os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)

    if not os.path.exists(ARQ_FLASH):
        salvar_csv(pd.DataFrame(columns=COLS_FLASH), ARQ_FLASH)

    if not os.path.exists(ARQ_REVISAO):
        salvar_csv(pd.DataFrame(columns=COLS_REVISAO), ARQ_REVISAO)


# ==========================================================
# CARGA / NORMALIZAÇÃO
# ==========================================================
def carregar_dados():
    garantir_arquivos()

    df_revisao = carregar_csv(ARQ_REVISAO, COLS_REVISAO)
    df_flash = carregar_csv(ARQ_FLASH, COLS_FLASH)

    for col in COLS_REVISAO:
        if col not in df_revisao.columns:
            df_revisao[col] = ""

    for col in ["League_flash", "League_padronizada", "Time_flash", "Time_padronizado_sugerido", "Motivo", "Source_URL"]:
        df_revisao[col] = df_revisao[col].fillna("").astype(str).str.strip()

    df_revisao["Score"] = pd.to_numeric(df_revisao["Score"], errors="coerce").fillna(0).astype(int)

    for col in COLS_FLASH:
        if col not in df_flash.columns:
            df_flash[col] = ""
        df_flash[col] = df_flash[col].fillna("").astype(str).str.strip()

    return df_revisao, df_flash


def salvar_dados(df_revisao, df_flash):
    if not df_flash.empty:
        df_flash = (
            df_flash
            .drop_duplicates(subset=["Time_flash"], keep="first")
            .sort_values("Time_flash")
            .reset_index(drop=True)
        )

    if not df_revisao.empty:
        df_revisao = (
            df_revisao
            .sort_values(by=["Score", "League_padronizada", "Time_flash"], ascending=[False, True, True])
            .reset_index(drop=True)
        )

    salvar_csv(df_revisao, ARQ_REVISAO)
    salvar_csv(df_flash, ARQ_FLASH)


# ==========================================================
# FILTROS
# ==========================================================
def filtrar_revisao(df, filtro):
    if filtro == "Score >= 85":
        return df[df["Score"] >= 85].copy()

    if filtro == "Score 80 a 84":
        return df[(df["Score"] >= 80) & (df["Score"] <= 84)].copy()

    if filtro == "Score 70 a 79":
        return df[(df["Score"] >= 70) & (df["Score"] <= 79)].copy()

    if filtro == "Score < 70":
        return df[df["Score"] < 70].copy()

    if filtro == "Todos":
        return df.copy()

    return pd.DataFrame(columns=df.columns)


# ==========================================================
# AÇÕES
# ==========================================================
def aprovar_registro(df_flash, time_flash, time_padronizado):
    novo = pd.DataFrame([{
        "Time_flash": str(time_flash).strip(),
        "Time_padronizado": str(time_padronizado).strip()
    }])

    df_flash = pd.concat([df_flash, novo], ignore_index=True)
    df_flash = df_flash.drop_duplicates(subset=["Time_flash"], keep="first")
    return df_flash


def obter_mask_registro(df_revisao, row):
    return (
        (df_revisao["League_flash"] == row["League_flash"]) &
        (df_revisao["League_padronizada"] == row["League_padronizada"]) &
        (df_revisao["Time_flash"] == row["Time_flash"]) &
        (df_revisao["Time_padronizado_sugerido"] == row["Time_padronizado_sugerido"]) &
        (df_revisao["Score"] == int(row["Score"]))
    )


def mostrar_detalhe(row):
    print("\n" + "=" * 80)
    print("DETALHE DO REGISTRO")
    print("=" * 80)
    print(f"Liga Flash       : {row.get('League_flash', '')}")
    print(f"Liga Padronizada : {row.get('League_padronizada', '')}")
    print(f"Time Flash       : {row.get('Time_flash', '')}")
    print(f"Sugestão         : {row.get('Time_padronizado_sugerido', '')}")
    print(f"Score            : {row.get('Score', '')}")
    print(f"Motivo           : {row.get('Motivo', '')}")
    print(f"URL              : {row.get('Source_URL', '')}")
    print("=" * 80 + "\n")


# ==========================================================
# QUESTIONARY MENUS
# ==========================================================
def escolher_filtro():
    return questionary.select(
        "Escolha o filtro:",
        choices=[
            "Score >= 85",
            "Score 80 a 84",
            "Score 70 a 79",
            "Score < 70",
            "Todos",
            "Sair"
        ]
    ).ask()


def escolher_acao_item():
    return questionary.select(
        "O que deseja fazer com este item?",
        choices=[
            "Aprovar sugestão",
            "Editar e aprovar",
            "Rejeitar",
            "Ver detalhe",
            "Voltar"
        ]
    ).ask()


def escolher_item_ou_comando(df_filtrado, filtro):
    if df_filtrado.empty:
        questionary.print("Nenhum registro nesse filtro.", style="bold fg:yellow")
        return {"tipo": "voltar"}

    choices = [
        questionary.Choice(
            title=f"[MODO COMANDO] Trabalhar em lote no filtro: {filtro}",
            value={"tipo": "comando"}
        ),
        questionary.Choice(
            title="[VOLTAR]",
            value={"tipo": "voltar"}
        )
    ]

    for i, (_, row) in enumerate(df_filtrado.iterrows(), start=1):
        flash = str(row["Time_flash"])[:32]
        sugestao = str(row["Time_padronizado_sugerido"])[:32]
        score = int(row["Score"])
        titulo = f"{i:>3} | {flash:<32} -> {sugestao:<32} ({score})"

        choices.append(
            questionary.Choice(
                title=titulo,
                value={"tipo": "item", "indice": i}
            )
        )

    return questionary.select(
        f"Selecione um item no filtro '{filtro}' (↑ ↓ ENTER):",
        choices=choices
    ).ask()


# ==========================================================
# MODO ITEM
# ==========================================================
def processar_item(df_revisao, df_flash, df_filtrado, indice):
    _, row = list(df_filtrado.iterrows())[indice - 1]

    while True:
        acao = escolher_acao_item()

        if acao is None or acao == "Voltar":
            return df_revisao, df_flash

        if acao == "Ver detalhe":
            mostrar_detalhe(row)
            continue

        if acao == "Aprovar sugestão":
            sugestao = str(row["Time_padronizado_sugerido"]).strip()
            if not sugestao:
                questionary.print("Esse item não tem sugestão.", style="bold fg:red")
                continue

            df_flash = aprovar_registro(df_flash, row["Time_flash"], sugestao)
            mask = obter_mask_registro(df_revisao, row)
            df_revisao = df_revisao.loc[~mask].copy()
            salvar_dados(df_revisao, df_flash)

            questionary.print(
                f"Aprovado: {row['Time_flash']} -> {sugestao}",
                style="bold fg:green"
            )
            return df_revisao, df_flash

        if acao == "Editar e aprovar":
            novo_nome = questionary.text(
                "Digite o Time_padronizado correto:"
            ).ask()

            if not novo_nome:
                questionary.print("Edição cancelada.", style="fg:yellow")
                continue

            df_flash = aprovar_registro(df_flash, row["Time_flash"], novo_nome)
            mask = obter_mask_registro(df_revisao, row)
            df_revisao = df_revisao.loc[~mask].copy()
            salvar_dados(df_revisao, df_flash)

            questionary.print(
                f"Salvo: {row['Time_flash']} -> {novo_nome}",
                style="bold fg:green"
            )
            return df_revisao, df_flash

        if acao == "Rejeitar":
            confirmar = questionary.confirm(
                f"Remover '{row['Time_flash']}' da revisão?"
            ).ask()

            if confirmar:
                mask = obter_mask_registro(df_revisao, row)
                df_revisao = df_revisao.loc[~mask].copy()
                salvar_dados(df_revisao, df_flash)

                questionary.print(
                    f"Rejeitado/removido: {row['Time_flash']}",
                    style="bold fg:yellow"
                )
                return df_revisao, df_flash


# ==========================================================
# MODO COMANDO EM LOTE
# ==========================================================
def parse_indices(texto, total):
    texto = texto.strip().lower()

    if texto == "all":
        return list(range(1, total + 1))

    partes = [p.strip() for p in texto.split(",") if p.strip()]
    indices = []

    for parte in partes:
        if "-" in parte:
            try:
                inicio, fim = parte.split("-", 1)
                inicio = int(inicio)
                fim = int(fim)
                if inicio <= fim:
                    indices.extend(range(inicio, fim + 1))
            except ValueError:
                continue
        else:
            try:
                indices.append(int(parte))
            except ValueError:
                continue

    indices = sorted(set(i for i in indices if 1 <= i <= total))
    return indices


def mostrar_lista_lote(df_filtrado, filtro):
    print("\n" + "=" * 100)
    print(f"FILTRO: {filtro} | REGISTROS: {len(df_filtrado)}")
    print("=" * 100)

    if df_filtrado.empty:
        print("Nenhum registro.")
        print("=" * 100)
        return

    for i, (_, row) in enumerate(df_filtrado.iterrows(), start=1):
        flash = str(row["Time_flash"])[:34]
        sugestao = str(row["Time_padronizado_sugerido"])[:34]
        score = int(row["Score"])
        print(f"{i:>3}  {flash:<34} -> {sugestao:<34} ({score})")

    print("=" * 100)
    print("Comandos:")
    print("a all        = aprovar todos")
    print("a 1,2,5      = aprovar itens")
    print("r 3,4        = rejeitar itens")
    print("e 2          = editar item")
    print("v 2          = ver detalhe")
    print("0            = voltar")
    print("=" * 100)


def selecionar_rows(df_filtrado, indices):
    base = list(df_filtrado.iterrows())
    selecionados = []

    for idx in indices:
        _, row = base[idx - 1]
        selecionados.append(row)

    return selecionados


def processar_aprovacao_lote(df_revisao, df_flash, selecionados):
    aprovados = 0
    pulados = 0

    for row in selecionados:
        sugestao = str(row["Time_padronizado_sugerido"]).strip()
        time_flash = str(row["Time_flash"]).strip()

        if not sugestao:
            pulados += 1
            continue

        df_flash = aprovar_registro(df_flash, time_flash, sugestao)
        mask = obter_mask_registro(df_revisao, row)
        df_revisao = df_revisao.loc[~mask].copy()
        aprovados += 1

    salvar_dados(df_revisao, df_flash)

    print(f"\nAprovados: {aprovados}")
    if pulados:
        print(f"Sem sugestão, pulados: {pulados}")

    return df_revisao, df_flash


def processar_rejeicao_lote(df_revisao, df_flash, selecionados):
    removidos = 0

    for row in selecionados:
        mask = obter_mask_registro(df_revisao, row)
        antes = len(df_revisao)
        df_revisao = df_revisao.loc[~mask].copy()
        if len(df_revisao) < antes:
            removidos += 1

    salvar_dados(df_revisao, df_flash)
    print(f"\nRejeitados/removidos: {removidos}")

    return df_revisao, df_flash


def editar_item_lote(df_revisao, df_flash, df_filtrado, indice_texto):
    try:
        indice = int(indice_texto)
    except ValueError:
        print("Índice inválido.")
        return df_revisao, df_flash

    if not (1 <= indice <= len(df_filtrado)):
        print("Índice fora da faixa.")
        return df_revisao, df_flash

    _, row = list(df_filtrado.iterrows())[indice - 1]
    mostrar_detalhe(row)

    novo_nome = input("Digite o Time_padronizado correto: ").strip()
    if not novo_nome:
        print("Edição cancelada.")
        return df_revisao, df_flash

    df_flash = aprovar_registro(df_flash, row["Time_flash"], novo_nome)
    mask = obter_mask_registro(df_revisao, row)
    df_revisao = df_revisao.loc[~mask].copy()
    salvar_dados(df_revisao, df_flash)

    print(f"\nSalvo: {row['Time_flash']} -> {novo_nome}")
    return df_revisao, df_flash


def ver_item_lote(df_filtrado, indice_texto):
    try:
        indice = int(indice_texto)
    except ValueError:
        print("Índice inválido.")
        return

    if not (1 <= indice <= len(df_filtrado)):
        print("Índice fora da faixa.")
        return

    _, row = list(df_filtrado.iterrows())[indice - 1]
    mostrar_detalhe(row)


def modo_comando(df_revisao, df_flash, filtro):
    while True:
        df_filtrado = filtrar_revisao(df_revisao, filtro)
        mostrar_lista_lote(df_filtrado, filtro)

        if df_filtrado.empty:
            input("Pressione ENTER para voltar...")
            return df_revisao, df_flash

        comando = input("Digite o comando: ").strip()

        if not comando:
            continue

        if comando == "0":
            return df_revisao, df_flash

        partes = comando.split(maxsplit=1)
        acao = partes[0].lower()

        if acao in ("a", "r"):
            if len(partes) < 2:
                print("Informe os índices.")
                continue

            indices = parse_indices(partes[1], len(df_filtrado))
            if not indices:
                print("Nenhum índice válido.")
                continue

            selecionados = selecionar_rows(df_filtrado, indices)

            if acao == "a":
                df_revisao, df_flash = processar_aprovacao_lote(df_revisao, df_flash, selecionados)
            else:
                df_revisao, df_flash = processar_rejeicao_lote(df_revisao, df_flash, selecionados)

        elif acao == "e":
            if len(partes) < 2:
                print("Informe um índice para editar.")
                continue
            df_revisao, df_flash = editar_item_lote(df_revisao, df_flash, df_filtrado, partes[1])

        elif acao == "v":
            if len(partes) < 2:
                print("Informe um índice para visualizar.")
                continue
            ver_item_lote(df_filtrado, partes[1])

        else:
            print("Comando inválido.")


# ==========================================================
# FLUXO PRINCIPAL
# ==========================================================
def main():
    df_revisao, df_flash = carregar_dados()

    while True:
        print("\n" + "=" * 60)
        print("REVISOR DE TIMES FLASH")
        print("=" * 60)
        print(f"Pendentes na revisão : {len(df_revisao)}")
        print(f"No dicionário flash  : {len(df_flash)}")
        print("=" * 60)

        filtro = escolher_filtro()

        if filtro is None or filtro == "Sair":
            print("Saindo...")
            break

        while True:
            df_filtrado = filtrar_revisao(df_revisao, filtro)

            escolha = escolher_item_ou_comando(df_filtrado, filtro)

            if not escolha or escolha["tipo"] == "voltar":
                break

            if escolha["tipo"] == "comando":
                df_revisao, df_flash = modo_comando(df_revisao, df_flash, filtro)
                continue

            if escolha["tipo"] == "item":
                df_revisao, df_flash = processar_item(
                    df_revisao=df_revisao,
                    df_flash=df_flash,
                    df_filtrado=df_filtrado,
                    indice=escolha["indice"]
                )


if __name__ == "__main__":
    main()