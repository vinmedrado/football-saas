import pandas as pd
from pathlib import Path

# ==============================
# Configurações
# ==============================
CAMINHO_BASE_LIGAS = Path("data/base_ligas.csv")
CAMINHO_SAIDA = Path("data/dicionario_times.csv")

# ==============================
# Função principal
# ==============================
def gerar_dicionario_times():
    # Carrega base de jogos padronizada
    df = pd.read_csv(CAMINHO_BASE_LIGAS)

    # Lista de países únicos
    paises = df["country"].dropna().unique()

    lista_times = []

    for pais in paises:
        df_pais = df[df["country"] == pais]

        # Lista de ligas do país
        ligas = df_pais["League_padronizada"].dropna().unique()

        for liga in ligas:
            df_liga = df_pais[df_pais["League_padronizada"] == liga]

            # Lista de times únicos na liga
            times_home = df_liga["Home"].dropna().unique()
            times_away = df_liga["Away"].dropna().unique()

            times_unicos = set(times_home) | set(times_away)

            for time in times_unicos:
                lista_times.append({
                    "country": pais,
                    "League_padronizada": liga,
                    "Time_original": time,
                    "Time_padronizado": time,  # inicialmente igual ao original
                    "Novo": False               # usado para marcar times novos futuramente
                })

    # Cria DataFrame final
    df_dicionario = pd.DataFrame(lista_times)

    # Ordena por país, liga e nome do time
    df_dicionario = df_dicionario.sort_values(["country", "League_padronizada", "Time_original"]).reset_index(drop=True)

    # Cria pasta de saída se não existir
    CAMINHO_SAIDA.parent.mkdir(parents=True, exist_ok=True)

    # Salva CSV
    df_dicionario.to_csv(CAMINHO_SAIDA, index=False)
    print(f"Dicionário de times salvo em: {CAMINHO_SAIDA}")
    print(f"Total de times: {len(df_dicionario)}")


# ==============================
# Executar script
# ==============================
if __name__ == "__main__":
    gerar_dicionario_times()