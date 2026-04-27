# ⚽ Football Data & Prediction Platform

## 🎯 Objetivo

Construir um sistema completo de engenharia de dados, backtest e previsão para mercados esportivos, com foco em identificar oportunidades com **valor esperado positivo (EV+)** utilizando Machine Learning.

---

## 🚀 O que o projeto faz

* Coleta automatizada de dados esportivos via engenharia reversa do Flashscore:

  * Eventos históricos
  * Estatísticas detalhadas das partidas
  * Odds de múltiplos mercados (FT, HT, Over/Under, BTTS, etc.)
* Pipeline incremental com controle de estado
* Padronização de ligas e times (dicionários + fuzzy matching)
* Engenharia de features:

  * Médias móveis
  * Desvio padrão
  * Z-score
  * Estatísticas por liga e por time
* Backtest por mercado (gols, escanteios, etc.)
* Treinamento de modelos de Machine Learning:

  * Random Forest
  * LightGBM
  * XGBoost
* Seleção automática dos melhores modelos
* Geração de previsões com:

  * Probabilidade
  * Valor esperado (EV)
  * Threshold por liga
* Gestão de banca com **Kelly Criterion (fracionado)**

---

## ⚙️ Engenharia de Dados (Flashscore)

O projeto utiliza engenharia reversa de endpoints internos do Flashscore para coleta de dados históricos e odds.

Funcionalidades:

* Extração automatizada de ligas e eventos
* Coleta de feeds de partidas (estatísticas detalhadas)
* Captura de odds por mercado e bookmaker
* Pipeline incremental com controle de estado
* Normalização e consolidação em base única

Pipeline estruturado em etapas:

* 01 → Coleta de eventos por liga
* 02 → Coleta de feeds das partidas
* 03 → Coleta de odds
* 04 → Parsing e normalização
* 05 → Construção da base unificada

---

## 📊 Fonte de Dados

* Base histórica inicial:
  https://github.com/futpythontrader/YouTube
* Dados complementares obtidos via engenharia reversa (Flashscore)

---

## 🧠 Arquitetura do Projeto

```
football_saas/
├── 01_scripts/                    ← etapa 1: pipeline inicial de dados
├── 02_validation/                 ← etapa 2: validação e transformação
├── 03_backtest/                   ← etapa 3: backtest + features
│   ├── runner.py
│   ├── functions.py
│   ├── features.py
│   ├── gerator_config.py
│   └── config.json
├── 04_ml/                         ← etapa 4: machine learning
│   ├── 01_dataset_builder.py
│   ├── 02_train_model.py
│   ├── 03_predict.py
│   ├── 04_banca.py
│   ├── datasets/
│   └── models/
├── 05_flashscore/                 ← engenharia de dados (scraping)
│   ├── scripts/
│   ├── config/
│   ├── output/
│   ├── logs/
│   └── state/
├── data/
├── jogos_futuros/
```

---

## 📈 Status do Projeto

🟡 Em desenvolvimento ativo

Atualmente em fase de:

* Validação das previsões em ambiente real
* Análise de ROI por mercado
* Ajuste de thresholds e filtros de EV
* Refinamento da qualidade dos dados coletados

---

## 🔜 Próximos passos

* Refinamento por liga e mercado
* Feature engineering avançado
* Otimização de modelos
* Automação completa do pipeline
* Criação de API e dashboard

---

## ⚙️ Configuração

Crie um arquivo `.env` baseado no `.env.example`:

```bash
cp .env.example .env
```

Exemplo de variáveis:

```
API_FOOTBALL_KEY=your_api_key_here
API_FOOTBALL_HOST=api-football-v1.p.rapidapi.com
```

---

## ⚠️ Observações

* Dados históricos, modelos treinados, logs e outputs não são versionados
* Projeto com fins educacionais e analíticos
* Foco em estatística aplicada e machine learning em dados esportivos

---

## 💡 Sobre o projeto

Este projeto representa a construção de um sistema completo de análise esportiva baseado em dados, combinando:

* Engenharia de Dados
* Engenharia Reversa
* Machine Learning
* Análise Estatística

---

