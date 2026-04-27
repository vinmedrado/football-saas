import pandas as pd
import numpy as np
import os
import pickle

from sklearn.ensemble import RandomForestClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import roc_auc_score, accuracy_score, brier_score_loss

import lightgbm as lgb
import xgboost as xgb

# ==============================
# CONFIG
# ==============================
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(BASE_DIR, "datasets")
MODEL_DIR   = os.path.join(BASE_DIR, "models")

os.makedirs(MODEL_DIR, exist_ok=True)

CONFIDENCE_THRESHOLD = 0.60

# ROI mínimo do backtest para treinar modelo
# (filtra mercados que passaram o MIN_ROI=0 mas têm ROI muito baixo)
MIN_ROI_BT = 0.0

# ==============================
# LOAD META DOS MERCADOS
# ==============================
print("=" * 55)
print("📂 Carregando meta dos mercados...")

meta_path = os.path.join(DATASET_DIR, "mercados_meta.pkl")
if not os.path.exists(meta_path):
    raise FileNotFoundError("❌ mercados_meta.pkl não encontrado. Rode 01_dataset_builder.py primeiro.")

with open(meta_path, "rb") as f:
    mercados_meta = pickle.load(f)

mercados_meta = [m for m in mercados_meta if m["roi_bt"] >= MIN_ROI_BT]
print(f"  ✅ {len(mercados_meta)} mercados para treinar")

# ==============================
# DEFINIÇÃO DOS MODELOS
# ==============================
def get_models(y_train):
    scale = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
    return {
        "Random Forest": RandomForestClassifier(
            n_estimators=300,
            max_depth=10,
            min_samples_leaf=20,
            class_weight="balanced",
            n_jobs=-1,
            random_state=42,
        ),
        "LightGBM": lgb.LGBMClassifier(
            n_estimators=500,
            learning_rate=0.05,
            num_leaves=31,
            min_child_samples=20,
            class_weight="balanced",
            n_jobs=-1,
            random_state=42,
            verbose=-1,
        ),
        "XGBoost": xgb.XGBClassifier(
            n_estimators=500,
            learning_rate=0.05,
            max_depth=6,
            min_child_weight=20,
            scale_pos_weight=scale,
            eval_metric="logloss",
            n_jobs=-1,
            random_state=42,
            verbosity=0,
        ),
    }

# ==============================
# TREINO POR MERCADO
# ==============================
print("\n" + "=" * 55)
print("🏋️  Treinando modelos por mercado...\n")

resumo_modelos = []

for meta in mercados_meta:

    market     = meta["market"]
    event      = meta["event"]
    roi_bt     = meta["roi_bt"]
    market_dir = os.path.join(DATASET_DIR, market)

    print(f"  🔄 [{market}] {event} | ROI_bt={roi_bt:+.3f}")

    # --- Carrega datasets ---
    X_train = pd.read_csv(os.path.join(market_dir, "X_train.csv"))
    y_train = pd.read_csv(os.path.join(market_dir, "y_train.csv")).squeeze()
    X_test  = pd.read_csv(os.path.join(market_dir, "X_test.csv"))
    y_test  = pd.read_csv(os.path.join(market_dir, "y_test.csv")).squeeze()

    models  = get_models(y_train)
    results = {}

    for name, model in models.items():
        calibrated = CalibratedClassifierCV(model, method="isotonic", cv=3)
        calibrated.fit(X_train, y_train)

        y_prob = calibrated.predict_proba(X_test)[:, 1]
        y_pred = (y_prob >= 0.5).astype(int)

        auc   = roc_auc_score(y_test, y_prob)
        acc   = accuracy_score(y_test, y_pred)
        brier = brier_score_loss(y_test, y_prob)

        mask_conf     = (y_prob >= CONFIDENCE_THRESHOLD) | (y_prob <= (1 - CONFIDENCE_THRESHOLD))
        coverage      = mask_conf.mean()
        acc_confident = accuracy_score(y_test[mask_conf], y_pred[mask_conf]) if mask_conf.sum() > 0 else 0.0

        results[name] = {
            "model"        : calibrated,
            "auc"          : auc,
            "accuracy"     : acc,
            "brier"        : brier,
            "coverage"     : coverage,
            "acc_confident": acc_confident,
            "score"        : 0.6 * auc + 0.4 * acc_confident,
        }

    # --- Melhor modelo ---
    best_name = max(results, key=lambda n: results[n]["score"])
    best      = results[best_name]

    print(f"     🥇 {best_name} | AUC={best['auc']:.4f} | Acurácia≥{int(CONFIDENCE_THRESHOLD*100)}%={best['acc_confident']:.4f} | Cobertura={best['coverage']:.1%}")

    # --- Salva modelo do mercado ---
    model_market_dir = os.path.join(MODEL_DIR, market)
    os.makedirs(model_market_dir, exist_ok=True)

    with open(os.path.join(model_market_dir, "model.pkl"), "wb") as f:
        pickle.dump(best["model"], f)

    model_meta = {
        "market"              : market,
        "event"               : event,
        "model_name"          : best_name,
        "auc"                 : best["auc"],
        "accuracy"            : best["accuracy"],
        "brier"               : best["brier"],
        "score"               : best["score"],
        "coverage"            : best["coverage"],
        "acc_confident"       : best["acc_confident"],
        "roi_bt"              : roi_bt,
        "winrate_bt"          : meta["winrate_bt"],
        "confidence_threshold": CONFIDENCE_THRESHOLD,
    }

    with open(os.path.join(model_market_dir, "meta.pkl"), "wb") as f:
        pickle.dump(model_meta, f)

    resumo_modelos.append(model_meta)

# ==============================
# RELATÓRIO FINAL
# ==============================
print("\n" + "=" * 55)
print("📊 RESUMO DOS MODELOS POR MERCADO:")
print("=" * 55)

df_resumo = pd.DataFrame(resumo_modelos).sort_values("score", ascending=False)

for _, r in df_resumo.iterrows():
    print(
        f"  {r['market']:<12} | {r['model_name']:<15} | "
        f"AUC={r['auc']:.4f} | "
        f"Acurácia≥{int(CONFIDENCE_THRESHOLD*100)}%={r['acc_confident']:.4f} | "
        f"ROI_bt={r['roi_bt']:+.3f}"
    )

# Salva resumo global dos modelos
resumo_path = os.path.join(MODEL_DIR, "resumo_modelos.pkl")
with open(resumo_path, "wb") as f:
    pickle.dump(resumo_modelos, f)

print(f"\n✅ {len(resumo_modelos)} modelos treinados e salvos em: {MODEL_DIR}")
print("=" * 55)
