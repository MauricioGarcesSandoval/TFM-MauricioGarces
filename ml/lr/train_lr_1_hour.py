import os
import argparse
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    roc_auc_score,
    average_precision_score,
    classification_report,
    roc_curve,
    precision_recall_curve
)

# ============================================================
# ARGUMENTOS
# ============================================================

parser = argparse.ArgumentParser(description="Train XGBoost + plots")

parser.add_argument("--input", required=True, help="Path parquet (wildcards)")
parser.add_argument("--sample-frac", type=float, default=0.05)

args = parser.parse_args()

INPUT_PATH = args.input
SAMPLE_FRAC = args.sample_frac

# ============================================================
# 1. LOAD DATA
# ============================================================

print("Loading parquet files...")

files = glob.glob(INPUT_PATH)

dfs = []
for f in files:
    print("Reading:", f)
    df_part = pd.read_parquet(f)
    dfs.append(df_part)

df = pd.concat(dfs, ignore_index=True)

print("Total rows:", len(df))

# ============================================================
# 2. CLEAN
# ============================================================

for c in ["target_window_start", "label_feature_window"]:
    if c in df.columns:
        df = df.drop(columns=[c])

# ============================================================
# 4. FEATURE ENGINEERING
# ============================================================

print("Feature engineering...")

df["feature_window_start"] = pd.to_datetime(df["feature_window_start"])

df["hour"] = df["feature_window_start"].dt.hour
df["dayofweek"] = df["feature_window_start"].dt.dayofweek

df["is_night"] = ((df["hour"] <= 6) | (df["hour"] >= 22)).astype(int)
df["is_weekend"] = (df["dayofweek"] >= 5).astype(int)

df["log_intensity"] = df["num_logs"] / (df["num_logs"].mean() + 1e-6)

if "host" in df.columns:
    df = df.drop(columns=["host"])

# ============================================================
# 5. TEMPORAL SPLIT
# ============================================================

print("Temporal split...")

df = df.sort_values("feature_window_start")

split_index = int(len(df) * 0.8)

train = df.iloc[:split_index].copy()
test = df.iloc[split_index:].copy()

print(f"Train before sampling: {len(train)}")
print(f"Test: {len(test)}")

# ============================================================
# 5.1 SUBSAMPLING ONLY TRAIN
# ============================================================

print("Sampling only training negatives...")

train_pos = train[train["label_next_hour"] == 1]
train_neg = train[train["label_next_hour"] == 0].sample(
    frac=SAMPLE_FRAC,
    random_state=42
)

train = pd.concat([train_pos, train_neg])

train = train.sample(frac=1, random_state=42).reset_index(drop=True)

print(f"Train after sampling: {len(train)}")
print(f"Positive train: {train['label_next_hour'].sum()}")
print(f"Negative train: {(train['label_next_hour'] == 0).sum()}")

print(f"Positive test: {test['label_next_hour'].sum()}")
print(f"Negative test: {(test['label_next_hour'] == 0).sum()}")

X_train = train.drop(columns=["label_next_hour", "feature_window_start", "month", "year"])
y_train = train["label_next_hour"]

X_test = test.drop(columns=["label_next_hour", "feature_window_start", "month", "year"])
y_test = test["label_next_hour"]

# ============================================================
# 6. ESCALADO
# ============================================================
scaler = StandardScaler()

X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.fit_transform(X_test)

# ============================================================
# 7. TRAIN
# ============================================================

print("\nTraining logistic regression")

model = LogisticRegression(
    max_iter=1000,
    n_jobs=-1
)

model.fit(X_train_scaled, y_train)

# ============================================================
# 8. EVALUATION
# ============================================================

print("\nEvaluating...")

y_pred_prob = model.predict_proba(X_test_scaled)[:, 1]

roc_auc = roc_auc_score(y_test, y_pred_prob)
pr_auc = average_precision_score(y_test, y_pred_prob)

print("ROC AUC:", roc_auc)
print("PR AUC:", pr_auc)

# ============================================================
# 9. MULTI-THRESHOLD ANALYSIS
# ============================================================

print("\nThreshold analysis:")

for t in [0.05, 0.1, 0.2]:
    y_pred = (y_pred_prob > t).astype(int)
    print("\nThreshold:", t)
    print(classification_report(y_test, y_pred))

# ============================================================
# OUTPUT DIRS
# ============================================================

os.makedirs("graficas_lr/hour=1", exist_ok=True)

# ============================================================
# 11. ROC CURVE
# ============================================================

fpr, tpr, _ = roc_curve(y_test, y_pred_prob)

plt.figure()
plt.plot(fpr, tpr, label="Model")
plt.plot([0, 1], [0, 1], linestyle="--", label="Random")
plt.xlabel("FPR")
plt.ylabel("TPR")
plt.title("ROC Curve")
plt.legend()
plt.grid()
plt.savefig("graficas_lr/hour=1/roc_curve.png")
plt.close()

# ============================================================
# 12. PRECISION-RECALL CURVE
# ============================================================

precision, recall, thresholds = precision_recall_curve(y_test, y_pred_prob)

plt.figure()
plt.plot(recall, precision)
plt.xlabel("Recall")
plt.ylabel("Precision")
plt.title("Precision-Recall Curve")
plt.grid()
plt.savefig("graficas_lr/hour=1/pr_curve.png")
plt.close()

# ============================================================
# 14. FEATURE IMPORTANCE
# ============================================================

coefficients = pd.Series(model.coef_[0], index=X_train.columns)
coefficients = coefficients.sort_values(ascending=True)

print("\nTop possitive features:")
print(coefficients.tail(10))

print("\nTop negative features:")
print(coefficients.head(10))

# ============================================================
# 13. SAVE MODEL
# ============================================================
import joblib
joblib.dump(model, "modelos/lr_model_1h.json")

print("\nDONE")
