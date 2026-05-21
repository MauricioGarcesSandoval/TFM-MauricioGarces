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

parser = argparse.ArgumentParser(description="Train Logistic Regression + plots")

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
# 3. SAMPLING
# ============================================================

print("Sampling...")

pos_df = df[df["label_next_hour"] == 1]
neg_df = df[df["label_next_hour"] == 0]  # .sample(frac=SAMPLE_FRAC, random_state=42)

df = pd.concat([pos_df, neg_df])

print("After sampling:", len(df))

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
# CLEAN NUMERICAL ISSUES
# ============================================================
numeric_cols = df.select_dtypes(include=[np.number]).columns

df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], np.nan)
df = df.dropna()

# ============================================================
# 5. SPLIT TEMPORAL
# ============================================================

df = df.sort_values("feature_window_start")

split_index = int(len(df) * 0.8)

train = df.iloc[:split_index]
test = df.iloc[split_index:]

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
    class_weight="balanced",
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
# 9. MULTI-THRESHOLD ANALYSIS (importante)
# ============================================================

print("\nThreshold analysis:")

for t in [0.05, 0.1, 0.2]:
    y_pred = (y_pred_prob > t).astype(int)
    print("\nThreshold:", t)
    print(classification_report(y_test, y_pred))

# ============================================================
# OUTPUT DIRS
# ============================================================

os.makedirs("graficas_lr/minutes=5", exist_ok=True)

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
plt.savefig("graficas_lr/minutes=5/roc_curve.png")
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
plt.savefig("graficas_lr/minutes=5/pr_curve.png")
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

joblib.dump(model, "lr_model_5m.json")

print("\nDONE ✅")
