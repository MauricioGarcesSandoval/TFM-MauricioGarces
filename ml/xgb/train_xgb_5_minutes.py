import argparse
import glob
import pandas as pd
import numpy as np
import xgboost as xgb
import matplotlib.pyplot as plt

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
# 3. SAMPLING
# ============================================================

print("Sampling...")

pos_df = df[df["label_next_hour"] == 1]
neg_df = df[df["label_next_hour"] == 0] #.sample(frac=SAMPLE_FRAC, random_state=42)

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
# 6. BALANCE
# ============================================================

pos = y_train.sum()
neg = len(y_train) - pos

scale_pos_weight = float(neg) / float(pos)

print("\nClass stats:")
print("Positives:", int(pos))
print("Negatives:", int(neg))
print("scale_pos_weight:", scale_pos_weight)

# ============================================================
# 7. TRAIN
# ============================================================

print("\nTraining...")

model = xgb.XGBClassifier(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.1,
    subsample=0.8,
    colsample_bytree=0.8,
    scale_pos_weight=scale_pos_weight,
    eval_metric="logloss",
    n_jobs=-1
)

model.fit(X_train, y_train)

# ============================================================
# 8. EVALUATION
# ============================================================

print("\nEvaluating...")

y_pred_prob = model.predict_proba(X_test)[:, 1]

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
# 10. FEATURE IMPORTANCE
# ============================================================

print("\nTop features:")

importance = pd.Series(model.feature_importances_, index=X_train.columns)
print(importance.sort_values(ascending=False).head(10))

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
plt.savefig("graficas/minutes=5/roc_curve.png")
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
plt.savefig("graficas/minutes=5/pr_curve.png")
plt.close()

# ============================================================
# 13. THRESHOLD CURVE
# ============================================================

precisions_vals = []
recalls_vals = []

for t in thresholds:
    y_pred = (y_pred_prob > t).astype(int)

    tp = ((y_pred == 1) & (y_test == 1)).sum()
    fp = ((y_pred == 1) & (y_test == 0)).sum()
    fn = ((y_pred == 0) & (y_test == 1)).sum()

    precision_val = tp / (tp + fp + 1e-6)
    recall_val = tp / (tp + fn + 1e-6)

    precisions_vals.append(precision_val)
    recalls_vals.append(recall_val)

plt.figure()
plt.plot(thresholds, precisions_vals, label="Precision")
plt.plot(thresholds, recalls_vals, label="Recall")
plt.xlabel("Threshold")
plt.ylabel("Score")
plt.title("Precision / Recall vs Threshold")
plt.legend()
plt.grid()
plt.savefig("graficas/minutes=5/threshold_curve.png")
plt.close()

# ============================================================
# 14. FEATURE IMPORTANCE
# ============================================================

importance = pd.Series(model.feature_importances_, index=X_train.columns)
importance = importance.sort_values(ascending=True)

plt.figure(figsize=(8,6))
importance.plot(kind="barh")
plt.title("Feature Importance")
plt.xlabel("Importance")
plt.tight_layout()
plt.savefig("graficas/minutes=5/feature_importance.png")
plt.close()

print("\nTop features:")
print(importance.sort_values(ascending=False).head(10))

# ============================================================
# 13. SAVE MODEL
# ============================================================

model.save_model("modelos/xgboost_model_5m.json")

print("\nDONE ✅")
