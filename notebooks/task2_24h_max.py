import sys
sys.path.append('..')
from src.models import *
import lightgbm as lgb

FEATURE_COLS_24H = ['avg_cpu', 'max_cpu', 'avg_cpu_lag1', 'avg_cpu_lag2', 'avg_cpu_lag3', 'avg_cpu_rolling_mean_3', 'avg_cpu_rolling_std_3', 'core_bucket', 'memory_bucket', 'hour_sin', 'hour_cos', 'dow_sin', 'dow_cos']
TARGET_COL_24H = 'target_24h_max'

print("Загрузка данных...")
train_24h = pd.read_csv('../data/train_24h.csv')
test_24h = pd.read_csv('../data/test_24h.csv')

print("Подготовка признаков...")
for df in [train_24h, test_24h]:
    df = clean_bucket_columns(df)
    df = create_lag_features(df, cols=['avg_cpu', 'max_cpu'], lags=[1, 2, 3])
    df = create_rolling_features(df, cols=['avg_cpu'], windows=[3])
    df = create_temporal_features(df)

print("Подготовка данных для обучения...")
X_train, y_train, imputer, scaler = prepare_features(train_24h, FEATURE_COLS_24H, TARGET_COL_24H)
X_test, y_test = prepare_features(test_24h, FEATURE_COLS_24H, TARGET_COL_24H, fit_imputer_scaler=False, imputer=imputer, scaler=scaler)

X_tr, X_val, y_tr, y_val = train_test_split(X_train, y_train, test_size=0.2, random_state=42)

print("\n=== LightGBM с тюнингом (24h max) ===")
best_params, best_value = tune_lightgbm(X_tr, y_tr, X_val, y_val, n_trials=20)
print(f"Best params: {best_params}")
print(f"Best val MAE: {best_value:.4f}")

lgb_model = lgb.LGBMRegressor(**best_params, random_state=42, n_jobs=-1, verbose=-1)
lgb_model.fit(X_train, y_train)
pred = lgb_model.predict(X_test)
evaluate_model(y_test, pred, "LightGBM (24h)")

print("\n=== SHAP Analysis ===")
importance_df, _ = explain_with_shap(lgb_model, X_test[:100], FEATURE_COLS_24H, 'lightgbm')
print("Top 10 features:")
print(importance_df.head(10))
