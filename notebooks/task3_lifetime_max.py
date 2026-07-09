import sys
sys.path.append('..')
from src.models import *

FEATURE_COLS_LIFETIME = ['avg_cpu', 'max_cpu', 'avg_cpu_lag1', 'avg_cpu_lag2', 'avg_cpu_lag3', 'avg_cpu_rolling_mean_3', 'avg_cpu_rolling_std_3', 'core_bucket', 'memory_bucket', 'hour_sin', 'hour_cos']
TARGET_COL_LIFETIME = 'target_lifetime_max'

print("Загрузка данных...")
train_life = pd.read_csv('../data/train_lifetime.csv')
test_life = pd.read_csv('../data/test_lifetime.csv')

print("Подготовка признаков...")
for df in [train_life, test_life]:
    df = clean_bucket_columns(df)
    df = create_lag_features(df, cols=['avg_cpu', 'max_cpu'], lags=[1, 2, 3])
    df = create_rolling_features(df, cols=['avg_cpu'], windows=[3])
    df = create_temporal_features(df)

print("Подготовка данных для обучения...")
X_train, y_train, imputer, scaler = prepare_features(train_life, FEATURE_COLS_LIFETIME, TARGET_COL_LIFETIME)
X_test, y_test = prepare_features(test_life, FEATURE_COLS_LIFETIME, TARGET_COL_LIFETIME, fit_imputer_scaler=False, imputer=imputer, scaler=scaler)

X_tr, X_val, y_tr, y_val = train_test_split(X_train, y_train, test_size=0.2, random_state=42)

print("\n=== Random Forest с тюнингом (lifetime max) ===")
best_params_rf, best_value_rf = tune_random_forest(X_tr, y_tr, X_val, y_val, n_trials=20)
print(f"Best params: {best_params_rf}")
print(f"Best val MAE: {best_value_rf:.4f}")

rf_model = RandomForestRegressor(**best_params_rf, random_state=42, n_jobs=-1)
rf_model.fit(X_train, y_train)
pred_rf = rf_model.predict(X_test)
evaluate_model(y_test, pred_rf, "Random Forest (lifetime)")

print("\n=== Ridge (lifetime max) ===")
ridge = Ridge(alpha=1.0)
ridge.fit(X_train, y_train)
pred_ridge = ridge.predict(X_test)
evaluate_model(y_test, pred_ridge, "Ridge (lifetime)")

print("\n=== Ensemble (Random Forest + Ridge) ===")
pred_ensemble = (pred_rf + pred_ridge) / 2
evaluate_model(y_test, pred_ensemble, "Ensemble (lifetime)")

print("\n=== SHAP Analysis ===")
importance_df, _ = explain_with_shap(rf_model, X_test[:100], FEATURE_COLS_LIFETIME, 'random_forest')
print("Top 10 features:")
print(importance_df.head(10))
