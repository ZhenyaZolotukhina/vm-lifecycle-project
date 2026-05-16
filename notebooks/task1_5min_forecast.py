 import *

STATIC_COLS = ['core_bucket', 'memory_bucket']
LOOKBACK = 12

print("Загрузка данных...")
train_5min = pd.read_csv('../data/train_5min.csv')
test_5min = pd.read_csv('../data/test_5min.csv')

print("Подготовка признаков...")
for df in [train_5min, test_5min]:
    df = clean_bucket_columns(df)
    df = create_lag_features(df, cols=['avg_cpu', 'max_cpu'], lags=[1, 2, 3])
    df = create_rolling_features(df, cols=['avg_cpu'], windows=[3])
    df = create_temporal_features(df)

print("\n=== ARIMA (5min) ===")
arima_results = []
for vmid in test_5min['vmid'].unique()[:10]:
    vm_data = test_5min[test_5min['vmid'] == vmid].sort_values('timestamp')
    series = vm_data['avg_cpu'].values
    if len(series) > 20:
        train_size = int(len(series) * 0.8)
        model = ARIMA(series[:train_size], order=(5, 1, 0))
        model_fit = model.fit()
        pred = model_fit.forecast(steps=len(series[train_size:]))
        mae = mean_absolute_error(series[train_size:], pred)
        arima_results.append(mae)
print(f"ARIMA MAE: {np.mean(arima_results):.4f}")

print("\n=== LSTM (5min) ===")
pred, actual, model, history = train_lstm(train_5min, test_5min, lookback=LOOKBACK, static_cols=STATIC_COLS, epochs=30)
evaluate_model(actual.flatten(), pred.flatten(), "LSTM (5min)")
