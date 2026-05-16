"""
My ML models for VM lifecycle prediction
Author: Mikhail
"""

import optuna
import shap
import lightgbm as lgb
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from tensorflow.keras.models import Model
from tensorflow.keras.layers import LSTM, Dense, Dropout, Input, Concatenate
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.optimizers import Adam

# ============= OPTUNA TUNING FUNCTIONS =============

def objective_lightgbm(trial, X_train, y_train, X_val, y_val):
    params = {
        'n_estimators': trial.suggest_int('n_estimators', 300, 1000, step=100),
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
        'max_depth': trial.suggest_int('max_depth', 5, 15),
        'num_leaves': trial.suggest_int('num_leaves', 20, 100),
        'subsample': trial.suggest_float('subsample', 0.6, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
        'reg_alpha': trial.suggest_float('reg_alpha', 0.0, 1.0),
        'reg_lambda': trial.suggest_float('reg_lambda', 0.0, 1.0),
        'random_state': 42,
        'n_jobs': -1,
        'verbose': -1
    }
    model = lgb.LGBMRegressor(**params)
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], eval_metric='mae', 
              callbacks=[lgb.early_stopping(50)], verbose=False)
    pred = model.predict(X_val)
    from sklearn.metrics import mean_absolute_error
    return mean_absolute_error(y_val, pred)

def tune_lightgbm(X_train, y_train, X_val, y_val, n_trials=30):
    study = optuna.create_study(direction='minimize', sampler=optuna.samplers.TPESampler(seed=42))
    study.optimize(lambda trial: objective_lightgbm(trial, X_train, y_train, X_val, y_val), 
                   n_trials=n_trials, show_progress_bar=True)
    return study.best_params, study.best_value

def objective_random_forest(trial, X_train, y_train, X_val, y_val):
    params = {
        'n_estimators': trial.suggest_int('n_estimators', 100, 500, step=50),
        'max_depth': trial.suggest_int('max_depth', 5, 20),
        'min_samples_split': trial.suggest_int('min_samples_split', 2, 20),
        'min_samples_leaf': trial.suggest_int('min_samples_leaf', 1, 10),
        'max_features': trial.suggest_categorical('max_features', ['sqrt', 'log2', None]),
        'random_state': 42,
        'n_jobs': -1
    }
    model = RandomForestRegressor(**params)
    model.fit(X_train, y_train)
    pred = model.predict(X_val)
    from sklearn.metrics import mean_absolute_error
    return mean_absolute_error(y_val, pred)

def tune_random_forest(X_train, y_train, X_val, y_val, n_trials=30):
    study = optuna.create_study(direction='minimize', sampler=optuna.samplers.TPESampler(seed=42))
    study.optimize(lambda trial: objective_random_forest(trial, X_train, y_train, X_val, y_val), 
                   n_trials=n_trials, show_progress_bar=True)
    return study.best_params, study.best_value

# ============= SHAP FUNCTIONS =============

def explain_with_shap(model, X_sample, feature_names, model_type='random_forest'):
    if model_type in ['random_forest', 'lightgbm']:
        explainer = shap.TreeExplainer(model)
    else:
        explainer = shap.Explainer(model, X_sample)
    shap_values = explainer.shap_values(X_sample)
    import matplotlib.pyplot as plt
    plt.figure(figsize=(12, 5))
    shap.summary_plot(shap_values, X_sample, feature_names=feature_names, show=False)
    plt.tight_layout()
    plt.show()
    importance_df = pd.DataFrame({'feature': feature_names, 
                                   'shap_importance': np.abs(shap_values).mean(axis=0)}).sort_values('shap_importance', ascending=False)
    return importance_df, shap_values

# ============= LSTM FUNCTIONS =============

def build_lstm_with_static(lookback, n_time_features, n_static_features, 
                           lstm_units=[64, 32], dropout_rate=0.2, learning_rate=0.001):
    time_input = Input(shape=(lookback, n_time_features), name='time_series')
    x = time_input
    for units in lstm_units[:-1]:
        x = LSTM(units, return_sequences=True)(x)
        x = Dropout(dropout_rate)(x)
    x = LSTM(lstm_units[-1], return_sequences=False)(x)
    x = Dropout(dropout_rate)(x)
    
    static_input = Input(shape=(n_static_features,), name='static_features')
    y = Dense(32, activation='relu')(static_input)
    y = Dropout(dropout_rate)(y)
    
    combined = Concatenate()([x, y])
    combined = Dense(16, activation='relu')(combined)
    output = Dense(1, activation='linear', name='output')(combined)
    
    model = Model(inputs=[time_input, static_input], outputs=output)
    model.compile(optimizer=Adam(learning_rate=learning_rate), loss='mse', metrics=['mae'])
    return model

def prepare_lstm_data(df, lookback, time_col='avg_cpu', static_cols=['core_bucket', 'memory_bucket']):
    from sklearn.preprocessing import MinMaxScaler, StandardScaler
    time_scaler = MinMaxScaler()
    time_scaled = time_scaler.fit_transform(df[time_col].values.reshape(-1, 1)).flatten()
    
    if static_cols and all(col in df.columns for col in static_cols):
        static_scaler = StandardScaler()
        static_scaled = static_scaler.fit_transform(df[static_cols].values)
    else:
        static_scaled = np.zeros((len(df), 1))
        static_scaler = None
    
    X_time, y = [], []
    for i in range(len(time_scaled) - lookback):
        X_time.append(time_scaled[i:i+lookback])
        y.append(time_scaled[i+lookback])
    
    X_time = np.array(X_time)
    y = np.array(y)
    
    if static_cols and all(col in df.columns for col in static_cols):
        X_static = np.array([static_scaled[i+lookback-1] for i in range(len(X_time))])
    else:
        X_static = np.zeros((len(X_time), 1))
    
    X_time = X_time.reshape((X_time.shape[0], X_time.shape[1], 1))
    X_static = X_static.reshape((X_static.shape[0], -1))
    
    return (X_time, X_static), y, time_scaler, static_scaler

def train_lstm(train_df, test_df, lookback=12, static_cols=['core_bucket', 'memory_bucket'], 
               lstm_units=[64, 32], dropout_rate=0.2, learning_rate=0.001, epochs=50, batch_size=32):
    (X_train_time, X_train_static), y_train, time_scaler, static_scaler = prepare_lstm_data(
        train_df, lookback, 'avg_cpu', static_cols)
    (X_test_time, X_test_static), y_test, _, _ = prepare_lstm_data(
        test_df, lookback, 'avg_cpu', static_cols)
    
    n_time_features = X_train_time.shape[2]
    n_static_features = X_train_static.shape[1]
    
    model = build_lstm_with_static(lookback, n_time_features, n_static_features, 
                                    lstm_units, dropout_rate, learning_rate)
    
    early_stop = EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True)
    history = model.fit([X_train_time, X_train_static], y_train, 
                        epochs=epochs, batch_size=batch_size, 
                        validation_split=0.2, callbacks=[early_stop], verbose=1)
    
    predictions = model.predict([X_test_time, X_test_static], verbose=0)
    predictions = time_scaler.inverse_transform(predictions)
    y_test_actual = time_scaler.inverse_transform(y_test.reshape(-1, 1))
    
    return predictions, y_test_actual, model, history
