# Google ClusterData 2019 — Cluster A preprocessing pipeline

Данная папка содержит скрипты для выгрузки, фильтрации, объединения и финальной подготовки подвыборки Google ClusterData 2019 Cluster A.

Пайплайн использовался для подготовки данных о жизненном цикле виртуальных машин и потреблении CPU/RAM. Итоговые таблицы предназначены для EDA, feature engineering и построения моделей прогнозирования нагрузки.

---

## Источник данных

Используется публичный датасет Google ClusterData 2019:

- Bucket: `gs://clusterdata_2019_a/`
- Основные таблицы:
  - `instance_usage-*.json.gz` — временные ряды фактического потребления ресурсов виртуальными машинами;
  - `instance_events-*.json.gz` — события жизненного цикла инстансов и информация о запрошенных ресурсах.

---

## Скрипты

### `build_google_a_sample.py`

Основной скрипт выгрузки и первичной обработки Google ClusterData 2019 Cluster A.

Он выполняет следующие шаги:

1. Получает список всех shard-файлов для `instance_usage` и `instance_events`.
2. Последовательно скачивает shard-файлы из публичного Google Cloud Storage bucket.
3. Обрабатывает данные потоково, чтобы не хранить полный датасет в оперативной памяти.
4. Формирует идентификатор виртуальной машины `vm_id` на основе:
   - `collection_id`;
   - `instance_index`.
5. Применяет детерминированное hash-сэмплирование:

   ```text
   hash(vm_id) % 16 == 0
   ```

6. Сохраняет обработанные части в формате Parquet.
7. Удаляет сырые `.json.gz` shard-файлы после обработки, чтобы не занимать лишнее место на диске.
8. Ведет журнал прогресса, чтобы при повторном запуске не обрабатывать уже готовые shard-файлы заново.
9. Подготавливает промежуточные таблицы `usage` и `events`.

> Важно: финальное объединение больших таблиц в старой версии скрипта оказалось слишком тяжелым для локального ноутбука. Поэтому для финальной подготовки данных используется отдельный скрипт `make_google_a_final_datasets.py`.

---

### `join_google_a_final_resume.py`

Скрипт для возобновляемого объединения больших `usage`- и `events`-таблиц через DuckDB.

Он используется, если нужно собрать полную ASOF-таблицу:

```text
out/google_a_final_joined_asof.parquet
```

Скрипт работает с уже подготовленными parquet-частями и не скачивает исходные `.json.gz` файлы заново.

---

### `make_google_a_final_datasets.py`

Скрипт финальной постобработки уже собранного ASOF-датасета.

Он не скачивает данные заново, а работает с файлом:

```text
out/google_a_final_joined_asof.parquet
```

На его основе формируются две итоговые таблицы:

1. Основная таблица временных рядов нагрузки:

   ```text
   out/google_a_usage_normalized.parquet
   ```

2. Справочник запрошенных ресурсов по виртуальным машинам:

   ```text
   out/google_a_vm_resources.parquet
   ```

Такое разделение было выбрано, чтобы:

- уменьшить размер основной таблицы;
- убрать технические и дублирующие поля;
- сохранить строки временных рядов, для которых доступны корректные значения requested CPU/RAM;
- сделать данные удобнее для EDA, feature engineering и моделирования;
- избежать падения ядра Jupyter при попытке загрузить слишком тяжелую объединенную таблицу целиком.

---

## Логика нормализации ресурсов

В исходном Google ClusterData значения `average_cpu`, `maximum_cpu` и `maximum_memory` представлены не как проценты, а как нормированные величины потребления ресурсов в шкале Google ClusterData.

Для корректной интерпретации нагрузки эти значения были нормализованы относительно ресурсов, запрошенных виртуальной машиной:

```text
average_cpu_normalized = average_cpu / requested_cpu
maximum_cpu_normalized = maximum_cpu / requested_cpu
maximum_memory_normalized = maximum_memory / requested_ram
```

В итоговой таблице `google_a_usage_normalized.parquet` поля называются:

- `average_cpu`;
- `maximum_cpu`;
- `maximum_memory`.

Однако важно: в финальной версии эти поля уже являются нормализованными относительно запрошенных ресурсов.

Например:

```text
average_cpu = 0.25
```

означает, что в среднем за интервал VM использовала около 25% от запрошенного CPU.

```text
maximum_cpu = 1.20
```

означает, что максимальная CPU-нагрузка в интервале составила около 120% от запрошенного CPU.

```text
maximum_memory = 0.80
```

означает, что максимальное потребление памяти составило около 80% от запрошенной RAM.

---

## Итоговые датасеты

После финальной обработки используются две таблицы.

---

### 1. `google_a_usage_normalized.parquet`

Основная таблица временных рядов нагрузки виртуальных машин.

Размер после обработки:

```text
около 4.88 GB
```

Количество строк:

```text
462 630 133
```

Колонки:

| Поле | Описание | Единица измерения / формат |
|---|---|---|
| `vm_id` | Уникальный идентификатор виртуальной машины, сформированный из `collection_id` и `instance_index` | строка |
| `trace_minute` | Временная отметка наблюдения, пересчитанная в минуты от начала трассы | минуты |
| `average_cpu` | Среднее потребление CPU за интервал, нормализованное относительно `requested_cpu` | доля от запрошенного CPU |
| `maximum_cpu` | Максимальное потребление CPU за интервал, нормализованное относительно `requested_cpu` | доля от запрошенного CPU |
| `maximum_memory` | Максимальное потребление памяти за интервал, нормализованное относительно `requested_ram` | доля от запрошенной RAM |
| `event_time` | Время события из `instance_events`, по которому были подтянуты актуальные requested-ресурсы | наносекунды от начала трассы |

---

### 2. `google_a_vm_resources.parquet`

Справочник запрошенных ресурсов виртуальных машин.

Размер после обработки:

```text
около 0.04 GB
```

Количество строк:

```text
4 657 580
```

Колонки:

| Поле | Описание | Единица измерения / формат |
|---|---|---|
| `vm_id` | Уникальный идентификатор виртуальной машины | строка |
| `requested_cpu` | Запрошенный объем CPU для VM из таблицы `instance_events` | нормированная величина Google ClusterData |
| `requested_ram` | Запрошенный объем RAM для VM из таблицы `instance_events` | нормированная величина Google ClusterData |

---

## Почему часть строк была отфильтрована

В исходном ASOF-датасете было:

```text
471 573 820 строк
```

В финальной основной таблице осталось:

```text
462 630 133 строки
```

Часть строк была исключена, потому что для них не удалось корректно подтянуть `requested_cpu` или `requested_ram`.

Такие строки нельзя использовать для нормализации, поскольку невозможно корректно посчитать:

```text
actual_usage / requested_resource
```

Поэтому в финальную таблицу попали только строки, для которых были доступны корректные requested-значения.

---

## Выходные файлы пайплайна

В папке `out/` могут формироваться следующие файлы и папки:

```text
out/
├── usage_parts/
├── events_parts/
├── sampled_vm_ids.txt
├── progress.txt
├── google_a_usage_sample.parquet
├── google_a_events_requested.parquet
├── google_a_final_joined_asof.parquet
├── google_a_usage_normalized.parquet
└── google_a_vm_resources.parquet
```

Назначение файлов:

| Файл / папка | Назначение |
|---|---|
| `usage_parts/` | Обработанные parquet-части таблицы `instance_usage` |
| `events_parts/` | Обработанные parquet-части таблицы `instance_events` |
| `sampled_vm_ids.txt` | Список VM, попавших в hash-подвыборку |
| `progress.txt` | Журнал уже обработанных shard-файлов |
| `google_a_usage_sample.parquet` | Объединенная usage-таблица после hash-сэмплирования |
| `google_a_events_requested.parquet` | Таблица событий и requested-ресурсов |
| `google_a_final_joined_asof.parquet` | Полная ASOF-таблица после объединения usage и events |
| `google_a_usage_normalized.parquet` | Финальная основная таблица с нормализованной нагрузкой |
| `google_a_vm_resources.parquet` | Справочник VM и запрошенных ресурсов |

---

## Установка зависимостей

Перед запуском необходимо установить Python-библиотеки:

```bash
pip install pandas pyarrow duckdb
```

Также требуется установленный Google Cloud SDK с доступной командой `gsutil`.

Проверка доступа к данным:

```bash
gsutil ls "gs://clusterdata_2019_a/instance_usage-*.json.gz" | head
```

Если команда возвращает список файлов вида:

```text
gs://clusterdata_2019_a/instance_usage-000000000000.json.gz
gs://clusterdata_2019_a/instance_usage-000000000001.json.gz
gs://clusterdata_2019_a/instance_usage-000000000002.json.gz
```

значит доступ к публичному bucket настроен корректно.

---

## Запуск выгрузки и первичной обработки

Из корня репозитория:

```bash
python3 scripts/google_cluster_2019_a/build_google_a_sample.py
```

На Windows команда может выглядеть так:

```powershell
python scripts\google_cluster_2019_a\build_google_a_sample.py
```

---

## Запуск объединения ASOF-таблицы

После того как parquet-части `usage_parts/` и `events_parts/` уже созданы, можно запустить объединение:

```bash
python3 scripts/google_cluster_2019_a/join_google_a_final_resume.py
```

Результат:

```text
out/google_a_final_joined_asof.parquet
```

---

## Запуск финальной подготовки датасетов

После того как файл

```text
out/google_a_final_joined_asof.parquet
```

уже создан, можно сформировать финальные рабочие таблицы:

```bash
python3 scripts/google_cluster_2019_a/make_google_a_final_datasets.py
```

На Windows:

```powershell
python scripts\google_cluster_2019_a\make_google_a_final_datasets.py
```

---

## Как читать финальные данные в Python

Не рекомендуется читать файл `google_a_final_joined_asof.parquet` целиком через pandas, так как он содержит сотни миллионов строк.

Для первичного просмотра можно читать только несколько строк:

```python
import pyarrow.parquet as pq

path = "out/google_a_usage_normalized.parquet"

pf = pq.ParquetFile(path)

print("Rows:", pf.metadata.num_rows)
print("Columns:", pf.schema.names)

sample = pf.read_row_group(0).to_pandas().head(10)
display(sample)
```

Для агрегатов и EDA по большому parquet-файлу удобнее использовать DuckDB:

```python
import duckdb

con = duckdb.connect()

df_stats = con.execute("""
    SELECT
        COUNT(*) AS n_rows,
        COUNT(DISTINCT vm_id) AS n_vm,
        MIN(trace_minute) AS min_trace_minute,
        MAX(trace_minute) AS max_trace_minute,
        AVG(average_cpu) AS mean_average_cpu,
        AVG(maximum_cpu) AS mean_maximum_cpu,
        AVG(maximum_memory) AS mean_maximum_memory
    FROM 'out/google_a_usage_normalized.parquet'
""").df()

display(df_stats)
```

Пример получения небольшой выборки:

```python
sample_df = con.execute("""
    SELECT *
    FROM 'out/google_a_usage_normalized.parquet'
    LIMIT 10000
""").df()

display(sample_df.head())
```

---

## Особенности пайплайна

Скрипты рассчитаны на длительную обработку большого датасета. Для надежности предусмотрены:

- последовательная обработка shard-файлов;
- сохранение промежуточных результатов;
- пропуск уже обработанных частей при повторном запуске;
- удаление сырых файлов после обработки;
- обработка некорректных JSON-строк без остановки всего процесса;
- разделение итоговых данных на основную таблицу временных рядов и справочник ресурсов VM.

Это позволяет продолжать обработку после сбоя, отключения ноутбука или потери соединения.
