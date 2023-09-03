# Анализ качества и очистка датасета мошеннических финансовых операций

## Анализ датасета мошеннических транзакций на наличие ошибочных данных

Изначально бакет не удалось загрузить через PySpark, т.к. заголовок имел неверную для csv-файла структуру

```bash
# tranaction_id | tx_datetime | customer_id | terminal_id | tx_amount | tx_time_seconds | tx_time_days | tx_fraud | tx_fraud_scenario
```

Поэтому перед загрузкой в PySpark-датафрейм все колонки были приведены к формату:

```bash
tx_id,tx_datetime,customer_id,terminal_id,tx_amount,tx_time_seconds,tx_time_days,tx_fraud,tx_fraud_scenario
```

Также у первой колонки было название было изменено на более читаемое. Функция для изменения заголовка:

```python
import os

def fix_txt(file: str):
    with open(file, encoding='utf-8') as f:
        header = f.readline()
        columns = header.split(" | ")
        columns[0] = "tx_id"
        new_header = f"{','.join(columns)}\n"
        with open(f"{os.path.splitext(file)[0]}_1.txt", encoding='utf-8', mode="w") as f1:
            f1.write(new_header)
            for i, line in enumerate(f):
                # пропускаем строку для заголовка, она была записана выше в др. формате
                if i == 0:
                    continue
                f1.write(line)
```

Бакет с обработанными колонками доступен по адресу в YC `s3://otus-mlops-course/raw/`.

После этого бакет был загружен через PySpark и проанализирован ([jupyter-ноутбук](data_preproc.ipynb)). Были обнаружены следующие проблемы:

* Неверный тип данных в колонке `terminal_id`: помимо целочисленных значений встречались `Err` и `None`. Объём некорректных данных **0.002%**
* Неверный формат даты в колонке `tx_datetime`: нельзя преобразовать к формату `TimeStamp`. Объём некорректных данных **0.0002%**
* Противоречивые данные в колонках `tx_time_days` и `tx_time_seconds`:

    ```
    floor(tx_time_seconds / n_seconds_day) != tx_time_days
    ```
    Объём некорректных данных **0.0002%**

* Дубликаты в колонке `tx_id`: у каждой транзакции должен быть уникальный id. Объём некорректных данных **0.0001%**

## Скрипт для очистки данных

Так как ошибочные данные составляют менее 1% от общего объёма датасета [скрипт для очистки данных](preproc.py) загружает данные с использованием схемы, в которой указана нужная типизация. То, что не получилось привести к нужному типу будет прочитано как `nan`, и все строки, содержащие `nan` в колонках `tx_datetime`, `terminal_id` будут удалены. Далее удаляются дубликаты в колонке `tx_id` и колонка `tx_time_days` пересчитывается через колонку `tx_time_seconds`. Обработанные данные сохраняются в формате `parquet` по адреcу `s3a://otus-mlops-course/user/ubuntu/processed/processed_data.parquet`.

[Обновлённая Kanban-доска](https://github.com/users/ayeffkay/projects/1/views/1?layout=board).