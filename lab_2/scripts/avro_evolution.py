#!/usr/bin/env python3
"""
Часть 2. Avro: поток процессинга и эволюция схемы. ШАБЛОН.

Avro — строковый формат с схемой внутри файла. Его место — не аналитика,
а поток: Kafka, Schema Registry, обмен между сервисами (лекция 9).
Здесь мы (1) пишем сырые операции процессинга в Avro и сравниваем размер
с Parquet и JSON, (2) меняем схему и смотрим, что читается, а что ломается.

Запуск: python scripts/avro_evolution.py [../lab_1/data]
"""
import os
import pathlib
import sys
import time

import pandas as pd
import duckdb
from fastavro import parse_schema, reader, writer

DATA_DIR = sys.argv[1] if len(sys.argv) > 1 else "../lab_1/data"
OUT = pathlib.Path(__file__).resolve().parent.parent / "data"
OUT.mkdir(exist_ok=True)

# Схема v1 — то, что процессинг отдаёт сегодня. Обратите внимание: типы
# явные, NULL разрешён только там, где объявлен union с "null".
SCHEMA_V1 = {
    "type": "record", "name": "Transaction", "namespace": "sinitsa.processing",
    "fields": [
        {"name": "txn_id",   "type": "string"},
        {"name": "txn_ts",   "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "card_id",  "type": "string"},
        {"name": "mcc",      "type": "string"},
        {"name": "channel",  "type": "string"},
        {"name": "amount",   "type": "double"},
        {"name": "currency", "type": "string"},
        {"name": "status",   "type": "string"},
        {"name": "decline_reason", "type": ["null", "string"], "default": None},
    ],
}

con = duckdb.connect()
rows = con.execute(f"""
    select txn_id, txn_ts, card_id, mcc, channel, amount, currency, status, decline_reason
    from read_parquet('{DATA_DIR}/processing/transactions_2026-07.parquet')
""").fetchall()
names = [f["name"] for f in SCHEMA_V1["fields"]]
records = [dict(zip(names, r)) for r in rows]
print(f"строк процессинга за июль: {len(records):,}")

# ── 1. Запись в Avro (deflate) и сравнение размеров ──────────────────────────
t0 = time.perf_counter()
with open(OUT / "txn_july.avro", "wb") as f:
    writer(f, parse_schema(SCHEMA_V1), records, codec="deflate")
print(f"Avro записан за {time.perf_counter() - t0:.1f} с")

# Те же 9 полей — в JSON Lines и в Parquet+zstd: сравнивать форматы честно
# можно только на одинаковом наборе колонок (в исходном Parquet их 12).
cols = ", ".join(names)
for name, opts in (("txn_july.jsonl", "format json"),
                   ("txn_july.parquet", "format parquet, compression zstd")):
    con.execute(f"""copy (select {cols}
                          from read_parquet('{DATA_DIR}/processing/transactions_2026-07.parquet'))
                    to '{OUT / name}' ({opts})""")
for name in ("txn_july.jsonl", "txn_july.avro", "txn_july.parquet"):
    print(f"  {name:22s} {os.path.getsize(OUT / name) / 1e6:7.1f} МБ")

# ── 2. Схема внутри файла: читатель узнаёт её из заголовка ───────────────────
with open(OUT / "txn_july.avro", "rb") as f:
    rd = reader(f)
    print("схема писателя (из заголовка файла):", [x["name"] for x in rd.writer_schema["fields"]])
    print("первая запись:", next(rd))

# ── 3. Эволюция: v2 добавляет поле С default — старые файлы читаются ─────────
SCHEMA_V2 = {**SCHEMA_V1, "fields": SCHEMA_V1["fields"] + [
    {"name": "terminal_id", "type": ["null", "string"], "default": None},
]}
with open(OUT / "txn_july.avro", "rb") as f:
    rec = next(reader(f, reader_schema=parse_schema(SCHEMA_V2)))
    print("v2-читатель читает v1-файл, terminal_id =", rec["terminal_id"])

# ── TODO 1. Схема v3: добавьте поле БЕЗ default (например, "terminal_id":
#           "string"). Прочитайте v1-файл с reader_schema=v3. Что произошло
#           и почему? Запишите текст ошибки в отчёт.


# SCHEMA_V3 = {**SCHEMA_V1, "fields": SCHEMA_V1["fields"] + [
#     {"name": "terminal_id", "type": ["null", "string"]}
# ]}

# with open(OUT / "txn_july.avro", "rb") as f:
#     rec = next(reader(f, reader_schema=parse_schema(SCHEMA_V3)))
#     print("v3-читатель читает v1-файл, terminal_id =", rec["terminal_id"])


# ── TODO 2. Переименуйте в v4 поле amount в amount_orig (без aliases).
#           Прочитайте v1-файл. Затем добавьте "aliases": ["amount"] к полю
#           и повторите. Сформулируйте правило: какие изменения схемы
#           безопасны для старых данных, какие — нет.

# SCHEMA_V4 = SCHEMA_V2.copy()

# next(f for f in SCHEMA_V4["fields"] if f["name"] == "amount").update({
#     "name": "amount_orig",
#     "aliases": ["amount"]
# })

# with open(OUT / "txn_july.avro", "rb") as f:
#     rec = next(reader(f, reader_schema=parse_schema(SCHEMA_V4)))
#     print("v4-читатель читает v1-файл, terminal_id =", rec["terminal_id"])

# ── TODO 3. Самое важное. Прочитайте из Avro только одну колонку:
#           суммируйте amount по всем записям через reader(f) и замерьте время.
#           Сравните с duckdb: select sum(amount) from read_parquet('data/txn_july.parquet').
#           Объясните разницу устройством форматов, а не «Python медленный»:
#           сколько байт Avro-читатель обязан распаковать и разобрать, чтобы
#           добраться до amount?

SCHEMA_V5 = SCHEMA_V1.copy()

SCHEMA_V5["fields"] = [{"name": "amount",   "type": "double"},]

t0 = time.perf_counter()
with open(OUT / "txn_july.avro", "rb") as f:
    avro_reader  = reader(f, reader_schema=parse_schema(SCHEMA_V1))
    df = pd.DataFrame(avro_reader)

    avro_sum_amount = df["amount"].sum()
    print(avro_sum_amount)

print(f"Avro сумма за {time.perf_counter() - t0} с")

t0 = time.perf_counter()
parquet_sum_amount = con.execute(f"""
select sum(amount) from read_parquet('data/txn_july.parquet');
""").fetchone()[0]
print(parquet_sum_amount)
print(f"Parquet sum(amount) за {time.perf_counter() - t0} с")