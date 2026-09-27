# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 — Bronze: Importaçã0 dos dados brutos do IMDb
# MAGIC
# MAGIC **Objetivo desta etapa:** trazer os arquivos oficiais do IMDb para a nuvem exatamente
# MAGIC como eles vêm da fonte, sem nenhuma transformação — apenas leitura

# COMMAND ----------

CATALOG = "workspace"          
BRONZE_SCHEMA = "bronze"
VOLUME_NAME = "raw_imdb"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{VOLUME_NAME}"
)

VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}"
print("Arquivos brutos serão salvos em:", VOLUME_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Download dos arquivos oficiais do IMDb
# MAGIC
# MAGIC Os arquivos são `.tsv.gz` (TSV comprimido). Baixados direto da fonte oficial 
# MAGIC

# COMMAND ----------

import urllib.request
import os

IMDB_BASE_URL = "https://datasets.imdbws.com"

FILES = [
    "title.basics.tsv.gz",
    "title.ratings.tsv.gz",
    "title.crew.tsv.gz",
    "name.basics.tsv.gz",
]

for filename in FILES:
    url = f"{IMDB_BASE_URL}/{filename}"
    dest = os.path.join(VOLUME_PATH, filename)
    if os.path.exists(dest):
        print(f"[skip] {filename} já existe em {dest}")
        continue
    print(f"[download] {url} -> {dest}")
    urllib.request.urlretrieve(url, dest)

print("Download concluído.")
dbutils.fs.ls(VOLUME_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leitura crua e gravação como Delta (Bronze)
# MAGIC
# MAGIC Tudo está sendo lido como **string** (sem nenhuma conversão de tipo aqui — isso é trabalho da
# MAGIC camada Silver) `\N` está como veio, sem converter para nulo, para preservar o
# MAGIC dado exatamente como chegou da fonte.

# COMMAND ----------

from pyspark.sql import functions as F

TABLES = {
    "title_basics": "title.basics.tsv.gz",
    "title_ratings": "title.ratings.tsv.gz",
    "title_crew": "title.crew.tsv.gz",
    "name_basics": "name.basics.tsv.gz",
}

for table_name, filename in TABLES.items():
    source_path = os.path.join(VOLUME_PATH, filename)

    df_raw = (
        spark.read.option("header", True)
        .option("sep", "\t")
        .option("inferSchema", False)   # tudo string na Bronze de propósito
        .csv(source_path)
        .withColumn("_source_file", F.lit(filename))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    target = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    df_raw.write.format("delta").mode("overwrite").saveAsTable(target)

    n = spark.table(target).count()
    print(f"[ok] {target}: {n:,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Checagem rápida
# MAGIC
# MAGIC Confirma que as 4 tabelas Bronze existem e mostra uma amostra de cada uma 

# COMMAND ----------

for table_name in TABLES:
    print(f"--- {table_name} ---")
    spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}").show(5, truncate=60)