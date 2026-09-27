# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 — Silver: limpeza e padronização
# MAGIC
# MAGIC Tratamento dos dados 
# MAGIC - conversão de tipos corretos (texto → inteiro, texto → double)
# MAGIC - tratamento do marcador de nulo do IMDb (`\N`)
# MAGIC - filtro para manter apenas filmes (`titleType = 'movie'`)
# MAGIC - remoção de duplicatas
# MAGIC - separação de colunas multivaloradas (`genres`, `directors`) em tabelas-ponte,
# MAGIC   já que um filme pode ter vários gêneros e vários diretores (relação N:N)
# MAGIC
# MAGIC

# COMMAND ----------

CATALOG = "workspace"          # << mesmo valor usado no notebook 01
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

from pyspark.sql import functions as F, types as T

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `silver.movies` — a partir de `bronze.title_basics`
# MAGIC
# MAGIC Transformações:
# MAGIC 1. Filtra `titleType = 'movie'` (descarta séries, episódios, shorts etc.) — o
# MAGIC    objetivo do trabalho é sobre **filmes**, não sobre todo o catálogo do IMDb.
# MAGIC 2. Converte `\N` em `NULL` real nas colunas usadas.
# MAGIC 3. Faz cast de `startYear` e `runtimeMinutes` para inteiro.
# MAGIC 4. Filtro de qualidade/acurácia: descarta `startYear` fora de [1874, ano atual + 1] e
# MAGIC    `runtimeMinutes` fora de [1, 800] — valores fora disso são erro de digitação/carga
# MAGIC    na fonte (ex: filme com 12345 minutos), não filmes reais.
# MAGIC 5. Remove duplicatas de `tconst` (chave primária esperada).

# COMMAND ----------

CURRENT_YEAR = 2026

bronze_basics = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.title_basics")

movies_silver = (
    bronze_basics
    .filter(F.col("titleType") == "movie")
    .withColumn(
        "startYear",
        F.when(F.col("startYear") == "\\N", None).otherwise(F.col("startYear").cast(T.IntegerType())),
    )
    .withColumn(
        "runtimeMinutes",
        F.when(F.col("runtimeMinutes") == "\\N", None).otherwise(F.col("runtimeMinutes").cast(T.IntegerType())),
    )
    .withColumn("genres", F.when(F.col("genres") == "\\N", None).otherwise(F.col("genres")))
    .dropDuplicates(["tconst"])
    .filter(
        (F.col("startYear").isNull()) | (F.col("startYear").between(1874, CURRENT_YEAR + 1))
    )
    .filter(
        (F.col("runtimeMinutes").isNull()) | (F.col("runtimeMinutes").between(1, 800))
    )
    .select(
        "tconst",
        F.col("primaryTitle").alias("primary_title"),
        "startYear",
        "runtimeMinutes",
        "genres",
    )
)

movies_silver.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SILVER_SCHEMA}.movies"
)
print("silver.movies:", spark.table(f"{CATALOG}.{SILVER_SCHEMA}.movies").count(), "linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `silver.ratings` — a partir de `bronze.title_ratings`
# MAGIC
# MAGIC Cast de `averageRating` (double) e `numVotes` (int). Filtro de acurácia:
# MAGIC `averageRating` deve estar em [0, 10] por definição do próprio IMDb — qualquer valor
# MAGIC fora disso indica erro de carga.

# COMMAND ----------

bronze_ratings = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.title_ratings")

ratings_silver = (
    bronze_ratings
    .withColumn("averageRating", F.col("averageRating").cast(T.DoubleType()))
    .withColumn("numVotes", F.col("numVotes").cast(T.IntegerType()))
    .dropDuplicates(["tconst"])
    .filter(F.col("averageRating").between(0, 10))
    .filter(F.col("numVotes") >= 0)
    .select(
        "tconst",
        F.col("averageRating").alias("average_rating"),
        F.col("numVotes").alias("num_votes"),
    )
)

ratings_silver.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SILVER_SCHEMA}.ratings"
)
print("silver.ratings:", spark.table(f"{CATALOG}.{SILVER_SCHEMA}.ratings").count(), "linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `silver.movie_genre_bridge` — explode `genres`
# MAGIC
# MAGIC `genres` chega como string tipo `"Comedy,Drama,Romance"`. Como um filme pertence a
# MAGIC vários gêneros ao mesmo tempo, modelei isso como uma tabela-ponte (1 linha por par
# MAGIC filme-gênero) em vez de manter a string — isso é o que permite agrupar por gênero na
# MAGIC análise (Pergunta de negócio 1).

# COMMAND ----------

movie_genre_bridge = (
    movies_silver
    .filter(F.col("genres").isNotNull())
    .withColumn("genre", F.explode(F.split(F.col("genres"), ",")))
    .select("tconst", "genre")
    .dropDuplicates(["tconst", "genre"])
)

movie_genre_bridge.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SILVER_SCHEMA}.movie_genre_bridge"
)
print(
    "silver.movie_genre_bridge:",
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.movie_genre_bridge").count(),
    "linhas",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `silver.movie_director_bridge` e `silver.names`
# MAGIC
# MAGIC `title.crew.directors` chega como `"nm0000233,nm0001392"` (lista de `nconst`). Mesma
# MAGIC lógica do gênero: explode em tabela-ponte filme↔diretor. `name.basics` vira uma tabela
# MAGIC de dimensão simples só com o nome de cada pessoa.

# COMMAND ----------

bronze_crew = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.title_crew")
bronze_names = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.name_basics")

movie_director_bridge = (
    bronze_crew
    .withColumn("directors", F.when(F.col("directors") == "\\N", None).otherwise(F.col("directors")))
    .filter(F.col("directors").isNotNull())
    .withColumn("nconst", F.explode(F.split(F.col("directors"), ",")))
    .select("tconst", "nconst")
    .dropDuplicates(["tconst", "nconst"])
    # mantém só diretores de filmes que sobreviveram aos filtros da silver.movies
    .join(movies_silver.select("tconst"), on="tconst", how="inner")
)

movie_director_bridge.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SILVER_SCHEMA}.movie_director_bridge"
)

names_silver = (
    bronze_names
    .dropDuplicates(["nconst"])
    .select("nconst", F.col("primaryName").alias("primary_name"))
)

names_silver.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{SILVER_SCHEMA}.names"
)

print(
    "silver.movie_director_bridge:",
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.movie_director_bridge").count(),
    "linhas",
)
print("silver.names:", spark.table(f"{CATALOG}.{SILVER_SCHEMA}.names").count(), "linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Checagem rápida

# COMMAND ----------

for t in ["movies", "ratings", "movie_genre_bridge", "movie_director_bridge", "names"]:
    print(f"--- silver.{t} ---")
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{t}").show(5, truncate=40)