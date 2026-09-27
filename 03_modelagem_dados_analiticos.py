# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 — Gold: modelagem final (Esquema Estrela)
# MAGIC
# MAGIC Montagem do modelo final, pronto (espero) para responder as perguntas de negócio: um fato de
# MAGIC avaliações (`fact_movie_ratings`) cercado de dimensões (`dim_movie`, `dim_director`) e
# MAGIC tabelas-ponte para as relações N:N (`bridge_movie_genre`, `bridge_movie_director`).
# MAGIC
# MAGIC

# COMMAND ----------

CATALOG = "workspace"          # << mesmo valor usado nos notebooks anteriores
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

from pyspark.sql import functions as F

movies = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.movies")
ratings = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.ratings")
names = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.names")
movie_genre_bridge = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.movie_genre_bridge")
movie_director_bridge = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.movie_director_bridge")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `gold.dim_movie`
# MAGIC
# MAGIC Uma linha por filme. Adiciona `decade` (derivada de `start_year`) para responder a
# MAGIC Pergunta de negócio 4 (evolução da nota média por década) sem repetir essa conta em
# MAGIC toda consulta de análise.

# COMMAND ----------

dim_movie = movies.select(
    "tconst",
    "primary_title",
    "startYear",
    (F.floor(F.col("startYear") / 10) * 10).alias("decade"),
    "runtimeMinutes",
).withColumnRenamed("startYear", "start_year").withColumnRenamed(
    "runtimeMinutes", "runtime_minutes"
)

dim_movie.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{GOLD_SCHEMA}.dim_movie"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `gold.dim_director`
# MAGIC
# MAGIC Fitra os diretores que realmente aparecem em algum filme do recorte (pra não ter que  carregar
# MAGIC a `name.basics` inteira, que também tem atores, roteiristas etc. sem relação com o projeto do meu 
# MAGIC MVP).

# COMMAND ----------

dim_director = (
    movie_director_bridge.select("nconst")
    .distinct()
    .join(names, on="nconst", how="left")
)

dim_director.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{GOLD_SCHEMA}.dim_director"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `gold.bridge_movie_genre` e `gold.bridge_movie_director`
# MAGIC
# MAGIC Copiadas diretamente da Silver — já estão na. estrutura certa (1 linha por par).

# COMMAND ----------

movie_genre_bridge.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{GOLD_SCHEMA}.bridge_movie_genre"
)
movie_director_bridge.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{GOLD_SCHEMA}.bridge_movie_director"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `gold.fact_movie_ratings`
# MAGIC
# MAGIC Uma linha por filme com nota e nº de votos — só para filmes que sobreviveram arduamente aos
# MAGIC filtros de qualidade da Silver

# COMMAND ----------

fact_movie_ratings = ratings.join(
    dim_movie.select("tconst"), on="tconst", how="inner"
)

fact_movie_ratings.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{GOLD_SCHEMA}.fact_movie_ratings"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Checagem rápida — cole print/screenshot no README (Seção 4)

# COMMAND ----------

for t in [
    "dim_movie",
    "dim_director",
    "bridge_movie_genre",
    "bridge_movie_director",
    "fact_movie_ratings",
]:
    full = f"{CATALOG}.{GOLD_SCHEMA}.{t}"
    n = spark.table(full).count()
    print(f"{full}: {n:,} linhas")

# COMMAND ----------

spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_movie_ratings").show(5)