# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 — Qualidade de Dados
# MAGIC
# MAGIC Checagens de **completude, consistência, unicidade, acurácia e outliers** sobre a
# MAGIC camada Bronze (antes dos filtros da Silver) — para documentar o que existia de
# MAGIC problema na fonte e confirmar que os filtros aplicados na Silver foram os corretos.
# MAGIC
# MAGIC Copie a saída de cada célula para o README (Seção 5), junto com a explicação de como
# MAGIC cada problema foi tratado.

# COMMAND ----------

CATALOG = "workspace"
BRONZE_SCHEMA = "bronze"
GOLD_SCHEMA = "gold"

from pyspark.sql import functions as F

bronze_basics = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.title_basics").filter(
    F.col("titleType") == "movie"
)
bronze_ratings = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.title_ratings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Completude — % de nulos / `\N` por coluna relevante

# COMMAND ----------

def pct_missing(df, col):
    total = df.count()
    missing = df.filter((F.col(col).isNull()) | (F.col(col) == "\\N")).count()
    return round(100 * missing / total, 2) if total else None


for col in ["startYear", "runtimeMinutes", "genres"]:
    print(f"title.basics.{col}: {pct_missing(bronze_basics, col)}% ausente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Unicidade — duplicatas de `tconst`

# COMMAND ----------

dup_basics = (
    bronze_basics.groupBy("tconst").count().filter(F.col("count") > 1).count()
)
dup_ratings = (
    bronze_ratings.groupBy("tconst").count().filter(F.col("count") > 1).count()
)
print(f"tconst duplicado em title.basics (filmes): {dup_basics}")
print(f"tconst duplicado em title.ratings: {dup_ratings}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Consistência — `startYear` fora do intervalo plausível

# COMMAND ----------

CURRENT_YEAR = 2026

inconsistent_years = bronze_basics.filter(
    (F.col("startYear") != "\\N")
    & (
        (F.col("startYear").cast("int") < 1874)
        | (F.col("startYear").cast("int") > CURRENT_YEAR + 1)
    )
).count()
print(f"Filmes com startYear fora de [1874, {CURRENT_YEAR + 1}]: {inconsistent_years}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Acurácia — `averageRating` fora de [0, 10] e `runtimeMinutes` absurdo

# COMMAND ----------

bad_ratings = bronze_ratings.filter(
    (F.col("averageRating").cast("double") < 0)
    | (F.col("averageRating").cast("double") > 10)
).count()
print(f"Notas fora de [0, 10]: {bad_ratings}")

bad_runtime = bronze_basics.filter(
    (F.col("runtimeMinutes") != "\\N")
    & (
        (F.col("runtimeMinutes").cast("int") <= 0)
        | (F.col("runtimeMinutes").cast("int") > 800)
    )
).count()
print(f"Filmes com runtimeMinutes fora de [1, 800]: {bad_runtime}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Outliers — `numVotes` via IQR
# MAGIC
# MAGIC `numVotes` tem distribuição muito assimétrica (poucos blockbusters concentram a maior
# MAGIC parte dos votos). Calculados os limites de outlier pelo método IQR só para
# MAGIC **documentar** a assimetria — os filme snão foram da análise, porque "ter muitos
# MAGIC votos" é justamente um dos sinais que a Pergunta 5 quer investigar. A decisão tomada na
# MAGIC análise foi usar um piso mínimo de votos (`numVotes >= 1000`) para reduzir ruído de
# MAGIC filmes pouco avaliados, em vez de descartar os extremos superiores.

# COMMAND ----------

q1, q3 = bronze_ratings.approxQuantile("numVotes", [0.25, 0.75], 0.01)
iqr = q3 - q1
lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
n_outliers = bronze_ratings.filter(
    (F.col("numVotes").cast("int") < lower) | (F.col("numVotes").cast("int") > upper)
).count()
print(f"Q1={q1}, Q3={q3}, limites=[{lower:.1f}, {upper:.1f}]")
print(f"Títulos com numVotes fora desse intervalo: {n_outliers}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Conferência pós-Silver/Gold
# MAGIC
# MAGIC Confirmação de que os filtros da Silver realmente eliminaram os problemas acima.

# COMMAND ----------

gold_movies = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_movie")
gold_ratings = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_movie_ratings")

print(
    "gold.dim_movie fora do intervalo de ano:",
    gold_movies.filter(
        (F.col("start_year") < 1874) | (F.col("start_year") > CURRENT_YEAR + 1)
    ).count(),
)
print(
    "gold.fact_movie_ratings fora de [0,10]:",
    gold_ratings.filter(
        (F.col("average_rating") < 0) | (F.col("average_rating") > 10)
    ).count(),
)
print(
    "gold.dim_movie com tconst duplicado:",
    gold_movies.groupBy("tconst").count().filter(F.col("count") > 1).count(),
)