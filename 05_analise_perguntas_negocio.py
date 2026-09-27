# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05 — Análise: tentando responder as perguntas de negócio
# MAGIC
# MAGIC Uma seção por pergunta (ver README, Seção 1). Rode cada célula, olhe o resultado,
# MAGIC escreva a discussão e tire o screenshot pedido pela Seção 8 da especificação.
# MAGIC
# MAGIC Em todas as perguntas usamos `num_votes >= 1000` como piso mínimo de relevância — um
# MAGIC filme com 3 votos pode ter nota 10 só por acaso; isso é ruído, não sinal.

# COMMAND ----------

CATALOG = "workspace"
GOLD_SCHEMA = "gold"

from pyspark.sql import functions as F

dim_movie = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_movie")
fact_ratings = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_movie_ratings")
bridge_genre = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.bridge_movie_genre")
bridge_director = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.bridge_movie_director")
dim_director = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_director")

MIN_VOTES = 1000
relevant_ratings = fact_ratings.filter(F.col("num_votes") >= MIN_VOTES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1 — Gênero está associado a nota média mais alta?

# COMMAND ----------

q1 = (
    bridge_genre
    .join(relevant_ratings, on="tconst", how="inner")
    .groupBy("genre")
    .agg(
        F.round(F.avg("average_rating"), 2).alias("avg_rating"),
        F.count("*").alias("n_filmes"),
    )
    .filter(F.col("n_filmes") >= 50)   # gênero precisa ter volume mínimo pra nota ser estável
    .orderBy(F.desc("avg_rating"))
)
q1.show(30, truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Equivalente em SQL, caso prefira rodar direto no editor SQL do Databricks:
# MAGIC -- SELECT genre, ROUND(AVG(average_rating), 2) AS avg_rating, COUNT(*) AS n_filmes
# MAGIC -- FROM workspace.gold.bridge_movie_genre g
# MAGIC -- JOIN workspace.gold.fact_movie_ratings r USING (tconst)
# MAGIC -- WHERE r.num_votes >= 1000
# MAGIC -- GROUP BY genre
# MAGIC -- HAVING COUNT(*) >= 50
# MAGIC -- ORDER BY avg_rating DESC

# COMMAND ----------

# MAGIC %md
# MAGIC `[COMPLETAR]` **Discussão:** quais gêneros ficaram no topo/base? Isso bate com a
# MAGIC intuição (ex: documentários e biografias tendem a notas mais altas; terror tende a
# MAGIC notas mais divididas)? Cite os números.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2 — Duração maior está associada a nota mais alta ou mais baixa?

# COMMAND ----------

q2_corr = dim_movie.join(relevant_ratings, on="tconst").stat.corr(
    "runtime_minutes", "average_rating"
)
print(f"Correlação linear (Pearson) runtime x rating: {q2_corr:.3f}")

q2_bucket = (
    dim_movie.join(relevant_ratings, on="tconst")
    .withColumn(
        "runtime_bucket",
        F.when(F.col("runtime_minutes") < 60, "< 60 min")
        .when(F.col("runtime_minutes") < 90, "60–89 min")
        .when(F.col("runtime_minutes") < 120, "90–119 min")
        .when(F.col("runtime_minutes") < 150, "120–149 min")
        .otherwise("150+ min"),
    )
    .groupBy("runtime_bucket")
    .agg(
        F.round(F.avg("average_rating"), 2).alias("avg_rating"),
        F.count("*").alias("n_filmes"),
    )
    .orderBy("avg_rating")
)
q2_bucket.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC `[COMPLETAR]` **Discussão:** a correlação é fraca, moderada ou forte? Positiva ou
# MAGIC negativa? O que os buckets de duração mostram na prática?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3 — Diretores com mais filmes têm nota mais alta e mais consistente?

# COMMAND ----------

MIN_FILMES_DIRETOR = 5

director_stats = (
    bridge_director
    .join(relevant_ratings, on="tconst")
    .join(dim_director, on="nconst")
    .groupBy("nconst", "primary_name")
    .agg(
        F.count("*").alias("n_filmes"),
        F.round(F.avg("average_rating"), 2).alias("avg_rating"),
        F.round(F.stddev("average_rating"), 2).alias("stddev_rating"),
    )
)

prolific = director_stats.filter(F.col("n_filmes") >= MIN_FILMES_DIRETOR)
occasional = director_stats.filter(F.col("n_filmes") < MIN_FILMES_DIRETOR)

print(f"Diretores com >= {MIN_FILMES_DIRETOR} filmes:")
prolific.agg(
    F.round(F.avg("avg_rating"), 2).alias("media_das_medias"),
    F.round(F.avg("stddev_rating"), 2).alias("media_do_desvio_padrao"),
    F.count("*").alias("n_diretores"),
).show()

print(f"Diretores com < {MIN_FILMES_DIRETOR} filmes:")
occasional.agg(
    F.round(F.avg("avg_rating"), 2).alias("media_das_medias"),
    F.round(F.avg("stddev_rating"), 2).alias("media_do_desvio_padrao"),
    F.count("*").alias("n_diretores"),
).show()

print("Top 15 diretores mais bem avaliados (com >= {} filmes):".format(MIN_FILMES_DIRETOR))
prolific.orderBy(F.desc("avg_rating")).show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC `[COMPLETAR]` **Discussão:** diretores prolíficos têm nota média maior ou menor que os
# MAGIC ocasionais? O desvio padrão (consistência) é menor entre os prolíficos, como se
# MAGIC esperaria de quem "aprendeu o ofício"?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 4 — Como a nota média evoluiu ao longo das décadas?

# COMMAND ----------

q4 = (
    dim_movie.join(relevant_ratings, on="tconst")
    .filter(F.col("decade").isNotNull())
    .groupBy("decade")
    .agg(
        F.round(F.avg("average_rating"), 2).alias("avg_rating"),
        F.count("*").alias("n_filmes"),
    )
    .orderBy("decade")
)
q4.show(20)

# Opcional: exibir como gráfico de linha no próprio notebook do Databricks
# display(q4)

# COMMAND ----------

# MAGIC %md
# MAGIC `[COMPLETAR]` **Discussão:** a nota média subiu, caiu ou ficou estável ao longo do
# MAGIC tempo? Cuidado: décadas recentes podem ter menos filmes com `num_votes >= 1000`
# MAGIC (menos tempo pra acumular votos) — comente esse viés na discussão.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 5 — Nº de votos (popularidade) está associado à nota média?

# COMMAND ----------

q5_corr = fact_ratings.withColumn(
    "log_votes", F.log1p(F.col("num_votes"))
).stat.corr("log_votes", "average_rating")
print(f"Correlação (Pearson) log(1+num_votes) x rating: {q5_corr:.3f}")

q5_bucket = (
    fact_ratings.withColumn(
        "votes_bucket",
        F.when(F.col("num_votes") < 1000, "< 1k")
        .when(F.col("num_votes") < 10000, "1k–10k")
        .when(F.col("num_votes") < 100000, "10k–100k")
        .otherwise("100k+"),
    )
    .groupBy("votes_bucket")
    .agg(
        F.round(F.avg("average_rating"), 2).alias("avg_rating"),
        F.count("*").alias("n_filmes"),
    )
)
q5_bucket.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC `[COMPLETAR]` **Discussão:** filmes mais votados (mais populares/alcançados) tendem a
# MAGIC ter nota mais alta? Isso pode indicar viés de seleção (só filme bom vira "popular") ou
# MAGIC o contrário?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussão geral
# MAGIC
# MAGIC `[COMPLETAR]` Conecte as 5 respostas de volta ao problema original: **quais
# MAGIC características estão associadas a filmes bem avaliados no IMDb?** Resuma em um
# MAGIC parágrafo o que os dados sugerem, e mencione a limitação principal (dataset do IMDb não
# MAGIC tem orçamento/bilheteria, então "sucesso" aqui é só nota do público, não sucesso
# MAGIC comercial).