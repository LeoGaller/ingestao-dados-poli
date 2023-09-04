# Databricks notebook source
from pyspark.sql.functions import lit

# COMMAND ----------

# path dos dados ORIGEM
landing_bkt_banks = 'gs://pece-poli-de/landing/banks/'
landing_bkt_claims = 'gs://pece-poli-de/landing/claims/'
landing_bkt_employees = 'gs://pece-poli-de/landing/employees/'

# COMMAND ----------

# path dos dados DESTINO
raw_bkt_banks = 'gs://pece-poli-de/bronze/banks/'
raw_bkt_claims = 'gs://pece-poli-de/bronze/claims/'
raw_bkt_employees = 'gs://pece-poli-de/bronze/employees/'

# COMMAND ----------

# MAGIC %md
# MAGIC ### BANKS

# COMMAND ----------

# Lendo csv e salvando em outro bucket como parquet - BANKS
bank_df = spark.read.csv(landing_bkt_banks, header=True, sep='\t')
# Salvando em parquet
bank_df.write.mode('overwrite').parquet(raw_bkt_banks) # Alterando o mode pode-se colocar como append

# COMMAND ----------

# MAGIC %md
# MAGIC ### CLAIMS

# COMMAND ----------

# Lendo csv e salvando em outro bucket como parquet - CLAIMS
claims_df = spark.read.format("csv").option("header", "true").option('delimiter',',').load(landing_bkt_claims)
# Salvando em parquet
claims_df.write.mode('overwrite').parquet(raw_bkt_claims)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EMPLOYEES

# COMMAND ----------

# Lendo csv e salvando em outro bucket como parquet - EMPLOYEES
### Os dados de employees possuem estruturas diferentes entao é necessário ler de forma diferente
# Lendo separado devido a estrutura diferente
employee_df_1 = spark.read.format("csv").option("header", "true").option('delimiter','|').load(landing_bkt_employees+"glassdoor_consolidado_join_match_less_v2.csv")
employee_df_2 = spark.read.format("csv").option("header", "true").option('delimiter','|').load(landing_bkt_employees+"glassdoor_consolidado_join_match_v2.csv")

# Criando colunas
employee_df_1 = employee_df_1.withColumn('Segmento', lit(''))
employee_df_2 = employee_df_2.withColumn('CNPJ', lit(''))

# Ordenando as colunas
columns = ["employer_name", "reviews_count", "culture_count", "salaries_count", "benefits_count", "employer-website", "employer-headquarters", "employer-founded", "employer-industry", "employer-revenue", "url", "Geral", "Cultura e valores", "Diversidade e inclusão", "Qualidade de vida", "Alta liderança", "Remuneração e benefícios", "Oportunidades de carreira", "Recomendam para outras pessoas(%)", "Perspectiva positiva da empresa(%)", "CNPJ", "Segmento", "Nome", "match_percent"]

employee_df_1 = employee_df_1.select(columns)
employee_df_2 = employee_df_2.select(columns)

# unindo os dados
employee_df = employee_df_1.union(employee_df_2)

# Salvando em parquet
employee_df.write.mode('overwrite').parquet(raw_bkt_employees)
