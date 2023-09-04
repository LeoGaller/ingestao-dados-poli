# Databricks notebook source
from pyspark.sql.functions import col, regexp_replace, avg, round

# COMMAND ----------

# Lendo arquivos para uniao
claims_df = spark.read.parquet("dbfs:/pece-poli-de/silver/claims")
employee_df = spark.read.parquet("dbfs:/pece-poli-de/silver/employees")
banks_df = spark.read.parquet("dbfs:/pece-poli-de/silver/banks")

# COMMAND ----------

display(claims_df)

# COMMAND ----------

display(employee_df)

# COMMAND ----------

display(banks_df)

# COMMAND ----------

# Criando alias para trabalho de juncao banks <--> claims
banks_df = banks_df.alias('banks_df')
claims_df = claims_df.alias('claims_df')
join_df = claims_df.join(banks_df, 'cnpj', 'inner').select(col('banks_df.cnpj').alias('cnpj_banks'),'claims_df.*').drop('cnpj').withColumnRenamed('cnpj_banks','cnpj')

# COMMAND ----------

join_df.count()

# COMMAND ----------

from pyspark.sql.functions import when, col, lit
employee_df = employee_df.withColumn('nome', when(col('nome').isin('SF3 CRÉDITO, FINANCIAMENTO E INVESTIMENTO'),'SANTANA  CRÉDITO, FINANCIAMENTO E INVESTIMENTO').otherwise(col('nome')))\
    .withColumn('nome', when(col('nome').isin('SOCIAL BANK BANCO MÚLTIPLO'),'BANCO CAPITAL').otherwise(col('nome')))

# COMMAND ----------

# unindo com tabela employees
employee_df = employee_df.alias('employee_df')
join_df = join_df.alias('join_df')

# tratando a coluna de indices
join_df.withColumn('índice', regexp_replace('índice', ',', '.'))

# Unindo os dados
join2_df = join_df.join( employee_df, 'nome' , 'left').select('join_df.nome',
                                                              'join_df.cnpj',
                                                              'join_df.categoria',
                                                              'join_df.quantidade_total_de_clientes_–_ccs_e_scr',
                                                              regexp_replace('join_df.índice', ',', '.').alias('índice'),
                                                              'join_df.quantidade_total_de_reclamações',
                                                              'employee_df.geral',
                                                              'employee_df.remuneração_e_benefícios')

# COMMAND ----------

join2_df.count()

# COMMAND ----------

# Estrutura final esperada:
# Nome do Banco
# CNPJ
# Classificação do Banco
# Quantidade de Clientes do Bancos
# Índice de reclamações
# Quantidade de reclamações
# Índice de satisfação dos funcionários dos bancos
# Índice de satisfação com salários dos funcionários dos bancos.

final_df = join2_df.select(col('nome').alias('Nome do Banco'), 
                           col('cnpj').alias('CNPJ'),
                           col('categoria').alias('Classificação'),
                           col('quantidade_total_de_clientes_–_ccs_e_scr').alias('Quantidade de Clientes do Bancos'),
                           col('índice').cast('integer').alias('Índice de reclamações'),
                           col('quantidade_total_de_reclamações').alias('Quantidade de reclamações'),
                           col('geral').alias('Índice de satisfação dos funcionários dos bancos'),
                           col('remuneração_e_benefícios').alias('Índice de satisfação com salários dos funcionários dos bancos'))

final_df = final_df.groupBy('Nome do Banco',
                            'CNPJ',
                            'Classificação')\
                   .agg(round(avg('Quantidade de Clientes do Bancos')).alias('Quantidade de Clientes do Bancos'),
                              avg('Índice de reclamações').alias('Índice de reclamações'),
                              avg('Quantidade de reclamações').alias('Quantidade de reclamações'),
                              avg('Índice de satisfação dos funcionários dos bancos').alias('Índice de satisfação dos funcionários dos bancos'),
                              avg('Índice de satisfação com salários dos funcionários dos bancos').alias('Índice de satisfação com salários dos funcionários dos bancos'))

# COMMAND ----------

# Gerando o parquet final da tabela
final_df.write.mode("overwrite").parquet("dbfs:/pece-poli-de/gold/tabela_final")

# COMMAND ----------

final_df.count()
