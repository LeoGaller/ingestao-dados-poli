# Databricks notebook source
# instalando pacote de data quality
%pip install great-expectations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passos para configuração do Data Quality

# COMMAND ----------

# Configurando data quality
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

# COMMAND ----------

# Criando o contexto de dados
context_root_dir = "/dbfs/great_expectations/"

# COMMAND ----------

# Instanciando o contexto de dados
context = gx.get_context(context_root_dir=context_root_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fim configuração data quality
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Iniciando leitura e processamento dos arquivos

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, split, col

# COMMAND ----------

# lendo arquivos
claims_df = spark.read.parquet('dbfs:/pece-poli-de/bronze/claims/')

# COMMAND ----------

# Colocando os dados em cache
claims_df.cache()

# COMMAND ----------

# Data Transformation for claims dataset
for column in claims_df.columns:
    claims_df = claims_df.withColumnRenamed(
        column, 
        column.replace("-","_").replace(" ","_").lower()
    )

claims_df = claims_df.withColumnRenamed('cnpj_if', 'cnpj')\
    .withColumnRenamed('instituição_financeira', 'nome')

for replacement_action in [
    ("nome", "- PRUDENCIAL", ""),
    ("nome","(\.+|\/+|\-+)", ""),
    ("nome"," \(conglomerado\)", ""),
    ("nome"," INSTITUIÇÃO DE PAGAMENTO", ""),
    ("nome","SOCIEDADE DE CRÉDITO, FINANCIAMENTO E INVESTIMENTO", "SCFI"),
    ("nome"," SA", ""),
]:
    claims_df = claims_df.withColumn(
        "nome", regexp_replace(
            replacement_action[0],
            replacement_action[1],
            replacement_action[2]
        )
    )

# COMMAND ----------

# Gravando no diretorio Silver
claims_df.write.mode("overwrite").parquet("dbfs:/pece-poli-de/silver/claims/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Iniciando passos de data quality
# MAGIC https://docs.greatexpectations.io/docs/tutorials/getting_started/how_to_use_great_expectations_in_databricks

# COMMAND ----------

# Capturar o arquivo PARQUET e salvar seu caminho
# i.e. 'dbfs:/pece-poli-de/silver/banks/'
def get_data_file_path(dbfs_path):
    file_name_list = dbutils.fs.ls(dbfs_path)
    file_list = []
    for i in file_name_list:
        if str(i.name).endswith('.parquet'):
            file_list.append(i.path)
    return file_list[0]

# COMMAND ----------

# Criando o datasource usando um dataframe
dataframe_datasource = context.sources.add_or_update_spark(
    name="spark_in_memory_datasource_claims",
)
file_path = get_data_file_path('dbfs:/pece-poli-de/silver/claims/')

# COMMAND ----------

# Criando o data asset
df = spark.read.parquet(file_path)
dataframe_asset = dataframe_datasource.add_dataframe_asset(
    name="claims_silver",
    dataframe=df,
)

# COMMAND ----------

# Contruindo a requisição batch
batch_request = dataframe_asset.build_batch_request()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Criando as expectations

# COMMAND ----------

# Criando o validador
expectation_suite_name = "validacao_claims"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

# COMMAND ----------

validator.columns()

# COMMAND ----------

# validação de colunas com valores nulos
validator.expect_column_values_to_not_be_null(column="categoria")
validator.expect_column_values_to_not_be_null(column="nome")
validator.expect_column_values_to_not_be_null(column="cnpj")
validator.expect_column_values_to_not_be_null(column="índice")
validator.expect_column_values_to_not_be_null(column="quantidade_de_reclamações_reguladas_procedentes")
validator.expect_column_values_to_not_be_null(column="quantidade_de_clientes_–_scr")

# validator.expect_column_values_to_be_between(
#     column="congestion_surcharge", min_value=0, max_value=1000
# )

# COMMAND ----------

# verificando se colunas obrigatórias existem
validator.expect_column_to_exist('cnpj')

# COMMAND ----------

# Saving the expectation
validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

# Configurando o chckpoints
my_checkpoint_name = "my_databricks_checkpoint"

checkpoint = Checkpoint(
    name=my_checkpoint_name,
    run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
    data_context=context,
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
    action_list=[
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
    ],
)

# COMMAND ----------

# Salvando o checkpoint
context.add_or_update_checkpoint(checkpoint=checkpoint)

# COMMAND ----------

# Verificando o checkpoint
checkpoint_result = checkpoint.run()

# COMMAND ----------

# Verificando toda a configuração do checkpoint
print(checkpoint.get_config().to_yaml_str())

# COMMAND ----------

# move the report to the data quality bucket
dbutils.fs.cp('dbfs:/great_expectations/uncommitted/data_docs/local_site','gs://pece-poli-de/data_quality/claims/', True)

# COMMAND ----------

import time
time.sleep(6)
