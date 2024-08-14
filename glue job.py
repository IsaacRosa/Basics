import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inicialização da sessão Spark
spark = SparkSession.builder \
    .appName("GlueJobDeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Parâmetro passado pelo gatilho externo (pode ser via argumento ou evento)
args = sys.argv
data_hoje = args[1]  # Espera uma string no formato 'YYYY-MM-DD'

# Data de ontem e hoje para filtragem de partições
data_ontem = (datetime.strptime(data_hoje, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

# Caminho da tabela Delta Lake
delta_table_path = "s3://your-bucket/your-delta-table/"

# Ler dados da partição de ontem
df_ontem = spark.read.format("delta").load(delta_table_path).filter(col("data_particao") == data_ontem)

# Ler dados da partição de hoje (assumindo que a partição já existe)
df_hoje = spark.read.format("delta").load(delta_table_path).filter(col("data_particao") == data_hoje)

# Transformação entre os dados de ontem e hoje (implementação necessária)
# df_transformado = (transformação)  # Exemplo: df_transformado = df_ontem.union(df_hoje).distinct()

# Atualizar a tabela Delta com os novos dados
# Supondo que a transformação resultou em df_transformado
# Se não houver necessidade de uma transformação complexa, você pode usar df_hoje diretamente
df_hoje.write.format("delta").mode("append").save(delta_table_path)

# Se necessário, você pode sobrescrever a tabela inteira ou apenas as partições relevantes
# df_transformado.write.format("delta").mode("overwrite").save(delta_table_path)

# Encerrando a sessão Spark
spark.stop()
