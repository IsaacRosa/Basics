#Criar 2 linhas
from pyspark.sql import Row

# Criar duas novas linhas
new_rows = [Row(A=value_A1, B=value_B1, C=value_C1, D=value_D1),  # Defina os valores
            Row(A=value_A2, B=value_B2, C=value_C2, D=value_D2)]

# Convertendo para DataFrame
new_df = spark.createDataFrame(new_rows)

# Fazer o append das novas linhas na tabela existente
df_transformado = df_filtered.union(new_df)



#Verificar se x = diferença entre y e z
from pyspark.sql.functions import datediff

# Verificar se G_restantes é igual à diferença de dias entre E_ctt e F_vv
df_transformado = df_transformado.withColumn("verificado_G_restantes", 
                                             col("G_restantes") == datediff(col("F_vv"), col("E_ctt")))



# Verificar se G_restantes é igual à diferença de dias entre E_ctt e F_vv
df_transformado = df_transformado.withColumn("verificado_G_restantes", 
                                             col("G_restantes") == datediff(col("F_vv"), col("E_ctt")))





#Diversas operações
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, datediff
from datetime import datetime, timedelta

# Inicialização da sessão Spark com Delta Lake configurado
spark = SparkSession.builder \
    .appName("GlueJobDeltaLakeTransformations") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Parâmetros recebidos do gatilho
data_hoje = "2024-08-14"
data_ontem = (datetime.strptime(data_hoje, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

# Caminho no S3 da tabela Delta Lake
delta_table_path = "s3://your-bucket/your-delta-table/"

# Ler os dados da tabela Delta Lake
df = spark.read.format("delta").load(delta_table_path)

# Filtrar os dados de ontem
df_ontem = df.filter(col("data") == data_ontem)

# Criar um DataFrame com as colunas padrão e suas versões com sufixo "_ontem"
df_combined = df_ontem.select(
    col("A"),
    col("B"),
    col("C"),
    col("D"),
    col("E"),
    col("F"),
    col("G"),
    col("H"),
    col("I"),
    col("J"),
    col("K"),
    col("L"),
    col("M"),
    col("N"),
    col("A").alias("A_ontem"),
    col("B").alias("B_ontem"),
    col("C").alias("C_ontem"),
    col("D").alias("D_ontem"),
    col("E").alias("E_ontem"),
    col("F").alias("F_ontem"),
    col("G").alias("G_ontem"),
    col("H").alias("H_ontem"),
    col("I").alias("I_ontem"),
    col("J").alias("J_ontem"),
    col("K").alias("K_ontem"),
    col("L").alias("L_ontem"),
    col("M").alias("M_ontem"),
    col("N").alias("N_ontem")
)

# Realizar as transformações para os dados de hoje usando as colunas sem sufixo
df_transformado = df_combined.withColumn(
    "K",
    col("K_ontem") + when(col("J_ontem").isNull(), datediff(lit(data_hoje), col("I_ontem"))).otherwise(datediff(lit(data_hoje), col("J_ontem")))
).withColumn(
    "N",
    (col("K") - col("K_ontem")) * col("H_ontem")
).withColumn(
    "L",
    col("L_ontem") + col("N")
).withColumn(
    "M",
    col("D_ontem") - col("L")
).withColumn(
    "data", lit(data_hoje)
)

# Remover as colunas de ontem
df_final = df_transformado.drop(
    "A_ontem", "B_ontem", "C_ontem", "D_ontem", "E_ontem", "F_ontem", 
    "G_ontem", "H_ontem", "I_ontem", "J_ontem", "K_ontem", "L_ontem", "M_ontem", "N_ontem"
)

# Escrever os dados transformados no Delta Lake com append
df_final.write.format("delta").mode("append").save(delta_table_path)

# Encerrar a sessão Spark
spark.stop()


