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
from pyspark.sql.functions import when, lit

# Calcular K_hoje
df_transformado = df_transformado.withColumn("K_hoje", 
                                             col("K_ontem") + when(col("J_ontem").isNull(), 
                                             col("data_hoje") - col("I_ontem"))
                                             .otherwise(col("data_hoje") - col("J_ontem")))

# Calcular N_hoje
df_transformado = df_transformado.withColumn("N_hoje", (col("K_hoje") - col("K_ontem")) * col("H_ontem"))

# Calcular L_hoje
df_transformado = df_transformado.withColumn("L_hoje", col("L_ontem") + col("N_hoje"))

# Calcular M_hoje
df_transformado = df_transformado.withColumn("M_hoje", col("D_ontem") - col("L_hoje"))

# Fazer o append da linha "hoje"
df_hoje = df_transformado.withColumn("data", lit(data_hoje))  # Definir a data de hoje

df_final = df_hoje.select("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K_hoje", "L_hoje", "M_hoje", "N_hoje")  # Selecionar colunas finais

