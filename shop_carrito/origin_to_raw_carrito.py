# Databricks notebook source
datalake = 'datalakeformacion1'
contenedor_origen = 'shop-carrito'
contenedor_destino = 'juancaceffo'
tablas = ['articulos', 'campanias_gp', 'shop_carrito_estados_pago', 'shop_carrito_estados', 'Shop_carrito','Shop_clientes','shop_formapago']

# COMMAND ----------

spark = SparkSession.builder.appName('DataFrame').getOrCreate()

# COMMAND ----------

access_key = dbutils.secrets.get(scope = "conexiondatalake", key = "accesskey")
spark.conf.set(f'fs.azure.account.key.{datalake}.dfs.core.windows.net', access_key)

# COMMAND ----------

for tabla in tablas:
    separador = ',' if (tabla in ['articulos', 'campanias_gp']) else ';'
    exec(f'''df_{tabla} = spark.read.load(
        f"abfss://{contenedor_origen}@{datalake}.dfs.core.windows.net/{tabla}.csv",
        format = "csv",
        sep = "{separador}",
        header = "true")''')

# COMMAND ----------

df_shop_formapago.display()

# COMMAND ----------

for tabla in tablas:
    exec(f"""df_{tabla}.write.csv(
        path=f'abfss://{contenedor_destino}@{datalake}.dfs.core.windows.net/shop-carrito/raw_data/{tabla}.csv',
        header=True,
        mode = 'overwrite')""")

