# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import re

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analisis de de carrito shop

# COMMAND ----------

container_origen = 'juancaceffo'
datalake = 'datalakeformacion1'

# COMMAND ----------

access_key = dbutils.secrets.get('conexiondatalake', 'accesskey')
spark.conf.set(f'fs.azure.account.key.{datalake}.dfs.core.windows.net', access_key)

# COMMAND ----------

df_Shop_carrito_table = spark.read.parquet(f"abfss://{container_origen}@{datalake}.dfs.core.windows.net/trusted/tablon_shop_carrito")

# COMMAND ----------

df_Shop_carrito_table.createOrReplaceTempView("shop_carrito")

# COMMAND ----------

# MAGIC %md
# MAGIC 3) Una vez que tengas el tablon shop_carrito, realizar los analisis correspondientes:
# MAGIC
# MAGIC a) Quiero saber qué cantidad de solicitudes de compra se encuentran en cada estado, y el porcentaje en relación al total.
# MAGIC
# MAGIC b) Quiero conocer los clientes principales de la empresa, generando un ranking por cantidad y otro por monto, teniendo en cuenta solamente las compras finalizadas (efectuadas).
# MAGIC
# MAGIC c) Queremos ver cuáles fueron los medios de pago más utilizados por los clientes
# MAGIC
# MAGIC d) Realizar un analisis donde me determine las campanias mas eficaces.
# MAGIC
# MAGIC e) realizar 2 analisis mas y que nos expliques los resultados obtenidos.

# COMMAND ----------

# MAGIC %md
# MAGIC a) Quiero saber qué cantidad de solicitudes de compra se encuentran en cada estado, y el porcentaje en relación al total.

# COMMAND ----------

DF_ESTADO_PAGOS = df_Shop_carrito_table.groupBy("estado_ShEstp").count().withColumnRenamed("estado_ShEstp","estado_compras")

# COMMAND ----------

DF_ESTADO_PAGOS.withColumn('porcentaje',round(col("count") * 100 / df_Shop_carrito_table.count(),3)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC b) Quiero conocer los clientes principales de la empresa, generando un ranking por cantidad y otro por monto, teniendo en cuenta solamente las compras finalizadas (efectuadas).

# COMMAND ----------

FILTER_PAGO = df_Shop_carrito_table.where(df_Shop_carrito_table.estado_ShEstp == "Pago")
def rankByColumn(column):
    return FILTER_PAGO.orderBy(column,ascending = False).withColumn("rank",monotonically_increasing_id()+1).select("rank","nombre","apellido",column)

# COMMAND ----------

rankByColumn("monto").limit(10).display()

# COMMAND ----------

rankByColumn("cantidad").limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC c) Queremos ver cuáles fueron los medios de pago más utilizados por los clientes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT formapago, count(1) as Usos  
# MAGIC FROM shop_carrito
# MAGIC GROUP BY formapago
# MAGIC ORDER BY Usos DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC d) Realizar un analisis donde me determine las campanias mas eficaces.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- una forma de determinar las campañas mas eficases es por el capital generado durante las mismas
# MAGIC SELECT campania, round(sum(monto_DLS),3) as monto_por_campania
# MAGIC FROM shop_carrito
# MAGIC WHERE estado_ShEstp == "Pago"
# MAGIC GROUP BY campania
# MAGIC ORDER BY monto_por_campania DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC e) realizar 2 analisis mas y que nos expliques los resultados obtenidos.

# COMMAND ----------

# MAGIC %sql 
# MAGIC --1) provincias donde los clientes pusieron mas dinero
# MAGIC SELECT provincia,round(sum(monto_DLS),3) as USD
# MAGIC FROM shop_carrito
# MAGIC GROUP BY provincia
# MAGIC ORDER BY USD DESC
# MAGIC LIMIT 10
# MAGIC --con esta query obtenemos las 10 provincias donde los clientes mas plata pusieron. Puede ser interesante para el analisis, como por ejemplo destinar mas capital en campañas en las provincia donde los clientes estan mas dispuestos a gastar

# COMMAND ----------

# MAGIC %sql
# MAGIC --cantidad de clientes masculinos y cantidad de clientes femeninos
# MAGIC SELECT sexo,count(1) as cant 
# MAGIC FROM shop_carrito
# MAGIC GROUP BY sexo
# MAGIC --este analisis puede llegar a ser interesante para realizar campañias destinadas a atraer el genero con menos comporas tiene
