# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC • Shop-carrito -> shop-clientes por el campo “id_cliente”
# MAGIC
# MAGIC • Shop-carrito -> shop_campanias por el campo “id_campania”
# MAGIC
# MAGIC • Shop-carrito -> shop_articulos por el campo “id_articulo_ppal”
# MAGIC
# MAGIC • Shop-carrito -> shop_formapago por el campo “id_fpago”
# MAGIC
# MAGIC • Shop-carrito -> shop_carrito_estados por el campo “estado”
# MAGIC
# MAGIC • Shop-carrito -> shop_carrito_estados_pago por el campo “estado_pago”
# MAGIC
# MAGIC Al finalizar guardar el tablon en formato parquet en una carpeta que diga Trusted, ejemplo {contenedor}/trusted/tablon_shop_carrito/

# COMMAND ----------

# MAGIC %md
# MAGIC ## variables de entorno

# COMMAND ----------

datalake = 'datalakeformacion1'
contenedor_origen = 'juancaceffo'
tablas = ['articulos', 'campanias_gp', 'shop_carrito_estados_pago', 'shop_carrito_estados', 'Shop_carrito','Shop_clientes','shop_formapago']

# COMMAND ----------

# MAGIC %md
# MAGIC ## inicializacion de spark y acceso al datalake

# COMMAND ----------

spark = SparkSession.builder.appName('DataFrame').getOrCreate()

# COMMAND ----------

access_key = dbutils.secrets.get(scope = "conexiondatalake", key = "accesskey")
spark.conf.set(f'fs.azure.account.key.{datalake}.dfs.core.windows.net', access_key)

# COMMAND ----------

# DBTITLE 1,cargado de df
for tabla in tablas:
    exec(f'''df_{tabla} = spark.read.options(inferSchema="true").csv(
        f"abfss://{contenedor_origen}@{datalake}.dfs.core.windows.net/shop-carrito/raw_data/{tabla}.csv",
        sep = ",",
        header = "true")''')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpieza df_Shop_carrito
# MAGIC ### buscamos columnas que no nos sirvan para el analisis y las dropeamos

# COMMAND ----------

#generamos una tupla para el comando drop con las columnas que creemos inecesarias para el analisis
COLUMNS_TO_DROP = ("entrecalles","direccion","numero","cp","localidad","estado_fecha","corte_generado_fecha","corte_generado","comentarios","comentarios_internos","ano","nota_credito","estado_logistico","id_oca","fecha_anulado","fecha_pago_pf","status_tango","fecha_aprobacion_bo","mail_fc_env","inbound","fecha_dia","fecha_mes","fecha_hora","fecha_ano","campania","piso","dpto","tarjeta_titular","tarjeta_pagos","tarjeta_aprobacion","tarjeta_numero","tarjeta_codigo","tarjeta_vencimiento","t_entrega","status_tango","fecha_entrega","franja_horaria","sms_enviado","mail_ap_bo","export_hermes","hermes","export_bi","mail_ap_bo","indice_hermes","wp_notific","donacion","recompra_empresa","pae_final","pae_final","costo_logistica","telefono","telefono_alternativo","asiento_refacturacion","epack","id_campania_recalc","fecha_fin_tratamiento","nro_guia","id_pais_negocio","fecha")

# COMMAND ----------

df_Shop_carrito = df_Shop_carrito.drop(*COLUMNS_TO_DROP)

# COMMAND ----------

# MAGIC %md
# MAGIC ### analizamos casos de columnas/filas nulas y removemos segun convenga

# COMMAND ----------

#funcion para eliminar todas las columnas que tengan un porcentaje x de nulos
def dropColumnsWithNullPercentege(df, percentage):
    TOTAL_PERCENTAGE = 100/df.count()
    list_cantNull_columns = df.select([sum(isnull(column).cast("int")).alias(column) for column in df.columns]).collect()[0].asDict()
    for column in list_cantNull_columns:
        if((TOTAL_PERCENTAGE*list_cantNull_columns[column]) >= percentage):
            df = df.drop(column)
    return df

# COMMAND ----------

#eliminamos columnas duplicadas si es que existen
df_Shop_carrito = df_Shop_carrito.dropDuplicates()

# COMMAND ----------

df_Shop_carrito = dropColumnsWithNullPercentege(df_Shop_carrito,70.0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Casteamos columnas y validamos informacion de las mismas
# MAGIC empezamos casteando las fechas a formato DateTime

# COMMAND ----------

COLUMNS_FECHAS =  [column for column in df_Shop_carrito.columns if "fecha" in column]

# COMMAND ----------

for column in COLUMNS_FECHAS:
    df_Shop_carrito = df_Shop_carrito.withColumn(column,to_date(from_unixtime(col(column))))

# COMMAND ----------

df_Shop_carrito.select(COLUMNS_FECHAS).dtypes

# COMMAND ----------

df_Shop_carrito = df_Shop_carrito.withColumnRenamed("fecha_unix","fecha")

# COMMAND ----------

#nromalizamos los campos provincia y courier
for column in ["provincia", "courier"]:    
    df_Shop_carrito = df_Shop_carrito.withColumn(column,upper(trim(col(column))))

# COMMAND ----------

#nomralizacion de emails
SPLIT_EMAIL = split(df_Shop_carrito.email,('@'))
df_Shop_carrito = df_Shop_carrito.withColumn("email",concat(SPLIT_EMAIL[0],lit("@"),lower(SPLIT_EMAIL[1])))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpieza de df_articulos
# MAGIC empezamos por remober columnas con un porcentaje de nulos mayor al 70%

# COMMAND ----------

df_articulos = dropColumnsWithNullPercentege(df_articulos,70.0)

# COMMAND ----------

df_articulos = df_articulos.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC eliminamos columnas inecesarias para el analisis

# COMMAND ----------

COLUMNS_TO_DROP = ["imagen1","opciones","pago_facil","sku","email_fc","parent_articulo","aviso_reposicion","tiempo_reposicion","costo_envio","URL","id_articulo_n","export_bi","regalia","id_cobranza","id_subnegocio","id_negocio","id_linea_producto","descripcion"]

# COMMAND ----------

df_articulos = df_articulos.drop(*COLUMNS_TO_DROP)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpieza df_Shop_clientes

# COMMAND ----------

#removemos las columnas con 70% o mas de nulos
df_Shop_clientes = dropColumnsWithNullPercentege(df_Shop_clientes,70.0)

# COMMAND ----------

#eliminamos filas duplicadas
df_Shop_clientes = df_Shop_clientes.dropDuplicates()

# COMMAND ----------

COLUMNS_TO_DROP = ["telefono_laboral","direccion","numero","cp","entrecalles","no_email","provincia","id_pais","export_hermes","id_fidelidad","crated","id_localidad","created","longitud","latitud"]

# COMMAND ----------

df_Shop_clientes = df_Shop_clientes.drop(*COLUMNS_TO_DROP)

# COMMAND ----------

df_Shop_clientes = df_Shop_clientes.withColumnRenamed("localidad","provincia")

# COMMAND ----------

#normalizacion de columnas string nombre, apellido
for column in ["nombre","apellido"]:
    df_Shop_clientes = df_Shop_clientes.withColumn(column,initcap(col(column)))

# COMMAND ----------

df_Shop_clientes = df_Shop_clientes.withColumn("provincia",upper(col("provincia"))) 

# COMMAND ----------

#normalizacion de la columna de emails
SPLIT_EMAIL = split(df_Shop_clientes.email,('@'))
df_Shop_clientes = df_Shop_clientes.withColumn("email",concat(SPLIT_EMAIL[0],lit("@"),lower(SPLIT_EMAIL[1])))


# COMMAND ----------

# MAGIC %md
# MAGIC ## limpieza de los df restantes

# COMMAND ----------

SMALL_DF = [df_campanias_gp,df_shop_carrito_estados,df_shop_carrito_estados_pago,df_shop_formapago]

# COMMAND ----------

for df in SMALL_DF:
    df = df.dropDuplicates()
    df = dropColumnsWithNullPercentege(df,70.0) 

# COMMAND ----------

COLUMNS_TO_DROP = ["idbase","dbsize","created","updated","estado"]

# COMMAND ----------

df_campanias_gp = df_campanias_gp.drop(*COLUMNS_TO_DROP)

# COMMAND ----------

COLUMNS_TO_DROP = ["descripcion","estado","image","cuotas","metodo","recuperable","asiento_tango","limitar_digitos","id_gc","n_estable_gc","n_estable_pn"]

# COMMAND ----------

df_shop_formapago = df_shop_formapago.drop(*COLUMNS_TO_DROP)

# COMMAND ----------

# MAGIC %md
# MAGIC ### funicones de cambio de nombre de columnas

# COMMAND ----------

#recibe dos listas y devuelve una lista con los elementos compartidos de ambas
def sameNamesBothLists(lista:list,lista_1:list):
    return [name for name in lista if name in lista_1]

# COMMAND ----------

#recibe un dataFrame una lista de las columnas que se desan cambiar y un string para concatenar a esas columnas que se desan cambiar
def newDfWithConcatStrDetColumns(df,columnsToChange:list,strConcat:str):
    NEW_COLUMNS = tuple([name+strConcat if name in columnsToChange else name for name in df.columns])
    return df.toDF(*NEW_COLUMNS)
    

# COMMAND ----------

#funcion para unificar las columnas que tenian iguales nombres con df_shop_carrito y los contenidos se relacionaban entre si
def unifySameColumns(df,ListNameColumns:list,modfNameSameColumn:str):
    for colum in ListNameColumns:
        MODF_COL = colum+modfNameSameColumn
        df = df.withColumn(colum,when(isnull(col(colum)),col(MODF_COL)).otherwise(col(colum)))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## • Shop-carrito -> shop-clientes por el campo “id_cliente”
# MAGIC
# MAGIC 1- investigamos columnas de los dos df

# COMMAND ----------

SAME_NAMES_CAR_CLI = sameNamesBothLists(df_Shop_clientes.columns,df_Shop_carrito.columns)

# COMMAND ----------

#cambiamos el nombre de las columnas de shop_carrito que son iguales en shop_clientes
df_Shop_clientes = newDfWithConcatStrDetColumns(df_Shop_clientes,SAME_NAMES_CAR_CLI,"_ShCli")

# COMMAND ----------

df_Shop_carrito = df_Shop_carrito.join(df_Shop_clientes,df_Shop_carrito.id_cliente == df_Shop_clientes.id_ShCli ,how="left")

# COMMAND ----------

SAME_NAMES_CAR_CLI.pop(0)

# COMMAND ----------

df_Shop_carrito = unifySameColumns(df_Shop_carrito,SAME_NAMES_CAR_CLI,"_ShCli")

# COMMAND ----------

df_Shop_carrito  = df_Shop_carrito.drop(*[name+"_ShCli" for name in SAME_NAMES_CAR_CLI])

# COMMAND ----------

# MAGIC %md
# MAGIC ## • Shop-carrito -> shop_campanias por el campo “id_campania”

# COMMAND ----------

SAME_COLUMNS_CAR_CAM = sameNamesBothLists(df_Shop_carrito.columns,df_campanias_gp.columns)

# COMMAND ----------

df_campanias_gp = newDfWithConcatStrDetColumns(df_campanias_gp,SAME_COLUMNS_CAR_CAM,"_ShCam")

# COMMAND ----------

df_Shop_carrito = df_Shop_carrito.join(df_campanias_gp,df_Shop_carrito.id_campania == df_campanias_gp.id_ShCam,how="left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## • Shop-carrito -> shop_articulos por el campo “id_articulo_ppal”

# COMMAND ----------

SAME_NAME_CAR_ART = sameNamesBothLists(df_Shop_carrito.columns,df_articulos.columns)

# COMMAND ----------

df_articulos = newDfWithConcatStrDetColumns(df_articulos,SAME_NAME_CAR_ART,"_ShArt")

# COMMAND ----------

df_Shop_carrito = df_Shop_carrito.join(df_articulos,df_Shop_carrito.id_articulo_ppal == df_articulos.id_ShArt,how="left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## • Shop-carrito -> shop_formapago por el campo “id_fpago”

# COMMAND ----------

SAME_NAME_CAR_FPAG = sameNamesBothLists(df_Shop_carrito.columns,df_shop_formapago.columns)

# COMMAND ----------

df_shop_formapago = newDfWithConcatStrDetColumns(df_shop_formapago,SAME_NAME_CAR_FPAG,"_ShFp") 

# COMMAND ----------

df_Shop_carrito = df_Shop_carrito.join(df_shop_formapago,df_Shop_carrito.id_fpago == df_shop_formapago.id_ShFp,how="left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## • Shop-carrito -> shop_carrito_estados por el campo “estado”

# COMMAND ----------

SAME_NAME_CAR_EST = sameNamesBothLists(df_Shop_carrito.columns,df_shop_carrito_estados.columns)

# COMMAND ----------

df_shop_carrito_estados = newDfWithConcatStrDetColumns(df_shop_carrito_estados,SAME_NAME_CAR_EST,"_ShEst")

# COMMAND ----------

df_Shop_carrito = df_Shop_carrito.join(df_shop_carrito_estados,df_Shop_carrito.estado == df_shop_carrito_estados.id_ShEst,how="left")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## • Shop-carrito -> shop_carrito_estados_pago por el campo “estado_pago”

# COMMAND ----------

SAME_NAME_CAR_ESTP = sameNamesBothLists(df_Shop_carrito.columns,df_shop_carrito_estados_pago.columns)

# COMMAND ----------

df_shop_carrito_estados_pago = newDfWithConcatStrDetColumns(df_shop_carrito_estados_pago,SAME_NAME_CAR_ESTP,"_ShEstp")

# COMMAND ----------

df_Shop_carrito = df_Shop_carrito.join(df_shop_carrito_estados_pago,df_Shop_carrito.estado_pago == df_shop_carrito_estados_pago.id_ShEstp,how="left")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ##guardamos la tabla de shop carrito modificada

# COMMAND ----------

df_Shop_carrito.write.parquet(
    path= f'abfss://{contenedor_origen}@{datalake}.dfs.core.windows.net/shop_carrito/trusted/tablon_shop_carrito',
    mode= "overwrite"
)
