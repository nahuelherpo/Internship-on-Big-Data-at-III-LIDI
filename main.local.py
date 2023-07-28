#
# Distributed solution to run on local
#
# Author: Herpo Nahuel
# Dec. 2022
#

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType
from Classes import TreeContext
from datetime import datetime
import sys
import os


####################################################
# EXECUTION
# spark-submit --py-files Classes.py main.local.py N
# --where N is the index of dataset (datos1 or datos2 and so on...)
####################################################

# Configuración Spark
conf = SparkConf().setMaster("local").setAppName("Framework")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
sc.setLogLevel("ERROR")  # Loggers


# Take index of directory
directory_index = sys.argv[1]

# LOAD THE FILES

# Tabla individuos (tabla principal)
columnas = ['ID', 'FECHA_NAC', 'VIVE_EN', 'ID_MADRE', 'ORDEN']

schema = StructType([
    StructField("ID", StringType(), True),
    StructField("FECHA_NAC", DateType(), True),
    StructField("VIVE_EN", StringType(), True),
    StructField("ID_MADRE", StringType(), True),
    StructField("ORDEN", IntegerType(), True),
])

individuos = spark.read.option("delimiter", ",").option(
    "dateFormat", "dd/MM/yyyy").csv("datos" + directory_index + "/individuos/", header="true", schema=schema)

tc = TreeContext(spark, individuos, columnas, "TABLA_BASE",
                 "ID", "ID_MADRE", '', "ORDEN")

# Producción - Tabla anexa
columnas = ["ID_PROD", "ID_VAC", "FECHA", "NRO_PROD"]

schema = StructType([
    StructField("ID_PROD", StringType(), True),
    StructField("ID_VAC", StringType(), True),
    StructField("FECHA", DateType(), True),
    StructField("NRO_PROD", IntegerType(), True),
])

produccion = spark.read.option("header", "true").option("delimiter", ",").option(
    "dateFormat", "dd/MM/yyyy").csv("datos" + directory_index + "/producciones/", schema=schema)

# Add the auxiliary table to the TreeContext
tc = tc.addDatum(produccion, columnas, "ID_VAC", "PRODUCCION", None)

# ========================================================================
#
#
# Set the first timestamp
t0 = datetime.timestamp(datetime.now())
# =================================CONSULTA===================================
print("Consulta: Vacas que han producido mas de 40L de leche, su madre produjo 40L de leche y tienen al menos 2 hermanas")
# 1. Filter only cows that have more than one sister
tc1 = tc.filter(lambda ind: ind.__sistersCount__() > 1)
#2. Cows that have more than 40L of production in any record
tc2 = tc1.filter(lambda ind: ind["NRO_PROD"].value() > 40)
#3. Cows that have a mother with more than 40L in some record
tc3 = tc2.filter(lambda ind: ind.mother["NRO_PROD"].value() > 40)
p = tc3.collect()  # "force" execution

# Calculate and print the time
time = datetime.timestamp(datetime.now()) - t0
print(time, " sec")
os.system("echo " + str(time) + " >> datos" + directory_index + "/times.txt")
# --------------------------------------------------------------------
#Save the result
if p != None:
    p.toPandas().to_csv('datos' + directory_index + '/distributed.csv', index=False)
else:
    os.system("echo ID,FECHA_NAC,VIVE_EN,ORDEN,sisters_count,NRO_PROD,ID_MADRE,mother_NRO_PROD > distributed.csv")
# ============================================================================
