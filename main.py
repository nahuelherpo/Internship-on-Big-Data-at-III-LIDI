from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, IntegerType, StringType
from Classes import Ind, TreeContext, Utils
from datetime import datetime
import sys
import os

# Take the number of trees from the arguments
directory_index = sys.argv[1]
print("Execution number:", directory_index)

####################################################
# EJECUCION
# spark-submit --py-files Classes.py main.py
####################################################

# Configuración Spark
conf = SparkConf().setMaster("local").setAppName("Framework")  # Obj
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
sc.setLogLevel("ERROR")  # Loggers

# CARGA DE ARCHIVOS

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

# Agrego la tabla auxiliar al TreeContext
tc = tc.addDatum(produccion, columnas, "ID_VAC", "PRODUCCION", None)

# ========================================================================
# Filtrado del árbol
print()
# Los filtros son de inclusión, obtengo como resultado los que cumplen la condición
#
# Set the first timestamp
t0 = datetime.timestamp(datetime.now())
# =================================CONSULTA===================================
print("Consulta: Vacas que han producido mas de 40L de leche, su madre produjo 40L de leche y tienen al menos 2 hermanas")
# Vacas que tienen mas de una herma
tc1 = tc.filtrar(lambda ind: ind.__sistersCount__() > 1)
# Vacas que tienen mas de 40L de produccion en algun registro
tc2 = tc1.filtrar(lambda ind: ind["NRO_PROD"].value() > 40)
# Vacas que tienen madre con mas de 40L en algun registro
tc3 = tc2.filtrar(lambda ind: ind.mother["NRO_PROD"].value() > 40)
p = tc3.collect()  # "forzar" ejecución
if p != None:
    print(p.show(200))
else:
    print(None)
# Calculate and print the time
time = datetime.timestamp(datetime.now()) - t0
print(time, " sec")
os.system("echo " + str(time) + " >> datos" + directory_index + "/times.txt")
# --------------------------------------------------------------------
exit()
# ============================================================================

# Me quedo con las vacas que tienen madre
print("Filtro 1: Vacas que tienen madre")
tc2 = tc.filtrar(lambda ind: ind.__motherExists__())
p = tc2.collect()  # "forzar" ejecución
if p != None:
    print(p.show(100))
else:
    print(None)
#
# Eliminar a los primeros partos
print("Filtro 2: Eliminar primeros partos")
tc4 = tc.filtrar(lambda ind: ind.__childrenOrder__() > 1)
p = tc4.collect()  # "forzar" ejecución
if p != None:
    print(p.show(100))
else:
    print(None)
#
# Eliminar a las vacas sin crias
print("Filtro 3: Eliminar vacas sin crias")
tc3 = tc.filtrar(lambda ind: ind.__childrenCount__() != 0)
p = tc3.collect()  # "forzar" ejecución
if p != None:
    print(p.show(100))
else:
    print(None)
#
# Me quedo con las vacas que tienen una sola cria
print("Filtro 4: Vacas que solo tienen una cria")
tc3 = tc.filtrar(lambda ind: ind.__childrenCount__() == 1)
p = tc3.collect()  # "forzar" ejecución
if p != None:
    print(p.show(100))
else:
    print(None)
#
##
print("Filtro 5: Vacas con tres crias")
tc3 = tc.filtrar(lambda ind: ind.__childrenCount__() == 3)
p = tc3.collect()  # "forzar" ejecución
if p != None:
    print(p.show(100))
else:
    print(None)

# Vacas que producieron menos de 15L de leche
print("Filtro 6: Filtrar los individuos con menos de 2 producciones")
tc2 = tc.filtrar(lambda ind: ind["NRO_PROD"].value() < 15)
p = tc2.collect()  # "forzar" ejecución
if p != None:
    print(p.show(1200))
else:
    print(None)

# Vacas que tienen mas de una hermana
print("Filtro 7: Vacas que tienen mas de una hermana")
tc5 = tc.filtrar(lambda ind: ind.__sistersCount__() > 1)
p = tc5.collect()  # "forzar" ejecución
if p != None:
    print(p.show(1200))
else:
    print(None)

# Vacas que tienen mas de una hermana
print("Filtro 8: Vacas con madre que tiene mas de una hermana")
tc5 = tc.filtrar(lambda ind: ind.mother.__sistersCount__() > 1)
p = tc5.collect()  # "forzar" ejecución
if p != None:
    print(p.show(1200))
else:
    print(None)

# Vacas que tienen mas de una hermana
print("Filtro 9: Vacas con madre que tiene mas de una hermana")
tc5 = tc.filtrar(lambda ind: ind.mother.__sistersCount__() > 1)
p = tc5.collect()  # "forzar" ejecución
if p != None:
    print(p.show(1200))
else:
    print(None)

# Vacas que producieron menos de 15L de leche
print("Filtro 10: Descendientes")
p = tc.descendents(4)
# p = tc5.collect()  # "forzar" ejecución
if p != None:
    print(p.show(400))
else:
    print(None)

# --------------------------------------------------------------------
print("Consulta: Vacas que hayan dado mas de 40L en un registro, que ademas tengan al menos una hermana que haya dado 40L o mas, que su abuela haya dado 40L o mas y que tenga al menos una nieta que haya dado 40L o mas en un registro de produccion")
# Vacas que tienen madre y con registor de produccion de mas de 40L
# mother.mother no se puede...
tc1 = tc.filtrar(lambda ind: ind.mother["NRO_PROD"].value() == 1)
tc1 = tc.filtrar(lambda ind: (
    ind["NRO_PROD"].value() == 1) and ind.mother["NRO_PROD"].value() == 1)
p = tc1.collect()  # "forzar" ejecución
# p = tc2.descendents(5)  # "forzar" ejecución
if p != None:
    print(p.show(200))
else:
    print(None)
# --------------------------------------------------------------------
exit()

# PARSEAR DIFERENTES TIPOS DE FUNCIONES, LAS .COUNT .VALUE Y LAS DE __
print("PRUEBA")
tc3 = tc.filtrar(lambda ind: 1 < ind.__childrenCount__()
                 and ind.__childrenCount__() < 3)
p = tc3.collect()  # "forzar" ejecución
print(p.show())

tc3 = tc.filtrar(lambda ind: 1 < ind.__childrenCount__()
                 and 3 > ind.__childrenCount__())
p = tc3.collect()  # "forzar" ejecución
print(p.show())

tc3 = tc.filtrar(lambda ind: ind.__childrenCount__()
                 > 1 and ind.__childrenCount__() < 3)
p = tc3.collect()  # "forzar" ejecución
print(p.show())

tc3 = tc.filtrar(lambda ind: ind.__childrenCount__()
                 > 1 and 3 > ind.__childrenCount__())
p = tc3.collect()  # "forzar" ejecución
print(p.show())

tc3 = tc.filtrar(lambda ind: ind.__motherExists__(
) and ind.__childrenCount__() > 2 and ind.__childrenOrder__() == 2)
p = tc3.collect()  # "forzar" ejecución
print(p.show())

# Eliminar a los crías cuya gestación tuvo menos de 7 controles lecheros
print("Filtro 5")
tc5 = tc.filtrar(lambda ind: ind["CONTROLES"].count()
                 > 7 and ind.__childrenCount__() < 3)
p = tc5.collect()  # "forzar" ejecución
print(p.show())

print("Filtro Nuevo5")
tc2 = tc.filtrar(lambda ind: ind["VIVE_EN"].value() == "2")
p = tc2.collect()  # "forzar" ejecución
print(p.show())
#
#print("Filtro Nuevo5.1")
tc8 = tc.filtrar(lambda ind: ind["TABLA_BASE"]["ORDEN"].value() > 2)
p = tc8.collect()  # "forzar" ejecución
print(p.show())

# filtra aquello individuos con menos de 2 producciones
print("Filtro Nuevo4.1")
tc8 = tc.filtrar(lambda ind: ind["NRO_PROD"].value() > 2)
p = tc8.collect()  # "forzar" ejecución
print(p.show())

print("Filtro Nuevo6")
tc2 = tc.filtrar(lambda ind: ind["ORDEN"].value()
                 == 1 or ind["ORDEN"].value() == 3)
p = tc2.collect()  # "forzar" ejecución
print(p.show())

print("Filtro Nuevo6.1")
tc2 = tc.filtrar(lambda ind: ind["ORDEN"].value() > 2)
p = tc2.collect()  # "forzar" ejecución
print(p.show())

print("Filtro Nuevo6")
tc2 = tc.filtrar(lambda ind: 1 < ind["ORDEN"].value() < 3)
p = tc2.collect()  # "forzar" ejecución
print(p.show())

print("Filtro Nuevo4")
tc8 = tc.filtrar(lambda ind: ind["PRODUCCION"]["CONTROLES"].count() > 2)
p = tc8.collect()  # "forzar" ejecución
print(p.show())

# Eliminar a los crías con menos de 6 controles lecheros
print("Filtro Nuevo1")
tc6 = tc.filtrar(lambda ind: ind["TABLA_BASE"]["CONTROLES"].count() > 5)
p = tc6.collect()  # "forzar" ejecución
print(p.show())

# Eliminar a los crías con menos de 2 producciones
print("Filtro Nuevo2")
#tc2 = tc.filtrar(lambda ind: len(ind["CONTROLES"]) >= 7)
tc7 = tc.filtrar(lambda ind: ind["PRODUCCION"].count() > 2)
p = tc7.collect()  # "forzar" ejecución
print(p.show())

# Eliminar a los crías con menos de 2 producciones
print("Filtro Nuevo3")
#tc2 = tc.filtrar(lambda ind: len(ind["CONTROLES"]) >= 7)
tc8 = tc.filtrar(lambda ind: ind["TABLA_BASE"]["PRODUCCION"].count() > 2)
p = tc8.collect()  # "forzar" ejecución
print(p.show())
print(p.show(50, False))

# Eliminar a las crías donde el tambo no es el mismo que el de la madre
print("Filtro 3.0")
tc2 = tc.filtrar(lambda ind: ind["VIVE_EN"].value()
                 == ind.mother["VIVE_EN"].value())
p = tc2.collect()  # "forzar" ejecución
print(p.show())

# Eliminar a las crías donde el tambo no es el mismo que el de la madre
print("Filtro 3.0")
tc2 = tc.filtrar(lambda ind: ind["VIVE_EN"].value()
                 != ind.mother["VIVE_EN"].value())
p = tc2.collect()  # "forzar" ejecución
print(p.show())

print("Filtro 3.1")
tc2 = tc.filtrar(
    lambda ind: ind.mother["VIVE_EN"].value() == ind["VIVE_EN"].value())
p = tc2.collect()  # "forzar" ejecución
print(p.show())

print("Filtro 3.1")
tc2 = tc.filtrar(lambda ind: ind.mother["TABLA_BASE"]["VIVE_EN"].value(
) == ind["TABLA_BASE"]["VIVE_EN"].value())
p = tc2.collect()  # "forzar" ejecución
print(p.show())

#
print("Filtro 3.2")
tc2 = tc.filtrar(
    lambda ind: ind.mother["VIVE_EN"].value() != ind["VIVE_EN"].value())
p = tc2.collect()  # "forzar" ejecución
print(p.show())

print("Filtro Nuevo7")
tc2 = tc.filtrar(lambda ind: ind.mother["VIVE_EN"].value() == "3")
p = tc2.collect()  # "forzar" ejecución
print(p.show())

tc2 = tc.filtrar(lambda ind: ind.mother["VIVE_EN"].value(
) == "3" and ind["ORDEN"].value() >= 2)
p = tc2.collect()  # "forzar" ejecución
print(p.show())

tc8 = tc.filtrar(lambda ind: ind["TABLA_BASE"]["PRODUCCION"].count() > 2 and (
    1 < ind["ORDEN"].value() < 3))
p = tc8.collect()  # "forzar" ejecución
print(p.show())

# ValueError: Some of types cannot be determined by the first 100 rows, please try again with sampling
##[Row(ID='1', FECHA_NAC=datetime.date(1991, 10, 4), VIVE_EN='1', ID_MADRE=None, ORDEN=1)]
# No lo quiere convertir a DF, es por un valor en None
print("Filtro Nuevo8")
tc2 = tc.filtrar(lambda ind: ind["FECHA_NAC"].value(
) == datetime.datetime.strptime("04/10/1991", '%d/%m/%Y').date())
p = tc2.collect()  # "forzar" ejecución
print(p.show())

tc2 = tc.filtrar(lambda ind: ind["FECHA"].value(
) == datetime.datetime.strptime("03/02/2000", '%d/%m/%Y').date())
p = tc2.collect()  # "forzar" ejecución
print(p.show())

# FUNCIONA
#(self.datum["PRODUCCION"][0].rdd).filter(lambda row:row["FECHA"] == datetime.datetime.strptime("03/02/2000",'%d/%m/%Y').date())
#[Row(ID='4', FECHA=datetime.date(2000, 2, 3), NRO_PROD=1)]
print("Filtro Nuevo8.1")
tc2 = tc.filtrar(lambda ind: ind.mother["FECHA"].value(
) == datetime.datetime.strptime("03/02/2000", '%d/%m/%Y').date())
p = tc2.collect()  # "forzar" ejecución
print(p.show())

print("Filtro 3.3")
tc2 = tc.filtrar(
    lambda ind: ind.mother["PRODUCCION"].count() < ind["PRODUCCION"].count())
p = tc2.collect()  # "forzar" ejecución
print(p.show())

#print("Filtro 3.4")
tc2 = tc.filtrar(lambda ind: ind["PRODUCCION"].count()
                 < ind.mother["PRODUCCION"].count())
p = tc2.collect()  # "forzar" ejecución
print(p.show())

# Eliminar las crías donde la dif de fechas es menor a 300
print("Filtro 2.1")
tc2 = tc.filtrar(lambda ind: ind["FECHA_NAC"].value()
                 > ind.mother["FECHA_NAC"].value())
p = tc2.collect()  # "forzar" ejecución
print(p.show())

print("Filtro 3.6")
tc2 = tc.filtrar(lambda ind: ind["TABLA_BASE"]["PRODUCCION"].count(
) < ind.mother["TABLA_BASE"]["PRODUCCION"].count())
p = tc2.collect()  # "forzar" ejecución
print(p.show())

# Eliminar las crías donde la dif de fechas es menor a 300
print("Filtro 2")
tc2 = tc.filtrar(lambda ind: ind["FECHA_NAC"].value(
) - ind.mother["FECHA_NAC"].value() > datetime.timedelta(days=800))
p = tc2.collect()  # "forzar" ejecución
print(p.show())

print("Filtro 3.5")
tc2 = tc.filtrar(lambda ind: ind.mother["PRODUCCION"]["CONTROLES"].count(
) > 3 and 3 < ind["PRODUCCION"]["CONTROLES"].count())
p = tc2.collect()  # "forzar" ejecución
print(p.show())

print("Filtro 3.5")
tc2 = tc.filtrar(lambda ind: ind.mother["PRODUCCION"]["CONTROLES"].count(
) > 4 and ind["PRODUCCION"]["CONTROLES"].count() <= 4)
p = tc2.collect()  # "forzar" ejecución
print(p.show())

print("Filtro 7.1")
tc2 = tc.filtrar(lambda ind: ind.__tieneHermanaSiguiente__())
df1 = tc2.collect()  # "forzar" ejecución
print(df1.show())
##
##print("Filtro 7.2")
tc2 = tc.filtrar(lambda ind: ind.__tieneHermanaPrevia__())
df2 = tc2.collect()  # "forzar" ejecución
print(df2.show())

tc2 = tc.filtrar(lambda ind: ind.__tieneHermanaSiguiente__()
                 or ind.__tieneHermanaPrevia__())
df1 = tc2.collect()  # "forzar" ejecución
print(df1.show())

# en cadena
print("Filtro 7.3")
tc2 = tc.filtrar(lambda ind: ind.__tieneHermanaPrevia__())
tc3 = tc2.filtrar(lambda ind: ind.__tieneHermanaSiguiente__())
p = tc3.collect()  # "forzar" ejecución
print(p.show())

print("Filtro Nuevo6")
tc8 = tc.filtrar(lambda ind: ind.__sistersCount__() > 1)
p = tc8.collect()  # "forzar" ejecución
print(p.show())

print("Filtro Nuevo7")
tc8 = tc.filtrar(lambda ind: ind.__granddaughterCount__() > 1)
p = tc8.collect()  # "forzar" ejecución
print(p.show())

tc2 = tc.filtrar(lambda ind: ind.mother.__motherExists__())
p = tc2.collect()  # "forzar" ejecución
print(p.show())

tc2 = tc.filtrar(lambda ind: not ind.mother.__motherExists__())
p = tc2.collect()  # "forzar" ejecución
print(p.show())

# Eliminar a los primeros partos
#print("Filtro 6")
tc4 = tc.filtrar(lambda ind: ind.mother.__childrenOrder__() >= 2)
p = tc4.collect()  # "forzar" ejecución
print(p.show())

# Eliminar a las vacas sin crias
#print("Filtro 4")
tc3 = tc.filtrar(lambda ind: 2 <= ind.mother.__childrenCount__())
p = tc3.collect()  # "forzar" ejecución
print(p.show())

print("Filtro 7.1")
tc2 = tc.filtrar(lambda ind: not ind.__tieneHermanaSiguiente__())
df1 = tc2.collect()  # "forzar" ejecución
print(df1.show())

#print("Filtro 7.1")
tc2 = tc.filtrar(lambda ind: ind.mother.__tieneHermanaSiguiente__())
df1 = tc2.collect()  # "forzar" ejecución
print(df1.show())

##print("Filtro 7.2")
tc2 = tc.filtrar(lambda ind: ind.mother.__tieneHermanaPrevia__())
df2 = tc2.collect()  # "forzar" ejecución
print(df2.show())

print("Filtro Nuevo6")
tc8 = tc.filtrar(lambda ind: ind.mother.__sistersCount__() > 1)
p = tc8.collect()  # "forzar" ejecución
print(p.show())

print("Filtro Nuevo7")
tc8 = tc.filtrar(lambda ind: ind.mother.__granddaughterCount__() > 1)
p = tc8.collect()  # "forzar" ejecución
print(p.show())

tc2 = tc.filtrar(lambda ind: not ind.__tieneHermanaSiguiente__())
df1 = tc2.collect()  # "forzar" ejecución
print(df1.show())

tc2 = tc.filtrar(lambda ind: ind.__tieneHermanaSiguiente__())
df1 = tc2.collect()  # "forzar" ejecución
print(df1.show())

tc2 = tc.filtrar(lambda ind: not ind.__tieneHermanaSiguiente__()
                 or not ind.__tieneHermanaPrevia__())
df1 = tc2.collect()  # "forzar" ejecución
print(df1.show())

print("Filtro 7.1")
tc2 = tc.filtrar(lambda ind: not ind.mother.__tieneHermanaSiguiente__())
df1 = tc2.collect()  # "forzar" ejecución
print(df1.show())

#print("Filtro 7.2")
tc2 = tc.filtrar(lambda ind: not ind.mother.__tieneHermanaPrevia__())
df2 = tc2.collect()  # "forzar" ejecución
print(df2.show())

tc2 = tc.filtrar(lambda ind: not ind.mother.__tieneHermanaSiguiente__(
) or not ind.mother.__tieneHermanaPrevia__())
df1 = tc2.collect()  # "forzar" ejecución
print(df1.show())

tc2 = tc.filtrar(lambda ind: ind["PRODUCCION"]["CONTROLES"].count() > 3)
p = tc2.collect()  # "forzar" ejecución
print(p.show())

tc2 = tc.filtrar(lambda ind: ind["CONTROLES"]["OTROS"].count() >= 0)
p = tc2.collect()  # "forzar" ejecución
print(p.show())

print("Filtro Nuevo5")
tc8 = tc.filtrar(lambda ind: ind["OTROS"]["DATITO"].value() == 0)
p = tc8.collect()  # "forzar" ejecución
print(p.show())

print("Filtro Nuevo5.1")
tc8 = tc.filtrar(lambda ind: ind["OTROS"]["DATITO"].value(
) < ind.mother["OTROS"]["DATITO"].value())
p = tc8.collect()  # "forzar" ejecución
print(p.show())
#
print("Filtro Nuevo5.2")
tc8 = tc.filtrar(lambda ind: ind.mother["OTROS"]["DATITO"].value(
) < ind["OTROS"]["DATITO"].value())
p = tc8.collect()  # "forzar" ejecución
print(p.show())

print("Filtro Nuevo4.1")
tc8 = tc.filtrar(lambda ind: ind["DATITO"].value()
                 > ind.mother["DATITO"].value())
p = tc8.collect()  # "forzar" ejecución
print(p.show())

print("Filtro Nuevo4.2")
tc8 = tc.filtrar(lambda ind: ind["DATITO"].value()
                 < ind.mother["DATITO"].value())
p = tc8.collect()  # "forzar" ejecución
print(p.show())
###############################################################
# CHEQUEO DE TAREAS DUPLICADAS
#tc5 = tc.filtrar(lambda ind: ind["CONTROLES"].count() > 7)
# p=tc5.collect() #"forzar" ejecución
# print(p.show())
#
#tc5 = tc.filtrar(lambda ind: ind["CONTROLES"].count() > 1 and ind["CONTROLES"].count() < 7)
# p=tc5.collect() #"forzar" ejecución
# print(p.show())
#
#tc5 = tc.filtrar(lambda ind: ind["CONTROLES"].count() > 1)
#tc6 = tc5.filtrar(lambda ind: ind["CONTROLES"].count() < 7)
# p=tc6.collect() #"forzar" ejecución
# print(p.show())
#
# CHEQUEO DE TAREAS DUPLICADAS
#tc2 = tc.filtrar(lambda ind: ind["ORDEN"].value()==2)
# p=tc2.collect() #"forzar" ejecución
# print(p.show())
#
#tc2 = tc.filtrar(lambda ind: 1 < ind["ORDEN"].value())
#tc3 = tc2.filtrar(lambda ind: ind["ORDEN"].value() <3)
# p=tc3.collect() #"forzar" ejecución
# print(p.show())
##
#tc2 = tc.filtrar(lambda ind: 1 < ind["ORDEN"].value() and ind["ORDEN"].value() <3)
# p=tc2.collect() #"forzar" ejecución
# print(p.show())

###############################################################
tc3 = tc.filtrar(lambda ind: ind.__childrenCount__()
                 >= 0 and ind.__childrenOrder__() == 2)
p = tc3.collect()  # "forzar" ejecución
print(p.show())

print("PRUEBA")
tc3 = tc.filtrar(lambda ind: ind.__childrenCount__() < 3 and 1 < ind.__childrenCount__(
) and ind["CONTROLES"].value() > 5 and ind["PRODUCCION"]["CONTROLES"].count() > 2)
p = tc3.collect()  # "forzar" ejecución
print(p.show())

print("PRUEBA")
tc3 = tc.filtrar(lambda ind: ind.__childrenCount__() > 0 and ind.__childrenCount__(
) > 1 and ind["CONTROLES"].value() > 5 and 2 > ind["PRODUCCION"]["CONTROLES"].count())
p = tc3.collect()  # "forzar" ejecución
print(p.show())

tc8 = tc.filtrar(lambda ind: ind["DATITO"].value() > 3)
p = tc8.collect()  # "forzar" ejecución
print(p.show())

tc8 = tc.filtrar(lambda ind: ind["OTROS"]["DATITO"].value() > 3)
p = tc8.collect()  # "forzar" ejecución
print(p.show())

tc8 = tc.filtrar(lambda ind: ind["OTROS"].count() >= 1)
p = tc8.collect()  # "forzar" ejecución
print(p.show())

tc8 = tc.filtrar(lambda ind: ind["TABLA_BASE"]["OTROS"].count() > 0)
p = tc8.collect()  # "forzar" ejecución
print(p.show())

tc2 = tc.filtrar(lambda ind: ind["CONTROLES"]["OTROS"].count() >= 0)
p = tc2.collect()  # "forzar" ejecución
print(p.show())

tc8 = tc.filtrar(lambda ind: ind.mother["DATITO"].value() > 3)
p = tc8.collect()  # "forzar" ejecución
print(p.show())

tc8 = tc.filtrar(lambda ind: ind.mother["OTROS"]["DATITO"].value() > 3)
p = tc8.collect()  # "forzar" ejecución
print(p.show())

tc8 = tc.filtrar(lambda ind: ind.mother["OTROS"].count() >= 1)
p = tc8.collect()  # "forzar" ejecución
print(p.show())

tc8 = tc.filtrar(lambda ind: ind.mother["TABLA_BASE"]["OTROS"].count() > 0)
p = tc8.collect()  # "forzar" ejecución
print(p.show())
#
tc2 = tc.filtrar(lambda ind: ind.mother["CONTROLES"]["OTROS"].count() >= 0)
p = tc2.collect()  # "forzar" ejecución
print(p.show())

###############################################################
tc2 = tc.filtrar(lambda ind: ind["PRODUCCION"]
                 ["CONTROLES"]["OTROS"].count() >= 0)
p = tc2.collect()  # "forzar" ejecución
print(p.show())

tc2 = tc.filtrar(lambda ind: ind["TABLA_BASE"]
                 ["PRODUCCION"]["CONTROLES"]["OTROS"].count() >= 0)
p = tc2.collect()  # "forzar" ejecución
print(p.show())

tc2 = tc.filtrar(
    lambda ind: ind.mother["PRODUCCION"]["CONTROLES"]["OTROS"].count() >= 0)
p = tc2.collect()  # "forzar" ejecución
print(p.show())

tc2 = tc.filtrar(lambda ind: ind.mother["TABLA_BASE"]
                 ["PRODUCCION"]["CONTROLES"]["OTROS"].count() >= 0)
p = tc2.collect()  # "forzar" ejecución
print(p.show())

###############################################################
###############################################################

#u = Utils.unionDF(df1,df2)
# print(u.show())
#
# (df1,nombreTabla1,columnas1,key1,df2,nombreTabla2,columnas2,key2)
#columnas1=['ID', 'FECHA_NAC', 'VIVE_EN', 'ID_MADRE', 'ORDEN']
# columnas2=["ID","FECHA","NRO_PROD"]
#u = Utils.joinDF(individuos,"TABLA_BASE",columnas1,"ID",produccion,"PROD",columnas2,"ID")
# print(u.show())
##

###############################################################
###############################################################

# Obtención de todas las duplas (hermana-hermana) consecutivas en nacimientos
print("Hermanas-hermanas consecutivas")
hermana_hermana = tc.sisters(2)
print(hermana_hermana.show())

print("Hermanas-hermanas-hermanas consecutivas")
hermana_hermana_hermana = tc.sisters(3)
print(hermana_hermana_hermana.show())

print("Hermanas-hermanas-hermanas-hermanas consecutivas")
hermana_hermana_hermana_hermana = tc.sisters(4)
print(hermana_hermana_hermana_hermana.show())

# Obtención de todas las posibles hermanas, sin importar el orden de nacimientos
print("Hermanas-hermanas no-consecutivas")
hermanas2 = tc.sisters(2, True)
print(hermanas2.show())
#
print("Hermanas-hermanas-hermanas no-consecutivas")
hermanas4 = tc.sisters(3, True)
print(hermanas4.show())

print("Hermanas-hermanas-hermanas-hermanas no-consecutivas")
hermanas4 = tc.sisters(4, True)
print(hermanas4.show())

####################################################
# DATAFRAMES CON COLUMNAS SEGUN FAMILIAR
#
# Obtención de todas las duplas (madre-hija)
print("Madres-hijas")
madre_hijas = tc.descendents(2)
print(madre_hijas.show())

# Obtención de todas las triplas (abuela-madre-hija)
print("Abuelas-madres-hijas")
abuela_madre_hijas = tc.descendents(3)
print(abuela_madre_hijas.show())

print("Abuelas-madres-hijas-nietas")
abuela_madre_hijas_nietas = tc.descendents(4)
print(abuela_madre_hijas_nietas.show())

# Obtención de todas las tuplas (abuela-madre-hija-nieta-bisnieta)
print("Abuelas-madres-hijas-nietas-bisnietas")
abuela_madre_hijas_nietas_bisnietas = tc.descendents(5)
print(abuela_madre_hijas_nietas_bisnietas.show())

# Obtención de todas las tuplas (abuela-madre-hija-nieta-bisnieta_tataranieta)
print("Abuelas-madres-hijas-nietas-bisnietas-tataranietas")
abuela_madre_hijas_nietas_bisnietas_tataranietas = tc.descendents(6)
print(abuela_madre_hijas_nietas_bisnietas_tataranietas.show())

print("Abuelas-madres-hijas-nietas-bisnietas-tataranietas-tataranietas")
abuela_madre_hijas_nietas_bisnietas_tataranietas_tataranietas = tc.descendents(
    7)
print(abuela_madre_hijas_nietas_bisnietas_tataranietas_tataranietas.show())

# PRUEBA CON FILTERED TREE
print("Filtro 6")
tc4 = tc.filtrar(lambda ind: ind.__childrenCount__() > 0)
p = tc4.collect()  # "forzar" ejecución
print(p.show())
hermana_hermana = tc4.sisters(2)
print(hermana_hermana.show())

tc4 = tc.filtrar(lambda ind: ind.__childrenCount__() > 0)
p = tc4.collect()  # "forzar" ejecución
madre_hijas = tc4.descendents(2)
print(madre_hijas.show())
