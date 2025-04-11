''' UNAD - Big Data - Procesamiento en batch
    Nombre del data set: Reporte Hurto por Modalidades Policía Nacional
    origen de datos: https://www.datos.gov.co/api/views/9vha-vh9n/rows.csv
    Autor: Gabriel Giraldo 
'''

#Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Se define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'

# Leer el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

# Análisis Exploratorio de datos
print('Análisis Exploratorio de Datos \n')

#imprimimos el esquema para saber que datos tenemos y sus tipos
print('Esquema del dataset (Datos y tipos) \n')
df.printSchema()

# Muestra las primeras filas del DataFrame
print('Exploración de los primeros 10 datos \n')
df.show(10)

# Estadisticas básicas 
print('Resumen de los datos \n')
df.summary().show()

# Aquí se puede observar que hay datos intercambiados entre CODIGO DANE y ARMAS MEDIOS

# Intercambiar los valores de las columnas ARMAS MEDIOS con CODIGO DANE (hay valores trocados)

df = df.withColumn('ARMAS MEDIOS', F.when(F.col('ARMAS MEDIOS').rlike('^[0-9]+$'), F.col('CODIGO DANE')).otherwise(F.col('ARMAS MEDIOS'))) \
       .withColumn('CODIGO DANE', F.when(F.col('CODIGO DANE').rlike('^[a-zA-Z]+$'), F.col('ARMAS MEDIOS')).otherwise(F.col('CODIGO DANE')))

# Verificar ajustes en valores  unicos en armas medios
unicos_armedio = df.select('ARMAS MEDIOS').distinct().collect()

# Imprimir valores únicos
print('\n Valores únicos de ARMAS / MEDIOS \n')
for fila in unicos_armedio:
    print(fila['ARMAS MEDIOS'])

# Aquí podemos observar que hay datos que podemos ajustar para el reporte

# Reemplazar los valores especificados por "ARMA BLANCA / CORTOPUNZANTE" y NO REPORTA y - por NO REPORTADO

df = df.withColumn('ARMAS MEDIOS', F.when(F.col('ARMAS MEDIOS').isin('CORTOPUNZANTES', 'CORTANTES', 'PUNZANTES'), 'ARMA BLANCA / CORTOPUNZANTE').otherwise(F.col('ARMAS MEDIOS')))\
       .withColumn('ARMAS MEDIOS', F.when(F.col('ARMAS MEDIOS').isin('NO REPORTA', '-'), 'NO REPORTADO').otherwise(F.col('ARMAS MEDIOS')))

# Verificar cambios realizados a los  valores armas medios
unicos_armedio = df.select('ARMAS MEDIOS').distinct().collect()

# Imprimir valores únicos
print('\n Valores únicos de ARMAS / MEDIOS despues de ajustarlos \n')
for fila in unicos_armedio:
    print(fila['ARMAS MEDIOS'])

# Se evidenció que existe variacion en mayuscula y minusculas 
# Por lo cual se debió unificar todo en mayuscula en Departamento y Municipio

df = df.withColumn('DEPARTAMENTO', F.upper(df['DEPARTAMENTO']))
df = df.withColumn('MUNICIPIO', F.upper(df['MUNICIPIO']))

# Ya los datos están acordes para su análisis

# Filtrar datos de hurto de vehículos y motocicletas
hv = df.filter(F.col('TIPO DE HURTO').isin(['HURTO AUTOMOTORES']))
hm = df.filter(F.col('TIPO DE HURTO').isin(['HURTO MOTOCICLETAS']))

# Fechas de la estadística
min_fecha = df.select(F.min('FECHA HECHO')).collect()[0][0]
max_fecha = df.select(F.max('FECHA HECHO')).collect()[0][0]

# Estadística de hurto a automotores
print(f'\n Estadísticas HURTO A AUTOMOTORES EN COLOMBIA \n  desde el {min_fecha} hasta el {max_fecha} \n')

# Top 10 departamentos más afectados
print('Los departamentos más afectados por el hurto a automotores son: \n')
vh_dptos = hv.groupBy('DEPARTAMENTO').agg(F.sum('CANTIDAD').alias('TOTAL_HURTOS')).orderBy(F.col('TOTAL_HURTOS').desc()).limit(10)
vh_dptos.show()

# Top 10 municipios más afectados
print('Los municipios más afectados por el hurto a automotores son: \n')
vh_municipios = hv.groupBy('MUNICIPIO').agg(F.sum('CANTIDAD').alias('TOTAL_HURTOS')).orderBy(F.col('TOTAL_HURTOS').desc()).limit(10)
vh_municipios.show()

# Top 10 armas utilizadas
print('Los medios o armas más comunes utilizadas en el hurto a automotores son: \n')
vh_mod = hv.groupBy('ARMAS MEDIOS').agg(F.sum('CANTIDAD').alias('TOTAL')).orderBy(F.col('TOTAL').desc()).limit(10)
vh_mod.show()

# Estadística de hurto a Motocicletas

print(f'\n Estadísticas HURTOS A MOTOCICLETAS EN COLOMBIA \n desde el {min_fecha} hasta el {max_fecha} \n')

# Top 10 departamentos más afectados
print('Los departamentos más afectados por el hurto a motocicletas son: \n')
mh_dptos = hm.groupBy('DEPARTAMENTO').agg(F.sum('CANTIDAD').alias('TOTAL_HURTOS')).orderBy(F.col('TOTAL_HURTOS').desc()).limit(10)
mh_dptos.show()

# Top 10 municipios más afectados
print('Los municipios más afectados por el hurto a motocicletas son: \n')
mh_municipios = hm.groupBy('MUNICIPIO').agg(F.sum('CANTIDAD').alias('TOTAL_HURTOS')).orderBy(F.col('TOTAL_HURTOS').desc()).limit(10)
mh_municipios.show()

# Top 10 armas utilizadas
print('Los medios o armas más comunes utilizadas en el hurto a motocicletas son: \n')
mh_mod = hm.groupBy('ARMAS MEDIOS').agg(F.sum('CANTIDAD').alias('TOTAL')).orderBy(F.col('TOTAL').desc()).limit(10)
mh_mod.show()
