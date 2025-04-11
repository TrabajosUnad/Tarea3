# Dataset: Quejas interpuestas ante las entidades vigiladas por la Superintendencia Financiera de Colombia y Defensores del Consumidor Financiero
# Fuente: https://www.datos.gov.co/Econom-a-y-Finanzas/Quejas-interpuestas-ante-las-entidades-vigiladas-p/hjqv-fp48/about_data
# Autor código: Claudia Orozco

# Importar librerías necesarias
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

# Seleccionar columnas específicas
df = df.select('TIPO_ENTIDAD','NOMBRE_ENTIDAD','FECHA_CORTE','NOMBRE_UNIDAD_CAPTURA','PRODUCTO','MOTIVO','QUEJAS_RECIBIDAS','QUEJAS_FINALIZADAS','QUEJAS_FINALIZAD_FAVOVOR_CONSUM','QUEJAS_FINALIZAD_FAVOR_ENTIDAD')

# Eliminar duplicados basados en todas las columnas
df = df.dropDuplicates()

# Convertir las columnas de tipo string a entero
df = df.withColumn('QUEJAS_RECIBIDAS', F.col('QUEJAS_RECIBIDAS').cast('int')) \
       .withColumn('QUEJAS_FINALIZADAS', F.col('QUEJAS_FINALIZADAS').cast('int')) \
       .withColumn('QUEJAS_FINALIZAD_FAVOR_CONSUM', F.col('QUEJAS_FINALIZAD_FAVOR_CONSUM').cast('int')) \
       .withColumn('QUEJAS_FINALIZAD_FAVOR_ENTIDAD', F.col('QUEJAS_FINALIZAD_FAVOR_ENTIDAD').cast('int'))

# Crear una ventana de participación para hacer el rankig
window = Window.partitionBy('TIPO_ENTIDAD').orderBy(F.col('QUEJAS_RECIBIDAS').desc())

# Agregar una nueva columna con el ranking basado en 'QUEJAS_RECIBIDAS' para cada 'TIPO_ENTIDAD'
df.rank = df.withColumn('ranking', F.rank().over(window))

# Imprimimos el esquema
df.rank.printSchema()

# Muestra las primeras filas del DataFrame
df.rank.show()

# Estadísticas básicas
df.rank.summary().show()
