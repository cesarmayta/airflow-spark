import sys
import requests
import jmespath
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pathlib import Path
import json
from airflow.models import Variable

print('=================================== LOADING VARIABLES ================================================')

pg_extract_host = Variable.get("pg_host")
pg_extract_user = Variable.get("pg_user")
pg_extract_pwd = Variable.get("password_pg_extract_pwd")
pg_extract_table = Variable.get("extract_table")
pg_load_host = Variable.get("pg_host")
pg_load_user = Variable.get("pg_user")
pg_load_pwd = Variable.get("password_pg_extract_pwd")
pg_load_table = Variable.get("load_table")

spark = SparkSession.builder \
    .config("spark.driver.extraClassPath", "/data/postgresql-42.2.5.jar") \
    .config("spark.jars", "postgresql-42.2.5.jar") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.5") \
    .config("spark.executor.extraClassPath", "/data/postgresql-42.2.5.jar") \
    .getOrCreate()

print('=================================== EXTRACT DATA ================================================')
try:
    df = spark.read \
        .format("jdbc") \
        .option("url", pg_extract_host) \
        .option("dbtable", pg_extract_table) \
        .option("user", pg_extract_user) \
        .option("password", pg_extract_pwd) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    

    print("DATAFRAME CREATED.")
    print(df.show())
except Exception as e:
    print(f"EXTRACT ERROR: {e}")

try:
    print('=================================== LOAD DATA ================================================')

    properties = {"user":pg_load_user,"password":pg_load_pwd,"driver":"org.postgresql.Driver","stringtype":"unspecified"}
    mode = "overwrite"
    df.write.jdbc(url=pg_load_host,table=pg_load_table,mode=mode,properties=properties)

except Exception as e:
    print(f"LOAD ERROR : {e}")

# Detener la sesión de Spark
print('=================================== FINISH PROCESS ================================================')
spark.stop()
 
 