import argparse
from pyspark.sql import SparkSession

if __name__ == "__main__":

    # Configuración de argumentos
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_path", required=True)
    parser.add_argument("--table_name", required=True)
    args, _ = parser.parse_known_args()

    # Inicialización de Spark
    spark_session = SparkSession.builder.appName("crear_tabla").getOrCreate()
    spark_context = spark_session.sparkContext

    
    # Lectura del archivo Parquet y creación de la tabla Delta
    parquet_file_path = args.file_path
    table_name = args.table_name

    print(f"Creando tabla Delta en {table_name} a partir de {parquet_file_path}")

    df = spark_session.read.format("parquet").load(parquet_file_path)
    df.write.mode("overwrite").format("delta").saveAsTable(table_name)
