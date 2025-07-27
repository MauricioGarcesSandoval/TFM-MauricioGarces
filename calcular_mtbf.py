from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType
import argparse
import numpy as np
import logging


def escribir_json_hdfs(df, hdfs_path):
    """
    Escribe un DataFrame en formato JSON en HDFS
    :param df: Dataframe a escribir en HDFS
    :param hdfs_path: Ruta HDFS donde se va a escribir el fichero
    """
    df.repartition(1).write.mode('overwrite').json(hdfs_path)
    logging.info("Datos escritos en HDFS en la ruta: {}".format(hdfs_path))


def calcular_mbtf(spark, path_log, date):
    """
    Calcula el tiempo medio entre errores
    :param spark: Sesion de Spark
    :param path_log: Ruta donde se encuentran los ficheros de entrada
    :param date: Fecha para calcular el MBTF
    :return: Datos en formato JSON
    """
    date = datetime.strptime(date, "%Y%m")

    # Valores para filtrar la tabla
    execution_year = date.year
    execution_month = date.month

    execution_date = "{}{}".format(execution_year, execution_month)

    resultados = []

    logging.info("Calculamos el MBTF para el anho {} y mes {}".format(execution_year, execution_month))

    # Creamos un dataframe filtrando por la fecha de entrada y por los registros que no aportan nada
    df_logs = (spark.read.parquet(path_log)
               .filter((col("year") == execution_year) & (col("month") == execution_month))
               .filter(~(col("msg").like("%HANDLING MCE MEMORY ERROR%"))))

    expresiones = {
        "channel": r"channel:(\d+)",
        "slot": r"slot:(\d+)",
        "page": r"page:(0x[0-9a-f]+)",
        "offset": r"offset:(0x[0-9a-f]+)",
        "grain": r"grain:(\d+)",
        "syndrome": r"syndrome:(0x[0-9a-f]+)",
        "err_code": r"err_code:(0x[0-9a-f]+:0x[0-9a-f]+)",
        "SystemAddress": r"SystemAddress:(0x[0-9a-f]+)",
        "ProcessorSocketId": r"ProcessorSocketId:(0x[0-9a-f]+)",
        "MemoryControllerId": r"MemoryControllerId:(0x[0-9a-f]+)",
        "ChannelAddress": r"ChannelAddress:(0x[0-9a-f]+)",
        "ChannelId": r"ChannelId:(0x[0-9a-f]+)",
        "RankAddress": r"RankAddress:(0x[0-9a-f]+)",
        "PhysicalRankId": r"PhysicalRankId:(0x[0-9a-f]+)",
        "DimmSlotId": r"DimmSlotId:(0x[0-9a-f]+)",
        "Row": r"Row:(0x[0-9a-f]+)",
        "Column": r"Column:(0x[0-9a-f]+)",
        "Bank": r"Bank:(0x[0-9a-f]+)",
        "BankGroup": r"BankGroup:(0x[0-9a-f]+)",
        "ChipSelect": r"ChipSelect:(0x[0-9a-f]+)",
        "ChipId": r"ChipId:(0x[0-9a-f]+)",
        "cpu_info": r"on\s+(.*?)\(",
    }

    # Aplicar las expresiones regulares y crear nuevas columnas
    for nombre_columna, expresion_regular in expresiones.items():
        df_logs = df_logs.withColumn(nombre_columna, regexp_extract("msg", expresion_regular, 1))

    df_logs = df_logs.withColumn("cpu_info", trim(df_logs["cpu_info"]))

    list_hosts = df_logs.select("host").distinct().rdd.flatMap(lambda x: x).collect()

    # Definir la ventana
    timestamp_window = Window.orderBy("timestamp")
    cpu_window = Window.partitionBy("cpu_info").orderBy("timestamp")
    min_timestamp_window = Window.orderBy("min_timestamp")

    for host in list_hosts:
        logging.info("Para el host {} tenemos lo siguiente:".format(host))

        time_diff_df = (df_logs
                        .filter(col("host") == host)
                        .orderBy("timestamp")
                        .withColumn("prev_timestamp", lag("timestamp").over(cpu_window))
                        .withColumn("row_number", row_number().over(cpu_window))
                        .withColumn("time_diff_seconds",
                                    (unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")))
                        .withColumn("time_diff_minuts", round(col("time_diff_seconds") / lit(60), 2))
                        .filter(col("time_diff_minuts").isNotNull()))

        if time_diff_df.limit(10).count() <= 5:
            logging.info("Son pocos logs para poder sacar alguna conclusion, pasamos al siguiente host.")
            logging.info("----------------------------------------------------------------------------")
            continue

        time_diff_seconds_list = time_diff_df.select("time_diff_seconds").rdd.flatMap(lambda x: x).collect()

        # Calcular percentiles
        percentiles = np.percentile(time_diff_seconds_list, [25, 50, 75, 90])

        # Crear lista de resultados para el host
        percentiles_result = []

        for idx, percentil_value in enumerate(percentiles):
            logging.info("Calculando para el percentil {} ({})".format(idx, percentil_value))
            percentil_resultado = "percentil_{}".format(idx)

            # Crear una columna que indique si el error esta dentro del rango de 60 segundos respecto al error anterior
            # Crear la columna de grupo acumulando la columna is_new_group
            df = (time_diff_df
                  .withColumn("is_new_group",
                              when(col("time_diff_seconds") >= percentil_value, 1).otherwise(0))
                  .withColumn("group_id", sum("is_new_group").over(timestamp_window)))

            grouped_df = (df
                          .groupBy("group_id")
                          .agg(min("timestamp").alias("min_timestamp"), count("err_code").alias("count"))
                          .orderBy("group_id"))

            df_count = df.count()
            grouped_df_count = grouped_df.count()

            logging.info("El count del dataframe es {} y el count despues de agrupar los errores es {}".format(df_count,
                                                                                                               grouped_df_count))

            grouped_mtbf_df = (grouped_df
                               .withColumn("prev_timestamp", lag("min_timestamp").over(min_timestamp_window))
                               .withColumn("time_diff_seconds",
                                           (unix_timestamp("min_timestamp") - unix_timestamp("prev_timestamp")))
                               .filter(col("time_diff_seconds").isNotNull()))

            if grouped_mtbf_df.limit(10).count() <= 2:
                logging.info("Son pocos logs para poder sacar alguna conclusion del mtbf, pasamos al siguiente host.")
                logging.info("----------------------------------------------------------------------------")
                continue

            mtbf = (grouped_mtbf_df.agg(avg("time_diff_seconds").alias("MTBF_seconds")).collect()[0]["MTBF_seconds"])

            # Convertir el MTBF a minutos, horas
            mtbf_minutes = mtbf / 60  # En minutos
            mtbf_hours = mtbf / 3600  # En horas

            logging.info("MTBF en segundos: " + str(mtbf))
            logging.info("MTBF en minutos: " + str(mtbf_minutes))
            logging.info("MTBF en horas: " + str(mtbf_hours))
            logging.info("----------------------------------------------------------------------------")

            # Guardar los resultados en el formato deseado
            percentiles_result.append({
                "percentil": percentil_resultado,
                "count_before": df_count,
                "count_after": grouped_df_count,
                "mbtf": mtbf
            })

        # Agregar los resultados al DataFrame final
        resultados.append({
            "fecha": execution_date,
            "host": host,
            "percentiles": percentiles_result
        })

    return resultados


def main():
    """
    Funcion main que crea un fichero JSON y lo escribe en HDFS
    """
    # Iniciamos el parser de los argumentos
    parser = argparse.ArgumentParser(description="Script PySpark con argumentos de entrada.")
    parser.add_argument("--path-log", required=True, help="configuration spark: path to read")
    parser.add_argument("--date", required=True, help="configuration spark: date, format yyyyMM")
    parser.add_argument("--path-output", required=True, help="configuration spark: path to write")
    args = parser.parse_args()

    path_log = args.path_log
    date = args.date
    path_output = args.path_output

    # Crear sesion de Spark
    spark = SparkSession.builder.appName("CALCULO_MBTF_{}".format(date)).getOrCreate()

    # Guardar resultados en JSON en HDFS
    hdfs_path = "{}/mbtf_{}".format(path_output, date)
    schema = StructType([
        StructField("fecha", StringType(), nullable=True),
        StructField("host", StringType(), nullable=True),
        StructField("percentiles", ArrayType(
            StructType([
                StructField("percentil", StringType(), nullable=True),
                StructField("count_before", IntegerType(), nullable=True),
                StructField("count_after", IntegerType(), nullable=True),
                StructField("mbtf", FloatType(), nullable=True)
            ])
        ), nullable=True)
    ])

    resultados = calcular_mbtf(spark, path_log, date)
    df_resultados = spark.createDataFrame(resultados, schema)
    escribir_json_hdfs(df_resultados, hdfs_path)

    logging.info("FIN: MTBF CALCULADOS")
    spark.stop()


if __name__ == '__main__':
    # Llamada a main
    main()
