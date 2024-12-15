from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import Window
import argparse
import numpy as np

if __name__ == '__main__':
    # Crear sesion de Spark
    spark = SparkSession \
        .builder \
        .appName("Calculo de MTBF ") \
        .getOrCreate()

    # Iniciamos el parser
    parser = argparse.ArgumentParser(description="Script PySpark con argumentos de entrada.")

    # Anadimos los argumentos
    parser.add_argument("--path-log", required=True, help="configuration spark: path to read")
    parser.add_argument("--date", required=True, help="configuration spark: date, format yyyyMM")
    parser.add_argument("--percentil", required=True,
                        help="configuration spark: percentil, 0,1,2 o 3 (percentil 25, 50, 75, 90 respectivamente)")


    # Read arguments from the command line
    args = parser.parse_args()

    path_log = args.path_log
    date = args.date
    percentil = args.percentil

    date = datetime.strptime(date, "%Y%m")

    # Valores para filtrar la tabla
    year = date.year
    month = date.month

    print("Calculamos el MBTF para la fecha {}".format(date))

    # Creamos un dataframe filtrando por la fecha de entrada y por los registros que no aportan nada
    df_logs = (spark.read.parquet(path_log)
               .filter((col("year") == year) & (col("month") == month))
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
        print("Para el host {} tenemos lo siguiente:".format(host))
        time_diff_df = (df_logs
            .filter(col("host") == host)
            .orderBy("timestamp")
            .withColumn("prev_timestamp", lag("timestamp").over(cpu_window))
            .withColumn("row_number", row_number().over(cpu_window))
            .withColumn("time_diff_seconds", (unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")))
            .withColumn("time_diff_minuts", round(col("time_diff_seconds") / lit(60), 2))
            .filter(col("time_diff_minuts").isNotNull()))

        time_diff_seconds_list = time_diff_df.select("time_diff_seconds").rdd.flatMap(lambda x: x).collect()

        # Calcular percentiles
        percentiles = np.percentile(time_diff_seconds_list, [25, 50, 75, 90])

        # Crear una columna que indique si el error esta dentro del rango de 60 segundos respecto al error anterior
        # Crear la columna de grupo acumulando la columna is_new_group
        df = (time_diff_df
              .withColumn("is_new_group",
                          when(col("time_diff_seconds") >= percentiles[percentil], 1).otherwise(0))
              .withColumn("group_id", sum("is_new_group").over(timestamp_window)))

        grouped_df = (df
            .groupBy("group_id")
            .agg(min("timestamp").alias("min_timestamp"), count("err_code").alias("count"))
            .orderBy("group_id"))

        print("El count total es {} y el count despues de agrupar es {}".format(df.count(), grouped_df.count()))

        mtbf = (grouped_df
            .withColumn("prev_timestamp", lag("min_timestamp").over(min_timestamp_window))
            .withColumn("time_diff_seconds", (unix_timestamp("min_timestamp") - unix_timestamp("prev_timestamp")))
            .filter(col("time_diff_seconds").isNotNull())
            .agg(avg("time_diff_seconds").alias("MTBF_seconds"))
            .collect()[0]["MTBF_seconds"])

        # Convertir el MTBF a minutos, horas
        mtbf_minutes = mtbf / 60  # En minutos
        mtbf_hours = mtbf / 3600  # En horas

        print("MTBF en segundos: " + str(mtbf))
        print("MTBF en minutos: " + str(mtbf_minutes))
        print("MTBF en horas: " + str(mtbf_hours))

    print("FIN: MTBF CALCULADOS")
    spark.stop()
