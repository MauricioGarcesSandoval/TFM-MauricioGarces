from pyspark import SparkContext
from pyspark.sql import HiveContext
from datetime import datetime, timedelta
import argparse
import pyspark.sql.functions as f

if __name__ == '__main__':
    sc = SparkContext(appName="GetMemoryErrorLogs")
    hc = HiveContext(sc)

    # Iniciamos el parser
    parser = argparse.ArgumentParser()

    # Anhadimos los argumentos
    parser.add_argument("--date-from", help="configuration spark first date")
    parser.add_argument("--date-to", help="configuration spark last date")
    parser.add_argument("--host", help="configuration spark host")

    # Read arguments from the command line
    args = parser.parse_args()

    # Revision del argumento --date-from
    if args.date_from:
        date_from = args.date_from

    # Revision del argumento --date-to
    if args.date_to:
        date_to = args.date_to

    fecha_inicio = datetime.strptime(date_from, "%Y%m%d")
    fecha_final = datetime.strptime(date_to, "%Y%m%d")

    # Valores para filtrar la tabla
    year = date_from[0:4]
    month = date_from[4:6]
    day = date_from[6:8]

    # Creamos un dataframe para sacar el esquema
    df_schema = hc.sql("select * from cesga.logs where year={0} and month={1} and day={2}".format(year, month, day))

    # Creamos el dataframe vacio para luego rellenarlo con datos de las fechas que queremos
    df_union = hc.createDataFrame(sc.emptyRDD(), df_schema.schema)

    while fecha_inicio <= fecha_final:
        fecha_inicio_str = fecha_inicio.strftime("%Y%m%d")

        df_actual = hc.sql("select * from cesga.logs").filter(
            (f.col("year") == fecha_inicio_str[:4]) &
            (f.col("month") == fecha_inicio_str[4:6]) &
            (f.col("day") == fecha_inicio_str[6:])
        )

        df_union = df_union.unionAll(df_actual)
        print("Se lee para la fecha {0}".format(fecha_inicio))

    fecha_inicio += timedelta(days=1)

    print("#########################################################################")
    print("#####################        PRIMER DATAFRAME          ##################")
    print("#########################################################################")

    # Creamos las columnas que nos interesan
    df_exploded = df_union.withColumn("priority", df_union.headers.getItem("priority")) \
        .withColumn("timestamp_1", df_union.headers.getItem("timestamp")) \
        .withColumn("host", df_union.headers.getItem("host")) \
        .withColumn("partition", df_union.headers.getItem("partition")) \
        .withColumn("Facility", df_union.headers.getItem("Facility")) \
        .withColumn("topic", df_union.headers.getItem("topic")) \
        .withColumn("Severity", df_union.headers.getItem("Severity")) \
        .withColumn("timestamp",
                    f.from_unixtime((f.col("timestamp_1").cast("bigint") / 1000).cast("bigint")).cast("timestamp")) \
        .withColumn("msg", f.decode(f.col("body"), "ISO-8859-1")) \
        .drop(f.col("body")).drop(f.col("timestamp_1"))

    print("#########################################################################")
    print("#####################        EXPLODED DATAFRAME        ##################")
    print("#########################################################################")

    # Si metemos argumento de host, se filtra aqui
    if args.host:
        host = args.host
        print("Se filtra por el host {0}".format(host))
        df_exploded = df_exploded.filter("host='{}'".format(host))

    # Filtramos por los hosts de FTIII y por el mensaje que nos interesa
    df_memory_errors = df_exploded \
        .select("timestamp", "host", "msg", "year", "month", "day") \
        .filter("host like 'login%' or host like 'c20%' or host like 'c21%' or host like 'hsm212%' or host like "
                "'osshsm213%' or host like 'oss21%'") \
        .filter("upper(msg) like '% EDAC %' and lower(msg) like '%memory%error%'")

    print("#########################################################################")
    print("#####################              EDAC                ##################")
    print("#########################################################################")
    
    df_memory_errors.write.mode('overwrite').option('compression', 'snappy') \
        .partitionBy('year', 'month', 'day', 'host').parquet('memory-errors')
    
    print("FIN: FICHERO ESCRITO")
    sc.stop()
