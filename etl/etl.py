from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException
import argparse
import time
import logging


def read_parquet_if_exists(spark, path):
    try:
        return spark.read.parquet(path)
    except AnalysisException:
        return None


def parse_args(parser):
    parser.add_argument("--year", type=int, required=True, help="configuration spark: year to filter")
    parser.add_argument("--month", type=int, required=True, help="configuration spark: month to filter")
    parser.add_argument("--input-path", required=True, help="configuration spark: input path")
    parser.add_argument("--output-path", required=True, help="configuration spark: output path")
    parser.add_argument("--window-size", required=True, help="configuration spark: windows size")

    # Read arguments from the command line
    args = parser.parse_args()

    year = args.year
    month = args.month
    input_path = args.input_path
    output_path = args.output_path
    window_size = args.window_size

    return year, month, input_path, output_path, window_size


def parse_window_size(ws: str):
    num, unit = ws.split()
    num = int(num)

    if unit.startswith("min"):
        return num * 60
    elif unit.startswith("hour"):
        return num * 3600
    else:
        raise ValueError("Unidad no soportada en WINDOW_SIZE")


def next_dates(year, month):
    if month == 12:
        next_year = year + 1
        next_month = 1
    else:
        next_year = year
        next_month = month + 1

    month_start = "{}-{:02d}-01 00:00:00".format(year, month)
    if month == 12:
        month_end = "{}-01-01 00:00:00".format(year + 1)
    else:
        month_end = "{}-{:02d}-01 00:00:00".format(year, month + 1)

    return next_year, next_month, month_start, month_end


def etl(spark, window_size, input_path, output_path, year, month):
    WINDOW_SECONDS = parse_window_size(window_size)
    NEXT_YEAR, NEXT_MONTH, MONTH_START, MONTH_END = next_dates(year, month)
    OVERLAP_SECONDS = WINDOW_SECONDS + 3600
    SALT_BUCKETS = 120

    regex_error = r".*?(\bCE\b|\bUE\b|\bSBE\b|\bDBE\b).*?CPU_SrcID#(\d+?)_(?:MC|Ha)#(\d+?)_Chan#(\d+?)_DIMM#(\d+?).*"

    # -------------------------
    # 1. LECTURA Y PREPROCESO
    # -------------------------

    df_month = spark.read.parquet("{}/year={}/month={:02d}".format(input_path, year, month)).drop("day")

    next_day_path = "{}/year={}/month={:02d}/day=01".format(input_path, NEXT_YEAR, NEXT_MONTH)
    df_next_day = read_parquet_if_exists(spark, next_day_path)

    if df_next_day is not None:
        df_next_day = df_next_day.filter(
            col("timestamp") < expr(
                "timestamp('{}') + interval {} seconds".format(
                    MONTH_END, OVERLAP_SECONDS
                )
            ))

        # Normalizar schema (por si entra 'day')
        for c in ["year", "month", "day"]:
            if c in df_next_day.columns:
                df_next_day = df_next_day.drop(c)
            if c in df_month.columns:
                df_month = df_month.drop(c)

        df_logs = df_month.unionByName(df_next_day)

    else:
        # Último mes del histórico: usamos solo el mes actual
        for c in ["year", "month", "day"]:
            if c in df_month.columns:
                df_month = df_month.drop(c)

        df_logs = df_month

    df = (
        df_logs
        .drop("topic", "Facility", "partition")
        .withColumn("timestamp_1", df_logs.headers.getItem("timestamp"))
        .withColumn("host", df_logs.headers.getItem("host"))
        .withColumn("Severity", df_logs.headers.getItem("Severity").cast("int"))
        .withColumn("timestamp",
                    from_unixtime((col("timestamp_1").cast("bigint") / lit(1000)).cast("bigint")).cast("timestamp"))
        .drop(col("body")).drop(col("timestamp_1")).drop(col("headers"))
        .withColumn("error_type", regexp_extract("msg", regex_error, 1))
        .withColumn("is_error", (col("error_type") != "").cast("int"))
        .withColumn("host",
                    when(col("host").isNull() | (trim(col("host")) == ""), lit("UNKNOWN_HOST"))
                    .otherwise(col("host")))
    )

    # -------------------------
    # 2. SALTING PARA CORREGIR EL DESBALANCEO DE HOST
    # -------------------------

    df = df.withColumn("salt", (floor(rand() * SALT_BUCKETS)).cast("int"))

    # -------------------------
    # 3. BUCKET DE TIEMPO
    # -------------------------

    df = (
        df
        .withColumn("feature_bucket_ts", floor(col("timestamp").cast("long") / WINDOW_SECONDS) * WINDOW_SECONDS)
        .withColumn("feature_window_start", from_unixtime("feature_bucket_ts").cast("timestamp"))
    )

    # -------------------------
    # 4. REPARTICION CORRECTA
    # -------------------------
    df = df.repartition(2000, "host", "salt")

    # -------------------------
    # 5. AGREGACION PARA FEATURES
    # -------------------------
    features_salted = (
        df
        .groupBy("feature_window_start", "host", "salt")
        .agg(
            count("*").alias("num_logs"),
            sum("is_error").alias("num_errors"),
            avg("Severity").alias("avg_severity"),
            max("Severity").alias("max_severity"),
            max("is_error").alias("label_feature_window")
        )
    )

    # -------------------------
    # 6. QUITAMOS EL SALT
    # -------------------------
    features = (
        features_salted
        .groupBy("feature_window_start", "host")
        .agg(
            sum("num_logs").alias("num_logs"),
            sum("num_errors").alias("num_errors"),
            avg("avg_severity").alias("avg_severity"),
            max("max_severity").alias("max_severity"),
            max("label_feature_window").alias("label_feature_window")
        )
    )

    # -------------------------
    # 7. CONSTRUCCIÓN DEL TARGET: ERROR EN LA PROXIMA HORA
    # -------------------------

    ONE_HOUR = 3600

    df = (
        df
        .withColumn("target_bucket_ts", floor(col("timestamp").cast("long") / ONE_HOUR) * ONE_HOUR)
        .withColumn("target_window_start", from_unixtime("target_bucket_ts").cast("timestamp"))
    )

    target = (
        df
        .groupBy("target_window_start", "host")
        .agg(
            max("is_error").alias("label_next_hour")
        )
    )

    # -------------------------
    # 8. SHIFT DEL TARGET
    # -------------------------

    target_shifted = target.withColumn("feature_window_start",
                                       expr("target_window_start - interval {} seconds".format(WINDOW_SECONDS)))

    # -------------------------
    # 9. JOIN ENTRE FEATURES Y TARGETS
    # -------------------------

    dataset = (
        features.join(
            target_shifted,
            on=["host", "feature_window_start"],
            how="left"
        )
        .fillna({"label_next_hour": 0})
        .withColumn("error_ratio", col("num_errors") / col("num_logs"))

    )

    dataset_month = (
        dataset.filter(
            (col("feature_window_start") >= MONTH_START) &
            (col("feature_window_start") < MONTH_END)
        )
    )

    dataset_month.repartition(2).write.mode("overwrite").parquet(
        "{}/year={}/month={:02d}".format(output_path, year, month))

    print("FIN: FICHEROS ESCRITOS")


def main():
    # Iniciamos el parser de los argumentos
    parser = argparse.ArgumentParser()
    year, month, input_path, output_path, window_size = parse_args(parser)

    # Crear sesion de Spark
    spark = SparkSession.builder.appName("ETL-next_hour-{}-{:02d}".format(year, month)).getOrCreate()

    etl(spark, window_size, input_path, output_path, year, month)

    logging.info("FIN: PARQUET GUARDADO")
    spark.stop()


if __name__ == "__main__":
    start = time.perf_counter()

    main()

    end = time.perf_counter()

    print("Tiempo: {} segundos".format(end - start))
