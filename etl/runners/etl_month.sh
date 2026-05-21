#!/bin/bash

# Validar input
if [ -z "$1" ]; then
  echo "Uso: ./etl_month.sh \"30 minutes\" | \"1 hour\" | \"5 minutes\" ..."
  exit 1
fi

WINDOW_SIZE="$1"

# Extraer número y unidad
VALUE=$(echo $WINDOW_SIZE | awk '{print $1}')
UNIT=$(echo $WINDOW_SIZE | awk '{print $2}')

# Normalizar nombres para rutas/logs
if [[ "$UNIT" == "minutes" ]]; then
  SUFFIX="${VALUE}_mins"
  OUTPUT_SUBPATH="minutes=${VALUE}"
elif [[ "$UNIT" == "hour" || "$UNIT" == "hours" ]]; then
  SUFFIX="${VALUE}_hour"
  OUTPUT_SUBPATH="hour=${VALUE}"
else
  echo "Unidad no soportada: $UNIT"
  exit 1
fi

START_YEAR=2021
START_MONTH=12

END_YEAR=2022
END_MONTH=12

year=$START_YEAR
month=$START_MONTH

while [ "$year" -lt "$END_YEAR" ] || ([ "$year" -eq "$END_YEAR" ] && [ "$month" -le "$END_MONTH" ])
do
  echo "=============================="
  echo "Ejecutando para $year-$month con window $WINDOW_SIZE"
  echo "=============================="

  spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 40 \
    --executor-cores 4 \
    --executor-memory 12G \
    --conf spark.executor.memoryOverhead=2G \
    --conf spark.sql.shuffle.partitions=2000 \
    --conf spark.defatult.parallelism=2000 \
    --conf spark.shuffle.compress=true \
    --conf spark.shuffle.spill.compress=true \
    --conf spark.dynamicAllocation.enabled=false \
    --name "ETL dataset ML ${year}-${month}" \
    etl.py \
      --year $year \
      --month $month \
      --window-size "$WINDOW_SIZE" \
      --input-path "memory-logs" \
      --output-path "data_output/features_ml/${OUTPUT_SUBPATH}" \
    > logs_spark/etl_parsed_window_${SUFFIX}_${year}_${month}.log 2>&1

  EXIT_CODE=$?

  if [ $EXIT_CODE -ne 0 ]; then
    echo "Falló $year-$month (exit code $EXIT_CODE), pero continúo con el siguiente..."
  else
    echo "OK $year-$month"
  fi

  # Incrementar mes
  month=$((month + 1))
  if [ "$month" -gt 12 ]; then
    month=1
    year=$((year + 1))
  fi

done

echo "Todos los jobs ejecutados"