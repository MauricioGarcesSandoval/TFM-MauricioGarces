#!/bin/bash

# Rango de fechas desde 20211214 hasta 20221231
FECHA_INICIAL="20211214"
FECHA_TOPE="20221231"

# Función para iterar fechas
fecha_inicio="$FECHA_INICIAL"

while [ "$fecha_inicio" -le "$FECHA_TOPE" ]; do
  fecha_fin=$(date -d "$fecha_inicio +10 days" +"%Y%m%d")

  if [ "$fecha_fin" -gt "$FECHA_TOPE" ]; then
        fecha_fin="$FECHA_TOPE"
  fi

  echo "Ejecutando spark-submit para la fecha: $fecha_inicio hasta $fecha_fin"

  spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 3 \
    --executor-cores 5 \
    --executor-memory 10G \
    --conf spark.executor.memoryOverhead=1G \
    --conf spark.yarn.submit.waitAppCompletion=false \
    --name "Collect logs $fecha_inicio" recoleccion_logs.py --date-from "$fecha_inicio" --date-to "$fecha_fin"

  fecha_inicio=$(date -d "$fecha_fin +1 day" +"%Y%m%d")
done
