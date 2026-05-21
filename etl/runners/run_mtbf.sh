#!/bin/bash

# Rango de fechas desde 202112 hasta 202212
start_date=$1 #202112
end_date="202212"
path_to_read=$2 #/user/tec_sis6/memory-errors

# Función para iterar fechas
while [ "$start_date" -le "$end_date" ]; do
    echo "Ejecutando spark-submit para la fecha: $current_date"

    spark-submit \
        --master yarn \
        --deploy-mode client \
        --num-executors 3 \
        --executor-cores 5 \
        --executor-memory 30G \
        --conf spark.yarn.submit.waitAppCompletion=false \
        --name "MTBF calculation" try.py --path-log "$path_to_read" --date "$start_date"

    # Calcular el siguiente mes
    year=${current_date:0:4}
    month=${current_date:4:2}
    if [ "$month" -eq 12 ]; then
        year=$((year + 1))
        month="01"
    else
        month=$((10#$month + 1))
        month=$(printf "%02d" $month)
    fi
    current_date="${year}${month}"
done
