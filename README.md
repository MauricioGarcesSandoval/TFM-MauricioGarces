# TFM-MauricioGarces

Este proyecto contiene dos scripts PySpark que se pueden ejecutar usando `spark-submit` y otro que es un fichero sh para calcular el mtbf y guardarlo en ficheros JSON. 

- El script [recoleccion_errores.py](etl/recoleccion_errores.py) requiere tres argumentos para funcionar correctamente. Hay que dejar claro que este script solo funciona sobre el cluster Finisterrae II del CESGA.
- El script [calcular_mtbf.py](etl/calcular_mtbf.py) requiere dos argumentos para funcionar correctamente y además funciona sobre el cluster Finisterrae III del CESGA.
- El script [run_mtbf.sh](etl/runners/run_mtbf.sh) requiere primero activar un entorno conda correcto y necesita dos argumentos.


## Requisitos Previos

- **Apache Spark**: Asegúrate de tener Apache Spark instalado y configurado correctamente en tu sistema.
- **Python**: El proyecto está basado en Python, así que asegúrate de tener un entorno de Python configurado.
- **PySpark**: Debe estar disponible en tu entorno de Python.

## Instalación

1. Clona el repositorio a tu máquina local:
    ```bash
    git clone https://github.com/MauricioGarcesSandoval/TFM-MauricioGarces.git
    cd TFM-MauricioGarces
    ```

2. Instala las dependencias necesarias:
    ```bash
    pip install -r requirements.txt
    ```

## Ejecución de los scripts:

### recoleccion_errores.py

El script se encuentra en el archivo `recoleccion_errores.py`. Este script requiere tres argumentos de entrada:

1. `--date-from`: Fecha desde la que queremos los logs.
2. `--date-to`: Fecha hasta la que queremos los logs.
3. `--host`: Nombre del host del cual queremos los logs.

### Ejemplo de uso:

Puedes ejecutar el script usando el siguiente comando `spark-submit`:

```bash
spark-submit --master yarn --deploy-mode client --num-executors 3 --executor-cores 5 --executor-memory 30G --conf spark.yarn.submit.waitAppCompletion=false --name 'Collect DRAM Memory Errors' recoleccion_errores.py --date-from 20220801 --date-to 20220901 --host host1
```
--date-from 20220801 --date-to 20220901 --host host1
**Donde**:
- `--date-from`: Fecha inicio con format yyyyMMdd: 20220801.
- `--date-to`: Fecha fin con format yyyyMMdd: 20220901.
- `--host`: Host del cluster: host1.

### calcular_mtbf.py

El script se encuentra en el archivo `calcular_mtbf.py`. Este script requiere dos argumentos de entrada:

1. `--path-log`: Ruta en HDFS donde están los logs.
2. `--date`: Fecha para calcular el mtbf.

### Ejemplo de uso:

Puedes ejecutar el script usando el siguiente comando `spark-submit`:

```bash
spark-submit --master yarn --deploy-mode client --num-executors 3 --executor-cores 5 --executor-memory 30G --conf spark.yarn.submit.waitAppCompletion=false --name 'MTBF calculation' calcular_mtbf.py --path-log "/user/tec_sis4/memory-errors" --date 202208 --output-path "/user/tec_sis6/logs_json"
```

### run_mtbf.sh

El script se encuentra en el archivo `run_mtbf.py`. Este script requiere dos argumentos de entrada:

1. Fecha de inicio, en formato yyyyMM
2. Ruta en HDFS donde están los logs.

### Ejemplo de uso:

Puedes ejecutar el script usando el siguiente comando `sh`:

```bash
module load anaconda3/2020.02
sh run_mtbf.sh 202112 /user/tec_sis6/memory-errors
```



## Estructura del Proyecto

```plaintext
TFM-MauricioGarces/
│
├── etl
├──── notebooks
├────── analisis_fallos.ipynb
├────── graficas_mtbf.ipynb
├──── runners
├────── etl_month.sh
├────── run_mtbf.sh
├────── run_recoleccion_logs.sh
├──── calcular_mtbf.py
├──── etl.py
├──── recoleccion_errores.py
├──── recoleccion_logs.py
├── ml
├──── lr
├────── graficas
├────── train_lr_1_hour.py
├────── train_lr_3_hour.py
├────── train_lr_5_minutes.py
├────── train_lr_30_minutes.py
├──── xgb
├────── graficas
├────── modelos
├──────── xgboost_model_1h.json
├──────── xgboost_model_3h.json
├──────── xgboost_model_5m.json
├──────── xgboost_model_30m.json
├────── train_xgb_1_hour.py
├────── train_xgb_3_hour.py
├────── train_xgb_5_minutes.py
├────── train_xgb_30_minutes.py
└── README.md
```

## Contacto

Si tienes preguntas o comentarios, puedes contactarme a través de [mgs803@alumnos.unican.es](mailto:mgs803@alumnos.unican.es).
