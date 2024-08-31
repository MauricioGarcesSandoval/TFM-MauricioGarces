# TFM-MauricioGarces

Este proyecto contiene un script PySpark que se puede ejecutar usando `spark-submit`. El script requiere tres argumentos para funcionar correctamente.
Hay que dejar claro que este proyecto solo funciona sobre el cluster Finisterrae II del CESGA.

## Requisitos Previos

- **Apache Spark**: Asegúrate de tener Apache Spark instalado y configurado correctamente en tu sistema.
- **Python**: El proyecto está basado en Python, así que asegúrate de tener un entorno de Python configurado.
- **PySpark**: Debe estar disponible en tu entorno de Python.

## Instalación

1. Clona el repositorio a tu máquina local:
    ```bash
    git clone https://github.com/MauricioGarcesSandoval/TFM-MauricioGarces.git
    cd nombre-del-repositorio
    ```

2. Instala las dependencias necesarias:
    ```bash
    pip install -r requirements.txt
    ```

## Ejecución del Script

El script principal se encuentra en el archivo `recoleccion_errores.py`. Este script requiere tres argumentos de entrada:

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

## Estructura del Proyecto

```plaintext
TFM-MauricioGarces/
│
├── recoleccion_errores.py
├── requirements.txt
└── README.md
```

## Notas

- Asegúrate de tener suficiente memoria y recursos asignados a Spark si estás trabajando con grandes conjuntos de datos.
- Puedes ajustar la configuración de Spark utilizando opciones adicionales en el comando `spark-submit`, como `--master`, `--executor-memory`, etc.

## Licencia

Este proyecto está bajo la licencia [Nombre de la Licencia]. Ver el archivo `LICENSE` para más detalles.

## Contacto

Si tienes preguntas o comentarios, puedes contactarme a través de [mgs803@alumnos.unican.es](mailto:mgs803@alumnos.unican.es).
