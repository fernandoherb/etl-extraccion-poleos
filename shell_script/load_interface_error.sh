#!/bin/bash

export PATH=$PATH:$HOME/bin
export PATH=$HOME/.local/bin:$PATH

MAIN_DIR="/home/vanti/Proyectos/"
#WORK_DIR=$MAIN_DIR"ctf-automation/"
SOURCE_CODE_DIR=$MAIN_DIR"etl-extraccion-poleos/"
PY_BIN_DIR=$MAIN_DIR"env1/bin/"
FECHA=$(date +"%Y-%m-%d %H:%M:%S")

echo "Imprimiendo fecha y hora"
echo "$FECHA"

## Activando entorno virtual
if [[ -f "$PY_BIN_DIR/activate" ]]; then
    echo "Activando entorno virtual"
    echo "Ruta entorno virtual: $PY_BIN_DIR"
    source "$PY_BIN_DIR/activate"
else
    echo "Error: No se encuentra el entorno virtual en $PY_BIN_DIR"
    exit 1
fi

cd "$SOURCE_CODE_DIR" || { echo "Error: No se pudo acceder al directorio $SOURCE_CODE_DIR"; exit 1; }

## Ejecutar script
echo "Ejecutar script  que descarga e inserta información de poleos de error de interfaces a dwh cada 4 minutos"
echo $SOURCE_CODE_DIR"main_error_interfaces.py"

python3 "$SOURCE_CODE_DIR"main_error_interfaces.py > $SOURCE_CODE_DIR"logs/log_load_python_error_interfaces.log" 2>&1

echo "Terminó el proceso"
## python3 main.py Etl-LoadStatus --from_date=$FECHA --to_date=$FECHA  >> $SOURCE_CODE_DIR"logs/out-etl-load-status.log"

