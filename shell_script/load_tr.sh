#!/bin/bash

export PATH=$PATH:$HOME/bin
export PATH=$HOME/.local/bin:$PATH

MAIN_DIR="/Users/flores_daysi/dev-crones/"
#WORK_DIR=$MAIN_DIR"ctf-automation/"
SOURCE_CODE_DIR=$MAIN_DIR"etl-extraccion-poleos/"
PY_BIN_DIR=$MAIN_DIR"virtual-space/v_etl_bestel/bin/"
FECHA=$(date +"%Y-%m-%d %H:%M:%S")

echo "Imprimiendo fecha y hora"
echo "$FECHA"

## Activando entorno virtual
echo "Activando entorno virtual"
echo "Ruta entorno virtual: "$PY_BIN_DIR
source $PY_BIN_DIR"activate"
cd "$SOURCE_CODE_DIR"

## Ejecutar script
echo "Ejecutar script  que descarga e inserta información de poleos a dwh cada 4 minutos"
echo $SOURCE_CODE_DIR"main_tiempo_respuesta.py"
python3 "$SOURCE_CODE_DIR"main_tiempo_respuesta.py > $SOURCE_CODE_DIR"logs/log_load_tr_python.log" 2>&1
echo "Terminó el proceso"
## python3 main.py Etl-LoadStatus --from_date=$FECHA --to_date=$FECHA  >> $SOURCE_CODE_DIR"logs/out-etl-load-status.log"

