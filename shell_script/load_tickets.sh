#!/bin/bash

export PATH=$PATH:$HOME/bin
export PATH=$HOME/.local/bin:$PATH

MAIN_DIR="/home/vanti/Proyectos/"
SOURCE_CODE_DIR=$MAIN_DIR"etl-extraccion-poleos/"
PY_BIN_DIR=$MAIN_DIR"env1/bin/"
FECHA=$(date +"%Y-%m-%d %H:%M:%S")

echo "Imprimiendo fecha y hora"
echo "$FECHA"

## Activando entorno virtual
echo "Activando entorno virtual"
echo "Ruta entorno virtual: "$PY_BIN_DIR
source $PY_BIN_DIR"activate"
cd "$SOURCE_CODE_DIR"

## Ejecutar script
echo "Ejecutar script  que descarga e inserta información de tickets a dwh cada 10 minutos"
echo $SOURCE_CODE_DIR"main_tickets_remedy.py"
python3 "$SOURCE_CODE_DIR"main_tickets_remedy.py > $SOURCE_CODE_DIR"logs/log_load_tr_python_carga_tickets.log" 2>&1
echo "Terminó el proceso"
## python3 main.py Etl-LoadStatus --from_date=$FECHA --to_date=$FECHA  >> $SOURCE_CODE_DIR"logs/out-etl-load-status.log"

