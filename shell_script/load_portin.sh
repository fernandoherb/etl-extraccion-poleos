#!/bin/bash

export PATH=$PATH:$HOME/bin
export PATH=$HOME/.local/bin:$PATH

MAIN_DIR="/home/reports-compartfon/"
WORK_DIR=$MAIN_DIR"update_portin/"
SOURCE_CODE_DIR=$WORK_DIR"etl-update-tblportin/"
PY_BIN_DIR=$MAIN_DIR"lineas_expiradas/py_env/bin/"
YESTERDAY=$(date --date="yesterday" +"%Y-%m-%d")
#YESTERDAY='2022-02-21'

echo "imprimiendo fecha"
echo "$YESTERDAY"

## Activando entorno virtual
echo "Activando entorno virtual"
source $PY_BIN_DIR"activate"
cd "$SOURCE_CODE_DIR"

## Ejecutar script
echo "Ejecutar script"
python3 main.py  >> $WORK_DIR"out_etl.log"

