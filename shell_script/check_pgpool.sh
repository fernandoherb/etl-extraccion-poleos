#!/bin/bash

# Variables
HOST="172.24.22.141"
PORT="9000"
USER="bestel_super"
DATABASE="dwh"
PASSWORD="6OfN1i0VHJ91y4MjxQ"
PGPOOL_SERVICE="pgpool"

# Obtener la fecha y hora actual
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

# Intentar conexión con psql
RESULT=$(PGPASSWORD=$PASSWORD psql -h $HOST -p $PORT -U $USER -d $DATABASE -c "SELECT 1;" 2>&1)


# Verificar si hay errores en la conexión
if [[ "$RESULT" == *"server closed the connection unexpectedly"* ]]; then
    echo "$TIMESTAMP - Error detectado: server closed the connection unexpectedly"
    echo "$TIMESTAMP - Reiniciando pgpool..."
    # Reiniciar el servicio pgpool
    systemctl restart $PGPOOL_SERVICE
    if [ $? -eq 0 ]; then
        echo "$TIMESTAMP - pgpool reiniciado correctamente"
    else
        echo "$TIMESTAMP - Error al reiniciar pgpool"
    fi
elif [[ "$RESULT" == *"could not connect to server: Connection refused"* ]]; then
    echo "$TIMESTAMP - Error detectado: could not connect to server: Connection refused"
    echo "$TIMESTAMP - Reiniciando pgpool..."
    # Reiniciar el servicio pgpool
    systemctl restart $PGPOOL_SERVICE
    if [ $? -eq 0 ]; then
        echo "$TIMESTAMP - pgpool reiniciado correctamente"
    else
        echo "$TIMESTAMP - Error al reiniciar pgpool"
    fi
else
    echo "$TIMESTAMP - Conexión exitosa"
fi

