import os
import sys
import logging
from datetime import datetime

import configs

LOG_LEVEL = logging.INFO
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s; - %(levelname)s; - %(message)s')

# Directory config params
ROOT_DIR = os.getcwd()
INPUT_FILES_DIR = '/input_files/'
OUTPUT_FILES_DIR = '/output_files/'
BPS_REGISTER_FILE = 'bps_loads_register.json'

#BASE_DATOS_LIVE_CONFIG = 'backup_crm_compartfon'

OUTPUT_ETL_LOAD_TIEMPO_RESPUESTA_QUERY_FILENAME = 'query_insert_detalle_tiempo_respuesta'
OUTPUT_ETL_LOAD_TRAFICO_INTERFACE_QUERY_FILENAME = 'query_insert_detalle_trafico_interface'
OUTPUT_ETL_LOAD_ERROR_INTERFACE_QUERY_FILENAME = 'query_insert_detalle_error_interface'
OUTPUT_ETL_LOAD_CARGA_CPU_QUERY_FILENAME = 'query_insert_detalle_carga_cpu'
OUTPUT_ETL_LOAD_CARGA_TICKETS_QUERY_FILENAME = 'query_insert_detalle_tickets'

OUTPUT_ETL_UPDATE_LOAD_TIEMPO_RESPUESTA_QUERY_FILENAME = 'query_update_detalle_tiempo_respuesta'
OUTPUT_ETL_UPDATE_LOAD_CARGA_CPU_QUERY_FILENAME = 'query_update_detalle_carga_cpu'
OUTPUT_ETL_UPDATE_LOAD_TRAFICO_INTERFACE_QUERY_FILENAME = 'query_update_detalle_trafico_interface'
OUTPUT_ETL_UPDATE_LOAD_ERROR_INTERFACE_QUERY_FILENAME = 'query_update_detalle_error_interface'

# Config schema
schema_public = 'public'
schema_historico = 'historico'
# Config tables
detalle_tiempo_respuesta = 'detalle_tiempo_respuesta_cpu_load'
detalle_trafico_interface = 'detalle_trafico_interface'
detalle_error_interface = 'detalle_errores_interface'
detalle_carga_cpu = 'detalle_carga_cpu'
dt_ticket = 'dt_ticket'
