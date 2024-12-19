import pandas as pd
import numpy as np
import datetime
from datetime import datetime
import logging
import traceback
import time

import configs
import utils
import database

import json


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

logger = logging.getLogger(__name__)
logger.setLevel(configs.LOG_LEVEL)

def main():
    logger.info('Init ETL-Preleads process')
    try:
        inicio = time.time()
        logger.info('Inicio %s',str(inicio))
        tickets  = extract_process()
        fin = time.time()
        logger.info('fin %s',str(fin))
        logger.info('Tiempo de ejecucion: %s',str(fin-inicio))
        logger.info('Pruebas de conexión y extracción tickets: %s',str(len(tickets)))
        logger.info(tickets.head(1))
        ## Procesando tabla de leads_lead
        
        fields = [*database.tbl_detalle_tickets_remedy().values()]
        transformed_data = transform_process(tickets, fields)
        logger.info(transformed_data.head(1))
        
        load_process(transformed_data, 'insert', configs.schema_public+'.'+ configs.dt_ticket, configs.OUTPUT_ETL_LOAD_CARGA_TICKETS_QUERY_FILENAME, fields)
    except Exception as e:
        logger.error("Error(s) ocurred in %s", str(e))
        logger.info('Done ETL-Preleads process with erros')
    else:   
        logger.info('Done ETL-Preleads process successfully')
    
def extract_process():
    logger.info('1. Extract data')
    try:
        tickets = database.execute_select_query_pandas_helix(database.query_select_detalle_tickets_helix())

        return tickets
    except Exception as e:
        logger.debug('traceback error %s:', str(traceback.format_exc()))
        raise Exception("extract data process: " + str(e))

def transform_process(data_frame, fields):
    logger.info('2. Transform data')
    try:
        # Apply preperson preprocessing (clean/normalize data)
        data_frame.columns = fields

        data_frame['fc_fechacreacion'] = datetime.now()
        vacios_nulos = data_frame[data_frame['sitio_id'].isna() | (data_frame['sitio_id'] == '')]
        print(str(len(vacios_nulos)))
        data_frame['actividad'] = data_frame['actividad'].apply(quitar_comillas)
        # Reemplazar valores vacíos por cero
        data_frame['sitio_id'] = data_frame['sitio_id'].replace('', 0)
        data_frame['sitio_id'] = data_frame['sitio_id'].replace(pd.NA, 0)

        # Aplicar la función solo a las columnas A, B y C
        columnas_a_convertir = ['tiempo_afectacion_minutos', 'tiempo_afectacion_atribuible', 'bst2_tiempo_afectacion_concil']
        data_frame[columnas_a_convertir] = data_frame[columnas_a_convertir].applymap(convertir_a_entero)
        print("Columnas en el DataFrame:", data_frame.columns)


        return data_frame
    except Exception as e:
        logger.debug('traceback error %s:', str(traceback.format_exc()))
        raise Exception("transform data process: " + str(e))

def load_process(data_frame, query, entitys, name_file,table_fields):
    logger.info('3. Load data')
    # Construct preleads inserts query, save query in text file and execute it
    try:
        if query == 'update':
            ids = data_frame['id']
            data_frame = data_frame[data_frame.columns[data_frame.columns.isin(table_fields)]].reindex(columns=table_fields)
            preleads_update_query = utils.construct_update_query(entity=entitys, data=data_frame.to_dict(orient='records'),id=ids) 
            utils.save_text_data(preleads_update_query, configs.ROOT_DIR + configs.OUTPUT_FILES_DIR + name_file + '.txt')
                        
        elif query == 'insert':
            data_frame = data_frame[data_frame.columns[data_frame.columns.isin(table_fields)]].reindex(columns=table_fields)
            preleads_inserts_query = utils.construct_insert_query(entity=entitys, fields=table_fields, data=data_frame.to_dict(orient='records')) 
            utils.save_text_data(preleads_inserts_query, configs.ROOT_DIR + configs.OUTPUT_FILES_DIR + name_file + '.txt')

            database.execute_insert_query_postg(preleads_inserts_query)

    except Exception as e:
        logger.debug('traceback error %s:', str(traceback.format_exc()))
        raise Exception("load preleads data process: " + str(e))
    finally:
        logger.info('\t Prepersons  loaded: %s', str(len(data_frame)))
        return data_frame
    
# Función para convertir valores en enteros y manejar NaN como None
def convertir_a_entero(valor):
    if pd.notna(valor):
        return int(valor)
    else:
        return pd.NA

# Función para quitar comillas simples y dobles
def quitar_comillas(valor):
    if pd.notna(valor):
        return valor.replace("'", "").replace('"', "")
    return valor

if __name__ == "__main__":
    main()

