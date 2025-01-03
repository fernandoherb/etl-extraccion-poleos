import pandas as pd
import numpy as np
import datetime
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
        trafico_interface  = extract_process()
        fin = time.time()
        logger.info('fin %s',str(fin))
        logger.info('Tiempo de ejecucion: %s',str(fin-inicio))
        logger.info('Pruebas de conexión y extracción trafico_interface: %s',str(len(trafico_interface)))
        ## Procesando tabla de leads_lead
        logger.info(trafico_interface.head(1))
        
        fields = [*database.tbl_detalle_trafico_interface().values()]
        transformed_data = transform_process(trafico_interface, fields)
        
        load_process(transformed_data, 'insert', configs.schema_public+'.'+ configs.detalle_trafico_interface, configs.OUTPUT_ETL_LOAD_TRAFICO_INTERFACE_QUERY_FILENAME, fields)
        load_process(transformed_data, 'insert', configs.schema_historico+'.'+ configs.detalle_trafico_interface_7d, configs.OUTPUT_ETL_LOAD_TRAFICO_INTERFACE_7D_QUERY_FILENAME, fields)
        database.execute_delete_postg("DELETE FROM historico.detalle_trafico_interface_7d WHERE fecha_creacion < CURRENT_TIMESTAMP - INTERVAL '24 hours';")       

    except Exception as e:
        logger.error("Error(s) ocurred in %s", str(e))
        logger.info('Done ETL-Preleads process with erros')
    else:   
        logger.info('Done ETL-Preleads process successfully')
    
def extract_process():
    logger.info('1. Extract data')
    try:
        # Load data: Lineas expiradas DB
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        str_date = str(yesterday)

        trafico_interfaces = database.select_data_frame(database.query_select_detalle_trafico_interface())

        return trafico_interfaces
    except Exception as e:
        logger.debug('traceback error %s:', str(traceback.format_exc()))
        raise Exception("extract data process: " + str(e))

def transform_process(data_frame, fields):
    logger.info('2. Transform data')
    try:
        # Apply preperson preprocessing (clean/normalize data)
        data_frame.columns = fields

        # Consulta el catalgo de nodos ############ CAMBIAR POR INTERFACES
        ct_interfaces = database.execute_select_query_pandas(database.query_select_ct_interface())
        # Encuentra los nodos faltantes usando `merge`
        nodos_faltantes = ct_interfaces[~ct_interfaces['interface_id'].isin(data_frame['interface_id'])]
        
        logger.info('Interfaces faltantes:::::: '+str(len(nodos_faltantes)))
                
        # Concatenamos `df_poleos` con `ct_interfaces_faltantes`
        data_frame = pd.concat([data_frame, nodos_faltantes], ignore_index=True)

        logger.info('Cuantos interfaces modificará:::::: '+str(len(data_frame)))
        ############ FIN CAMBIAR POR INTERFACES

        return data_frame
    except Exception as e:
        logger.debug('traceback error %s:', str(traceback.format_exc()))
        raise Exception("transform data process: " + str(e))

def load_process(data_frame, query, entitys, name_file,table_fields):
    logger.info('3. Load data')
    # Construct preleads inserts query, save query in text file and execute it
    try:
        if query == 'update':
            ids = data_frame['interface_id']
            data_frame = data_frame[data_frame.columns[data_frame.columns.isin(table_fields)]].reindex(columns=table_fields)
            preleads_update_query = utils.v_dos_construct_update_query(entity=entitys, data=data_frame.to_dict(orient='records'),id=ids) 
            utils.save_text_data(preleads_update_query, configs.ROOT_DIR + configs.OUTPUT_FILES_DIR + name_file + '.txt')
            
            database.execute_update_query_beste_postg(preleads_update_query)
            
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

if __name__ == "__main__":
    main()
