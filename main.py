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
        tiempo_respuesta, trafico_interface, carga_cpu  = extract_process()
        fin = time.time()
        logger.info('fin %s',str(fin))
        logger.info('Tiempo de ejecucion: %s',str(fin-inicio))
        logger.info('Pruebas de conexión y extracción tiempo_respuesta: %s, trafico_interface: %s y carga_cpu: %s ',str(len(tiempo_respuesta)), str(len(trafico_interface)), str(len(carga_cpu)))
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

        trafico_interface = database.select_data_frame(database.query_select_detalle_trafico_interface())
        tiempo_respuesta = database.select_data_frame(database.query_select_detalle_tiempo_respuesta_carga_cpu())
        carga_cpu = database.select_data_frame(database.query_select_detalle_carga_cpu())

        return tiempo_respuesta, trafico_interface, carga_cpu
    except Exception as e:
        logger.debug('traceback error %s:', str(traceback.format_exc()))
        raise Exception("extract data process: " + str(e))

def transform_process(data_frame, id_tabla):
    logger.info('2. Transform data')
    try:
        # Apply preperson preprocessing (clean/normalize data)
        if id_tabla == 'tblLeads':
            data_frame = utils.leads_lead_preprocessing(data_frame,' database.query_leads_lead_get_row()')

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

            database.execute_insert_query(preleads_inserts_query)

    except Exception as e:
        logger.debug('traceback error %s:', str(traceback.format_exc()))
        raise Exception("load preleads data process: " + str(e))
    finally:
        logger.info('\t Prepersons  loaded: %s', str(len(data_frame)))
        return data_frame

def user_idm_delete(data_frame):
    logger.info('4. Verify and delete data in IDM')

    try:
        # It´s neccesary to verify how many there are in infobip
        df2 = utils.user_idm_delete(data_frame)
        return df2
    except Exception as e:
        logger.debug('traceback error %s:', str(traceback.format_exc()))
        raise Exception("match infobip data process: " + str(e))

def vInfobipData(data_frame):
    logger.info('5. Verify data in Infobip')

    try:
        # It´s neccesary to verify how many there are in infobip
        df2 = utils.vInfobipData(data_frame)
        return df2
    except Exception as e:
        logger.debug('traceback error %s:', str(traceback.format_exc()))
        raise Exception("match infobip data process: " + str(e))

def uInfobipData(data_frame):
    logger.info('6. update data in Infobip')
    try:
        # when i know how many exits, we´ll try of update information in Infobip
        df2 = utils.uInfobipData(data_frame)
        return df2
    except Exception as e:
        logger.debug('traceback error %s:', str(traceback.format_exc()))
        raise Exception("Update infobip data process: " + str(e))

if __name__ == "__main__":
    main()
