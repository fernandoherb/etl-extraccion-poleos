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
        logger.info('Inicio %s', str(inicio))

        # Fecha específica para procesar
        date_to_extract = datetime.date(2024, 12, 3)
        intervals = generate_time_intervals(date_to_extract, interval_minutes=5)

        for start_time, end_time in intervals:
            logger.info(f'Processing interval: {start_time} to {end_time}')
            carga_cpu = extract_process(start_time, end_time)

            if not carga_cpu.empty:
                logger.info(f'Datos extraídos: {len(carga_cpu)} registros.')
                fields = [*database.tbl_detalle_carga_cpu().values()]
                transformed_data = transform_process(carga_cpu, fields)
                load_process(
                    transformed_data, 
                    'insert', 
                    configs.schema_historico + '.' + 'detalle_carga_cpu_historial',
                    'test', 
                    fields
                )
            else:
                logger.info(f'No se encontraron datos para el intervalo {start_time} - {end_time}')

        fin = time.time()
        logger.info('fin %s', str(fin))
        logger.info('Tiempo de ejecucion: %s', str(fin - inicio))

    except Exception as e:
        logger.error("Error(s) ocurred in %s", str(e))
        logger.info('Done ETL-Preleads process with errors')
    else:
        logger.info('Done ETL-Preleads process successfully')


    
def extract_process(start_time, end_time):
    logger.info(f'1. Extract data from {start_time} to {end_time}')
    try:
        # Modificar la consulta para que tome un rango explícito
        query = f"""
        SELECT ccd.NodeID, ccd.[Timestamp], ccd.MinLoad, ccd.MaxLoad, ccd.AvgLoad, ccd.TotalMemory, 
               ccd.MinMemoryUsed, ccd.MaxMemoryUsed, ccd.AvgMemoryUsed, ccd.PercentMemoryUsed, ccd.Weight  
        FROM CPULoad_CS_Detail ccd
        INNER JOIN NodesCustomProperties ncp ON ccd.NodeID = ncp.NodeID
        WHERE ncp.CustomerName = 'SAT' 
          AND ccd.[Timestamp] >= '{start_time}' 
          AND ccd.[Timestamp] < '{end_time}'
        """
        carga_cpu = database.select_data_frame(query)
        return carga_cpu
    except Exception as e:
        logger.debug('traceback error %s:', str(traceback.format_exc()))
        raise Exception("extract data process: " + str(e))

def generate_time_intervals(date_to_extract, interval_minutes=5):
    start_time = datetime.datetime.combine(date_to_extract, datetime.time.min)
    #end_time = datetime.datetime.combine(date_to_extract, datetime.time.max)
    end_time = datetime.datetime.combine(date_to_extract, datetime.time(12, 20))

    intervals = []
    while start_time < end_time:
        next_time = start_time + datetime.timedelta(minutes=interval_minutes)
        intervals.append((start_time, next_time))
        start_time = next_time
    return intervals

def transform_process(data_frame, fields):
    logger.info('2. Transform data')
    try:
        # Apply preperson preprocessing (clean/normalize data)
        #data_frame = utils.leads_lead_preprocessing(data_frame)
        data_frame.columns = fields

        # Aplicar la función solo a las columnas A, B y C
        columnas_a_convertir = ['min_load', 'max_load', 'avg_load_cpu', 'total_memory']
        data_frame[columnas_a_convertir] = data_frame[columnas_a_convertir].applymap(convertir_a_entero)

        # Consulta el catalgo de nodos
        ct_nodos = database.execute_select_query_pandas(database.query_select_ct_nodo_cpu())
        # Encuentra los nodos faltantes usando `merge`
        nodos_faltantes = ct_nodos[~ct_nodos['nodo_id'].isin(data_frame['nodo_id'])]
        
        logger.info('Nodos faltantes:::::: '+str(len(nodos_faltantes)))
                
        # Concatenamos `df_poleos` con `nodos_faltantes`
        data_frame = pd.concat([data_frame, nodos_faltantes], ignore_index=True)

        logger.info('Cuantos nodos modificará:::::: '+str(len(data_frame)))

        return data_frame
    except Exception as e:
        logger.debug('traceback error %s:', str(traceback.format_exc()))
        raise Exception("transform data process: " + str(e))

def load_process(data_frame, query, entitys, name_file,table_fields):
    logger.info('3. Load data')
    # Construct preleads inserts query, save query in text file and execute it
    try:
        if query == 'update': ## Se tiene que actualizar en el principal
            ids = data_frame['nodo_id']
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
