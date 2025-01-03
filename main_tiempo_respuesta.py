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
        tiempo_respuesta  = extract_process()
        fin = time.time()
        logger.info('fin %s',str(fin))
        logger.info('Tiempo de ejecucion: %s',str(fin-inicio))
        logger.info('Pruebas de conexión y extracción tiempo_respuesta: %s',str(len(tiempo_respuesta)))
        ## Procesando tabla de leads_lead
        logger.info(tiempo_respuesta.head(1))
        
        fields = [*database.tbl_detalle_tiempo_respuesta().values()]
        transformed_data = transform_process(tiempo_respuesta, fields)
        
        load_process(transformed_data, 'insert', configs.schema_public+'.'+ configs.detalle_tiempo_respuesta, configs.OUTPUT_ETL_LOAD_TIEMPO_RESPUESTA_QUERY_FILENAME, fields)
        load_process(transformed_data, 'insert', configs.schema_historico+'.'+ configs.detalle_tiempo_respuesta_7d, configs.OUTPUT_ETL_LOAD_TIEMPO_RESPUESTA_7D_QUERY_FILENAME, fields)
        database.execute_delete_postg("DELETE FROM historico.detalle_tiempo_respuesta_cpu_load_7d WHERE fecha_creacion < CURRENT_TIMESTAMP - INTERVAL '24 hours';")      
    
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

        tiempo_respuesta = database.select_data_frame(database.query_select_detalle_tiempo_respuesta_carga_cpu())

        return tiempo_respuesta
    except Exception as e:
        logger.debug('traceback error %s:', str(traceback.format_exc()))
        raise Exception("extract data process: " + str(e))

def transform_process(data_frame, fields):
    logger.info('2. Transform data')
    try:
        # Apply preperson preprocessing (clean/normalize data)
        data_frame.columns = fields

        # Aplicar la función solo a las columnas A, B y C
        columnas_a_convertir = ['min_response_time', 'max_response_time', 'avg_response_time', 'porcent_loss', 'min_load', 'max_load', 'avg_load_cpu', 'total_memory']
        data_frame[columnas_a_convertir] = data_frame[columnas_a_convertir].applymap(convertir_a_entero)

        # Consulta el catalgo de nodos
        ct_nodos = database.execute_select_query_pandas(database.query_select_ct_nodo())
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

            if 'public' in entitys:
                logger.info('Insert -  se checan e insertan registros con cambios en estatus')
                status_data = data_frame[['nodo_id', 'availability']].drop_duplicates()
                check_and_insert_status(status_data)

            preleads_inserts_query = utils.construct_insert_query(entity=entitys, fields=table_fields, data=data_frame.to_dict(orient='records')) 
            utils.save_text_data(preleads_inserts_query, configs.ROOT_DIR + configs.OUTPUT_FILES_DIR + name_file + '.txt')

            database.execute_insert_query_postg(preleads_inserts_query)

    except Exception as e:
        logger.debug('traceback error %s:', str(traceback.format_exc()))
        raise Exception("load preleads data process: " + str(e))
    finally:
        logger.info('\t Prepersons  loaded: %s', str(len(data_frame)))
        return data_frame

def check_and_insert_status(data_frame):
    """
    Verifica los cambios de estado de nodos y realiza inserciones masivas si hay cambios.
    """
    try:
        logger.info('**************************************')
        logger.info('Verificando cambios de estado en nodos')

        # Cargar el estado actual de todos los nodos en un DataFrame
        query = """
            SELECT DISTINCT ON (nodo_id) nodo_id, estatus, fecha_movimiento
            FROM public.detalle_estatus_nodos
            ORDER BY nodo_id, fecha_movimiento DESC;
        """
        current_status_df = database.execute_select_query_pandas(query)

        # Asegurar que los tipos de nodo_id sean consistentes
        data_frame['nodo_id'] = data_frame['nodo_id'].astype(int)
        current_status_df['nodo_id'] = current_status_df['nodo_id'].astype(int)

        # Convertir columnas de estatus y availability a enteros
        data_frame['availability'] = pd.to_numeric(data_frame['availability'], errors='coerce').fillna(0).astype(int)
        current_status_df['estatus'] = pd.to_numeric(current_status_df['estatus'], errors='coerce').fillna(0).astype(int)

        if current_status_df.empty:
            logger.info("No existen registros previos. Insertando todos los nuevos registros.")
            # Si no hay registros previos, inserta todo el data_frame directamente
            to_insert = data_frame.copy()
        else:
            # Comparar estados actuales con los nuevos
            merged_df = data_frame.merge(
                current_status_df,
                on='nodo_id',
                how='left',
                suffixes=('_new', '_current')
            )

            # Filtrar solo los nodos con cambios en el estatus
            to_insert = merged_df[
                (merged_df['availability'] != merged_df['estatus']) & merged_df['estatus'].notna()
            ][['nodo_id', 'availability']]

            # También incluir nodos nuevos (donde el estatus actual es nulo)
            new_nodes = merged_df[
                merged_df['estatus'].isna()
            ][['nodo_id', 'availability']]

            to_insert = pd.concat([to_insert, new_nodes], ignore_index=True)

            logger.info(f"Registros a insertar o actualizar: {len(to_insert)}")

        if not to_insert.empty:
            # Añadir columnas adicionales necesarias para la inserción
            to_insert['fecha_movimiento'] = pd.Timestamp.now()

            # Renombrar columnas para coincidir con la tabla
            to_insert = to_insert.rename(columns={
                'availability': 'estatus'
            })

            # Construir e insertar los datos
            insert_query = utils.construct_insert_query(
                entity='public.detalle_estatus_nodos',
                fields=['nodo_id', 'estatus', 'fecha_movimiento'],
                data=to_insert.to_dict(orient='records')
            )
            database.execute_insert_query_postg(insert_query)
            logger.info(f"Se insertaron {len(to_insert)} registros con cambios.")
        else:
            logger.info("No hay cambios en los estados de los nodos.")

        # Eliminar registros antiguos
        database.execute_delete_postg("DELETE FROM public.detalle_estatus_nodos WHERE fecha_movimiento < CURRENT_TIMESTAMP - INTERVAL '24 hours';")      

        logger.info('**************************************')

    except Exception as e:
        logger.error(f"Error al verificar o insertar estados: {str(e)}")
        raise

# Función para convertir valores en enteros y manejar NaN como None
def convertir_a_entero(valor):
    if pd.notna(valor):
        return int(valor)
    else:
        return pd.NA

if __name__ == "__main__":
    main()
