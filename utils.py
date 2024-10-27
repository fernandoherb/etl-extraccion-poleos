from numpy.lib import utils
from numpy.lib.function_base import insert
import pandas as pd
import numpy as np
from pandas import DataFrame
import datetime as dt
import json 
import os.path
from pandas.core.tools.datetimes import to_datetime

import database
import configs

def load_csv_data(path: str, delimiter: str):
    bps_data_frame = pd.read_csv(path, sep=delimiter, header=0, engine='python')
    return bps_data_frame

def save_text_data(text_data, filename: str):
    if( text_data == None):
        return
    with open(filename, "w") as text_file:
        text_file.write(text_data)

def construct_insert_query(entity: str, fields: dict, data: dict):
    if len(data) == 0:
        return
    fields = ', '.join([*fields])
    inserts = '\n'
    inserts += f' INSERT INTO {entity} \n  ({fields}) \n VALUES\n'
    index = 0
    for index in range(0, len(data)):
        row = data[index]
        values = ''
        for value in [*row.values()]:
            if str(value) == 'nan' or str(value) == '<NA>' or str(value) == 'None':
                values += "NULL, "
            else:
                values += "'" + str(value) + "', "

        values = values.rstrip(', ')
        inserts += f'  ({values}),\n'
    inserts = inserts.rstrip(',\n')
    inserts += ';\n'
    #inserts += '\n RETURNING ID, bp;\n'
    return inserts

def construct_update_query(entity: str, data: dict, id:dict):
    if len(data) == 0:
        return
    updates = 'BEGIN;\n'
    for index in range(0, len(data)):
        updates += f' UPDATE  {entity} SET '
        values = ''
        row = data[index]
        k = 0
        for key in [*row.keys()]:
            updates += key + " = "            
            i = 0
            for value in [*row.values()]:
                if i == k:
                    values = ''
                    if str(value) == 'nan' or str(value) == '<NA>' or str(value) == 'None':
                        values += "NULL, "
                    else:
                        values += "'" + str(value) + "', "
                    updates += values 
                i = i + 1
            k = k + 1
        updates = updates.rstrip(', ')
        updates += " where id = '"+id[index]+"'; \n"
    updates += 'COMMIT;'
    #print(updates)
    return updates

#### Función para generar querys de actualización
def v_dos_construct_update_query(entity: str, data: dict, id: dict):
    if len(data) == 0:
        return
    
    updates = 'BEGIN;\n'
    for index, row in enumerate(data):
        updates += f'UPDATE {entity} SET '
        
        set_statements = []
        for key, value in row.items():
            if str(value) in ['nan', '<NA>', 'None']:
                set_statements.append(f"{key} = NULL")
            else:
                set_statements.append(f"{key} = '{value}'")
        
        updates += ", ".join(set_statements)
        
        updates += f" WHERE nodo_id = '{id[index]}'; \n"
    
    updates += 'COMMIT;'
    #print(updates)
    return updates

def construct_delete_query(entity, leads_frame):
    ## Construcción de delete para tablas
    query =  'BEGIN;\n DELETE FROM ' + entity + ' WHERE id in (\n'
    values = '\n'
    for row in leads_frame['id']:
        #print(row)
        values += str(row)+', \n'
    values = values.rstrip(', \n')
    query += values + '\n);\n COMMIT;'
    return query

def leads_lead_preprocessing(leads_frame: DataFrame, query):
    
    #Truncante table Leads in CompartfonBI

    #leads_frame = database.execute_select_query_pandas(database.query_select_lead())
    print('Cantidad de Portablilidades::: ' + str(len(leads_frame)))
    #leads_frame['EsMaxCom'] = leads_frame['EsMaxCom'].astype(int)
    print(leads_frame.info())
    
    # Rename columns and apply filters to 'Base exclientes'
    leads_frame = leads_frame.replace(r'^\s*$', np.nan, regex=True)
    
    leads_frame['status'] = leads_frame['status'].fillna(0).astype(int)
    leads_frame['celular_contacto'] = leads_frame['celular_contacto'].fillna(0).astype(int)
    leads_frame['num_alt_contacto'] = leads_frame['num_alt_contacto'].fillna(0).astype(int)
    leads_frame['cp'] = leads_frame['cp'].fillna(0).astype(int)
    leads_frame['telefono_entrega_sim_1'] = leads_frame['telefono_entrega_sim_1'].fillna(0).astype(int)
    leads_frame['telefono_entrega_sim_2'] = leads_frame['telefono_entrega_sim_2'].fillna(0).astype(int)

    leads_frame['portabilidad_dn_actual'] = leads_frame['portabilidad_dn_actual'].fillna(0).astype(int)
    leads_frame['portabilidad_dn_temporal'] = leads_frame['portabilidad_dn_temporal'].fillna(0).astype(int)
    leads_frame['portabilidad_nip_portabilidad'] = leads_frame['portabilidad_nip_portabilidad'].fillna(0).astype(int)
    leads_frame['num_nomina'] = leads_frame['num_nomina'].fillna(0).astype(int)

    #leads_frame = leads_frame.head(2000)
#    leads_frame.fillna(np.)
    print(leads_frame.head(5))

    return leads_frame

def databases_compare():

    rev_bp = utils.load_csv_data(configs.ROOT_DIR + configs.INPUT_FILES_DIR + 'leads_dash.csv', '|')
    rev_bp = rev_bp.fillna('SIN INFORMACION')
    prepersons_frame = prepersons_frame.fillna('WITHOUT_INFORMATION')

    prepersons_frame['BP'] = prepersons_frame['num_cliente_compartamos']
    prepersons_frame['revision'] = 'SI'

    rev_bp = pd.merge(rev_bp, prepersons_frame.loc[:,['revision', 'num_cliente_compartamos','BP']], how='left', on= ['BP'])

    #rev_bp['check'] = prepersons_frame.BPS.isin(rev_bp.BP)
    rev_bp.to_csv(configs.ROOT_DIR + configs.INPUT_FILES_DIR + 'rev_dash.csv', header=True, index=False)
    print(rev_bp['revision'].value_counts())

def getToday():
    return str(dt.date.today().strftime('%Y-%m-%d'))