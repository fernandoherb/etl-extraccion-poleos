import os
import psycopg2
import psycopg2.extras
#import _scproxy
import pymssql
# import pyodbc
import mysql.connector as sql
import pandas as pd
from pandas import DataFrame
from dotenv import load_dotenv
load_dotenv()

def get_credentials_prod():
    return {
        'host': os.getenv("HOST_DB_PROD"),
        'database': os.getenv("NAME_DB_PROD"),
        'user': os.getenv("USER_DB_PROD"),
        'password': os.getenv("PWD_DB_PROD")
    }

def get_credentials_h():
    return {
        'host': os.getenv("HOST_DB_H"),
        'database': os.getenv("NAME_DB_H"),
        'user': os.getenv("USER_DB_H"),
        'port': os.getenv("PORT_DB_H"),
        'password': os.getenv("PWD_DB_H")
    }

def get_credentials():
    return {
        'host': os.getenv("HOST_DB"),
        'database': os.getenv("NAME_DB"),
        'user': os.getenv("USER_DB"),
        'port': os.getenv("PORT_DB"),
        'password': os.getenv("PWD_DB")
    }

def sql_server_conection_prod():
    db_credentials_prod = get_credentials_prod()
    try:
        conn = pymssql.connect(db_credentials_prod.get('host'), db_credentials_prod.get('user'), db_credentials_prod.get('password'), db_credentials_prod.get('database'))
        print('Conexion exitosa production!!!!!!')
    # OK! conexión exitosa
    except Exception as e:
    # Atrapar error
        print("Ocurrió un error al conectar a SQL Server: ", e)
    return conn

## Excute select to database SQL Server
def select_data_frame(query):
    df = pd.read_sql_query(query, con=sql_server_conection_prod())
    return df


## Connection to database Postgres
def get_connection_postg():
    try:
        db_credentials = get_credentials()
        conn = psycopg2.connect(**db_credentials)
    except psycopg2.Error as e:
        # Captura y maneja las excepciones de psycopg2
        print("Error de PostgreSQL:", e)
    return conn

## Excute select to database Postgres
def execute_select_query_pandas(query: str):
    conn = get_connection_postg()
    results = pd.read_sql(query, conn)
    conn.close()
    return results

## Connection to database Postgres
def get_connection_postg_helix():
    try:
        db_credentials = get_credentials_h()
        conn = psycopg2.connect(**db_credentials)
    except psycopg2.Error as e:
        # Captura y maneja las excepciones de psycopg2
        print("Error de PostgreSQL:", e)
    return conn

## Excute select to database Postgres
def execute_select_query_pandas_helix(query: str):
    conn = get_connection_postg_helix()
    results = pd.read_sql(query, conn)
    conn.close()
    return results

## Insert to database Postgres
def execute_insert_query_postg(query: str):
    conn = get_connection_postg()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    rest = cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()

def query_select_detalle_tickets_helix():
    return """
        /* Nuevo reporte solicitado */
        select submitter "usuarioreporta",
        incident_number "numreportesoc",
        incident_number "numreportebestel",
        responsabilidad_del_incidente_ "responsable",
        assignee "ingasignado",  
        regexp_replace(detailed_decription, '[^a-zA-Z0-9: ]', '', 'g') "detalle",
        resolution "actividad",
        to_char(to_timestamp(submit_date),'YYYY-MM-DD HH24:MI:SS') "fc_fechacreacion",
        status,
        priority "prioridad",
        impact "severidad",
        resolution_category_tier_2 "descripcion",
        CASE  
            WHEN (tiempo_de_afectacion_minutos) > 0 then 1 else 0	
            end as boafectacion,
        case
            WHEN bst2_tiempo_afectación_concil > 0 then 1 else 0
            end as boatribuible,
        1 as ticketstatus,
        case
            when resolution_category_tier_2 = 'Servicios' then 1
            when resolution_category_tier_2 = 'Física' then 2
            when resolution_category_tier_2 = 'Lógica' then 3
            when resolution_category_tier_2 = 'Revision' then 4
            else 5
            end as tipofalla_id,  
        1 as tiporrequerimiento_id,
        CASE  WHEN resolution_category='Causa de falla' then 1 else 2 end as tickettipo_id,
        CASE  
            when impact = 3000 then 3
            when impact = 2000 then 2
            when impact = 1000 then 1
            else 1
        END
        as criticidadticket_id,
        CASE  
            WHEN position('CID: ID' IN detailed_decription) > 0 THEN
                        substring(
                        regexp_replace(
                        substring(
                        detailed_decription,
                        position('CID: ID' IN detailed_decription) + length('CID: ID'),12), '[^0-9]', '', 'g')FROM 1 FOR 6)
            
            WHEN position('ID' IN detailed_decription) > 0 THEN
                substring(
                regexp_replace(
                        substring(
                        detailed_decription,
                        position('ID' IN detailed_decription) + length('ID'),12), '[^0-9]', '', 'g')FROM 1 FOR 3)
                ELSE     
                    substring(regexp_replace(detailed_decription, '[^0-9]', '', 'g') FROM 1 FOR 3)            
                END AS sitio_id,
                
                --substring(regexp_replace(detailed_decription, '[^0-9]/d+', '', '') FROM 1 FOR 3) as id_3,
                --substring(regexp_replace(detailed_decription, '[^0-9]', '', 'g') FROM 1 FOR 3)
        to_char(to_timestamp(submit_date),'YYYY-MM-DD HH24:MI:SS')  "fc_inicioticket",
        to_char(to_timestamp(closed_date),'YYYY-MM-DD HH24:MI:SS')  "fc_finticket",
        case
        WHEN (closed_date-submit_date) > 0 then (closed_date-submit_date)/3600.0
                else 0
                end as duracion,
                
        tiempo_de_afectacion_minutos "tiempo_afectacion_minutos",
        tiempo_afectación_atribuible_ "tiempo_afectacion_atribuible",
        bst2_tiempo_afectación_concil "bst2_tiempo_afectacion_concil"
        
        FROM hpd_help_desk
        WHERE (organization ='SERVICIO DE ADMINISTRACION TRIBUTARIA' OR organization ='SERVICIO DE ADMINISTRACION TRIBUTARIA SAT') AND
        to_timestamp(submit_date) AT TIME ZONE 'America/Mexico_City'  >= CURRENT_TIMESTAMP AT TIME ZONE 'America/Mexico_City' - INTERVAL '1 hour' 
        order by submit_date desc;
        """

def query_select_detalle_trafico_interface():
    return """   
        SELECT itd.InterfaceID, CONVERT(DATETIME, SWITCHOFFSET(itd.Timestamp, '-06:00')) as Timestampp, itd.NodeID, itd.In_Averagebps, itd.In_Minbps, itd.In_Maxbps, itd.In_TotalBytes, itd.In_TotalPkts, itd.In_AvgUniCastPkts, itd.In_MinUniCastPkts, itd.In_MaxUniCastPkts, itd.In_AvgMultiCastPkts, itd.In_MinMultiCastPkts, itd.In_MaxMultiCastPkts, itd.Out_Averagebps, itd.Out_Minbps, itd.Out_Maxbps, itd.Out_TotalBytes, itd.Out_TotalPkts, itd.Out_AvgUniCastPkts, itd.Out_MaxUniCastPkts, itd.Out_MinUniCastPkts, itd.Out_AvgMultiCastPkts, itd.Out_MinMultiCastPkts, itd.Out_MaxMultiCastPkts, itd.Weight
        FROM InterfaceTraffic_CS_Detail itd
        inner join (
            select itcd.NodeID, itcd.InterfaceID , max(itcd.[Timestamp]) as Timestamp
            from InterfaceTraffic_CS_Detail itcd 
            inner join NodesCustomProperties ncp on itcd.NodeID = ncp.NodeID
            where ncp.CustomerName = 'SAT' AND SWITCHOFFSET(itcd.[Timestamp], '-06:00') > DATEADD(SECOND, -360, CURRENT_TIMESTAMP) 
            group by itcd.NodeID, itcd.InterfaceID
        ) as sub on sub.Timestamp = itd.[Timestamp] and sub.NodeID = itd.NodeID and sub.InterfaceID = itd.InterfaceID;
    """

def query_select_detalle_error_interface():
    return """   
        SELECT ncp.NodeID, iecd.[Timestamp] AS fecha_origen,iecd.InterfaceID,iecd.In_Discards, iecd.In_Errors, iecd.Out_Discards, iecd.Out_Errors, iecd.LateCollisions, iecd.CRCAlignErrors,iecd.Weight
        FROM InterfaceErrors_CS_Detail AS iecd
        INNER JOIN NodesCustomProperties ncp ON iecd.NodeID = ncp.NodeID
        INNER JOIN (
            SELECT iitcd.NodeID, iitcd.InterfaceID, MAX(iitcd.[Timestamp]) AS Timestamp
            FROM InterfaceErrors_CS_Detail iitcd
            INNER JOIN NodesCustomProperties ncp ON iitcd.NodeID = ncp.NodeID
            WHERE ncp.CustomerName = 'SAT' 
            AND iitcd.[Timestamp] > DATEADD(SECOND, -360, CURRENT_TIMESTAMP)
            GROUP BY iitcd.NodeID, iitcd.InterfaceID
        ) AS sub ON sub.Timestamp = iecd.[Timestamp] 
        AND sub.NodeID = iecd.NodeID 
        AND sub.InterfaceID = iecd.InterfaceID
        WHERE iecd.[Timestamp] >= DATEADD(SECOND, -150, sub.Timestamp) AND iecd.[Timestamp] <= DATEADD(SECOND, +120, sub.Timestamp);
    """

def query_select_detalle_tiempo_respuesta_carga_cpu():
    return """   
        SELECT ncp.NodeID ,rtcd.[Timestamp] as fecha_origen, ccd.[Timestamp] as fecha_origen_cpu, rtcd.MinResponseTime ,rtcd.MaxResponseTime ,rtcd.AvgResponseTime ,rtcd.PercentLoss ,rtcd.Availability 
        , ccd.MinLoad ,ccd.MinLoad ,ccd.AvgLoad ,ccd.TotalMemory ,ccd.MinMemoryUsed ,ccd.MaxMemoryUsed ,ccd.AvgMemoryUsed ,ccd.PercentMemoryUsed , rtcd.Weight 
        from ResponseTime_CS_Detail as rtcd
        inner join NodesCustomProperties ncp on rtcd.NodeID = ncp.NodeID
        left join CPULoad_CS_Detail ccd on ccd.NodeID = rtcd.NodeID --AND rtcd.[Timestamp] = ccd.[Timestamp] 
        inner join (
            select itcd.NodeID , max(itcd.[Timestamp]) as Timestamp
            from ResponseTime_CS_Detail itcd 
            inner join NodesCustomProperties ncp on itcd.NodeID = ncp.NodeID
            where ncp.CustomerName = 'SAT' AND itcd.[Timestamp] > DATEADD(SECOND, -360, CURRENT_TIMESTAMP) 
            group by itcd.NodeID
        ) as sub on sub.Timestamp = rtcd.[Timestamp] and sub.NodeID = rtcd.NodeID --and sub.InterfaceID = itcd.InterfaceID 
        where ccd.[Timestamp] >= DATEADD(SECOND, -150, rtcd.[Timestamp]) and ccd.[Timestamp] <= DATEADD(SECOND, +120, rtcd.[Timestamp]);
    """

def query_select_detalle_carga_cpu():
    return """   
        SELECT ccd.NodeID ,ccd.[Timestamp] ,ccd.MinLoad ,ccd.MaxLoad ,ccd.AvgLoad ,ccd.TotalMemory ,ccd.MinMemoryUsed ,ccd.MaxMemoryUsed ,ccd.AvgMemoryUsed ,ccd.PercentMemoryUsed ,ccd.Weight  
        FROM CPULoad_CS_Detail ccd  inner join (
            select itcd.NodeID , max(itcd.[Timestamp]) as Timestamp
            from CPULoad_CS_Detail itcd 
            inner join NodesCustomProperties ncp on itcd.NodeID = ncp.NodeID
            where ncp.CustomerName = 'SAT' AND itcd.[Timestamp] > DATEADD(SECOND, -360, CURRENT_TIMESTAMP) 
            group by itcd.NodeID
        ) as sub on sub.Timestamp = ccd.[Timestamp] and sub.NodeID = ccd.NodeID;
    """

def tbl_detalle_tickets_remedy():
    return {
    "usuarioreporta": "usuarioreporta", "numreportesoc": "numreportesoc", "numreportebestel": "numreportebestel", "responsable": "responsable", 
    "ingasignado": "ingasignado", "detalle": "detalle", "actividad": "actividad", "fc_fechacreacion": "fc_fechacreacion", "status": "status", 
    "prioridad": "prioridad", "severidad": "severidad", "descripcion": "descripcion", "boafectacion": "boafectacion", "boatribuible": "boatribuible", 
    "ticketstatus_id": "ticketstatus_id", "tipofalla_id": "tipofalla_id", "tiporequerimiento_id": "tiporequerimiento_id", "tickettipo_id": "tickettipo_id", 
    "criticidadticket_id": "criticidadticket_id", "sitio_id": "sitio_id", "fc_inicioticket": "fc_inicioticket", "fc_finticket": "fc_finticket", 
    "duracion": "duracion", "tiempo_afectacion_minutos": "tiempo_afectacion_minutos", "tiempo_afectacion_atribuible": "tiempo_afectacion_atribuible", 
    "bst2_tiempo_afectacion_concil": "bst2_tiempo_afectacion_concil"
    }

def tbl_detalle_trafico_interface():
    return {
    "interface_id": "interface_id", "fecha_origen": "fecha_origen", "nodo_id": "nodo_id", "in_avg_bps": "in_avg_bps"
    , "in_min_bps": "in_min_bps", "in_max_bps": "in_max_bps", "in_total_bytes": "in_total_bytes", "in_total_pkts": "in_total_pkts"
    , "in_avg_unicast_pkts": "in_avg_unicast_pkts", "in_min_unicast_pkts": "in_min_unicast_pkts"
    , "in_max_unicast_pkts": "in_max_unicast_pkts", "in_avg_multicast_pkts": "in_avg_multicast_pkts"
    , "in_min_multicast_pkts": "in_min_multicast_pkts", "in_max_multicast_pkts": "in_max_multicast_pkts"
    , "out_avg_bps": "out_avg_bps", "out_min_bps": "out_min_bps", "out_max_bps": "out_max_bps", "out_total_bytes": "out_total_bytes"
    , "out_total_pkts": "out_total_pkts", "out_avg_unicast_pkts": "out_avg_unicast_pkts", "out_min_unicast_pkts": "out_min_unicast_pkts"
    , "out_max_unicast_pkts": "out_max_unicast_pkts", "out_avg_multicast_pkts": "out_avg_multicast_pkts"
    , "out_min_multicast_pkts": "out_min_multicast_pkts", "out_max_multicast_pkts": "out_max_multicast_pkts", "weight": "weight"
    }

def tbl_detalle_tiempo_respuesta():
    return {
    "nodo_id": "nodo_id", "fecha_origen": "fecha_origen", "fecha_origen_cpu": "fecha_origen_cpu", "min_response_time": "min_response_time"
    , "max_response_time": "max_response_time", "avg_response_time": "avg_response_time", "porcent_loss": "porcent_loss"
    , "availability": "availability", "min_load": "min_load", "max_load": "max_load", "avg_load_cpu": "avg_load_cpu"
    , "total_memory": "total_memory", "min_memory_used": "min_memory_used", "max_memory_used": "max_memory_used"
    , "avg_memory_used": "avg_memory_used", "porcent_memory_used": "porcent_memory_used", "weight": "weight"
    }

def tbl_detalle_carga_cpu():
    return {
    "nodo_id": "nodo_id", "fecha_origen ": "fecha_origen", "min_load ": "min_load", "max_load ": "max_load", "avg_load_cpu ": "avg_load_cpu"
    , "total_memory ": "total_memory", "min_memory_used ": "min_memory_used", "max_memory_used ": "max_memory_used", "avg_memory_used ": "avg_memory_used"
    , "porcent_memory_used ": "porcent_memory_used", "weight ": "weight"
    }

def tbl_detalle_error_interface():
    return {
    "nodo_id": "nodo_id", "fecha_origen ": "fecha_origen", "interface_id ": "interface_id", "in_discards ": "in_discards", "in_errors ": "in_errors"
    , "out_discards ": "out_discards", "out_errors ": "out_errors", "late_collisions ": "late_collisions", "crca_lign_errors ": "crca_lign_errors"
    , "weight ": "weight"
    }
