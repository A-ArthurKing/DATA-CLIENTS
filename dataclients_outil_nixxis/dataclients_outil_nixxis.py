import pyodbc
from google.cloud import bigquery
from datetime import datetime, date, time, timedelta
import os
import json
import logging
import traceback
import sys
from typing import Dict, List
import re
import time
import tempfile

from google.api_core.exceptions import NotFound
from google.api_core.exceptions import GoogleAPIError

###############################################################################
# --- Logging Configuration ---
###############################################################################

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)-8s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('sync_bigquery_nixxis.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

###############################################################################
# --- Global Configuration ---
###############################################################################
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\DATA_PROJECT\AGENT_TRACKING\OUTIL_NIXXIS\data-project-438313-8e99289157d6.json'

project_id = 'data-project-438313'
dataset_id = 'dataproject_agents_tracking'
REPORTS_BASE_DIR = r'C:\DATA_PROJECT\AGENT_TRACKING\OUTIL_NIXXIS\reports'

sqlserver_config = {    
    'driver': '{SQL Server}',
    'server': '10.67.0.218',
    'database': 'bp127_ContactRouteReport',
    'uid': 'MCC_RO1',
    'pwd': 'ap4t4eUw'
}

TABLES_CONFIG = {
    'tracking_outil_nixxis': {
        'sqlserver_from_clause': '[bp127_ContactRouteReport]..AgentStates Ast INNER JOIN [bp127_admin]..Agents ad ON Ast.AgentId = ad.Id INNER JOIN [bp127_ContactRouteReport]..DIT_AgentState Ags ON Ast.StateId = Ags.Id LEFT JOIN [bp127_admin]..Pauses p ON Ast.Data = p.Id',
        'sqlserver_group_by_clause': '''
            FORMAT(Ast.LocalDateTime, 'yyyy-MM-dd'),
            FORMAT(Ast.LocalDateTime, 'yyyy-MM-dd HH:mm:ss'),
            ad.Account,
            ad.SupGroupKey,
            CONCAT(ad.FirstName, ' ', ad.LastName),
            Ags.Description,
            p.Description
        ''',
        'sqlserver_filter_clause': "WHERE LocalDateTime >= DATEADD(day, -1, GETDATE())",
        'sqlserver_columns_map': {
            'DateJour': "FORMAT(Ast.LocalDateTime, 'yyyy-MM-dd')",
            'HeureRetrait': "FORMAT(Ast.LocalDateTime, 'yyyy-MM-dd HH:mm:ss')",
            'Account': 'ad.Account',
            'SupGroupKey': 'ad.SupGroupKey',
            'NomPrenom': "CONCAT(ad.FirstName, ' ', ad.LastName)",
            'Retrait': 'Ags.Description',
            'PauseType': 'p.Description',
            'Duree': "FORMAT(DATEADD(SECOND, SUM(Ast.Duration), 0), 'HH:mm:ss')"
        },
        'final_bq_schema': {
            'IdUnique': 'STRING',
            'DateJour': 'DATE',
            'HeureRetrait': 'DATETIME',
            'Account': 'STRING',
            'SupGroupKey': 'STRING',
            'NomPrenom': 'STRING',
            'Retrait': 'STRING',
            'PauseType': 'STRING',
            # La colonne à risque 'Duree' est maintenant en STRING
            'Duree': 'STRING',
        }
    }
}

sync_results = {}

###############################################################################
# --- Utility Functions ---
###############################################################################

def create_final_table_if_not_exists(client: bigquery.Client, table_name: str, schema_dict: Dict[str, str]):
    table_ref_str = f"{project_id}.{dataset_id}.{table_name}"
    try:
        client.get_table(table_ref_str)
        logger.info(f"🟢 Table '{table_name}' existe.")
    except NotFound:
        logger.info(f"🛠️ Table '{table_name}' non trouvée. Création...")
        try:
            schema = [bigquery.SchemaField(name, type) for name, type in schema_dict.items()]
            schema.append(bigquery.SchemaField("_synced_at", "DATETIME", mode="REQUIRED"))
            table = bigquery.Table(table_ref_str, schema=schema)
            client.create_table(table)
            logger.info(f"✅ Table '{table_name}' créée.")
        except Exception as e:
            logger.error(f"❌ Échec de création de la table '{table_name}': {e}", exc_info=True)
            raise

def load_data_via_batch_job(client: bigquery.Client, table_name: str, data_to_load: List[Dict]):
    if not data_to_load:
        logger.info(f"Aucune donnée à charger pour la table {table_name}.")
        return 0

    table_ref_str = f"{project_id}.{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, encoding='utf-8', suffix='.json') as temp_file:
        temp_file_path = temp_file.name
        try:
            for row in data_to_load:
                temp_file.write(json.dumps(row, default=str) + '\n')
            temp_file.close()
            with open(temp_file_path, "rb") as source_file:
                job = client.load_table_from_file(source_file, table_ref_str, job_config=job_config)
                logger.info(f"🚀 Job de chargement BigQuery démarré pour '{table_name}': {job.job_id}")
                job.result(timeout=900)
                logger.info(f"✅ Chargement terminé. Lignes chargées: {job.output_rows}")
                return job.output_rows
        finally:
            os.remove(temp_file_path)

class DBConnection:
    def __enter__(self):
        conn_str = ';'.join([f"{k}={v}" for k, v in sqlserver_config.items()])
        try:
            self.conn = pyodbc.connect(conn_str)
            logger.info("✅ Connexion SQL Server établie.")
            return self.conn
        except pyodbc.Error as err:
            logger.critical(f"❌ ERREUR CONNEXION SQL Server: {err}")
            raise SystemExit(1)
    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'conn'):
            self.conn.close()

def save_sync_report(sync_results_data: Dict):
    report_dir = os.path.join(REPORTS_BASE_DIR, datetime.now().strftime("%Y-%m"), datetime.now().strftime("%d"))
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, f"sync_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(sync_results_data, f, indent=2, ensure_ascii=False, default=str)
    logger.info(f"📝 Rapport enregistré: {report_path}")

###############################################################################
# --- Main Sync Logic ---
###############################################################################
def transfer_table(table_name: str, config: Dict, sync_results_ref: Dict):
    start_time_process = datetime.now()
    table_result = {
        'table_name': table_name, 'start_time': start_time_process.isoformat(), 'end_time': None, 'status': 'pending',
        'stats': {'source_rows_read': 0, 'rows_loaded': 0}, 'error': None
    }
    logger.info(f"\n{'='*50}\n🔄 DEBUT TRAITEMENT {table_name.upper()} ({start_time_process})\n{'='*50}")

    try:
        sqlserver_columns_map = config['sqlserver_columns_map']
        final_bq_schema = config['final_bq_schema']
        
        sqlserver_from_clause = config['sqlserver_from_clause']
        sqlserver_group_by_clause = config.get('sqlserver_group_by_clause', '')
        sqlserver_filter_clause = config.get('sqlserver_filter_clause', '')
        sqlserver_select_columns = [f"{expr} AS [{alias}]" for alias, expr in sqlserver_columns_map.items()]
        
        sqlserver_query = f"SELECT {', '.join(sqlserver_select_columns)} FROM {sqlserver_from_clause} {sqlserver_filter_clause} GROUP BY {sqlserver_group_by_clause}"
        sqlserver_query = re.sub(r'\s+', ' ', sqlserver_query).strip()

        bq_client = bigquery.Client()
        create_final_table_if_not_exists(bq_client, table_name, final_bq_schema)

        with DBConnection() as sqlserver_conn:
            sqlserver_cursor = sqlserver_conn.cursor()
            logger.info(f"Query SQL Server pour {table_name}: {sqlserver_query}")
            sqlserver_cursor.execute(sqlserver_query)
            
            column_names = [column[0] for column in sqlserver_cursor.description]
            sqlserver_data = [dict(zip(column_names, row)) for row in sqlserver_cursor.fetchall()]
            
            table_result['stats']['source_rows_read'] = len(sqlserver_data)
            logger.info(f"🗃️ {len(sqlserver_data)} lignes lues de SQL Server.")

        if not sqlserver_data:
            table_result['status'] = 'success'
            sync_results_ref['tables'][table_name] = table_result
            return

        rows_to_load = []
        current_time_iso = datetime.now().isoformat()

        for row in sqlserver_data:
            prepared_row = {}
            # Génération de la clé unique
            unique_id_parts = [
                str(row.get('DateJour', '')),
                str(row.get('HeureRetrait', '')),
                str(row.get('Account', '')),
                str(row.get('NomPrenom', '')),
                str(row.get('Retrait', '')),
                str(row.get('PauseType', ''))
            ]
            prepared_row['IdUnique'] = '_'.join(part.replace(' ', '_') for part in unique_id_parts)

            for col_name, bq_type in final_bq_schema.items():
                if col_name == 'IdUnique':
                    continue
                
                value = row.get(col_name)
                
                if value is None:
                    prepared_row[col_name] = None
                    continue
                
                try:
                    if bq_type == 'DATETIME' and isinstance(value, (datetime, str)):
                        # pyodbc peut retourner des str ou des objets datetime
                        if isinstance(value, str):
                            value = datetime.fromisoformat(value)
                        prepared_row[col_name] = value.strftime('%Y-%m-%d %H:%M:%S.%f')
                    elif bq_type == 'DATE' and isinstance(value, (date, datetime, str)):
                        if isinstance(value, str):
                            value = date.fromisoformat(value)
                        prepared_row[col_name] = value.strftime('%Y-%m-%d')
                    else:
                        prepared_row[col_name] = str(value)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Impossible de formater la valeur '{value}' pour la colonne '{col_name}'. Utilisation de la valeur en tant que chaîne. Erreur: {e}")
                    prepared_row[col_name] = str(value)

            prepared_row["_synced_at"] = current_time_iso
            rows_to_load.append(prepared_row)
        
        inserted_count = load_data_via_batch_job(bq_client, table_name, rows_to_load)
        table_result['stats']['rows_loaded'] = inserted_count
        table_result['status'] = 'success'

    except Exception as e:
        logger.error(f"❌ ERREUR irrécupérable pour {table_name}: {e}", exc_info=True)
        table_result.update({'status': 'failed', 'error': str(e)})
    finally:
        end_time_process = datetime.now()
        table_result['end_time'] = end_time_process.isoformat()
        sync_results_ref['tables'][table_name] = table_result
        if table_result['status'] == 'failed':
             sync_results_ref['failed_tables'] += 1
             sync_results_ref['failed_table_names'].append(table_name)

###############################################################################
# --- Main Function ---
###############################################################################

def main():
    sync_results = {
        'start_time': datetime.now().isoformat(),'end_time': None,'total_tables': len(TABLES_CONFIG),
        'success_tables': 0,'failed_tables': 0,'failed_table_names': [],'tables': {}
    }
    exit_code_final = 0
    
    try:
        logger.info(f"\n{'='*50}\n🚀 DÉMARRAGE SYNCHRO\n{'='*50}\n")
        
        for table_name, config in TABLES_CONFIG.items():
            transfer_table(table_name, config, sync_results)

        end_time_script = datetime.now()
        sync_results['end_time'] = end_time_script.isoformat()
        
        successful_tables = [name for name, res in sync_results['tables'].items() if res.get('status') == 'success']
        sync_results['success_tables'] = len(successful_tables)
        sync_results['failed_tables'] = sync_results['total_tables'] - sync_results['success_tables']
        sync_results['failed_table_names'] = [name for name, res in sync_results['tables'].items() if res.get('status') != 'success']

        total_duration = (end_time_script - datetime.fromisoformat(sync_results['start_time'])).total_seconds()
        logger.info(f"\n{'='*50}\n🏁 SYNCHRO TERMINÉE\n⏱️ Durée totale: {total_duration:.2f}s")
        logger.info(f"📊 Succès: {sync_results['success_tables']}/{sync_results['total_tables']}")
        if sync_results['failed_tables'] > 0:
            logger.info(f"❌ Échecs: {sync_results['failed_tables']} ({', '.join(sync_results['failed_table_names'])})")
            exit_code_final = 1
        logger.info("="*50)
    except Exception as e_global:
        logger.critical(f"❌ ERREUR GLOBALE: {e_global}", exc_info=True)
        exit_code_final = 2
    finally:
        if sync_results.get('end_time') is None:
            sync_results['end_time'] = datetime.now().isoformat()
        save_sync_report(sync_results)
        sys.exit(exit_code_final)

if __name__ == "__main__":
    main()