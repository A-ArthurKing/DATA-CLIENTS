import pyodbc
from google.cloud import bigquery
from datetime import datetime, date, time
import os
import json
import logging
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
        logging.FileHandler('sync_bigquery_hermes.log', encoding='utf-8'), 
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

###############################################################################
# --- Global Configuration ---
###############################################################################
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\DATA_PROJECT\AGENT_TRACKING\OUTIL_HERMES\data-project-438313-8e99289157d6.json'
project_id = 'data-project-438313'
dataset_id = 'dataproject_agents_tracking'
REPORTS_BASE_DIR = r'C:\DATA_PROJECT\AGENT_TRACKING\OUTIL_HERMES\reports'

sqlserver_config = {
    'driver': '{SQL Server}',
    'server': '192.168.1.241',
    'database': 'ondata',
    'uid': 'sa',
    'pwd': 'sa'
}

TABLES_CONFIG = {
    'tracking_outil_hermes': { 
        'sqlserver_from_clause': '''
            dbo.ODActions 
            INNER JOIN dbo.States ON dbo.ODActions.State = dbo.States.StateID AND dbo.States.StateType = dbo.ODActions.OriginatorType 
            INNER JOIN WebAdmin.dbo.SuperviseGroupAgent ON WebAdmin.dbo.SuperviseGroupAgent.AgentId = dbo.ODActions.OriginatorID 
            INNER JOIN WebAdmin.dbo.SuperviseGroup ON WebAdmin.dbo.SuperviseGroupAgent.SuperviseGroupOid = WebAdmin.dbo.SuperviseGroup.Oid 
            INNER JOIN REPORT_AUTO.dbo.SIRH_LINK_SIRH_HERMES ON REPORT_AUTO.dbo.SIRH_LINK_SIRH_HERMES.LogHermes = dbo.ODActions.OriginatorID
        ''',
        'sqlserver_filter_clause': "WHERE ODActions.ActionUniversalTimeString >= FORMAT(DATEADD(day, -1, GETDATE()), 'yyyyMMddHHmmssfff')",
        
        ## CORRECTION 1: La requête SQL est simplifiée pour ne renvoyer que du texte brut, sans TRY_CONVERT
        'sqlserver_columns_map': {
            'DateRaw': "SUBSTRING(dbo.ODActions.ActionUniversalTimeString, 1, 8)",
            'HeureRaw': "SUBSTRING(dbo.ODActions.ActionUniversalTimeString, 9, 6)",
            'Login': 'ODActions.OriginatorID',
            'Matricule': 'REPORT_AUTO.dbo.SIRH_LINK_SIRH_HERMES.mat',
            'Agent': "CONCAT(REPORT_AUTO.dbo.SIRH_LINK_SIRH_HERMES.firstname , ' ', REPORT_AUTO.dbo.SIRH_LINK_SIRH_HERMES.lastname)",
            'Projet': 'WebAdmin.dbo.SuperviseGroup.Description',
            'Description': 'States.Description',
            'Temps': '(CAST(ODActions.Duration AS FLOAT)) / 1000.0' # Durée en secondes
        },
        'final_bq_schema': {
            'IdUnique': 'STRING',
            'Date': 'DATE',
            'Heure': 'TIME',
            'Login': 'STRING',
            'Matricule': 'STRING',
            'Agent': 'STRING',
            'Projet': 'STRING',
            'Description': 'STRING',
            'Temps': 'FLOAT64'
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

def load_data_via_batch_job(client: bigquery.Client, table_name: str, data_to_load: List[Dict], schema: List[bigquery.SchemaField]):
    if not data_to_load:
        logger.info(f"Aucune donnée à charger pour la table {table_name}.")
        return 0

    table_ref_str = f"{project_id}.{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
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
            self.conn = pyodbc.connect(conn_str, autocommit=True)
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
    logger.info(f"\n{'='*50}\n🔄 DEBUT TRAITEMENT {table_name.upper()}\n{'='*50}")

    try:
        sqlserver_columns_map = config['sqlserver_columns_map']
        final_bq_schema_dict = config['final_bq_schema']
        
        sqlserver_select_columns = [f"{expr} AS [{alias}]" for alias, expr in sqlserver_columns_map.items()]
        sqlserver_query = f"SELECT {', '.join(sqlserver_select_columns)} FROM {config['sqlserver_from_clause']} {config.get('sqlserver_filter_clause', '')}"
        
        bq_client = bigquery.Client()
        create_final_table_if_not_exists(bq_client, table_name, final_bq_schema_dict)
        final_bq_schema = [bigquery.SchemaField(name, type) for name, type in final_bq_schema_dict.items()]
        final_bq_schema.append(bigquery.SchemaField("_synced_at", "DATETIME", mode="REQUIRED"))

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
            
            # Copier les valeurs qui ne nécessitent pas de transformation
            for col in ['Login', 'Matricule', 'Agent', 'Projet', 'Description', 'Temps']:
                prepared_row[col] = row.get(col)

            # ## CORRECTION 2: Parser les dates et heures brutes en Python avec gestion d'erreur
            date_raw = row.get('DateRaw')
            heure_raw = row.get('HeureRaw')

            try:
                prepared_row['Date'] = datetime.strptime(date_raw, '%Y%m%d').strftime('%Y-%m-%d') if date_raw else None
            except (ValueError, TypeError):
                logger.warning(f"Format de date brut invalide '{date_raw}'. La valeur sera NULL.")
                prepared_row['Date'] = None
            
            try:
                if heure_raw and len(heure_raw) == 6:
                    time_obj = datetime.strptime(heure_raw, '%H%M%S').time()
                    prepared_row['Heure'] = time_obj.isoformat()
                else:
                    prepared_row['Heure'] = None
            except (ValueError, TypeError):
                logger.warning(f"Format d'heure brut invalide '{heure_raw}'. La valeur sera NULL.")
                prepared_row['Heure'] = None

            # Génération de la clé unique
            unique_id_parts = [str(prepared_row.get(k, '')) for k in ['Date', 'Heure', 'Login']]
            prepared_row['IdUnique'] = '_'.join(part.replace(' ', '_') for part in unique_id_parts)

            prepared_row["_synced_at"] = current_time_iso
            rows_to_load.append(prepared_row)
        
        inserted_count = load_data_via_batch_job(bq_client, table_name, rows_to_load, final_bq_schema)
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