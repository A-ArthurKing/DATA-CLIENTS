import mysql.connector
from google.cloud import bigquery
from datetime import datetime, date, time, timedelta
import os
import json
import logging
import traceback
import sys
from typing import Dict, List, Tuple
import re
import time
import tempfile

from google.api_core.exceptions import NotFound
from google.api_core.exceptions import GoogleAPIError

################################################################################
# --- Logging Configuration ---
################################################################################
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)-8s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('sync_gestion_paie_heures_corrigees.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

################################################################################
# --- Global Configuration ---
################################################################################
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\DATA_PROJECT\AGENT_TRACKING\OUTIL_GESTION_PAIE\data-project-438313-8e99289157d6.json'

project_id = 'data-project-438313'
dataset_id = 'dataproject_agents_tracking'
REPORTS_BASE_DIR = r'C:\DATA_PROJECT\AGENT_TRACKING\OUTIL_GESTION_PAIE\reports'

mysql_config = {
    'user': 'gestion_paie', 'password': 'Mcc$2012', 'host': '192.168.1.17',
    'database': 'gestionpaie', 'connect_timeout': 300,
    'raise_on_warnings': True, 'use_pure': True
}

TABLES_CONFIG = {
    'tracking_outil_gestion_paie_heures_corrigees': {
        'mysql_from_clause': 'heures_corrigees',
        'mysql_columns_map': {
            'uuid': 'uuid', 'matricule': 'matricule', 'date': 'date', 'heure_ht': 'heure_ht', 
            'heure_hp': 'heure_hp', 'projet': 'projet', 'updated_at': 'updated_at',
            'cloture_sup': 'cloture_sup', 'cloture_rh': 'cloture_rh', 'LastName': 'LastName', 
            'FirstName': 'FirstName', 'Equipe': 'Equipe', 'heure_hc': 'heure_hc',
            'heure_hf': 'heure_hf', 'heure_total': 'heure_total', 'heure_ecart': 'heure_ecart', 
            'TYPE_CONGE': 'TYPE_CONGE', 'TYPE_FORMATION': 'TYPE_FORMATION'
        },
        # Schéma de la table BigQuery. NOTE: Toutes les colonnes de temps sont en STRING.
        'final_bq_schema': {
            'uuid': 'STRING',
            'matricule': 'STRING',
            'date': 'DATE',
            'projet': 'STRING',
            'updated_at': 'DATETIME',
            'cloture_sup': 'STRING',
            'cloture_rh': 'STRING',
            'LastName': 'STRING',
            'FirstName': 'STRING',
            'Equipe': 'STRING',
            'TYPE_CONGE': 'STRING',
            'TYPE_FORMATION': 'STRING',
            # Les colonnes à risque sont en STRING pour garantir l'importation
            'heure_ht': 'STRING',
            'heure_hp': 'STRING',
            'heure_hc': 'STRING',
            'heure_hf': 'STRING',
            'heure_total': 'STRING',
            'heure_ecart': 'STRING'
        }
    }
}

sync_results = {}

################################################################################
# --- Utility Functions ---
################################################################################

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
        try:
            self.conn = mysql.connector.connect(**mysql_config)
            logger.info("✅ Connexion MySQL établie.")
            return self.conn
        except mysql.connector.Error as err:
            logger.critical(f"❌ ERREUR CONNEXION MySQL: {err}")
            raise SystemExit(1)
    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'conn') and self.conn.is_connected():
            self.conn.close()

def save_sync_report(sync_results_data: Dict):
    report_dir = os.path.join(REPORTS_BASE_DIR, datetime.now().strftime("%Y-%m"), datetime.now().strftime("%d"))
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, f"sync_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(sync_results_data, f, indent=2, ensure_ascii=False, default=str)
    logger.info(f"📝 Rapport enregistré: {report_path}")

################################################################################
# --- Main Sync Logic ---
################################################################################
def transfer_table(table_name: str, config: Dict, sync_results_ref: Dict):
    start_time_process = datetime.now()
    table_result = {
        'table_name': table_name, 'start_time': start_time_process.isoformat(), 'end_time': None, 'status': 'pending',
        'stats': {'source_rows_read': 0, 'rows_loaded': 0}, 'error': None
    }
    logger.info(f"\n{'='*50}\n🔄 DEBUT TRAITEMENT {table_name.upper()} ({start_time_process})\n{'='*50}")

    try:
        mysql_columns_map = config['mysql_columns_map']
        final_bq_schema = config['final_bq_schema']
        
        mysql_from_clause = config['mysql_from_clause']
        mysql_select_columns = [f"{expr} AS `{alias}`" for alias, expr in mysql_columns_map.items()]
        mysql_query = f"SELECT {', '.join(mysql_select_columns)} FROM {mysql_from_clause}"
        
        bq_client = bigquery.Client()
        create_final_table_if_not_exists(bq_client, table_name, final_bq_schema)

        with DBConnection() as mysql_conn:
            mysql_cursor = mysql_conn.cursor(dictionary=True)
            logger.info(f"Query MySQL pour {table_name}: {mysql_query}")
            mysql_cursor.execute(mysql_query)
            mysql_data = mysql_cursor.fetchall()
            table_result['stats']['source_rows_read'] = len(mysql_data)
            logger.info(f"🗃️ {len(mysql_data)} lignes lues de MySQL.")

        if not mysql_data:
            table_result['status'] = 'success'
            sync_results_ref['tables'][table_name] = table_result
            return

        rows_to_load = []
        current_time_iso = datetime.now().isoformat()

        for row in mysql_data:
            prepared_row = {}
            for col_name, bq_type in final_bq_schema.items():
                value = row.get(col_name)
                
                if value is None:
                    prepared_row[col_name] = None
                    continue

                # Formater les valeurs pour correspondre au schéma BQ
                try:
                    if bq_type == 'DATETIME' and isinstance(value, datetime):
                        prepared_row[col_name] = value.strftime('%Y-%m-%d %H:%M:%S.%f')
                    elif bq_type == 'DATE' and isinstance(value, (date, datetime)):
                        prepared_row[col_name] = value.strftime('%Y-%m-%d')
                    else:
                        # Pour tout le reste (y compris les heures en STRING), on convertit simplement en chaîne
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

################################################################################
# --- Main Function ---
################################################################################

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