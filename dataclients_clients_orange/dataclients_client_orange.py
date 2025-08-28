import os
import json
import logging
import tempfile
import traceback
import sys
from typing import Dict, List, Set
import csv
from datetime import datetime, date
import glob

from google.cloud import bigquery
from google.api_core.exceptions import NotFound, GoogleAPIError

###############################################################################
# --- Logging Configuration ---
###############################################################################

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)-8s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('sync_client_orange.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

###############################################################################
# --- Global Configuration ---
###############################################################################

# Chemin du fichier d'authentification BigQuery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\aakendenguesonnet\Documents\Scrip-python\project_send_data_to_bigquery\DATA-CLIENTS\dataclients_clients_orange\data-project-438313-8e99289157d6.json'

project_id = 'data-project-438313'
dataset_id = 'dataproject_agents_tracking'

# Chemin d'accès au répertoire source des fichiers CSV
SOURCE_DIR = r'\\192.168.50.8\ftp_orange$'
FILE_PATTERN = 'MCC_extraction_agent_*.csv'
# Chemin où stocker les fichiers de rapport temporaires
REPORTS_BASE_DIR = r'C:\Users\aakendenguesonnet\Documents\Scrip-python\project_send_data_to_bigquery\DATA-CLIENTS\dataclients_clients_orange\reports'

TABLES_CONFIG = {
    'tracking_client_orange': {
        'key_field': 'OBJECT_NAME',
        'columns': [
            'OBJECT_NAME', 'PRESENTATION_NAME', 'DATE_YYYYMMDD', 'T_INBOUND',
            'T_LOGIN', 'T_CONSULT', 'T_DIALING', 'T_OUTBOUND', 'T_RINGING',
            'T_TALK', 'AUX_WC', 'AUX_REPOS', 'AUX_FORMATION', 'AUX_ADMINISTRATIF',
            'AUX_TECHNIQUE', 'AUX_DEBRIEF', 'AUX_DELEGATION', 'AUX_REUNION',
            'T_HOLD', 'T_WORK', 'T_WAIT'
        ],
        'type_mappings': {
            'OBJECT_NAME': 'STRING',
            'PRESENTATION_NAME': 'STRING',
            'DATE_YYYYMMDD': 'DATE',
            'T_INBOUND': 'STRING',
            'T_LOGIN': 'STRING',
            'T_CONSULT': 'STRING',
            'T_DIALING': 'STRING',
            'T_OUTBOUND': 'STRING',
            'T_RINGING': 'STRING',
            'T_TALK': 'STRING',
            'AUX_WC': 'STRING',
            'AUX_REPOS': 'STRING',
            'AUX_FORMATION': 'STRING',
            'AUX_ADMINISTRATIF': 'STRING',
            'AUX_TECHNIQUE': 'STRING',
            'AUX_DEBRIEF': 'STRING',
            'AUX_DELEGATION': 'STRING',
            'AUX_REUNION': 'STRING',
            'T_HOLD': 'STRING',
            'T_WORK': 'STRING',
            'T_WAIT': 'STRING',
        }
    }
}

sync_results = {
    'start_time': None, 'end_time': None, 'total_tables': 0, 'success_tables': 0,
    'failed_tables': 0, 'failed_table_names': [], 'tables': {}
}

###############################################################################
# --- Utility Functions ---
###############################################################################

def format_milliseconds_to_hhmmss(total_milliseconds: float) -> str:
    """Convertit un nombre de millisecondes en une chaîne au format HH:MM:SS."""
    if total_milliseconds is None or total_milliseconds == '':
        return None
    try:
        total_seconds = int(total_milliseconds) // 1000
        
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours:02}:{minutes:02}:{seconds:02}"
    except (ValueError, TypeError):
        return None

def create_table_if_not_exists(client: bigquery.Client, table_name: str, tables_config: Dict) -> bool:
    """Crée la table BigQuery si elle n'existe pas, en ajoutant la colonne source_file."""
    table_ref_str = f"{project_id}.{dataset_id}.{table_name}"
    config = tables_config[table_name]

    try:
        existing_table = client.get_table(table_ref_str)
        logger.info(f"🟢 Table '{table_name}' existe. Vérification du schéma...")

        schema_updates = []
        existing_columns = {field.name for field in existing_table.schema}

        if 'source_file' not in existing_columns:
            schema_updates.append(bigquery.SchemaField("source_file", "STRING", mode="NULLABLE"))

        for column_name_bq, col_type_bq in config['type_mappings'].items():
            if column_name_bq not in existing_columns:
                 schema_updates.append(bigquery.SchemaField(column_name_bq, col_type_bq, mode="NULLABLE"))
        
        if schema_updates:
            existing_table.schema = list(existing_table.schema) + schema_updates
            client.update_table(existing_table, ["schema"])
            logger.info(f"✅ Colonnes ajoutées/mises à jour à '{table_name}': {[f.name for f in schema_updates]}")
        
        return False
    except NotFound:
        logger.info(f"🛠️ Table '{table_name}' non trouvée. Création...")
        try:
            schema_fields = [
                bigquery.SchemaField("source_file", "STRING", mode="NULLABLE")
            ]
            for column_name_bq, col_type_bq in config['type_mappings'].items():
                schema_fields.append(bigquery.SchemaField(column_name_bq, col_type_bq))

            table = bigquery.Table(table_ref_str, schema=schema_fields)
            client.create_table(table)
            logger.info(f"✅ Table '{table_name}' créée. Attente de propagation...")
            import time
            time.sleep(10)
            logger.info(f"⏳ Reprise après attente pour '{table_name}'.")
            return True
        except Exception as e:
            logger.error(f"❌ Échec de la création de la table '{table_name}': {e}", exc_info=True)
            raise

def prepare_row_for_bq(row_data: Dict, file_name: str, tables_config: Dict) -> Dict:
    """Prépare une ligne pour l'insertion dans BigQuery en convertissant les types."""
    prepared_row = {'source_file': file_name}
    table_name = 'tracking_client_orange'
    table_config = tables_config[table_name]
    type_mappings = table_config.get('type_mappings', {})

    for csv_col_name in table_config['columns']:
        col_value = row_data.get(csv_col_name)

        if col_value is None or (isinstance(col_value, str) and str(col_value).strip() == ''):
            prepared_row[csv_col_name] = None
            continue

        target_bq_type = type_mappings.get(csv_col_name)

        try:
            if target_bq_type == 'DATE':
                prepared_row[csv_col_name] = datetime.strptime(str(col_value), '%Y%m%d').date().isoformat()
            elif csv_col_name.startswith(('T_', 'AUX_')) and target_bq_type == 'STRING':
                prepared_row[csv_col_name] = format_milliseconds_to_hhmmss(col_value)
            elif target_bq_type in ['FLOAT64', 'NUMERIC']:
                prepared_row[csv_col_name] = float(str(col_value).replace(',', '.'))
            elif target_bq_type == 'INT64':
                prepared_row[csv_col_name] = int(float(str(col_value).replace(',', '.')))
            else:
                prepared_row[csv_col_name] = str(col_value)
        except (ValueError, TypeError) as e:
            logging.warning(f"Conversion échouée pour la colonne '{csv_col_name}' avec la valeur '{col_value}'. Erreur: {e}. Affectation de 'None'.")
            prepared_row[csv_col_name] = None
    
    return prepared_row

def load_data_via_batch_job(client: bigquery.Client, table_name: str, data_to_load: List[Dict], table_result: Dict) -> int:
    """Charge les données dans BigQuery en utilisant un job par lots (append)."""
    bq_table_ref_str = f"{project_id}.{dataset_id}.{table_name}"
    bq_table_ref = client.get_table(bq_table_ref_str) 

    temp_file_path = None
    inserted_rows = 0
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8', suffix='.json', dir=REPORTS_BASE_DIR) as temp_file:
            temp_file_path = temp_file.name
            for row in data_to_load:
                json.dump(row, temp_file, ensure_ascii=False)
                temp_file.write('\n')
        logger.info(f"📄 Données préparées écrites dans le fichier temporaire: {temp_file_path}")

        load_job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=bq_table_ref.schema 
        )

        with open(temp_file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, bq_table_ref, job_config=load_job_config)
            logger.info(f"🚀 Job de chargement BigQuery démarré pour '{table_name}': {job.job_id}")
            job.result(timeout=900) 
        
        inserted_rows = job.output_rows
        logger.info(f"✅ Chargement batch pour '{table_name}' terminé. Lignes chargées: {inserted_rows}")
        table_result['status'] = 'success'

    except GoogleAPIError as e:
        logger.exception(f"❌ Erreur BigQuery lors du chargement batch pour {table_name}: {e}")
        table_result['error'] = str(e)
        table_result['status'] = 'failed'
    except Exception as e:
        logger.exception(f"❌ Erreur inattendue lors du chargement batch pour {table_name}: {e}")
        table_result['error'] = str(e)
        table_result['status'] = 'failed'
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            logger.info(f"🗑️ Fichier temporaire supprimé: {temp_file_path}")
    return inserted_rows
    
def ensure_report_directory() -> str | None:
    now = datetime.now()
    report_dir = os.path.join(REPORTS_BASE_DIR, now.strftime("%Y-%m"), now.strftime("%d"))
    try:
        os.makedirs(report_dir, exist_ok=True)
        return report_dir
    except Exception as e:
        logger.error(f"❌ Erreur création dossier rapport: {e}")
        return None

def save_sync_report(sync_results_data: Dict, table_name: str):
    report_dir = ensure_report_directory()
    if not report_dir: return
    try:
        report_path = os.path.join(report_dir, f"sync_report_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(sync_results_data, f, indent=2, ensure_ascii=False, default=str)
        logger.info(f"📝 Rapport enregistré: {report_path}")
    except Exception as e:
        logger.error(f"❌ Erreur enregistrement rapport: {e}")

###############################################################################
# --- Main Sync Logic ---
###############################################################################
def process_csv_files(table_name: str, tables_config: Dict, sync_results_ref: Dict):
    start_time_process = datetime.now()
    table_result = {'table_name': table_name, 'start_time': start_time_process.isoformat(), 'end_time': None, 'status': 'success', 'table_created': False, 'stats': {'files_processed': 0, 'files_skipped': 0, 'total_rows_inserted': 0}, 'error': None}
    logger.info(f"\n{'='*50}\n🔄 DEBUT TRAITEMENT FICHIERS CSV VERS {table_name.upper()} ({start_time_process})\n{'='*50}")

    bq_client = None
    try:
        bq_client = bigquery.Client()
        bq_table_ref_str = f"{project_id}.{dataset_id}.{table_name}"

        table_result['table_created'] = create_table_if_not_exists(bq_client, table_name, TABLES_CONFIG)

        imported_files: Set[str] = set()
        try:
            query_imported_files = f"SELECT DISTINCT source_file FROM `{bq_table_ref_str}` WHERE source_file IS NOT NULL"
            query_job = bq_client.query(query_imported_files)
            for row in query_job.result():
                imported_files.add(row.source_file)
            logger.info(f"✅ {len(imported_files)} fichiers déjà trouvés dans BigQuery.")
        except NotFound:
            logger.info(f"Table '{table_name}' n'existe pas. Pas de fichiers importés à vérifier.")
        except Exception as e:
            logger.error(f"❌ Erreur de lecture des fichiers déjà importés: {e}", exc_info=True)
            table_result['error'] = str(e)
            table_result['status'] = 'failed'
            return

        source_files = glob.glob(os.path.join(SOURCE_DIR, FILE_PATTERN))
        logger.info(f"🔍 {len(source_files)} fichiers CSV trouvés à l'emplacement source.")

        for file_path in source_files:
            file_name = os.path.basename(file_path)

            if file_name in imported_files:
                logger.info(f"⏭️ Fichier '{file_name}' déjà importé. Ignoré.")
                table_result['stats']['files_skipped'] += 1
                continue
            
            logger.info("-" * 50)
            logger.info(f"📂 Traitement du nouveau fichier: '{file_name}'...")
            
            data_to_load = []
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    csv_reader = csv.DictReader(f, delimiter=';')
                    for row in csv_reader:
                        prepared_row = prepare_row_for_bq(row, file_name, tables_config)
                        data_to_load.append(prepared_row)
                
                if data_to_load:
                    inserted_rows = load_data_via_batch_job(bq_client, table_name, data_to_load, table_result)
                    if inserted_rows > 0:
                        table_result['stats']['total_rows_inserted'] += inserted_rows
                        table_result['stats']['files_processed'] += 1
                        logger.info(f"✅ Fichier '{file_name}' importé avec succès. {inserted_rows} lignes ajoutées.")
                        imported_files.add(file_name)
                    else:
                        logger.warning(f"⚠️ Fichier '{file_name}' traité mais 0 ligne insérée.")
                else:
                    logger.warning(f"⚠️ Fichier '{file_name}' est vide ou illisible. Ignoré.")

            except Exception as e:
                logger.error(f"❌ Erreur lors du traitement du fichier '{file_name}': {e}", exc_info=True)
                table_result['status'] = 'failed'
                table_result['error'] = f"Erreur de traitement du fichier '{file_name}': {str(e)}"
                
    except Exception as e:
        logger.error(f"❌ ERREUR GLOBALE: {e}", exc_info=True)
        table_result['status'] = 'failed'
        table_result['error'] = str(e)
        if table_name not in sync_results_ref.get('failed_table_names', []):
            sync_results_ref.setdefault('failed_tables', 0); sync_results_ref['failed_tables'] += 1
            sync_results_ref.setdefault('failed_table_names', []).append(table_name)
    finally:
        end_time_process = datetime.now()
        table_result['end_time'] = end_time_process.isoformat()
        duration = (end_time_process - start_time_process).total_seconds()
        logger.info(f"\n✅ Fin traitement {table_name} ({end_time_process}). Durée: {duration:.2f}s\n{'='*50}")
        sync_results_ref['tables'][table_name] = table_result
        
###############################################################################
# --- Main Function ---
###############################################################################

def main():
    global TABLES_CONFIG, sync_results

    sync_results.update({'start_time': datetime.now().isoformat(), 'total_tables': len(TABLES_CONFIG), 'success_tables': 0, 'failed_tables': 0, 'failed_table_names': []})
    exit_code_final = 0
    try:
        logger.info(f"\n{'='*50}\n🚀 DÉMARRAGE SYNCHRO FICHIERS CSV\nProjet: {project_id}.{dataset_id}\nSource: {SOURCE_DIR}\n{'='*50}\n")
        
        if not ensure_report_directory():
            logger.critical("Impossible de créer le répertoire de rapports. Arrêt du script.")
            sys.exit(1)

        for table_name_iter in TABLES_CONFIG:
            sync_results['tables'][table_name_iter] = {}
            process_csv_files(table_name_iter, TABLES_CONFIG, sync_results)

        end_time_script = datetime.now()
        sync_results['end_time'] = end_time_script.isoformat()
        total_duration = (end_time_script - datetime.fromisoformat(sync_results['start_time'])).total_seconds()

        current_success_count, current_failed_count, current_failed_names = 0, 0, []
        for tn, tres in sync_results['tables'].items():
            if tres.get('status') == 'success': current_success_count +=1
            else:
                current_failed_count +=1
                if tn not in current_failed_names : current_failed_names.append(tn)
        sync_results.update({'success_tables': current_success_count, 'failed_tables': current_failed_count, 'failed_table_names': current_failed_names})

        logger.info(f"\n{'='*50}\n🏁 SYNCHRO TERMINÉE ({end_time_script})\n⏱️ Durée totale: {total_duration:.2f}s")
        logger.info(f"📊 Succès: {sync_results['success_tables']}/{sync_results['total_tables']}")
        if sync_results['failed_tables'] > 0: logger.info(f"❌ Échecs: {sync_results['failed_tables']} ({', '.join(sync_results['failed_table_names'])})"); exit_code_final = 1
        logger.info("="*50)
    except KeyboardInterrupt: logger.warning("⏹️ Interruption utilisateur."); exit_code_final = 1
    except Exception as e_global: logger.critical(f"❌ ERREUR GLOBALE: {e_global}", exc_info=True); exit_code_final = 2
    finally:
        if sync_results.get('end_time') is None: sync_results['end_time'] = datetime.now().isoformat()
        save_sync_report(sync_results, list(TABLES_CONFIG.keys())[0])
        sys.exit(exit_code_final)

if __name__ == "__main__":
    main()