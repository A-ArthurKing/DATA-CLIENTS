import os
import json
import logging
import time
import pandas as pd
import re
import io

from datetime import datetime
from google.api_core.exceptions import NotFound
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import gspread
from google.cloud import bigquery

###############################################################################
# --- Configuration du Log ---
###############################################################################

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)-8s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('sync_google_sheets_temu.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

###############################################################################
# --- Configuration Globale ---
###############################################################################

# CHEMIN D'ACCÈS 
CREDENTIALS_PATH = r'C:\Users\aakendenguesonnet\Documents\Scrip-python\project_send_data_to_bigquery\DATA-CLIENTS\dataclients_outil_gestion_paie\data-project-438313-8e99289157d6.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_PATH

PROJECT_ID = 'data-project-438313'
DATASET_ID = 'dataproject_agents_tracking'
DESTINATION_TABLE_ID = 'tracking_clients_temu_fr'

DRIVE_FOLDER_ID = '1mIIPGVEFER91I5YFYH1-OcJzzN6-_VUx'

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/bigquery"
]

###############################################################################
# --- Fonctions Utilitaires ---
###############################################################################

def get_google_credentials():
    return Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)

def get_processed_files_from_bq(bq_client) -> set:
    """Récupère la liste des fichiers déjà traités en se basant sur la table de destination."""
    destination_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{DESTINATION_TABLE_ID}"
    processed_files = set()
    try:
        query = f"SELECT DISTINCT source_filename FROM `{destination_table_ref}`"
        query_job = bq_client.query(query)
        for row in query_job.result():
            processed_files.add(row.source_filename)
        logger.info(f"🔎 {len(processed_files)} fichiers déjà traités trouvés dans BigQuery.")
    except NotFound:
        logger.warning(f"La table de destination '{DESTINATION_TABLE_ID}' n'existe pas encore. Aucun fichier n'a été traité.")
    return processed_files

def list_new_files_in_drive(drive_service, processed_files: set) -> list:
    """Liste les fichiers dans le dossier Drive et filtre ceux qui sont nouveaux."""
    if 'ID_DE_VOTRE_DOSSIER' in DRIVE_FOLDER_ID:
        logger.critical("🛑 L'ID du dossier Google Drive n'a pas été configuré. Arrêt du script.")
        raise ValueError("DRIVE_FOLDER_ID is not set.")

    logger.info(f"📂 Interrogation du dossier Drive ID: {DRIVE_FOLDER_ID}")
    try:
        query = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false and mimeType='application/vnd.google-apps.spreadsheet'"
        results = drive_service.files().list(
            q=query,
            pageSize=100,
            fields="nextPageToken, files(id, name)"
        ).execute()
        
        all_files = results.get('files', [])
        new_files = [f for f in all_files if f['name'] not in processed_files]
        
        logger.info(f"📊 Trouvé {len(all_files)} fichiers au total. {len(new_files)} sont nouveaux à traiter.")
        return new_files
    except Exception as e:
        logger.error(f"❌ Erreur lors de la communication avec l'API Google Drive: {e}", exc_info=True)
        return []

def extract_date_from_filename(filename: str) -> str:
    """Extrait la date du nom du fichier au format 'ddmmyyyy'."""
    match = re.search(r'(\d{8})', filename)
    if match:
        date_str = match.group(0)
        return f"{date_str[4:8]}-{date_str[2:4]}-{date_str[0:2]}"
    return None

def convert_time_to_string(time_value):
    """Convertit une valeur de temps en chaîne de caractères."""
    if isinstance(time_value, datetime):
        return time_value.strftime('%H:%M:%S')
    return str(time_value)

def generate_and_save_report(report_data, start_time):
    """Génère un rapport JSON de l'exécution et le sauvegarde dans le dossier 'Rapports'."""
    # Obtenir le chemin du répertoire du script en cours d'exécution
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Construire le chemin complet du dossier des rapports
    reports_folder = os.path.join(script_dir, "Rapports")
    
    if not os.path.exists(reports_folder):
        os.makedirs(reports_folder)
    
    timestamp = start_time.strftime('%Y%m%d_%H%M%S')
    report_filename = f"sync_report_temu_fr_{timestamp}.json"
    report_path = os.path.join(reports_folder, report_filename)
    
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"📄 Rapport d'exécution sauvegardé : {report_path}")
    
###############################################################################
# --- Logique de Synchronisation Principale ---
###############################################################################

def sync_sheet_to_bigquery(sheet_client, bq_client, file_to_process: dict):
    file_id = file_to_process['id']
    file_name = file_to_process['name']
    logger.info(f"\n{'='*50}\n🔄 DEBUT TRAITEMENT: {file_name}\n{'='*50}")

    report_entry = {
        "file_name": file_name,
        "start_time": datetime.now().isoformat(),
        "status": "failure",
        "error": None,
        "rows_inserted": 0
    }

    try:
        spreadsheet = sheet_client.open_by_key(file_id)
        worksheet = spreadsheet.sheet1
        data = worksheet.get_all_values()
        
        if len(data) < 2:
            logger.warning(f"⚠️ Le fichier '{file_name}' contient moins de 2 lignes. Ignoré.")
            report_entry["error"] = "Le fichier est vide."
            return False, report_entry

        headers = [h.strip() for h in data[0]]
        records = data[1:]
        df = pd.DataFrame(records, columns=headers)
        
        if len(df.columns) < 3:
            logger.error("❌ Structure de fichier invalide. Au moins 3 colonnes attendues (Matricule, Nom, Heures).")
            report_entry["error"] = "Structure de fichier invalide."
            return False, report_entry

        final_df = pd.DataFrame()
        final_df['Ligne'] = range(1, len(df) + 1)
        final_df['matricule'] = df.iloc[:, 0].astype(str)
        final_df['nom_et_prenom'] = df.iloc[:, 1].astype(str)
        final_df['total_heure'] = df.iloc[:, 2].apply(convert_time_to_string)
        
        file_date = extract_date_from_filename(file_name)
        final_df['date_suivi'] = file_date if file_date else None
        final_df['source_filename'] = file_name
        final_df['date_importation'] = datetime.now().isoformat()
        
        logger.info(f"✅ {len(final_df)} lignes prêtes pour l'importation vers BigQuery.")

        bq_table_ref_str = f"{PROJECT_ID}.{DATASET_ID}.{DESTINATION_TABLE_ID}"
        schema = [
            bigquery.SchemaField('Ligne', 'INTEGER'),
            bigquery.SchemaField('matricule', 'STRING'),
            bigquery.SchemaField('nom_et_prenom', 'STRING'),
            bigquery.SchemaField('total_heure', 'STRING'),
            bigquery.SchemaField('date_suivi', 'STRING'),
            bigquery.SchemaField('source_filename', 'STRING'),
            bigquery.SchemaField('date_importation', 'TIMESTAMP')
        ]

        try:
            bq_client.get_table(bq_table_ref_str)
        except NotFound:
            logger.info(f"🛠️ Table '{DESTINATION_TABLE_ID}' non trouvée. Création...")
            table = bigquery.Table(bq_table_ref_str, schema=schema)
            bq_client.create_table(table)
            logger.info(f"✅ Table '{DESTINATION_TABLE_ID}' créée.")
            time.sleep(5)

        csv_data = final_df.to_csv(index=False).encode('utf-8')
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1
        )
        
        job = bq_client.load_table_from_file(
            file_obj=io.BytesIO(csv_data),
            destination=bq_table_ref_str,
            job_config=job_config
        )
        job.result()

        logger.info(f"✅ {job.output_rows} lignes chargées avec succès dans '{DESTINATION_TABLE_ID}' depuis '{file_name}'.")
        
        report_entry["status"] = "success"
        report_entry["rows_inserted"] = job.output_rows
        return True, report_entry

    except Exception as e:
        logger.error(f"❌ ERREUR LORS DU TRAITEMENT de '{file_name}': {e}", exc_info=True)
        report_entry["error"] = str(e)
        return False, report_entry

###############################################################################
# --- Fonction Principale ---
###############################################################################

def main():
    logger.info(f"\n{'='*50}\n🚀 DÉMARRAGE SYNCHRO GOOGLE SHEETS\n{'='*50}\n")
    start_time = datetime.now()
    
    success_count = 0
    failure_count = 0
    report_summary = {
        "start_time": start_time.isoformat(),
        "end_time": None,
        "total_files": 0,
        "success_files": 0,
        "failed_files": 0,
        "files": []
    }

    try:
        creds = get_google_credentials()
        drive_service = build('drive', 'v3', credentials=creds)
        bq_client = bigquery.Client(credentials=creds, project=PROJECT_ID)
        sheet_client = gspread.authorize(creds)

        processed_files = get_processed_files_from_bq(bq_client)
        new_files_to_process = list_new_files_in_drive(drive_service, processed_files)
        report_summary["total_files"] = len(new_files_to_process)

        if not new_files_to_process:
            logger.info("✅ Aucun nouveau fichier à traiter. Terminé.")
            return

        for file_info in new_files_to_process:
            is_success, report_entry = sync_sheet_to_bigquery(sheet_client, bq_client, file_info)
            report_summary["files"].append(report_entry)
            if is_success:
                success_count += 1
            else:
                failure_count += 1

    except Exception as e:
        logger.critical(f"❌ ERREUR GLOBALE INATTENDUE: {e}", exc_info=True)
        report_summary["error"] = str(e)
    finally:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        report_summary["end_time"] = end_time.isoformat()
        report_summary["success_files"] = success_count
        report_summary["failed_files"] = failure_count

        generate_and_save_report(report_summary, start_time)

        logger.info(f"\n{'='*50}\n🏁 SYNCHRO TERMINÉE")
        logger.info(f"⏱️ Durée totale: {duration:.2f}s")
        logger.info(f"👍 Fichiers traités avec succès: {success_count}")
        logger.info(f"👎 Fichiers en échec: {failure_count}")
        logger.info("="*50)


if __name__ == "__main__":
    main()