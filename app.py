"""
TIRE SYNC SERVICE - SIMPLIFIED VERSION
=======================================
Uses MOTOR TireTechSmart pre-joined data for simpler, faster syncs.

Endpoints:
- POST /sync/motor   - Sync MOTOR TireTechSmart data
- POST /sync/usventure - Sync USVenture inventory (TODO)
- GET /health        - Health check
- GET /status        - Recent sync history

Environment Variables Required:
- MOTOR_FTP_HOST, MOTOR_FTP_USER, MOTOR_FTP_PASSWORD
- SUPABASE_URL, SUPABASE_SERVICE_KEY
- SENDGRID_API_KEY, ALERT_EMAIL
- SYNC_API_KEY (optional)
"""

import os
import io
import re
import csv
import zipfile
import logging
from datetime import datetime, timezone
from functools import wraps

import paramiko
from flask import Flask, request, jsonify
from supabase import create_client, Client
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

class Config:
    # MOTOR FTP
    MOTOR_FTP_HOST = os.environ.get('MOTOR_FTP_HOST', 'delivery.motor.com')
    MOTOR_FTP_USER = os.environ.get('MOTOR_FTP_USER', 'revyourcause_MIS')
    MOTOR_FTP_PASSWORD = os.environ.get('MOTOR_FTP_PASSWORD', '')
    MOTOR_FTP_PATH = '/Specifications_Data/TireTech/'
    
    # Supabase
    SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
    SUPABASE_SERVICE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')
    
    # SendGrid
    SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY', '')
    ALERT_EMAIL = os.environ.get('ALERT_EMAIL', '')
    ALERT_FROM_EMAIL = os.environ.get('ALERT_FROM_EMAIL', 'sporcher@myjiffytires.com')
    
    # Security
    SYNC_API_KEY = os.environ.get('SYNC_API_KEY', '')


def get_supabase() -> Client:
    """Create Supabase client."""
    return create_client(Config.SUPABASE_URL, Config.SUPABASE_SERVICE_KEY)


# =============================================================================
# AUTHENTICATION
# =============================================================================

def require_api_key(f):
    """Decorator to require API key for webhook endpoints."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not Config.SYNC_API_KEY:
            return f(*args, **kwargs)
        
        api_key = request.headers.get('X-API-Key')
        if api_key != Config.SYNC_API_KEY:
            logger.warning(f"Unauthorized sync attempt from {request.remote_addr}")
            return jsonify({'error': 'Unauthorized'}), 401
        
        return f(*args, **kwargs)
    return decorated


# =============================================================================
# ALERTING
# =============================================================================

def send_alert(subject: str, body: str, is_error: bool = False):
    """Send email alert via SendGrid."""
    if not Config.SENDGRID_API_KEY or not Config.ALERT_EMAIL:
        logger.warning("SendGrid not configured, skipping alert")
        return
    
    try:
        prefix = "🚨" if is_error else "✅"
        full_subject = f"{prefix} Tire Sync: {subject}"
        
        message = Mail(
            from_email=Config.ALERT_FROM_EMAIL,
            to_emails=Config.ALERT_EMAIL,
            subject=full_subject,
            html_content=f"""
            <h2>{full_subject}</h2>
            <pre style="background: #f4f4f4; padding: 15px; border-radius: 5px;">
{body}
            </pre>
            <p style="color: #666; font-size: 12px;">
                Sent from Tire Sync Service at {datetime.now(timezone.utc).isoformat()}
            </p>
            """
        )
        
        sg = SendGridAPIClient(Config.SENDGRID_API_KEY)
        sg.send(message)
        logger.info(f"Alert sent: {subject}")
        
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")


# =============================================================================
# SFTP FUNCTIONS
# =============================================================================

def connect_motor_sftp():
    """Establish SFTP connection to MOTOR."""
    logger.info(f"Connecting to MOTOR SFTP: {Config.MOTOR_FTP_HOST}")
    
    transport = paramiko.Transport((Config.MOTOR_FTP_HOST, 22))
    transport.connect(username=Config.MOTOR_FTP_USER, password=Config.MOTOR_FTP_PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    return sftp, transport


def find_latest_smart_zip(sftp) -> str:
    """Find the latest MOTOR_TireTechSmart_*.zip file."""
    sftp.chdir(Config.MOTOR_FTP_PATH)
    files = sftp.listdir()
    
    # Filter for TireTechSmart zips (not regular TireTech)
    pattern = re.compile(r'^MOTOR_TireTechSmart_(\d{8})\.zip$')
    zip_files = []
    
    for f in files:
        match = pattern.match(f)
        if match:
            date_str = match.group(1)
            zip_files.append((f, date_str))
    
    if not zip_files:
        raise FileNotFoundError("No MOTOR_TireTechSmart_*.zip files found")
    
    # Sort by date descending, get latest
    zip_files.sort(key=lambda x: x[1], reverse=True)
    latest_file = zip_files[0][0]
    
    logger.info(f"Found latest MOTOR Smart file: {latest_file}")
    return latest_file


def download_and_extract_smart_csv(sftp, filename: str) -> str:
    """Download Smart zip and extract the tire-smart-submodel-vehicles.csv."""
    logger.info(f"Downloading {filename}...")
    
    with io.BytesIO() as zip_buffer:
        sftp.getfo(filename, zip_buffer)
        zip_buffer.seek(0)
        
        with zipfile.ZipFile(zip_buffer, 'r') as zf:
            # Find the tire-smart-submodel-vehicles.csv file
            for name in zf.namelist():
                if 'tire-smart-submodel-vehicles' in name.lower() and name.endswith('.csv'):
                    logger.info(f"  Extracting {name}")
                    return zf.read(name).decode('utf-8-sig')
            
            # If exact name not found, list what's in the zip
            logger.error(f"  Files in zip: {zf.namelist()}")
            raise FileNotFoundError("tire-smart-submodel-vehicles.csv not found in zip")


# =============================================================================
# DATA SYNC
# =============================================================================

def clean_value(value, field_type='string'):
    """Clean and convert a value based on expected type."""
    if value is None or value == '' or value == 'NULL':
        return None
    
    value = str(value).strip()
    
    if field_type == 'integer':
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    elif field_type == 'numeric':
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    elif field_type == 'bigint':
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    elif field_type == 'boolean':
        return value.lower() in ('true', '1', 'yes')
    else:
        return value if value else None


def sync_smart_vehicles(supabase: Client, csv_content: str) -> dict:
    """
    Sync Smart Vehicles CSV to tt_smart_vehicles table.
    Uses streaming to minimize memory usage.
    """
    # Field mapping: CSV column -> (DB column, type)
    field_map = {
        'FG_FMK': ('fg_fmk', 'bigint'),
        'FG_ChassisID': ('fg_chassis_id', 'integer'),
        'FG_ModelID': ('fg_model_id', 'integer'),
        'VCDB_VehicleID': ('vcdb_vehicle_id', 'integer'),
        'VCDB_BaseVehicleID': ('vcdb_base_vehicle_id', 'integer'),
        'Year': ('year', 'integer'),
        'VCDB_MakeID': ('vcdb_make_id', 'integer'),
        'MakeName': ('make_name', 'string'),
        'VCDB_ModelID': ('vcdb_model_id', 'integer'),
        'ModelName': ('model_name', 'string'),
        'VCDB_SubmodelID': ('vcdb_submodel_id', 'integer'),
        'SubmodelName': ('submodel_name', 'string'),
        'VCDB_BodyTypeID': ('vcdb_body_type_id', 'integer'),
        'BodyTypeName': ('body_type_name', 'string'),
        'VCDB_DriveTypeID': ('vcdb_drive_type_id', 'integer'),
        'DriveTypeName': ('drive_type_name', 'string'),
        'VCDB_RegionID': ('vcdb_region_id', 'integer'),
        'RegionName': ('region_name', 'string'),
        'CustomNote': ('custom_note', 'string'),
        'PMetric': ('p_metric', 'string'),
        'TireSize': ('tire_size', 'string'),
        'LoadIndex': ('load_index', 'string'),
        'SpeedIndex': ('speed_index', 'string'),
        'LoadRange': ('load_range', 'string'),
        'TireSizeR': ('tire_size_rear', 'string'),
        'LoadIndexR': ('load_index_rear', 'string'),
        'SpeedIndexR': ('speed_index_rear', 'string'),
        'LoadRangeR': ('load_range_rear', 'string'),
        'RimWidth': ('rim_width', 'numeric'),
        'RimDiameter': ('rim_diameter', 'numeric'),
        'RimSize': ('rim_size', 'string'),
        'RimWidthR': ('rim_width_rear', 'numeric'),
        'RimDiameterR': ('rim_diameter_rear', 'numeric'),
        'RimSizeR': ('rim_size_rear', 'string'),
        'BoltPattern': ('bolt_pattern', 'string'),
        'Hubbore': ('hubbore', 'numeric'),
        'HubboreR': ('hubbore_rear', 'numeric'),
        'IsStaggered': ('is_staggered', 'boolean'),
        'SmartSubmodelDescription': ('smart_submodel_description', 'string'),
        'NumSizesForSubmodel': ('num_sizes_for_submodel', 'integer'),
    }
    
    batch_size = 5000
    inserted = 0
    errors = 0
    batch = []
    total_records = 0
    
    # Truncate table first
    logger.info("  Truncating tt_smart_vehicles...")
    supabase.table('tt_smart_vehicles').delete().neq('created_at', '1900-01-01').execute()
    
    reader = csv.DictReader(io.StringIO(csv_content))
    first_row = True
    
    for row in reader:
        total_records += 1
        
        # Normalize keys
        normalized = {k.strip(): v for k, v in row.items()}
        
        # Log first row for debugging
        if first_row:
            logger.info(f"  CSV columns: {list(normalized.keys())}")
            first_row = False
        
        # Transform record
        transformed = {}
        for csv_col, (db_col, field_type) in field_map.items():
            value = normalized.get(csv_col)
            transformed[db_col] = clean_value(value, field_type)
        
        batch.append(transformed)
        
        # Insert when batch is full
        if len(batch) >= batch_size:
            try:
                supabase.table('tt_smart_vehicles').insert(batch).execute()
                inserted += len(batch)
                logger.info(f"  Inserted batch: {inserted:,} / {total_records:,} records")
            except Exception as e:
                logger.error(f"  Error inserting batch: {e}")
                errors += len(batch)
            batch = []
    
    # Insert remaining records
    if batch:
        try:
            supabase.table('tt_smart_vehicles').insert(batch).execute()
            inserted += len(batch)
            logger.info(f"  Inserted final batch: {inserted:,} / {total_records:,} records")
        except Exception as e:
            logger.error(f"  Error inserting final batch: {e}")
            errors += len(batch)
    
    logger.info(f"  Completed: {inserted:,} inserted, {errors:,} errors")
    return {'inserted': inserted, 'errors': errors, 'total': total_records}


def sync_motor_data():
    """
    Main function to sync MOTOR TireTechSmart data.
    """
    start_time = datetime.now(timezone.utc)
    results = {
        'status': 'running',
        'started_at': start_time.isoformat(),
        'source_file': None,
        'tables': {}
    }
    
    supabase = get_supabase()
    sftp = None
    transport = None
    log_id = None
    
    try:
        # Log sync start
        log_entry = supabase.table('tire_data_sync_log').insert({
            'sync_type': 'motor_tiretech_smart',
            'status': 'running',
            'started_at': start_time.isoformat()
        }).execute()
        log_id = log_entry.data[0]['id']
        
        # Connect to SFTP
        sftp, transport = connect_motor_sftp()
        
        # Find and download latest Smart zip
        latest_zip = find_latest_smart_zip(sftp)
        results['source_file'] = latest_zip
        
        # Extract and sync the CSV
        csv_content = download_and_extract_smart_csv(sftp, latest_zip)
        logger.info(f"Processing Smart Vehicles CSV...")
        
        result = sync_smart_vehicles(supabase, csv_content)
        results['tables']['tt_smart_vehicles'] = result
        
        # Update sync log
        end_time = datetime.now(timezone.utc)
        results['status'] = 'completed'
        results['completed_at'] = end_time.isoformat()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        
        supabase.table('tire_data_sync_log').update({
            'status': 'completed',
            'source_file': latest_zip,
            'records_inserted': result['inserted'],
            'completed_at': end_time.isoformat()
        }).eq('id', log_id).execute()
        
        # Send success alert
        send_alert(
            f"MOTOR Smart Sync Complete",
            f"""Source: {latest_zip}
Duration: {results['duration_seconds']:.1f} seconds
Records: {result['inserted']:,} inserted, {result['errors']:,} errors
""",
            is_error=False
        )
        
        return results
        
    except Exception as e:
        logger.error(f"MOTOR sync failed: {e}")
        results['status'] = 'failed'
        results['error'] = str(e)
        
        # Update sync log
        if log_id:
            try:
                supabase.table('tire_data_sync_log').update({
                    'status': 'failed',
                    'error_message': str(e),
                    'completed_at': datetime.now(timezone.utc).isoformat()
                }).eq('id', log_id).execute()
            except:
                pass
        
        # Send failure alert
        send_alert(
            f"MOTOR Smart Sync FAILED",
            f"""Error: {str(e)}

Please check the logs for details.
""",
            is_error=True
        )
        
        raise
        
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()


# =============================================================================
# ROUTES
# =============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'service': 'tire-sync-service',
        'version': '2.0-smart',
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/sync/motor', methods=['POST'])
@require_api_key
def trigger_motor_sync():
    """Webhook endpoint to trigger MOTOR TireTechSmart sync."""
    logger.info("MOTOR Smart sync triggered via webhook")
    
    try:
        results = sync_motor_data()
        return jsonify(results), 200
        
    except Exception as e:
        logger.error(f"MOTOR sync failed: {e}")
        return jsonify({
            'status': 'failed',
            'error': str(e)
        }), 500


@app.route('/sync/usventure', methods=['POST'])
@require_api_key
def trigger_usventure_sync():
    """Webhook endpoint to trigger USVenture inventory sync."""
    return jsonify({
        'status': 'not_implemented',
        'message': 'USVenture sync not yet implemented. Waiting for file delivery to be restored.'
    }), 501


@app.route('/status', methods=['GET'])
def sync_status():
    """Get recent sync history."""
    try:
        supabase = get_supabase()
        result = supabase.table('tire_data_sync_log')\
            .select('*')\
            .order('started_at', desc=True)\
            .limit(10)\
            .execute()
        
        return jsonify({
            'recent_syncs': result.data
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
