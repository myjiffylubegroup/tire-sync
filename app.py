"""
TIRE SYNC SERVICE
=================
Render Web Service for syncing tire data from FTP sources to Supabase.

Endpoints:
- POST /sync/motor   - Sync MOTOR TireTech data (vehicle fitment)
- POST /sync/usventure - Sync USVenture data (inventory/pricing) [TODO]
- GET /health        - Health check

Environment Variables Required:
- MOTOR_FTP_HOST, MOTOR_FTP_USER, MOTOR_FTP_PASSWORD
- SUPABASE_URL, SUPABASE_SERVICE_KEY
- SENDGRID_API_KEY, ALERT_EMAIL
- SYNC_API_KEY (optional - for securing webhook endpoints)
"""

import os
import io
import re
import csv
import zipfile
import tempfile
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
    ALERT_FROM_EMAIL = os.environ.get('ALERT_FROM_EMAIL', 'turbo@myjiffylube.com')
    
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
        # If no API key configured, skip auth (for initial setup)
        if not Config.SYNC_API_KEY:
            return f(*args, **kwargs)
        
        # Check header
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
        # Add emoji prefix for quick visual scanning
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
# MOTOR TIRETECH SYNC
# =============================================================================

def connect_motor_sftp():
    """Establish SFTP connection to MOTOR."""
    logger.info(f"Connecting to MOTOR SFTP: {Config.MOTOR_FTP_HOST}")
    
    transport = paramiko.Transport((Config.MOTOR_FTP_HOST, 22))
    transport.connect(username=Config.MOTOR_FTP_USER, password=Config.MOTOR_FTP_PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    return sftp, transport


def find_latest_motor_zip(sftp) -> str:
    """Find the latest MOTOR_TireTech_*.zip file (not TireTechSmart)."""
    sftp.chdir(Config.MOTOR_FTP_PATH)
    files = sftp.listdir()
    
    # Filter for standard TireTech zips (not Smart)
    pattern = re.compile(r'^MOTOR_TireTech_(\d{8})\.zip$')
    zip_files = []
    
    for f in files:
        match = pattern.match(f)
        if match:
            date_str = match.group(1)
            zip_files.append((f, date_str))
    
    if not zip_files:
        raise FileNotFoundError("No MOTOR_TireTech_*.zip files found")
    
    # Sort by date descending, get latest
    zip_files.sort(key=lambda x: x[1], reverse=True)
    latest_file = zip_files[0][0]
    
    logger.info(f"Found latest MOTOR file: {latest_file}")
    return latest_file


def download_and_extract_zip(sftp, filename: str) -> dict:
    """Download zip file and extract CSVs to memory."""
    logger.info(f"Downloading {filename}...")
    
    # Download to memory
    with io.BytesIO() as zip_buffer:
        sftp.getfo(filename, zip_buffer)
        zip_buffer.seek(0)
        
        # Extract CSVs
        csv_files = {}
        with zipfile.ZipFile(zip_buffer, 'r') as zf:
            for name in zf.namelist():
                if name.endswith('.csv'):
                    logger.info(f"  Extracting {name}")
                    csv_files[name] = zf.read(name).decode('utf-8-sig')
        
        return csv_files


def parse_csv_to_records(csv_content: str) -> list:
    """Parse CSV string to list of dictionaries with normalized column names."""
    reader = csv.DictReader(io.StringIO(csv_content))
    records = []
    first_row = True
    for row in reader:
        # Normalize keys: strip whitespace, keep original case
        normalized = {k.strip(): v for k, v in row.items()}
        
        # Log column names from first row for debugging
        if first_row:
            logger.info(f"  CSV columns found: {list(normalized.keys())}")
            logger.info(f"  First record: {normalized}")
            first_row = False
            
        records.append(normalized)
    return records


def find_column(record: dict, possible_names: list) -> str:
    """Find a column value by trying multiple possible column names."""
    for name in possible_names:
        # Try exact match
        if name in record:
            return record[name]
        # Try case-insensitive match
        for key in record.keys():
            if key.lower() == name.lower():
                return record[key]
    return None


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
    else:
        return value if value else None


def sync_motor_table(supabase: Client, table_name: str, records: list, field_map: dict) -> dict:
    """
    Sync records to a Supabase table.
    Uses truncate + insert for full refresh.
    
    Returns: {'inserted': count, 'errors': count}
    """
    logger.info(f"Syncing {len(records)} records to {table_name}")
    
    # Log actual column names from first record for debugging
    if records:
        logger.info(f"  CSV columns: {list(records[0].keys())}")
    
    # Transform records using field map
    transformed = []
    for record in records:
        row = {}
        for csv_col, (db_col, field_type) in field_map.items():
            # Try exact match first, then case-insensitive
            value = None
            if csv_col in record:
                value = record[csv_col]
            else:
                # Case-insensitive search
                for key in record.keys():
                    if key.lower() == csv_col.lower():
                        value = record[key]
                        break
            row[db_col] = clean_value(value, field_type)
        transformed.append(row)
    
    # Log sample transformed record for debugging
    if transformed:
        logger.info(f"  Sample transformed record: {transformed[0]}")
    
    # Truncate table
    logger.info(f"  Truncating {table_name}...")
    supabase.table(table_name).delete().neq('created_at', '1900-01-01').execute()
    
    # Insert in batches (larger batches = faster, but watch for Supabase limits)
    batch_size = 5000
    inserted = 0
    errors = 0
    
    for i in range(0, len(transformed), batch_size):
        batch = transformed[i:i + batch_size]
        try:
            supabase.table(table_name).insert(batch).execute()
            inserted += len(batch)
            logger.info(f"  Inserted batch {i // batch_size + 1}: {len(batch)} records")
        except Exception as e:
            logger.error(f"  Error inserting batch: {e}")
            errors += len(batch)
    
    return {'inserted': inserted, 'errors': errors}


def sync_motor_data():
    """
    Main function to sync all MOTOR TireTech data.
    Returns summary of sync operation.
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
    
    try:
        # Log sync start
        log_entry = supabase.table('tire_data_sync_log').insert({
            'sync_type': 'motor_tiretech',
            'status': 'running',
            'started_at': start_time.isoformat()
        }).execute()
        log_id = log_entry.data[0]['id']
        
        # Connect to SFTP
        sftp, transport = connect_motor_sftp()
        
        # Find and download latest zip
        latest_zip = find_latest_motor_zip(sftp)
        results['source_file'] = latest_zip
        
        csv_files = download_and_extract_zip(sftp, latest_zip)
        
        # Define field mappings for each table
        # Format: {'csv_column': ('db_column', 'type')}
        
        chassis_map = {
            'ChassisID': ('chassis_id', 'integer'),
            'BoltPattern': ('bolt_pattern', 'string'),
            'Hubbore': ('hubbore', 'numeric'),
            'HubboreRear': ('hubbore_rear', 'numeric'),
            'MaxWheelLoad': ('max_wheel_load', 'integer'),
            'Nutorbolt': ('nut_or_bolt', 'string'),
            'NutBoltThreadType': ('nut_bolt_thread_type', 'string'),
            'NutBoltHex': ('nut_bolt_hex', 'integer'),
            'BoltLength': ('bolt_length', 'numeric'),
            'Minboltlength': ('min_bolt_length', 'integer'),
            'Maxboltlength': ('max_bolt_length', 'integer'),
            'NutBoltTorque': ('nut_bolt_torque', 'string'),
            'AxleWeightFront': ('axle_weight_front', 'string'),
            'AxleWeightRear': ('axle_weight_rear', 'string'),
            'TPMS': ('tpms', 'string'),
            'StudLength': ('stud_length', 'string'),
        }
        
        # Note: Vehicles.csv from DataDefinitions only has ID columns, not Name columns
        # The actual CSV may have more - using case-insensitive matching as fallback
        vehicles_map = {
            'VehicleID': ('vehicle_id', 'integer'),
            'BaseVehicleID': ('base_vehicle_id', 'integer'),
            'YearID': ('year_id', 'integer'),
            'MakeID': ('make_id', 'integer'),
            'MakeName': ('make_name', 'string'),  # May not exist in all versions
            'ModelID': ('model_id', 'integer'),
            'ModelName': ('model_name', 'string'),  # May not exist in all versions
            'SubmodelID': ('submodel_id', 'integer'),
            'SubModelName': ('submodel_name', 'string'),  # May not exist in all versions
            'DriveTypeID': ('drive_type_id', 'integer'),
            'DriveTypeName': ('drive_type_name', 'string'),  # May not exist in all versions
            'BodyTypeID': ('body_type_id', 'integer'),
            'BodyTypeName': ('body_type_name', 'string'),  # May not exist in all versions
            'BodyNumDoorsID': ('body_num_doors_id', 'integer'),
            'BodyNumDoors': ('body_num_doors', 'string'),  # May not exist in all versions
            'BedLengthID': ('bed_length_id', 'integer'),
            'BedLength': ('bed_length', 'string'),  # May not exist in all versions
            'VehicleTypeID': ('vehicle_type_id', 'integer'),
            'VehicleTypeName': ('vehicle_type_name', 'string'),  # May not exist in all versions
            'RegionID': ('region_id', 'integer'),
            'RegionName': ('region_name', 'string'),  # May not exist in all versions
            'FG_CustomNote': ('fg_custom_note', 'string'),
            'FG_Body': ('fg_body', 'string'),
            'FG_Option': ('fg_option', 'string'),
            'FG_ChassisID': ('fg_chassis_id', 'integer'),
            'FG_ModelID': ('fg_model_id', 'integer'),
            'FG_FMK': ('fg_fmk', 'bigint'),
        }
        
        chassis_models_map = {
            'ChassisID': ('chassis_id', 'integer'),
            'ModelID': ('model_id', 'integer'),
            'PMetric': ('p_metric', 'string'),
            'TireSize': ('tire_size', 'string'),
            'LoadIndex': ('load_index', 'string'),
            'SpeedRating': ('speed_rating', 'string'),
            'TireSizeRear': ('tire_size_rear', 'string'),
            'LoadIndexRear': ('load_index_rear', 'string'),
            'SpeedRatingRear': ('speed_rating_rear', 'string'),
            'WheelSize': ('wheel_size', 'string'),
            'WheelSizeRear': ('wheel_size_rear', 'string'),
            'RunflatFront': ('runflat_front', 'string'),
            'RunflatRear': ('runflat_rear', 'string'),
            'ExtraLoadFront': ('extra_load_front', 'string'),
            'ExtraLoadRear': ('extra_load_rear', 'string'),
            'TPFrontPSI': ('tp_front_psi', 'string'),
            'TPRearPSI': ('tp_rear_psi', 'string'),
            'OffsetMinF': ('offset_min_f', 'numeric'),
            'OffsetMaxF': ('offset_max_f', 'numeric'),
            'OffsetMinR': ('offset_min_r', 'numeric'),
            'OffsetMaxR': ('offset_max_r', 'numeric'),
            'RimWidth': ('rim_width', 'numeric'),
            'RimDiameter': ('rim_diameter', 'numeric'),
        }
        
        plus_sizes_map = {
            'ChassisID': ('chassis_id', 'integer'),
            'PlusSizeType': ('plus_size_type', 'string'),
            'WheelSize': ('wheel_size', 'string'),
            'Tire1': ('tire_1', 'string'),
            'Tire2': ('tire_2', 'string'),
            'Tire3': ('tire_3', 'string'),
            'Tire4': ('tire_4', 'string'),
            'Tire5': ('tire_5', 'string'),
            'Tire6': ('tire_6', 'string'),
            'Tire7': ('tire_7', 'string'),
            'Tire8': ('tire_8', 'string'),
            'OffsetMin': ('offset_min', 'numeric'),
            'OffsetMax': ('offset_max', 'numeric'),
        }
        
        minus_sizes_map = {
            'ChassisID': ('chassis_id', 'integer'),
            'WheelSize': ('wheel_size', 'string'),
            'TireSize': ('tire_size', 'string'),
            'FrontRearOrBoth': ('front_rear_or_both', 'string'),
            'OffsetMin': ('offset_min', 'numeric'),
            'OffsetMax': ('offset_max', 'numeric'),
        }
        
        # Sync each table (order matters for foreign keys)
        sync_order = [
            ('Chassis.csv', 'tt_chassis', chassis_map),
            ('Vehicles.csv', 'tt_vehicles', vehicles_map),
            ('ChassisModels.csv', 'tt_chassis_models', chassis_models_map),
            ('PlusSizes.csv', 'tt_plus_sizes', plus_sizes_map),
            ('MinusSizes.csv', 'tt_minus_sizes', minus_sizes_map),
        ]
        
        total_inserted = 0
        total_errors = 0
        
        for csv_name, table_name, field_map in sync_order:
            if csv_name in csv_files:
                records = parse_csv_to_records(csv_files[csv_name])
                result = sync_motor_table(supabase, table_name, records, field_map)
                results['tables'][table_name] = result
                total_inserted += result['inserted']
                total_errors += result['errors']
            else:
                logger.warning(f"CSV not found in zip: {csv_name}")
                results['tables'][table_name] = {'error': 'CSV not found'}
        
        # Update sync log
        end_time = datetime.now(timezone.utc)
        results['status'] = 'completed'
        results['completed_at'] = end_time.isoformat()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        
        supabase.table('tire_data_sync_log').update({
            'status': 'completed',
            'source_file': latest_zip,
            'records_inserted': total_inserted,
            'completed_at': end_time.isoformat()
        }).eq('id', log_id).execute()
        
        # Send success alert
        send_alert(
            f"MOTOR Sync Complete",
            f"""Source: {latest_zip}
Duration: {results['duration_seconds']:.1f} seconds
Records Inserted: {total_inserted}
Errors: {total_errors}

Table Details:
{chr(10).join(f"  {t}: {r}" for t, r in results['tables'].items())}
""",
            is_error=False
        )
        
        return results
        
    except Exception as e:
        logger.error(f"MOTOR sync failed: {e}")
        results['status'] = 'failed'
        results['error'] = str(e)
        
        # Update sync log
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
            f"MOTOR Sync FAILED",
            f"""Error: {str(e)}

Please check the logs for details.
""",
            is_error=True
        )
        
        raise
        
    finally:
        # Clean up SFTP connection
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
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/sync/motor', methods=['POST'])
@require_api_key
def trigger_motor_sync():
    """
    Webhook endpoint to trigger MOTOR TireTech sync.
    Called by Zapier when new file is detected.
    """
    logger.info("MOTOR sync triggered via webhook")
    
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
    """
    Webhook endpoint to trigger USVenture inventory sync.
    TODO: Implement once USVenture file delivery is restored.
    """
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
