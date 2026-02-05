"""
TIRE SYNC SERVICE - v2.3
=========================
Syncs tire data from MOTOR and USVenture to Supabase.

Endpoints:
- POST /sync/motor     - Sync MOTOR TireTechSmart data (vehicle fitment)
- POST /sync/usventure - Sync USVenture inventory (pricing & stock)
- GET /health          - Health check
- GET /status          - Recent sync history

Environment Variables Required:
- MOTOR_FTP_HOST, MOTOR_FTP_USER, MOTOR_FTP_PASSWORD
- USVENTURE_FTP_HOST, USVENTURE_FTP_USER, USVENTURE_FTP_PASSWORD
- SUPABASE_URL, SUPABASE_SERVICE_KEY
- SENDGRID_API_KEY, ALERT_EMAIL
- SYNC_API_KEY (optional)

Changelog:
- v2.3 (2026-02-05): CRITICAL FIX - Download and validate data BEFORE truncating tables
                     Prevents data loss when SFTP connections fail
- v2.2 (2026-01-28): Added sync locking to prevent concurrent runs
- v2.1: Added USVenture inventory sync endpoint
"""

import os
import io
import re
import csv
import zipfile
import logging
from datetime import datetime, timezone, timedelta
from functools import wraps
from ftplib import FTP_TLS

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
    # MOTOR FTP (SFTP)
    MOTOR_FTP_HOST = os.environ.get('MOTOR_FTP_HOST', 'delivery.motor.com')
    MOTOR_FTP_USER = os.environ.get('MOTOR_FTP_USER', 'revyourcause_MIS')
    MOTOR_FTP_PASSWORD = os.environ.get('MOTOR_FTP_PASSWORD', '')
    MOTOR_FTP_PATH = '/Specifications_Data/TireTech/'
    
    # USVenture FTP (FTPS)
    USVENTURE_FTP_HOST = os.environ.get('USVENTURE_FTP_HOST', 'usventure.files.com')
    USVENTURE_FTP_USER = os.environ.get('USVENTURE_FTP_USER', '')
    USVENTURE_FTP_PASSWORD = os.environ.get('USVENTURE_FTP_PASSWORD', '')
    USVENTURE_FTP_PATH = '/USAutoForce/JiffyLube_C20219/'
    USVENTURE_FILENAME = 'USAutoForceInventory.csv'
    
    # Supabase
    SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
    SUPABASE_SERVICE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')
    
    # SendGrid
    SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY', '')
    ALERT_EMAIL = os.environ.get('ALERT_EMAIL', '')
    ALERT_FROM_EMAIL = os.environ.get('ALERT_FROM_EMAIL', 'sporcher@myjiffytires.com')
    
    # Security
    SYNC_API_KEY = os.environ.get('SYNC_API_KEY', '')
    
    # Warehouse mapping
    WAREHOUSE_FRESNO = '4703'
    WAREHOUSE_SANTA_CLARITA = '4708'
    
    # Sync lock settings
    SYNC_LOCK_TIMEOUT_MINUTES = 10  # Consider a sync "stuck" after this many minutes
    
    # Minimum record thresholds for validation (prevents loading empty/corrupt files)
    MIN_MOTOR_RECORDS = 100000  # MOTOR file should have ~124k records
    MIN_USVENTURE_RECORDS = 5000  # USVenture file should have ~12k unique parts


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
# SYNC LOCKING
# =============================================================================

def check_sync_lock(supabase: Client, sync_type: str) -> dict:
    """
    Check if a sync of the given type is already running.
    
    Returns:
        dict with keys:
        - locked: bool - True if sync should be blocked
        - message: str - Explanation
        - stale_id: int or None - ID of stale lock to clean up
    """
    timeout_threshold = datetime.now(timezone.utc) - timedelta(minutes=Config.SYNC_LOCK_TIMEOUT_MINUTES)
    
    # Check for any running syncs of this type
    result = supabase.table('tire_data_sync_log')\
        .select('id, started_at, status')\
        .eq('sync_type', sync_type)\
        .eq('status', 'running')\
        .order('started_at', desc=True)\
        .limit(1)\
        .execute()
    
    if not result.data:
        # No running syncs, we're clear
        return {'locked': False, 'message': 'No active sync', 'stale_id': None}
    
    running_sync = result.data[0]
    started_at_str = running_sync['started_at']
    
    # Parse the timestamp
    if isinstance(started_at_str, str):
        # Handle various timestamp formats
        started_at_str = started_at_str.replace('+00:00', '+0000').replace('Z', '+0000')
        try:
            started_at = datetime.strptime(started_at_str[:26] + started_at_str[-5:], '%Y-%m-%dT%H:%M:%S.%f%z')
        except ValueError:
            try:
                started_at = datetime.strptime(started_at_str[:19], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
            except ValueError:
                # If we can't parse, assume it's stale
                logger.warning(f"Could not parse timestamp: {started_at_str}, treating as stale")
                return {
                    'locked': False, 
                    'message': f'Found stale sync (unparseable timestamp), cleaning up',
                    'stale_id': running_sync['id']
                }
    else:
        started_at = started_at_str
    
    # Check if the running sync is stale (older than timeout threshold)
    if started_at < timeout_threshold:
        logger.warning(f"Found stale sync lock (ID: {running_sync['id']}, started: {started_at})")
        return {
            'locked': False,
            'message': f'Found stale sync (started {started_at}), cleaning up and proceeding',
            'stale_id': running_sync['id']
        }
    
    # There's an active, recent sync running
    minutes_running = (datetime.now(timezone.utc) - started_at).total_seconds() / 60
    return {
        'locked': True,
        'message': f'Sync already in progress (ID: {running_sync["id"]}, running for {minutes_running:.1f} minutes)',
        'stale_id': None
    }


def cleanup_stale_lock(supabase: Client, stale_id: int):
    """Mark a stale sync as failed."""
    try:
        supabase.table('tire_data_sync_log').update({
            'status': 'failed',
            'error_message': 'Marked as failed - sync exceeded timeout threshold (presumed stuck/crashed)',
            'completed_at': datetime.now(timezone.utc).isoformat()
        }).eq('id', stale_id).execute()
        logger.info(f"Cleaned up stale sync lock (ID: {stale_id})")
    except Exception as e:
        logger.error(f"Failed to clean up stale lock: {e}")


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
# MOTOR SFTP FUNCTIONS
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
    
    pattern = re.compile(r'^MOTOR_TireTechSmart_(\d{8})\.zip$')
    zip_files = []
    
    for f in files:
        match = pattern.match(f)
        if match:
            date_str = match.group(1)
            zip_files.append((f, date_str))
    
    if not zip_files:
        raise FileNotFoundError("No MOTOR_TireTechSmart_*.zip files found")
    
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
            for name in zf.namelist():
                if 'tire-smart-submodel-vehicles' in name.lower() and name.endswith('.csv'):
                    logger.info(f"  Extracting {name}")
                    return zf.read(name).decode('utf-8-sig')
            
            logger.error(f"  Files in zip: {zf.namelist()}")
            raise FileNotFoundError("tire-smart-submodel-vehicles.csv not found in zip")


# =============================================================================
# USVENTURE FTPS FUNCTIONS
# =============================================================================

def connect_usventure_ftps():
    """Establish FTPS connection to USVenture."""
    logger.info(f"Connecting to USVenture FTPS: {Config.USVENTURE_FTP_HOST}")
    
    ftp = FTP_TLS()
    ftp.connect(Config.USVENTURE_FTP_HOST, 21)
    ftp.login(Config.USVENTURE_FTP_USER, Config.USVENTURE_FTP_PASSWORD)
    ftp.prot_p()  # Switch to secure data connection
    
    logger.info("USVenture FTPS connection established")
    return ftp


def download_usventure_csv(ftp) -> str:
    """Download the USAutoForceInventory.csv file."""
    ftp.cwd(Config.USVENTURE_FTP_PATH)
    
    logger.info(f"Downloading {Config.USVENTURE_FILENAME}...")
    
    csv_buffer = io.BytesIO()
    ftp.retrbinary(f'RETR {Config.USVENTURE_FILENAME}', csv_buffer.write)
    csv_buffer.seek(0)
    
    # Decode the CSV content
    content = csv_buffer.read().decode('utf-8-sig')
    logger.info(f"Downloaded {len(content)} bytes")
    
    return content


# =============================================================================
# DATA SYNC - MOTOR
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


def prepare_smart_vehicles_data(csv_content: str) -> list:
    """
    Parse and transform MOTOR Smart Vehicles CSV into database records.
    Returns list of transformed records ready for insert.
    Does NOT touch the database - just prepares the data.
    """
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
    
    records = []
    reader = csv.DictReader(io.StringIO(csv_content))
    first_row = True
    
    for row in reader:
        normalized = {k.strip(): v for k, v in row.items()}
        
        if first_row:
            logger.info(f"  CSV columns: {list(normalized.keys())}")
            first_row = False
        
        transformed = {}
        for csv_col, (db_col, field_type) in field_map.items():
            value = normalized.get(csv_col)
            transformed[db_col] = clean_value(value, field_type)
        
        records.append(transformed)
    
    logger.info(f"  Prepared {len(records):,} records from CSV")
    return records


def insert_smart_vehicles(supabase: Client, records: list) -> dict:
    """
    Truncate table and insert prepared records.
    Only called AFTER data has been downloaded and validated.
    """
    batch_size = 5000
    inserted = 0
    errors = 0
    total_records = len(records)
    
    # NOW we truncate - only after we have validated data ready
    logger.info("  Truncating tt_smart_vehicles...")
    supabase.table('tt_smart_vehicles').delete().neq('created_at', '1900-01-01').execute()
    
    # Insert in batches
    for i in range(0, total_records, batch_size):
        batch = records[i:i + batch_size]
        try:
            supabase.table('tt_smart_vehicles').insert(batch).execute()
            inserted += len(batch)
            logger.info(f"  Inserted batch: {inserted:,} / {total_records:,} records")
        except Exception as e:
            logger.error(f"  Error inserting batch: {e}")
            errors += len(batch)
    
    logger.info(f"  Completed: {inserted:,} inserted, {errors:,} errors")
    return {'inserted': inserted, 'errors': errors, 'total': total_records}


# =============================================================================
# DATA SYNC - USVENTURE
# =============================================================================

def prepare_usventure_data(csv_content: str) -> list:
    """
    Parse and transform USVenture inventory CSV into database records.
    Aggregates quantities by warehouse (Fresno 4703, Santa Clarita 4708).
    Returns list of transformed records ready for insert.
    Does NOT touch the database - just prepares the data.
    """
    total_records = 0
    
    # Dictionary to aggregate by part_number
    inventory_map = {}
    
    reader = csv.DictReader(io.StringIO(csv_content))
    first_row = True
    
    for row in reader:
        total_records += 1
        
        if first_row:
            logger.info(f"  CSV columns: {list(row.keys())}")
            first_row = False
        
        part_number = row.get('PartNumber', '').strip()
        if not part_number:
            continue
        
        warehouse_code = row.get('D365WarehouseCode', '').strip()
        quantity = clean_value(row.get('QuantityAvailable', '0'), 'numeric') or 0
        
        # If we haven't seen this part number yet, create the record
        if part_number not in inventory_map:
            inventory_map[part_number] = {
                'part_number': part_number,
                'brand_code': clean_value(row.get('BrandCode'), 'string'),
                'sales_class': clean_value(row.get('SalesClass'), 'string'),
                'upc': clean_value(row.get('UPC'), 'string'),
                'discontinued': clean_value(row.get('DiscontinuedFlag', 'False'), 'boolean'),
                'is_idle': clean_value(row.get('IsIdle', 'False'), 'boolean'),
                'tire_type': clean_value(row.get('TireType'), 'string'),
                'name': clean_value(row.get('Name'), 'string'),
                'description': clean_value(row.get('Description'), 'string'),
                'width': clean_value(row.get('Width'), 'integer'),
                'aspect_ratio': clean_value(row.get('AspectRatio'), 'integer'),
                'rim_diameter': clean_value(row.get('Rim'), 'integer'),
                'tire_size': clean_value(row.get('TireSize'), 'string'),
                'speed_rating': clean_value(row.get('SpeedRating'), 'string'),
                'load_rating': clean_value(row.get('LoadRating'), 'string'),
                'load_range': clean_value(row.get('LoadRange'), 'string'),
                'ply_rating': clean_value(row.get('PlyRating'), 'string'),
                'utqg': clean_value(row.get('UTQG'), 'string'),
                'load_capacity': clean_value(row.get('LoadCapacity'), 'string'),
                'weight': clean_value(row.get('Weight'), 'numeric'),
                'tread_depth': clean_value(row.get('TreadDepth'), 'string'),
                'sidewall': clean_value(row.get('Sidewall'), 'string'),
                'ev_compatible': clean_value(row.get('EVCompatible', 'False'), 'boolean'),
                'run_flat': clean_value(row.get('RunFlat', 'False'), 'boolean'),
                'snowflake': clean_value(row.get('Snowflake', 'False'), 'boolean'),
                'noise_canceling': clean_value(row.get('NoiseCancelingTechnology', 'False'), 'boolean'),
                'warranty': clean_value(row.get('Warranty'), 'string'),
                'fet': clean_value(row.get('FET', '0'), 'numeric'),
                'cost': clean_value(row.get('Cost'), 'numeric'),
                'retail_price': clean_value(row.get('RetailPrice'), 'numeric'),
                'map_price': clean_value(row.get('Map'), 'numeric'),
                'account_number': clean_value(row.get('AccountNumber'), 'string'),
                'qty_fresno': 0,
                'qty_santa_clarita': 0,
                'last_synced_at': datetime.now(timezone.utc).isoformat(),
            }
        
        # Add quantity to appropriate warehouse
        if warehouse_code == Config.WAREHOUSE_FRESNO:
            inventory_map[part_number]['qty_fresno'] += quantity
        elif warehouse_code == Config.WAREHOUSE_SANTA_CLARITA:
            inventory_map[part_number]['qty_santa_clarita'] += quantity
        else:
            # Unknown warehouse - add to general quantity
            inventory_map[part_number]['qty_fresno'] += quantity
            logger.warning(f"  Unknown warehouse code: {warehouse_code} for part {part_number}")
    
    records = list(inventory_map.values())
    logger.info(f"  Prepared {len(records):,} unique parts from {total_records:,} CSV rows")
    
    return records


def insert_usventure_inventory(supabase: Client, records: list) -> dict:
    """
    Truncate table and insert prepared records.
    Only called AFTER data has been downloaded and validated.
    """
    batch_size = 2000
    inserted = 0
    errors = 0
    total_records = len(records)
    
    # NOW we truncate - only after we have validated data ready
    logger.info("  Truncating tire_inventory...")
    supabase.table('tire_inventory').delete().neq('created_at', '1900-01-01').execute()
    
    # Insert in batches
    for i in range(0, total_records, batch_size):
        batch = records[i:i + batch_size]
        try:
            supabase.table('tire_inventory').insert(batch).execute()
            inserted += len(batch)
            logger.info(f"  Inserted batch: {inserted:,} / {total_records:,} records")
        except Exception as e:
            logger.error(f"  Error inserting batch: {e}")
            errors += len(batch)
    
    logger.info(f"  Completed: {inserted:,} inserted, {errors:,} errors")
    
    return {
        'unique_parts': total_records,
        'inserted': inserted,
        'errors': errors
    }


# =============================================================================
# SYNC ORCHESTRATION
# =============================================================================

def sync_motor_data():
    """
    Main function to sync MOTOR TireTechSmart data.
    
    SAFE SYNC PATTERN:
    1. Download data from SFTP
    2. Parse and validate data (check minimum record count)
    3. Only if validation passes: truncate and insert
    4. If any step fails before truncate: existing data is preserved
    """
    supabase = get_supabase()
    
    # =========================================================================
    # CHECK SYNC LOCK - Prevent concurrent runs
    # =========================================================================
    lock_status = check_sync_lock(supabase, 'motor_tiretech_smart')
    
    if lock_status['locked']:
        logger.warning(f"MOTOR sync blocked: {lock_status['message']}")
        return {
            'status': 'skipped',
            'reason': lock_status['message'],
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    # Clean up stale lock if found
    if lock_status['stale_id']:
        cleanup_stale_lock(supabase, lock_status['stale_id'])
    
    # =========================================================================
    # PROCEED WITH SYNC
    # =========================================================================
    start_time = datetime.now(timezone.utc)
    results = {
        'status': 'running',
        'started_at': start_time.isoformat(),
        'source_file': None,
        'tables': {}
    }
    
    sftp = None
    transport = None
    log_id = None
    
    try:
        log_entry = supabase.table('tire_data_sync_log').insert({
            'sync_type': 'motor_tiretech_smart',
            'status': 'running',
            'started_at': start_time.isoformat()
        }).execute()
        log_id = log_entry.data[0]['id']
        
        # =====================================================================
        # STEP 1: DOWNLOAD - If this fails, existing data is preserved
        # =====================================================================
        logger.info("Step 1: Connecting to MOTOR SFTP and downloading...")
        sftp, transport = connect_motor_sftp()
        latest_zip = find_latest_smart_zip(sftp)
        results['source_file'] = latest_zip
        
        csv_content = download_and_extract_smart_csv(sftp, latest_zip)
        logger.info(f"  Download complete: {len(csv_content):,} bytes")
        
        # =====================================================================
        # STEP 2: PARSE AND VALIDATE - If this fails, existing data is preserved
        # =====================================================================
        logger.info("Step 2: Parsing and validating data...")
        prepared_records = prepare_smart_vehicles_data(csv_content)
        
        # Validate minimum record count
        if len(prepared_records) < Config.MIN_MOTOR_RECORDS:
            raise ValueError(
                f"Data validation failed: Only {len(prepared_records):,} records found, "
                f"minimum required is {Config.MIN_MOTOR_RECORDS:,}. "
                f"Aborting to preserve existing data."
            )
        
        logger.info(f"  Validation passed: {len(prepared_records):,} records ready")
        
        # =====================================================================
        # STEP 3: TRUNCATE AND INSERT - Only reached if download & validation passed
        # =====================================================================
        logger.info("Step 3: Truncating table and inserting new data...")
        result = insert_smart_vehicles(supabase, prepared_records)
        results['tables']['tt_smart_vehicles'] = result
        
        # =====================================================================
        # SUCCESS
        # =====================================================================
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
        
        send_alert(
            f"MOTOR Smart Sync Complete",
            f"""Source: {latest_zip}
Duration: {results['duration_seconds']:.1f} seconds
Records: {result['inserted']:,} inserted, {result['errors']:,} errors

Safe sync pattern: Data validated before truncate.
""",
            is_error=False
        )
        
        return results
        
    except Exception as e:
        logger.error(f"MOTOR sync failed: {e}")
        results['status'] = 'failed'
        results['error'] = str(e)
        
        if log_id:
            try:
                supabase.table('tire_data_sync_log').update({
                    'status': 'failed',
                    'error_message': str(e),
                    'completed_at': datetime.now(timezone.utc).isoformat()
                }).eq('id', log_id).execute()
            except:
                pass
        
        send_alert(
            f"MOTOR Smart Sync FAILED",
            f"""Error: {str(e)}

IMPORTANT: Existing data was preserved (truncate only happens after successful download & validation).

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


def sync_usventure_data():
    """
    Main function to sync USVenture inventory data.
    
    SAFE SYNC PATTERN:
    1. Download data from FTPS
    2. Parse and validate data (check minimum record count)
    3. Only if validation passes: truncate and insert
    4. If any step fails before truncate: existing data is preserved
    """
    supabase = get_supabase()
    
    # =========================================================================
    # CHECK SYNC LOCK - Prevent concurrent runs
    # =========================================================================
    lock_status = check_sync_lock(supabase, 'usventure_inventory')
    
    if lock_status['locked']:
        logger.warning(f"USVenture sync blocked: {lock_status['message']}")
        return {
            'status': 'skipped',
            'reason': lock_status['message'],
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    # Clean up stale lock if found
    if lock_status['stale_id']:
        cleanup_stale_lock(supabase, lock_status['stale_id'])
    
    # =========================================================================
    # PROCEED WITH SYNC
    # =========================================================================
    start_time = datetime.now(timezone.utc)
    results = {
        'status': 'running',
        'started_at': start_time.isoformat(),
        'source_file': Config.USVENTURE_FILENAME,
    }
    
    ftp = None
    log_id = None
    
    try:
        log_entry = supabase.table('tire_data_sync_log').insert({
            'sync_type': 'usventure_inventory',
            'status': 'running',
            'source_file': Config.USVENTURE_FILENAME,
            'started_at': start_time.isoformat()
        }).execute()
        log_id = log_entry.data[0]['id']
        
        # =====================================================================
        # STEP 1: DOWNLOAD - If this fails, existing data is preserved
        # =====================================================================
        logger.info("Step 1: Connecting to USVenture FTPS and downloading...")
        ftp = connect_usventure_ftps()
        csv_content = download_usventure_csv(ftp)
        logger.info(f"  Download complete: {len(csv_content):,} bytes")
        
        # =====================================================================
        # STEP 2: PARSE AND VALIDATE - If this fails, existing data is preserved
        # =====================================================================
        logger.info("Step 2: Parsing and validating data...")
        prepared_records = prepare_usventure_data(csv_content)
        
        # Validate minimum record count
        if len(prepared_records) < Config.MIN_USVENTURE_RECORDS:
            raise ValueError(
                f"Data validation failed: Only {len(prepared_records):,} unique parts found, "
                f"minimum required is {Config.MIN_USVENTURE_RECORDS:,}. "
                f"Aborting to preserve existing data."
            )
        
        logger.info(f"  Validation passed: {len(prepared_records):,} unique parts ready")
        
        # =====================================================================
        # STEP 3: TRUNCATE AND INSERT - Only reached if download & validation passed
        # =====================================================================
        logger.info("Step 3: Truncating table and inserting new data...")
        result = insert_usventure_inventory(supabase, prepared_records)
        results['sync_result'] = result
        
        # =====================================================================
        # SUCCESS
        # =====================================================================
        end_time = datetime.now(timezone.utc)
        results['status'] = 'completed'
        results['completed_at'] = end_time.isoformat()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        
        supabase.table('tire_data_sync_log').update({
            'status': 'completed',
            'records_processed': len(prepared_records),
            'records_inserted': result['inserted'],
            'completed_at': end_time.isoformat()
        }).eq('id', log_id).execute()
        
        send_alert(
            f"USVenture Inventory Sync Complete",
            f"""Source: {Config.USVENTURE_FILENAME}
Duration: {results['duration_seconds']:.1f} seconds
Unique Parts: {result['unique_parts']:,}
Inserted: {result['inserted']:,}
Errors: {result['errors']:,}

Warehouse Breakdown:
- Fresno (4703): Aggregated
- Santa Clarita (4708): Aggregated

Safe sync pattern: Data validated before truncate.
""",
            is_error=False
        )
        
        return results
        
    except Exception as e:
        logger.error(f"USVenture sync failed: {e}")
        results['status'] = 'failed'
        results['error'] = str(e)
        
        if log_id:
            try:
                supabase.table('tire_data_sync_log').update({
                    'status': 'failed',
                    'error_message': str(e),
                    'completed_at': datetime.now(timezone.utc).isoformat()
                }).eq('id', log_id).execute()
            except:
                pass
        
        send_alert(
            f"USVenture Inventory Sync FAILED",
            f"""Error: {str(e)}

IMPORTANT: Existing data was preserved (truncate only happens after successful download & validation).

Please check the logs for details.
""",
            is_error=True
        )
        
        raise
        
    finally:
        if ftp:
            try:
                ftp.quit()
            except:
                ftp.close()


# =============================================================================
# ROUTES
# =============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'service': 'tire-sync-service',
        'version': '2.3',
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/sync/motor', methods=['POST'])
@require_api_key
def trigger_motor_sync():
    """Webhook endpoint to trigger MOTOR TireTechSmart sync."""
    logger.info("MOTOR Smart sync triggered via webhook")
    
    try:
        results = sync_motor_data()
        
        # Return 200 for skipped syncs (not an error, just blocked by lock)
        if results.get('status') == 'skipped':
            return jsonify(results), 200
        
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
    logger.info("USVenture inventory sync triggered via webhook")
    
    try:
        results = sync_usventure_data()
        
        # Return 200 for skipped syncs (not an error, just blocked by lock)
        if results.get('status') == 'skipped':
            return jsonify(results), 200
        
        return jsonify(results), 200
        
    except Exception as e:
        logger.error(f"USVenture sync failed: {e}")
        return jsonify({
            'status': 'failed',
            'error': str(e)
        }), 500


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
