"""
TIRE SYNC SERVICE - v3.1
=========================
Syncs tire data from MOTOR and USVenture to Supabase.

Endpoints:
- POST /sync/motor     - Sync MOTOR TireTechSmart data (vehicle fitment)
- POST /sync/usventure - Sync USVenture inventory (pricing & stock)
- POST /sync/ewt       - Sync MOTOR GEN4.5 Mechanical EWT (labor times)
- GET /health          - Health check
- GET /status          - Recent sync history

Environment Variables Required:
- MOTOR_FTP_HOST, MOTOR_FTP_USER, MOTOR_FTP_PASSWORD
- USVENTURE_FTP_HOST, USVENTURE_FTP_USER, USVENTURE_FTP_PASSWORD
- SUPABASE_URL, SUPABASE_SERVICE_KEY
- SENDGRID_API_KEY, ALERT_EMAIL
- SYNC_API_KEY (optional)

Changelog:
- v3.1 (2026-03-18): EWT streaming rewrite - process one make at a time to fix OOM.
                      Peak memory now = zip buffer (~254MB) + one make's records.
                      Confirmed dataset: 57 makes, ~1.3M labor + application records.
- v3.0 (2026-03-18): Added MOTOR GEN4.5 Mechanical EWT sync (/sync/ewt).
                      Loads all make files into ewt_labor + ewt_applications tables.
- v2.5 (2026-02-18): SFTP timeout hardening - explicit connect/channel timeouts,
                      keepalive packets, exponential backoff (30/60/120/240s), 4 retry attempts
- v2.4 (2026-02-05): Added automatic retry logic (3 attempts, 30s delay) for connection failures
- v2.3 (2026-02-05): CRITICAL FIX - Download and validate data BEFORE truncating tables
- v2.2 (2026-01-28): Added sync locking to prevent concurrent runs
- v2.1: Added USVenture inventory sync endpoint
"""

import gc
import os
import io
import re
import csv
import time
import zipfile
import logging
from datetime import datetime, timezone, timedelta
from functools import wraps
from ftplib import FTP_TLS

import socket
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
    MOTOR_EWT_PATH = '/ECommerce_Data/GEN4.5_MechLabor/'

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
    # EWT sync can run 5-15 min — use longer stale threshold than other syncs
    SYNC_LOCK_TIMEOUT_MINUTES = 120

    # Minimum record thresholds for validation
    MIN_MOTOR_RECORDS = 100000      # MOTOR TireTech file should have ~124k records
    MIN_USVENTURE_RECORDS = 5000    # USVenture file should have ~12k unique parts
    MIN_EWT_MAKE_FILES = 50         # EWT zip should have at least 50 make pairs
                                    # (confirmed 57 makes in Jan 2026 dataset)

    # Retry settings
    MAX_RETRY_ATTEMPTS = 4
    RETRY_DELAY_SECONDS = 30  # Base delay (exponential backoff: 30, 60, 120, 240)

    # SFTP/FTPS timeout settings (seconds)
    SFTP_CONNECT_TIMEOUT = 30       # Max time to establish SSH/FTP connection
    SFTP_CHANNEL_TIMEOUT = 600      # Max time for file download operations
                                    # EWT zip is ~254MB - needs longer timeout


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

    result = supabase.table('tire_data_sync_log')\
        .select('id, started_at, status')\
        .eq('sync_type', sync_type)\
        .eq('status', 'running')\
        .order('started_at', desc=True)\
        .limit(1)\
        .execute()

    if not result.data:
        return {'locked': False, 'message': 'No active sync', 'stale_id': None}

    running_sync = result.data[0]
    started_at_str = running_sync['started_at']

    if isinstance(started_at_str, str):
        started_at_str = started_at_str.replace('+00:00', '+0000').replace('Z', '+0000')
        try:
            started_at = datetime.strptime(started_at_str[:26] + started_at_str[-5:], '%Y-%m-%dT%H:%M:%S.%f%z')
        except ValueError:
            try:
                started_at = datetime.strptime(started_at_str[:19], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
            except ValueError:
                logger.warning(f"Could not parse timestamp: {started_at_str}, treating as stale")
                return {
                    'locked': False,
                    'message': 'Found stale sync (unparseable timestamp), cleaning up',
                    'stale_id': running_sync['id']
                }
    else:
        started_at = started_at_str

    if started_at < timeout_threshold:
        logger.warning(f"Found stale sync lock (ID: {running_sync['id']}, started: {started_at})")
        return {
            'locked': False,
            'message': f'Found stale sync (started {started_at}), cleaning up and proceeding',
            'stale_id': running_sync['id']
        }

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
    """Establish SFTP connection to MOTOR with explicit timeouts."""
    logger.info(f"Connecting to MOTOR SFTP: {Config.MOTOR_FTP_HOST}")

    sock = socket.create_connection(
        (Config.MOTOR_FTP_HOST, 22),
        timeout=Config.SFTP_CONNECT_TIMEOUT
    )

    transport = paramiko.Transport(sock)
    transport.set_keepalive(15)
    transport.connect(username=Config.MOTOR_FTP_USER, password=Config.MOTOR_FTP_PASSWORD)

    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.get_channel().settimeout(Config.SFTP_CHANNEL_TIMEOUT)

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


def download_motor_data_with_retry() -> tuple:
    """
    Download MOTOR TireTechSmart data with automatic retry on connection failures.

    Returns:
        tuple: (csv_content: str, source_file: str)
    """
    last_error = None

    for attempt in range(1, Config.MAX_RETRY_ATTEMPTS + 1):
        sftp = None
        transport = None

        try:
            logger.info(f"MOTOR download attempt {attempt}/{Config.MAX_RETRY_ATTEMPTS}")

            sftp, transport = connect_motor_sftp()
            latest_zip = find_latest_smart_zip(sftp)
            csv_content = download_and_extract_smart_csv(sftp, latest_zip)

            logger.info(f"  Download successful on attempt {attempt}")
            return csv_content, latest_zip

        except Exception as e:
            last_error = e
            logger.warning(f"  Attempt {attempt} failed: {str(e)}")

            if attempt < Config.MAX_RETRY_ATTEMPTS:
                delay = Config.RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
                logger.info(f"  Waiting {delay} seconds before retry (exponential backoff)...")
                time.sleep(delay)

        finally:
            if sftp:
                try:
                    sftp.close()
                except:
                    pass
            if transport:
                try:
                    transport.close()
                except:
                    pass

    raise Exception(f"MOTOR download failed after {Config.MAX_RETRY_ATTEMPTS} attempts. Last error: {str(last_error)}")


# =============================================================================
# USVENTURE FTPS FUNCTIONS
# =============================================================================

def connect_usventure_ftps():
    """Establish FTPS connection to USVenture with explicit timeout."""
    logger.info(f"Connecting to USVenture FTPS: {Config.USVENTURE_FTP_HOST}")

    ftp = FTP_TLS()
    ftp.connect(Config.USVENTURE_FTP_HOST, 21, timeout=Config.SFTP_CONNECT_TIMEOUT)
    ftp.login(Config.USVENTURE_FTP_USER, Config.USVENTURE_FTP_PASSWORD)
    ftp.prot_p()

    logger.info("USVenture FTPS connection established")
    return ftp


def download_usventure_csv(ftp) -> str:
    """Download the USAutoForceInventory.csv file."""
    ftp.cwd(Config.USVENTURE_FTP_PATH)

    logger.info(f"Downloading {Config.USVENTURE_FILENAME}...")

    csv_buffer = io.BytesIO()
    ftp.retrbinary(f'RETR {Config.USVENTURE_FILENAME}', csv_buffer.write)
    csv_buffer.seek(0)

    content = csv_buffer.read().decode('utf-8-sig')
    logger.info(f"Downloaded {len(content)} bytes")

    return content


def download_usventure_data_with_retry() -> str:
    """
    Download USVenture data with automatic retry on connection failures.

    Returns:
        str: CSV content
    """
    last_error = None

    for attempt in range(1, Config.MAX_RETRY_ATTEMPTS + 1):
        ftp = None

        try:
            logger.info(f"USVenture download attempt {attempt}/{Config.MAX_RETRY_ATTEMPTS}")

            ftp = connect_usventure_ftps()
            csv_content = download_usventure_csv(ftp)

            logger.info(f"  Download successful on attempt {attempt}")
            return csv_content

        except Exception as e:
            last_error = e
            logger.warning(f"  Attempt {attempt} failed: {str(e)}")

            if attempt < Config.MAX_RETRY_ATTEMPTS:
                delay = Config.RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
                logger.info(f"  Waiting {delay} seconds before retry (exponential backoff)...")
                time.sleep(delay)

        finally:
            if ftp:
                try:
                    ftp.quit()
                except:
                    try:
                        ftp.close()
                    except:
                        pass

    raise Exception(f"USVenture download failed after {Config.MAX_RETRY_ATTEMPTS} attempts. Last error: {str(last_error)}")


# =============================================================================
# SHARED DATA UTILITY
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


# =============================================================================
# DATA SYNC - MOTOR TIRETECH SMART
# =============================================================================

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

    logger.info("  Truncating tt_smart_vehicles...")
    supabase.table('tt_smart_vehicles').delete().neq('created_at', '1900-01-01').execute()

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

        if warehouse_code == Config.WAREHOUSE_FRESNO:
            inventory_map[part_number]['qty_fresno'] += quantity
        elif warehouse_code == Config.WAREHOUSE_SANTA_CLARITA:
            inventory_map[part_number]['qty_santa_clarita'] += quantity
        else:
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

    logger.info("  Truncating tire_inventory...")
    supabase.table('tire_inventory').delete().neq('created_at', '1900-01-01').execute()

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
# DATA SYNC - MOTOR EWT (GEN4.5 Mechanical Estimated Work Times)
# =============================================================================

def find_latest_ewt_zip(sftp) -> str:
    """
    Find the latest Mechanical_EWT_ACES_*.zip in the GEN4.5_MechLabor directory.
    Falls back to any .zip if date-stamped pattern not found.
    """
    sftp.chdir(Config.MOTOR_EWT_PATH)
    files = sftp.listdir()

    logger.info(f"  Files in EWT directory: {files}")

    pattern = re.compile(r'^Mechanical_EWT_ACES_(\d{8})\.zip$', re.IGNORECASE)
    zip_files = []

    for f in files:
        match = pattern.match(f)
        if match:
            date_str = match.group(1)
            zip_files.append((f, date_str))

    if zip_files:
        zip_files.sort(key=lambda x: x[1], reverse=True)
        latest_file = zip_files[0][0]
        logger.info(f"  Found latest EWT file: {latest_file}")
        return latest_file

    zip_fallback = [f for f in files if f.lower().endswith('.zip')]
    if zip_fallback:
        logger.info(f"  Using fallback zip: {zip_fallback[0]}")
        return zip_fallback[0]

    raise FileNotFoundError(
        f"No EWT zip file found in {Config.MOTOR_EWT_PATH}. Files present: {files}"
    )


def download_ewt_zip_to_buffer(sftp, filename: str) -> tuple:
    """
    Download EWT zip into memory and build an index of make pairs.

    The full zip bytes are held in RAM (~254MB). Individual make file contents
    are NOT decoded here — they are read one at a time during streaming insert.

    Returns:
        tuple: (
            zip_bytes: bytes,
            source_file: str,
            make_pairs: list of (make_name, labor_zip_path, app_zip_path)
        )
    """
    logger.info(f"  Downloading EWT zip: {filename} (~254MB, may take a minute)...")

    zip_buffer = io.BytesIO()
    sftp.getfo(filename, zip_buffer)
    zip_bytes = zip_buffer.getvalue()
    logger.info(f"  Download complete: {len(zip_bytes):,} bytes")

    make_pairs = []
    skipped = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as zf:
        all_names = zf.namelist()
        logger.info(f"  Zip contains {len(all_names)} entries")

        # Build a basename → full path lookup
        name_map = {}
        for full_path in all_names:
            basename = os.path.basename(full_path)
            if basename:
                name_map[basename] = full_path

        # Find all labor files and pair with their application file
        for basename, full_path in name_map.items():
            if not basename.lower().endswith('.txt'):
                skipped.append(basename)
                continue
            if '_Application_VCdbAttribute_xRef' in basename:
                # Skip engine disambiguation files (Phase 2)
                skipped.append(basename)
                continue
            if '_Application.txt' in basename:
                # Will be paired from the labor side
                continue

            # This is a labor file: e.g. "FIAT.txt"
            make_name = basename[:-len('.txt')]
            app_basename = f"{make_name}_Application.txt"
            app_path = name_map.get(app_basename)

            if not app_path:
                logger.warning(f"  No application file found for {make_name}, skipping")
                skipped.append(basename)
                continue

            # Skip near-empty files (e.g. Yugo at 80 bytes)
            labor_info = zf.getinfo(full_path)
            if labor_info.file_size < 100:
                logger.info(f"  Skipping near-empty file: {basename} ({labor_info.file_size} bytes)")
                skipped.append(basename)
                continue

            make_pairs.append((make_name, full_path, app_path))

    logger.info(f"  Indexed {len(make_pairs)} make pairs, {len(skipped)} skipped")
    return zip_bytes, filename, make_pairs


def download_ewt_data_with_retry() -> tuple:
    """
    Download MOTOR EWT zip with automatic retry on connection failures.

    Returns:
        tuple: (zip_bytes: bytes, source_file: str, make_pairs: list)
    """
    last_error = None

    for attempt in range(1, Config.MAX_RETRY_ATTEMPTS + 1):
        sftp = None
        transport = None

        try:
            logger.info(f"EWT download attempt {attempt}/{Config.MAX_RETRY_ATTEMPTS}")

            sftp, transport = connect_motor_sftp()
            latest_zip = find_latest_ewt_zip(sftp)
            zip_bytes, source_file, make_pairs = download_ewt_zip_to_buffer(sftp, latest_zip)

            logger.info(f"  EWT download successful on attempt {attempt}")
            return zip_bytes, source_file, make_pairs

        except Exception as e:
            last_error = e
            logger.warning(f"  Attempt {attempt} failed: {str(e)}")

            if attempt < Config.MAX_RETRY_ATTEMPTS:
                delay = Config.RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
                logger.info(f"  Waiting {delay} seconds before retry (exponential backoff)...")
                time.sleep(delay)

        finally:
            if sftp:
                try:
                    sftp.close()
                except:
                    pass
            if transport:
                try:
                    transport.close()
                except:
                    pass

    raise Exception(f"EWT download failed after {Config.MAX_RETRY_ATTEMPTS} attempts. Last error: {str(last_error)}")


def parse_ewt_labor_file(make_name: str, content: str) -> list:
    """
    Parse one [Make].txt labor file into records for ewt_labor.

    Pipe-delimited columns:
      MechanicalEstimatingID | Motor_DB_Section | Motor_DB_Group | Motor_DB_SubGroup |
      Motor_DB_Operation | QualifierDescription | FactoryTime | MOTORTime |
      IsAdditionalOperation | SectionApplication | MOTOR_DB_Description |
      SkillCode | Motor_DB_Footnote
    """
    records = []
    reader = csv.DictReader(io.StringIO(content), delimiter='|')

    for row in reader:
        normalized = {k.strip().lstrip('\ufeff'): (v.strip() if v else v)
                      for k, v in row.items()}

        mechanical_estimating_id = clean_value(
            normalized.get('MechanicalEstimatingID'), 'bigint'
        )
        if not mechanical_estimating_id:
            continue

        records.append({
            'mechanical_estimating_id': mechanical_estimating_id,
            'make_name': make_name,
            'motor_db_section': clean_value(normalized.get('Motor_DB_Section'), 'string'),
            'motor_db_group': clean_value(normalized.get('Motor_DB_Group'), 'string'),
            'motor_db_subgroup': clean_value(normalized.get('Motor_DB_SubGroup'), 'string'),
            'motor_db_operation': clean_value(normalized.get('Motor_DB_Operation'), 'string'),
            'qualifier_description': clean_value(normalized.get('QualifierDescription'), 'string'),
            'factory_time': clean_value(normalized.get('FactoryTime'), 'numeric'),
            'motor_time': clean_value(normalized.get('MOTORTime'), 'numeric'),
            'is_additional_operation': (
                normalized.get('IsAdditionalOperation', 'No').strip().lower() == 'yes'
            ),
            'section_application': clean_value(normalized.get('SectionApplication'), 'string'),
            'motor_db_description': clean_value(normalized.get('MOTOR_DB_Description'), 'string'),
            'skill_code': clean_value(normalized.get('SkillCode'), 'string'),
            'motor_db_footnote': clean_value(normalized.get('Motor_DB_Footnote'), 'string'),
        })

    return records


def parse_ewt_application_file(make_name: str, content: str) -> list:
    """
    Parse one [Make]_Application.txt file into records for ewt_applications.

    Pipe-delimited columns:
      ApplicationID | MechanicalEstimatingID | BaseVehicleID | RegionID |
      VehicleToEngineConfigID

    BaseVehicleID joins to tt_smart_vehicles.vcdb_base_vehicle_id for YMM lookup —
    no ACES VCdb license needed.
    """
    records = []
    reader = csv.DictReader(io.StringIO(content), delimiter='|')

    for row in reader:
        normalized = {k.strip().lstrip('\ufeff'): (v.strip() if v else v)
                      for k, v in row.items()}

        application_id = clean_value(normalized.get('ApplicationID'), 'bigint')
        mechanical_estimating_id = clean_value(normalized.get('MechanicalEstimatingID'), 'bigint')
        base_vehicle_id = clean_value(normalized.get('BaseVehicleID'), 'integer')

        if not application_id or not mechanical_estimating_id or not base_vehicle_id:
            continue

        records.append({
            'application_id': application_id,
            'mechanical_estimating_id': mechanical_estimating_id,
            'base_vehicle_id': base_vehicle_id,
            'region_id': clean_value(normalized.get('RegionID'), 'integer'),
            'vehicle_to_engine_config_id': clean_value(
                normalized.get('VehicleToEngineConfigID'), 'integer'
            ),
            'make_name': make_name,  # denormalized for debugging
        })

    return records


def insert_batch(supabase: Client, table: str, records: list, batch_size: int = 1000) -> dict:
    """
    Insert a list of records into a Supabase table in batches.
    Returns {'inserted': int, 'errors': int}.
    """
    inserted = 0
    errors = 0

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            supabase.table(table).insert(batch).execute()
            inserted += len(batch)
        except Exception as e:
            logger.error(f"  Batch insert error ({table}, offset {i}): {e}")
            errors += len(batch)

    return {'inserted': inserted, 'errors': errors}


# =============================================================================
# SYNC ORCHESTRATION
# =============================================================================

def sync_motor_data():
    """
    Main function to sync MOTOR TireTechSmart data.

    SAFE SYNC PATTERN with RETRY:
    1. Download data from SFTP (with up to 4 retry attempts)
    2. Parse and validate data (check minimum record count)
    3. Only if validation passes: truncate and insert
    4. If any step fails before truncate: existing data is preserved
    """
    supabase = get_supabase()

    lock_status = check_sync_lock(supabase, 'motor_tiretech_smart')

    if lock_status['locked']:
        logger.warning(f"MOTOR sync blocked: {lock_status['message']}")
        return {
            'status': 'skipped',
            'reason': lock_status['message'],
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

    if lock_status['stale_id']:
        cleanup_stale_lock(supabase, lock_status['stale_id'])

    start_time = datetime.now(timezone.utc)
    results = {
        'status': 'running',
        'started_at': start_time.isoformat(),
        'source_file': None,
        'tables': {}
    }

    log_id = None

    try:
        log_entry = supabase.table('tire_data_sync_log').insert({
            'sync_type': 'motor_tiretech_smart',
            'status': 'running',
            'started_at': start_time.isoformat()
        }).execute()
        log_id = log_entry.data[0]['id']

        logger.info("Step 1: Downloading from MOTOR SFTP (with retry)...")
        csv_content, latest_zip = download_motor_data_with_retry()
        results['source_file'] = latest_zip
        logger.info(f"  Download complete: {len(csv_content):,} bytes from {latest_zip}")

        logger.info("Step 2: Parsing and validating data...")
        prepared_records = prepare_smart_vehicles_data(csv_content)

        if len(prepared_records) < Config.MIN_MOTOR_RECORDS:
            raise ValueError(
                f"Data validation failed: Only {len(prepared_records):,} records found, "
                f"minimum required is {Config.MIN_MOTOR_RECORDS:,}. "
                f"Aborting to preserve existing data."
            )

        logger.info(f"  Validation passed: {len(prepared_records):,} records ready")

        logger.info("Step 3: Truncating table and inserting new data...")
        result = insert_smart_vehicles(supabase, prepared_records)
        results['tables']['tt_smart_vehicles'] = result

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
            "MOTOR Smart Sync Complete",
            f"""Source: {latest_zip}
Duration: {results['duration_seconds']:.1f} seconds
Records: {result['inserted']:,} inserted, {result['errors']:,} errors

Features: Auto-retry enabled ({Config.MAX_RETRY_ATTEMPTS} attempts), safe sync pattern.
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
            "MOTOR Smart Sync FAILED",
            f"""Error: {str(e)}

Retry attempts: {Config.MAX_RETRY_ATTEMPTS} (all exhausted)
Existing data: PRESERVED (truncate only happens after successful download & validation)

Please check the logs for details.
""",
            is_error=True
        )

        raise


def sync_usventure_data():
    """
    Main function to sync USVenture inventory data.

    SAFE SYNC PATTERN with RETRY:
    1. Download data from FTPS (with up to 4 retry attempts)
    2. Parse and validate data (check minimum record count)
    3. Only if validation passes: truncate and insert
    4. If any step fails before truncate: existing data is preserved
    """
    supabase = get_supabase()

    lock_status = check_sync_lock(supabase, 'usventure_inventory')

    if lock_status['locked']:
        logger.warning(f"USVenture sync blocked: {lock_status['message']}")
        return {
            'status': 'skipped',
            'reason': lock_status['message'],
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

    if lock_status['stale_id']:
        cleanup_stale_lock(supabase, lock_status['stale_id'])

    start_time = datetime.now(timezone.utc)
    results = {
        'status': 'running',
        'started_at': start_time.isoformat(),
        'source_file': Config.USVENTURE_FILENAME,
    }

    log_id = None

    try:
        log_entry = supabase.table('tire_data_sync_log').insert({
            'sync_type': 'usventure_inventory',
            'status': 'running',
            'source_file': Config.USVENTURE_FILENAME,
            'started_at': start_time.isoformat()
        }).execute()
        log_id = log_entry.data[0]['id']

        logger.info("Step 1: Downloading from USVenture FTPS (with retry)...")
        csv_content = download_usventure_data_with_retry()
        logger.info(f"  Download complete: {len(csv_content):,} bytes")

        logger.info("Step 2: Parsing and validating data...")
        prepared_records = prepare_usventure_data(csv_content)

        if len(prepared_records) < Config.MIN_USVENTURE_RECORDS:
            raise ValueError(
                f"Data validation failed: Only {len(prepared_records):,} unique parts found, "
                f"minimum required is {Config.MIN_USVENTURE_RECORDS:,}. "
                f"Aborting to preserve existing data."
            )

        logger.info(f"  Validation passed: {len(prepared_records):,} unique parts ready")

        logger.info("Step 3: Truncating table and inserting new data...")
        result = insert_usventure_inventory(supabase, prepared_records)
        results['sync_result'] = result

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
            "USVenture Inventory Sync Complete",
            f"""Source: {Config.USVENTURE_FILENAME}
Duration: {results['duration_seconds']:.1f} seconds
Unique Parts: {result['unique_parts']:,}
Inserted: {result['inserted']:,}
Errors: {result['errors']:,}

Warehouse Breakdown:
- Fresno (4703): Aggregated
- Santa Clarita (4708): Aggregated

Features: Auto-retry enabled ({Config.MAX_RETRY_ATTEMPTS} attempts), safe sync pattern.
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
            "USVenture Inventory Sync FAILED",
            f"""Error: {str(e)}

Retry attempts: {Config.MAX_RETRY_ATTEMPTS} (all exhausted)
Existing data: PRESERVED (truncate only happens after successful download & validation)

Please check the logs for details.
""",
            is_error=True
        )

        raise


def sync_ewt_data():
    """
    Main function to sync MOTOR GEN4.5 Mechanical EWT data.

    STREAMING PATTERN — one make at a time to keep memory flat:
    1. Download zip into buffer (~254MB, held in RAM throughout)
    2. Build index of make pairs from zip directory (no file contents yet)
    3. Validate: must have >= MIN_EWT_MAKE_FILES make pairs
    4. Truncate both ewt_labor and ewt_applications
    5. For each make: read from zip → parse labor → insert → parse apps → insert → gc
       Peak memory = zip buffer + one make's parsed records at any point

    Triggered monthly via Zapier. Runtime: ~5-15 minutes for 57 makes.
    """
    supabase = get_supabase()

    # =========================================================================
    # CHECK SYNC LOCK
    # =========================================================================
    lock_status = check_sync_lock(supabase, 'motor_ewt')

    if lock_status['locked']:
        logger.warning(f"EWT sync blocked: {lock_status['message']}")
        return {
            'status': 'skipped',
            'reason': lock_status['message'],
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

    if lock_status['stale_id']:
        cleanup_stale_lock(supabase, lock_status['stale_id'])

    start_time = datetime.now(timezone.utc)
    results = {
        'status': 'running',
        'started_at': start_time.isoformat(),
        'source_file': None,
        'tables': {}
    }

    log_id = None

    try:
        log_entry = supabase.table('tire_data_sync_log').insert({
            'sync_type': 'motor_ewt',
            'status': 'running',
            'started_at': start_time.isoformat()
        }).execute()
        log_id = log_entry.data[0]['id']

        # =====================================================================
        # STEP 1: DOWNLOAD ZIP + BUILD MAKE INDEX
        # Holds ~254MB zip in RAM. No file contents decoded yet.
        # =====================================================================
        logger.info("Step 1: Downloading EWT zip from MOTOR SFTP (with retry)...")
        zip_bytes, source_file, make_pairs = download_ewt_data_with_retry()
        results['source_file'] = source_file
        logger.info(f"  Indexed {len(make_pairs)} make pairs from {source_file}")

        # =====================================================================
        # STEP 2: VALIDATE FILE COUNT (pre-truncate safety gate)
        # =====================================================================
        if len(make_pairs) < Config.MIN_EWT_MAKE_FILES:
            raise ValueError(
                f"EWT validation failed: Only {len(make_pairs)} make files found, "
                f"minimum required is {Config.MIN_EWT_MAKE_FILES}. "
                f"Aborting to preserve existing data."
            )

        logger.info(f"  Validation passed: {len(make_pairs)} makes ready")

        # =====================================================================
        # STEP 3: TRUNCATE BOTH TABLES — point of no return
        # Only reached after successful download + file count validation.
        # =====================================================================
        logger.info("Step 3: Truncating ewt_labor and ewt_applications...")
        supabase.table('ewt_labor').delete().neq('mechanical_estimating_id', -1).execute()
        supabase.table('ewt_applications').delete().neq('application_id', -1).execute()
        logger.info("  Tables truncated.")

        # =====================================================================
        # STEP 4: STREAM ONE MAKE AT A TIME
        # Read from zip → parse → insert → discard → gc → next make
        # =====================================================================
        total_labor_inserted = 0
        total_labor_errors = 0
        total_app_inserted = 0
        total_app_errors = 0
        makes_processed = 0
        makes_failed = 0

        with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as zf:
            for make_name, labor_zip_path, app_zip_path in make_pairs:
                try:
                    # LABOR: read → parse → insert → free
                    labor_content = zf.read(labor_zip_path).decode('utf-8-sig')
                    labor_records = parse_ewt_labor_file(make_name, labor_content)
                    del labor_content

                    labor_result = insert_batch(supabase, 'ewt_labor', labor_records)
                    total_labor_inserted += labor_result['inserted']
                    total_labor_errors += labor_result['errors']
                    del labor_records
                    gc.collect()

                    # APPLICATIONS: read → parse → insert → free
                    app_content = zf.read(app_zip_path).decode('utf-8-sig')
                    app_records = parse_ewt_application_file(make_name, app_content)
                    del app_content

                    app_result = insert_batch(supabase, 'ewt_applications', app_records)
                    total_app_inserted += app_result['inserted']
                    total_app_errors += app_result['errors']
                    del app_records
                    gc.collect()

                    makes_processed += 1
                    logger.info(
                        f"  [{makes_processed}/{len(make_pairs)}] {make_name}: "
                        f"{labor_result['inserted']} labor, {app_result['inserted']} apps"
                    )

                except Exception as e:
                    logger.error(f"  Failed to process make {make_name}: {e}")
                    makes_failed += 1
                    gc.collect()
                    continue

        # Free zip buffer now that all makes are processed
        del zip_bytes
        gc.collect()

        # =====================================================================
        # SUCCESS
        # =====================================================================
        end_time = datetime.now(timezone.utc)
        results['status'] = 'completed'
        results['completed_at'] = end_time.isoformat()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        results['makes_processed'] = makes_processed
        results['makes_failed'] = makes_failed
        results['tables'] = {
            'ewt_labor': {'inserted': total_labor_inserted, 'errors': total_labor_errors},
            'ewt_applications': {'inserted': total_app_inserted, 'errors': total_app_errors}
        }

        total_inserted = total_labor_inserted + total_app_inserted

        supabase.table('tire_data_sync_log').update({
            'status': 'completed',
            'source_file': source_file,
            'records_inserted': total_inserted,
            'completed_at': end_time.isoformat()
        }).eq('id', log_id).execute()

        send_alert(
            "MOTOR EWT Sync Complete",
            f"""Source: {source_file}
Duration: {results['duration_seconds'] / 60:.1f} minutes
Makes Processed: {makes_processed} / {len(make_pairs)} ({makes_failed} failed)

ewt_labor:
  Inserted: {total_labor_inserted:,}
  Errors:   {total_labor_errors:,}

ewt_applications:
  Inserted: {total_app_inserted:,}
  Errors:   {total_app_errors:,}

BrakeFinder and mechanical estimating data is now current.
Memory strategy: streaming (one make at a time).
""",
            is_error=False
        )

        return results

    except Exception as e:
        logger.error(f"EWT sync failed: {e}")
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
            "MOTOR EWT Sync FAILED",
            f"""Error: {str(e)}

Note: If failure occurred after the truncate step, ewt_labor and ewt_applications
may be partially populated. Re-triggering the sync will truncate and reload cleanly.

Please check tire_data_sync_log and Render logs for details.
""",
            is_error=True
        )

        raise


# =============================================================================
# ROUTES
# =============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'service': 'tire-sync-service',
        'version': '3.1',
        'features': {
            'auto_retry': True,
            'max_attempts': Config.MAX_RETRY_ATTEMPTS,
            'retry_delay_base_seconds': Config.RETRY_DELAY_SECONDS,
            'retry_strategy': 'exponential_backoff',
            'sftp_connect_timeout': Config.SFTP_CONNECT_TIMEOUT,
            'sftp_channel_timeout': Config.SFTP_CHANNEL_TIMEOUT,
            'keepalive_interval': 15,
            'safe_sync_pattern': True,
            'ewt_memory_strategy': 'streaming_per_make',
            'syncs': ['motor_tiretech_smart', 'usventure_inventory', 'motor_ewt']
        },
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
        return jsonify({'status': 'failed', 'error': str(e)}), 500


@app.route('/sync/usventure', methods=['POST'])
@require_api_key
def trigger_usventure_sync():
    """Webhook endpoint to trigger USVenture inventory sync."""
    logger.info("USVenture inventory sync triggered via webhook")

    try:
        results = sync_usventure_data()
        return jsonify(results), 200
    except Exception as e:
        logger.error(f"USVenture sync failed: {e}")
        return jsonify({'status': 'failed', 'error': str(e)}), 500


@app.route('/sync/ewt', methods=['POST'])
@require_api_key
def trigger_ewt_sync():
    """
    Webhook endpoint to trigger MOTOR GEN4.5 EWT sync.

    Streams one make at a time.
    Peak memory: ~254MB zip buffer + one make's parsed records.
    Runtime: ~5-15 minutes for 57 makes.
    Trigger monthly via Zapier.
    """
    logger.info("MOTOR EWT sync triggered via webhook")

    try:
        results = sync_ewt_data()
        return jsonify(results), 200
    except Exception as e:
        logger.error(f"EWT sync failed: {e}")
        return jsonify({'status': 'failed', 'error': str(e)}), 500


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

        return jsonify({'recent_syncs': result.data})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
