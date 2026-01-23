"""
TIRE SYNC SERVICE - v2.1
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
"""

import os
import io
import re
import csv
import zipfile
import logging
from datetime import datetime, timezone
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
    WAREHOUSE_SANTA_CLARITA = '4308'


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


def sync_smart_vehicles(supabase: Client, csv_content: str) -> dict:
    """Sync Smart Vehicles CSV to tt_smart_vehicles table."""
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
    
    logger.info("  Truncating tt_smart_vehicles...")
    supabase.table('tt_smart_vehicles').delete().neq('created_at', '1900-01-01').execute()
    
    reader = csv.DictReader(io.StringIO(csv_content))
    first_row = True
    
    for row in reader:
        total_records += 1
        normalized = {k.strip(): v for k, v in row.items()}
        
        if first_row:
            logger.info(f"  CSV columns: {list(normalized.keys())}")
            first_row = False
        
        transformed = {}
        for csv_col, (db_col, field_type) in field_map.items():
            value = normalized.get(csv_col)
            transformed[db_col] = clean_value(value, field_type)
        
        batch.append(transformed)
        
        if len(batch) >= batch_size:
            try:
                supabase.table('tt_smart_vehicles').insert(batch).execute()
                inserted += len(batch)
                logger.info(f"  Inserted batch: {inserted:,} / {total_records:,} records")
            except Exception as e:
                logger.error(f"  Error inserting batch: {e}")
                errors += len(batch)
            batch = []
    
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


# =============================================================================
# DATA SYNC - USVENTURE
# =============================================================================

def sync_usventure_inventory(supabase: Client, csv_content: str) -> dict:
    """
    Sync USVenture inventory CSV to tire_inventory table.
    Aggregates quantities by warehouse (Fresno 4703, Santa Clarita 4308).
    """
    
    batch_size = 2000
    total_records = 0
    errors = 0
    
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
    
    logger.info(f"  Processed {total_records:,} rows into {len(inventory_map):,} unique parts")
    
    # Truncate existing data
    logger.info("  Truncating tire_inventory...")
    supabase.table('tire_inventory').delete().neq('created_at', '1900-01-01').execute()
    
    # Insert in batches
    records = list(inventory_map.values())
    inserted = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            supabase.table('tire_inventory').insert(batch).execute()
            inserted += len(batch)
            logger.info(f"  Inserted batch: {inserted:,} / {len(records):,} records")
        except Exception as e:
            logger.error(f"  Error inserting batch: {e}")
            errors += len(batch)
    
    logger.info(f"  Completed: {inserted:,} inserted, {errors:,} errors")
    
    return {
        'total_rows': total_records,
        'unique_parts': len(inventory_map),
        'inserted': inserted,
        'errors': errors
    }


# =============================================================================
# SYNC ORCHESTRATION
# =============================================================================

def sync_motor_data():
    """Main function to sync MOTOR TireTechSmart data."""
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
        log_entry = supabase.table('tire_data_sync_log').insert({
            'sync_type': 'motor_tiretech_smart',
            'status': 'running',
            'started_at': start_time.isoformat()
        }).execute()
        log_id = log_entry.data[0]['id']
        
        sftp, transport = connect_motor_sftp()
        latest_zip = find_latest_smart_zip(sftp)
        results['source_file'] = latest_zip
        
        csv_content = download_and_extract_smart_csv(sftp, latest_zip)
        logger.info(f"Processing Smart Vehicles CSV...")
        
        result = sync_smart_vehicles(supabase, csv_content)
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
    """Main function to sync USVenture inventory data."""
    start_time = datetime.now(timezone.utc)
    results = {
        'status': 'running',
        'started_at': start_time.isoformat(),
        'source_file': Config.USVENTURE_FILENAME,
    }
    
    supabase = get_supabase()
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
        
        ftp = connect_usventure_ftps()
        csv_content = download_usventure_csv(ftp)
        
        logger.info("Processing USVenture inventory...")
        result = sync_usventure_inventory(supabase, csv_content)
        results['sync_result'] = result
        
        end_time = datetime.now(timezone.utc)
        results['status'] = 'completed'
        results['completed_at'] = end_time.isoformat()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        
        supabase.table('tire_data_sync_log').update({
            'status': 'completed',
            'records_processed': result['total_rows'],
            'records_inserted': result['inserted'],
            'completed_at': end_time.isoformat()
        }).eq('id', log_id).execute()
        
        send_alert(
            f"USVenture Inventory Sync Complete",
            f"""Source: {Config.USVENTURE_FILENAME}
Duration: {results['duration_seconds']:.1f} seconds
Total Rows: {result['total_rows']:,}
Unique Parts: {result['unique_parts']:,}
Inserted: {result['inserted']:,}
Errors: {result['errors']:,}

Warehouse Breakdown:
- Fresno (4703): Aggregated
- Santa Clarita (4308): Aggregated
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
        'version': '2.1',
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
    logger.info("USVenture inventory sync triggered via webhook")
    
    try:
        results = sync_usventure_data()
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
