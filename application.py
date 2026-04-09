# from flask import Flask, render_template, jsonify, request
# from flask_cors import CORS
# from flask_socketio import SocketIO, emit
# import requests
# from datetime import datetime
# import json
# import logging
# import boto3
# import os
# import threading
# import time

# app = Flask(__name__)
# app.config['SECRET_KEY'] = "dfjhdfdff$%5kgg520"
# socketio = SocketIO(app, cors_allowed_origins="*", ping_timeout=60, ping_interval=25)
# CORS(app)

# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # API Gateway endpoint for Lambda fetch
# API_URL = "https://qbfk4w3e1a.execute-api.us-east-1.amazonaws.com/default/x24211541-lambda-fetch"

# # SNS Configuration for alerts
# SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', 'arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:water-quality-alerts')
# sns_client = boto3.client('sns', region_name='us-east-1')

# # Track sent alerts to avoid duplicates
# sent_alerts = set()

# # Real-time data storage
# latest_data = []
# last_fetch_time = None
# fetch_lock = threading.Lock()

# # Alert thresholds
# THRESHOLDS = {
#     'ph': {'min': 6.5, 'max': 8.5, 'critical_min': 6.0, 'critical_max': 9.0},
#     'turbidity': {'max': 5.0, 'critical_max': 10.0},
#     'dissolved_oxygen': {'min': 6.0, 'critical_min': 4.0},
#     'conductivity': {'min': 150, 'max': 500, 'critical_min': 100, 'critical_max': 600},
#     'temperature': {'min': 20, 'max': 30, 'critical_min': 15, 'critical_max': 35}
# }

# def fetch_water_data():
#     """Fetch water quality data from API Gateway"""
#     global latest_data, last_fetch_time
    
#     try:
#         logger.info(f"Fetching data from API: {API_URL}")
#         response = requests.get(API_URL, timeout=10)
        
#         if response.status_code == 200:
#             data = response.json()
            
#             # Handle different response formats
#             if isinstance(data, dict):
#                 if 'body' in data:
#                     try:
#                         data = json.loads(data['body']) if isinstance(data['body'], str) else data['body']
#                     except:
#                         data = []
#                 elif 'Items' in data:
#                     data = data['Items']
#                 elif 'items' in data:
#                     data = data['items']
            
#             # Ensure data is a list
#             if not isinstance(data, list):
#                 data = []
            
#             # Sort by timestamp (newest first)
#             if data and len(data) > 0:
#                 data.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            
#             with fetch_lock:
#                 latest_data = data
#                 last_fetch_time = datetime.now()
            
#             # Prepare serializable data (remove datetime objects)
#             serializable_data = []
#             for record in data[:100]:
#                 record_copy = {}
#                 for key, value in record.items():
#                     if key != 'datetime' and not isinstance(value, datetime):
#                         record_copy[key] = value
#                 serializable_data.append(record_copy)
            
#             # Emit to all connected clients
#             if serializable_data:
#                 socketio.emit('data_update', {
#                     'timestamp': datetime.now().isoformat(),
#                     'record_count': len(serializable_data),
#                     'latest': serializable_data[0] if serializable_data else None,
#                     'all_data': serializable_data
#                 }, namespace='/')
            
#             logger.info(f"Successfully fetched {len(data)} records")
#             return data
#         else:
#             logger.error(f"API returned status {response.status_code}")
#             with fetch_lock:
#                 return latest_data if latest_data else []
            
#     except requests.exceptions.RequestException as e:
#         logger.error(f"API request failed: {str(e)}")
#         with fetch_lock:
#             return latest_data if latest_data else []

# def background_fetcher():
#     """Background thread to fetch data every 2 seconds"""
#     logger.info("Real-time fetcher started - updating every 2 seconds")
#     while True:
#         try:
#             fetch_water_data()
#         except Exception as e:
#             logger.error(f"Background fetcher error: {e}")
#         time.sleep(2)

# # Start background thread
# fetcher_thread = threading.Thread(target=background_fetcher, daemon=True)
# fetcher_thread.start()

# @socketio.on('connect')
# def handle_connect():
#     """Handle client WebSocket connection"""
#     logger.info('Client connected to real-time stream')
    
#     with fetch_lock:
#         if latest_data:
#             # Prepare serializable data
#             serializable_data = []
#             for record in latest_data[:100]:
#                 record_copy = {}
#                 for key, value in record.items():
#                     if key != 'datetime' and not isinstance(value, datetime):
#                         record_copy[key] = value
#                 serializable_data.append(record_copy)
            
#             emit('initial_data', {
#                 'data': serializable_data,
#                 'timestamp': datetime.now().isoformat()
#             }, namespace='/')

# @socketio.on('disconnect')
# def handle_disconnect():
#     """Handle client disconnection"""
#     logger.info('Client disconnected from real-time stream')

# def check_alert_thresholds(record):
#     """Check if any thresholds are exceeded and return alerts"""
#     alerts = []
    
#     ph = record.get('ph')
#     if ph:
#         if ph > THRESHOLDS['ph']['critical_max'] or ph < THRESHOLDS['ph']['critical_min']:
#             alerts.append(f"CRITICAL: pH {ph}")
#         elif ph > THRESHOLDS['ph']['max'] or ph < THRESHOLDS['ph']['min']:
#             alerts.append(f"WARNING: pH {ph}")
    
#     turbidity = record.get('turbidity')
#     if turbidity:
#         if turbidity > THRESHOLDS['turbidity']['critical_max']:
#             alerts.append(f"CRITICAL: Turbidity {turbidity} NTU")
#         elif turbidity > THRESHOLDS['turbidity']['max']:
#             alerts.append(f"WARNING: Turbidity {turbidity} NTU")
    
#     do = record.get('dissolved_oxygen')
#     if do:
#         if do < THRESHOLDS['dissolved_oxygen']['critical_min']:
#             alerts.append(f"CRITICAL: Dissolved Oxygen {do} mg/L")
#         elif do < THRESHOLDS['dissolved_oxygen']['min']:
#             alerts.append(f"WARNING: Dissolved Oxygen {do} mg/L")
    
#     cond = record.get('conductivity')
#     if cond:
#         if cond > THRESHOLDS['conductivity']['critical_max'] or cond < THRESHOLDS['conductivity']['critical_min']:
#             alerts.append(f"CRITICAL: Conductivity {cond} uS/cm")
#         elif cond > THRESHOLDS['conductivity']['max'] or cond < THRESHOLDS['conductivity']['min']:
#             alerts.append(f"WARNING: Conductivity {cond} uS/cm")
    
#     temp = record.get('temperature')
#     if temp:
#         if temp > THRESHOLDS['temperature']['critical_max'] or temp < THRESHOLDS['temperature']['critical_min']:
#             alerts.append(f"CRITICAL: Temperature {temp}C")
#         elif temp > THRESHOLDS['temperature']['max'] or temp < THRESHOLDS['temperature']['min']:
#             alerts.append(f"WARNING: Temperature {temp}C")
    
#     return alerts

# def send_sns_alert(alert_data):
#     """Send SNS email notification for an alert"""
#     try:
#         alert_key = f"{alert_data.get('timestamp')}_{alert_data.get('sensor_id')}"
        
#         if alert_key in sent_alerts:
#             logger.info(f"Skipping duplicate alert: {alert_key}")
#             return False
        
#         alerts_list = alert_data.get('alerts', [])
#         severity = "CRITICAL" if any("CRITICAL" in a for a in alerts_list) else "WARNING"
        
#         subject = f"WATER QUALITY ALERT - {severity}"
        
#         message = f"""
# WATER QUALITY ALERT - {severity}

# Sensor: {alert_data.get('sensor_id', 'Unknown')}
# Time: {alert_data.get('timestamp', 'Unknown')}

# Readings:
# - pH: {alert_data.get('ph', 'N/A')}
# - Turbidity: {alert_data.get('turbidity', 'N/A')} NTU
# - Dissolved Oxygen: {alert_data.get('dissolved_oxygen', 'N/A')} mg/L
# - Conductivity: {alert_data.get('conductivity', 'N/A')} uS/cm
# - Temperature: {alert_data.get('temperature', 'N/A')} C

# Alerts:
# {chr(10).join(f'- {a}' for a in alerts_list)}

# Please investigate immediately.
# """
        
#         response = sns_client.publish(
#             TopicArn=SNS_TOPIC_ARN,
#             Subject=subject,
#             Message=message
#         )
        
#         sent_alerts.add(alert_key)
        
#         if len(sent_alerts) > 1000:
#             sent_alerts.clear()
        
#         logger.info(f"SNS alert sent: {response['MessageId']}")
#         return True
        
#     except Exception as e:
#         logger.error(f"Failed to send SNS alert: {str(e)}")
#         return False

# def process_alerts(data):
#     """Process data and send alerts for new alerts"""
#     if not data:
#         return
    
#     for record in data[:5]:
#         if record.get('alert'):
#             alerts = check_alert_thresholds(record)
#             if alerts:
#                 record['alerts'] = alerts
#                 send_sns_alert(record)

# def filter_data_by_date(data, start_date, end_date):
#     """Filter data by date range"""
#     if not start_date and not end_date:
#         return data
    
#     filtered = []
#     start = datetime.fromisoformat(start_date) if start_date else None
#     end = datetime.fromisoformat(end_date) if end_date else None
    
#     for record in data:
#         try:
#             record_date = datetime.fromisoformat(record.get('timestamp', '').replace('Z', '+00:00'))
#             if start and record_date < start:
#                 continue
#             if end and record_date > end:
#                 continue
#             filtered.append(record)
#         except:
#             filtered.append(record)
    
#     return filtered

# def calculate_statistics(data):
#     """Calculate statistics from water quality data"""
#     if not data:
#         return {}
    
#     latest = data[0] if data else {}
    
#     ph_values = [d['ph'] for d in data if d.get('ph')]
#     turbidity_values = [d['turbidity'] for d in data if d.get('turbidity')]
#     do_values = [d['dissolved_oxygen'] for d in data if d.get('dissolved_oxygen')]
#     conductivity_values = [d['conductivity'] for d in data if d.get('conductivity')]
#     temp_values = [d['temperature'] for d in data if d.get('temperature')]
    
#     stats = {
#         'current': {
#             'ph': latest.get('ph', '--'),
#             'turbidity': latest.get('turbidity', '--'),
#             'dissolved_oxygen': latest.get('dissolved_oxygen', '--'),
#             'conductivity': latest.get('conductivity', '--'),
#             'temperature': latest.get('temperature', '--'),
#             'alert': latest.get('alert', False),
#             'alerts': check_alert_thresholds(latest)
#         },
#         'averages': {
#             'ph': round(sum(ph_values) / len(ph_values), 2) if ph_values else 0,
#             'turbidity': round(sum(turbidity_values) / len(turbidity_values), 2) if turbidity_values else 0,
#             'dissolved_oxygen': round(sum(do_values) / len(do_values), 2) if do_values else 0,
#             'conductivity': round(sum(conductivity_values) / len(conductivity_values), 2) if conductivity_values else 0,
#             'temperature': round(sum(temp_values) / len(temp_values), 2) if temp_values else 0
#         },
#         'totals': {
#             'total_readings': len(data),
#             'alert_count': sum(1 for d in data if d.get('alert', False)),
#             'alert_percentage': round((sum(1 for d in data if d.get('alert', False)) / len(data)) * 100, 2) if data else 0
#         }
#     }
    
#     return stats

# @app.route('/')
# def dashboard():
#     """Render the dashboard"""
#     return render_template('dashboard.html')

# @app.route('/api/data')
# def get_data():
#     """API endpoint to get filtered water quality data"""
#     start_date = request.args.get('start_date')
#     end_date = request.args.get('end_date')
    
#     with fetch_lock:
#         data = latest_data.copy() if latest_data else []
    
#     filtered_data = filter_data_by_date(data, start_date, end_date)
#     process_alerts(filtered_data[:5])
    
#     # Remove datetime objects for JSON
#     for record in filtered_data:
#         if 'datetime' in record:


from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import requests
from datetime import datetime
import json
import logging
import boto3
import os
import threading
import time

application = Flask(__name__)

app = application

app.config['SECRET_KEY'] = "dfjhdfdff$%5kgg520"
socketio = SocketIO(app, cors_allowed_origins="*")
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API Gateway endpoint for Lambda fetch
API_URL = "https://qbfk4w3e1a.execute-api.us-east-1.amazonaws.com/default/x24211541-lambda-fetch"

# SNS Configuration for alerts
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:069266646059:x24211541-sns'
sns_client = boto3.client('sns', region_name='us-east-1')

# Track sent alerts to avoid duplicates
sent_alerts = set()

# Real-time data storage
latest_data = []
fetch_lock = threading.Lock()

# Alert thresholds
THRESHOLDS = {
    'ph': {'min': 6.5, 'max': 8.5, 'critical_min': 6.0, 'critical_max': 9.0},
    'turbidity': {'max': 5.0, 'critical_max': 10.0},
    'dissolved_oxygen': {'min': 6.0, 'critical_min': 4.0},
    'conductivity': {'min': 150, 'max': 500, 'critical_min': 100, 'critical_max': 600},
    'temperature': {'min': 20, 'max': 30, 'critical_min': 15, 'critical_max': 35}
}

def fetch_water_data():
    """Fetch water quality data from API Gateway"""
    global latest_data
    
    try:
        logger.info(f"Fetching data from API: {API_URL}")
        response = requests.get(API_URL, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, dict):
                if 'body' in data:
                    try:
                        data = json.loads(data['body']) if isinstance(data['body'], str) else data['body']
                    except:
                        data = []
                elif 'Items' in data:
                    data = data['Items']
                elif 'items' in data:
                    data = data['items']
            
            if not isinstance(data, list):
                data = []
            
            if data and len(data) > 0:
                data.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            
            with fetch_lock:
                latest_data = data
            
            # Emit to all connected clients
            if data:
                socketio.emit('new_data', {
                    'data': data[:50],
                    'timestamp': datetime.now().isoformat()
                })
            
            logger.info(f"Fetched {len(data)} records")
            return data
        else:
            logger.error(f"API returned status {response.status_code}")
            return []
            
    except Exception as e:
        logger.error(f"API request failed: {str(e)}")
        return []

def background_fetcher():
    """Background thread to fetch data every 2 seconds"""
    logger.info("Real-time fetcher started - updating every 2 seconds")
    while True:
        try:
            fetch_water_data()
        except Exception as e:
            logger.error(f"Background fetcher error: {e}")
        time.sleep(2)

# Start background thread
fetcher_thread = threading.Thread(target=background_fetcher, daemon=True)
fetcher_thread.start()

@socketio.on('connect')
def handle_connect():
    """Handle client WebSocket connection"""
    logger.info('Client connected')
    with fetch_lock:
        if latest_data:
            emit('initial_data', {'data': latest_data[:50]})

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('Client disconnected')

def check_alert_thresholds(record):
    alerts = []
    
    ph = record.get('ph')
    if ph and (ph > 8.5 or ph < 6.5):
        alerts.append(f"pH {ph}")
    
    turbidity = record.get('turbidity')
    if turbidity and turbidity > 5:
        alerts.append(f"Turbidity {turbidity}")
    
    do = record.get('dissolved_oxygen')
    if do and do < 6:
        alerts.append(f"Low DO {do}")
    
    return alerts

def send_sns_alert(alert_data):
    try:
        alert_key = f"{alert_data.get('timestamp')}_{alert_data.get('sensor_id')}"
        
        if alert_key in sent_alerts:
            return False
        
        alerts_list = alert_data.get('alerts', [])
        subject = f"WATER QUALITY ALERT - {', '.join(alerts_list)}"
        
        message = f"""
WATER QUALITY ALERT

Sensor: {alert_data.get('sensor_id', 'Unknown')}
Time: {alert_data.get('timestamp', 'Unknown')}

Readings:
- pH: {alert_data.get('ph', 'N/A')}
- Turbidity: {alert_data.get('turbidity', 'N/A')} NTU
- Dissolved Oxygen: {alert_data.get('dissolved_oxygen', 'N/A')} mg/L
- Conductivity: {alert_data.get('conductivity', 'N/A')} uS/cm
- Temperature: {alert_data.get('temperature', 'N/A')} C

Please investigate immediately.
"""
        
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        
        sent_alerts.add(alert_key)
        if len(sent_alerts) > 1000:
            sent_alerts.clear()
        
        logger.info(f"SNS alert sent")
        return True
    except Exception as e:
        logger.error(f"Failed to send SNS alert: {str(e)}")
        return False

def process_alerts(data):
    if not data:
        return
    
    for record in data[:5]:
        if record.get('alert'):
            alerts = check_alert_thresholds(record)
            if alerts:
                record['alerts'] = alerts
                send_sns_alert(record)

def filter_data_by_date(data, start_date, end_date):
    if not start_date and not end_date:
        return data
    
    filtered = []
    start = datetime.fromisoformat(start_date) if start_date else None
    end = datetime.fromisoformat(end_date) if end_date else None
    
    for record in data:
        try:
            record_date = datetime.fromisoformat(record.get('timestamp', '').replace('Z', '+00:00'))
            if start and record_date < start:
                continue
            if end and record_date > end:
                continue
            filtered.append(record)
        except:
            filtered.append(record)
    
    return filtered

def calculate_statistics(data):
    if not data:
        return {}
    
    latest = data[0] if data else {}
    
    ph_values = [d['ph'] for d in data if d.get('ph')]
    turbidity_values = [d['turbidity'] for d in data if d.get('turbidity')]
    do_values = [d['dissolved_oxygen'] for d in data if d.get('dissolved_oxygen')]
    conductivity_values = [d['conductivity'] for d in data if d.get('conductivity')]
    temp_values = [d['temperature'] for d in data if d.get('temperature')]
    
    stats = {
        'current': {
            'ph': latest.get('ph', '--'),
            'turbidity': latest.get('turbidity', '--'),
            'dissolved_oxygen': latest.get('dissolved_oxygen', '--'),
            'conductivity': latest.get('conductivity', '--'),
            'temperature': latest.get('temperature', '--'),
            'alert': latest.get('alert', False)
        },
        'totals': {
            'total_readings': len(data),
            'alert_count': sum(1 for d in data if d.get('alert', False))
        }
    }
    
    return stats

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/api/data')
def get_data():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    with fetch_lock:
        data = latest_data.copy() if latest_data else []
    
    filtered_data = filter_data_by_date(data, start_date, end_date)
    process_alerts(filtered_data[:5])
    
    return jsonify(filtered_data)

@app.route('/api/stats')
def get_stats():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    with fetch_lock:
        data = latest_data.copy() if latest_data else []
    
    filtered_data = filter_data_by_date(data, start_date, end_date)
    stats = calculate_statistics(filtered_data)
    
    return jsonify(stats)

@app.route('/api/send-test-alert')
def send_test_alert():
    test_alert = {
        'sensor_id': 'water-monitor',
        'timestamp': datetime.now().isoformat(),
        'ph': 5.5,
        'turbidity': 12.5,
        'dissolved_oxygen': 3.2,
        'conductivity': 650,
        'temperature': 36,
        'alert': True,
        'alerts': ['pH 5.5', 'Low DO 3.2']
    }
    
    success = send_sns_alert(test_alert)
    
    if success:
        return jsonify({'message': 'Test alert sent successfully'})
    else:
        return jsonify({'error': 'Failed to send test alert'}), 500

if __name__ == '__main__':
    print("\n" + "="*60)
    print("WATER QUALITY DASHBOARD - REAL-TIME MODE")
    print("="*60)
    print(f"API Endpoint: {API_URL}")
    print(f"Update Interval: 2 seconds")
    print(f"Dashboard URL: http://localhost:5000")
    print("="*60 + "\n")
    
    socketio.run(app, debug=True, host='0.0.0.0', port=5000, allow_unsafe_werkzeug=True)