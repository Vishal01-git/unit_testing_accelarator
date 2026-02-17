from flask import Flask, render_template, request, jsonify, send_from_directory, Response, stream_with_context
import os
import uuid
import logging
import json
import glob
from datetime import datetime
from unit_test_validator import Validator
from metadata_fetcher import MetadataFetcher

app = Flask(__name__)
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(PROJECT_DIR, 'reports')

if not os.path.exists(REPORTS_DIR):
    os.makedirs(REPORTS_DIR)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/reports/<filename>')
def report(filename):
    return send_from_directory(REPORTS_DIR, filename)

@app.route('/history', methods=['GET'])
def get_history():
    files = glob.glob(os.path.join(REPORTS_DIR, "*.html"))
    history = []
    for f in files:
        stats = os.stat(f)
        filename = os.path.basename(f)
        created_at = datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        history.append({'filename': filename, 'created_at': created_at, 'timestamp': stats.st_mtime})
    history.sort(key=lambda x: x['timestamp'], reverse=True)
    return jsonify(history)

@app.route('/fetch_tables', methods=['POST'])
def fetch_tables():
    try:
        data = request.json
        args = {
            'aws_region': data['aws-region'],
            's3_staging': data['s3-staging'],
            'athena_db': data['athena-db'],
            'athena_workgroup': data.get('athena-workgroup', 'primary'),
            'mssql_server': data['mssql-server'],
            'mssql_db': data['mssql-db'],
            'mssql_driver': data.get('mssql-driver', 'ODBC Driver 17 for SQL Server'),
            'auth_method': data.get('auth-method', 'mfa'),
            'mssql_user': data['mssql-username'],
            'mssql_password': data.get('mssql-password', '')
        }
        
        fetcher = MetadataFetcher(args)
        athena_tables = fetcher.get_athena_tables()
        sql_tables = fetcher.get_sql_tables()
        
        return jsonify({'status': 'success', 'athena_tables': athena_tables, 'sql_tables': sql_tables})
    except Exception as e:
        logger.error(f"Metadata fetch error: {e}")
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/run_script', methods=['POST'])
def run_script():
    data = request.json
    
    required_fields = ['aws-region', 'athena-db', 'mssql-server', 'mssql-db', 'mappings', 'mssql-username']
    for field in required_fields:
        if field not in data or not data[field]:
            return jsonify({'type': 'error', 'message': f"Missing required field: {field}"}), 400

    def generate_logs():
        run_id = str(uuid.uuid4())
        report_filename = f"report_{run_id}.html"
        report_file_path = os.path.join(REPORTS_DIR, report_filename)

        config = {
            "aws-region": data['aws-region'],
            "s3-staging": data['s3-staging'],
            "athena-db": data['athena-db'],
            "athena-workgroup": data.get('athena-workgroup', 'primary'),
            "mssql-server": data['mssql-server'],
            "mssql-db": data['mssql-db'],
            "mssql-driver": data.get('mssql-driver', 'ODBC Driver 17 for SQL Server'),
            "auth-method": data.get('auth-method', 'mfa'),
            "mssql-user": data['mssql-username'],
            "mssql-password": data.get('mssql-password', ''),
            "mappings": data.get('mappings', {})
        }

        validator = Validator(
            config=config, 
            output_path=report_file_path,
            tests=data.get('tests', 'all'),
            sample_size=data.get('sample-size', 100),
            verbose=True
        )
        
        def progress_callback(msg):
            yield json.dumps({"type": "log", "message": msg}) + "\n"

        try:
            success, message = validator.run(progress_callback=progress_callback)
            yield json.dumps({
                "type": "result",
                "status": "success" if success else "error",
                "output": message,
                "report_url": f'/reports/{report_filename}' if success else None
            }) + "\n"
        except Exception as e:
            yield json.dumps({"type": "error", "message": str(e)}) + "\n"

    return Response(stream_with_context(generate_logs()), mimetype='application/x-json-stream')

if __name__ == '__main__':
    app.run(debug=True, port=5001)