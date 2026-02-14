# app.py
from flask import Flask, render_template, request, jsonify, send_from_directory
import os
import uuid
import logging
from unit_test_validator import Validator

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

@app.route('/run_script', methods=['POST'])
def run_script():
    try:
        data = request.json
        
        # Basic validation
        required_fields = ['aws-region', 'athena-db', 'mssql-server', 'mssql-db', 'mappings', 'mssql-user']
        for field in required_fields:
            if field not in data or not data[field]:
                return jsonify({'status': 'error', 'output': f"Missing required field: {field}"})

        run_id = str(uuid.uuid4())
        report_filename = f"report_{run_id}.html"
        report_file_path = os.path.join(REPORTS_DIR, report_filename)

        # Construct Configuration with new Auth and Sample Size parameters
        config = {
            "aws-region": data['aws-region'],
            "s3-staging": data['s3-staging'],
            "athena-db": data['athena-db'],
            "athena-workgroup": data.get('athena-workgroup', 'primary'),
            "mssql-server": data['mssql-server'],
            "mssql-db": data['mssql-db'],
            "mssql-driver": data.get('mssql-driver', 'ODBC Driver 17 for SQL Server'),
            "auth-method": data.get('auth-method', 'mfa'),
            "mssql-user": data['mssql-user'],
            "mssql-password": data.get('mssql-password', ''),
            "mappings": data.get('mappings', {})
        }

        # Initialize Validator with dynamic sample size
        validator = Validator(
            config=config, 
            output_path=report_file_path,
            tests=data.get('tests', 'all'),
            sample_size=data.get('sample-size', 100), # Pass dynamic sample size
            verbose=True
        )
        
        success, message = validator.run()

        if success:
            return jsonify({
                'status': 'success',
                'output': message,
                'report_url': f'/reports/{report_filename}'
            })
        else:
            return jsonify({
                'status': 'error',
                'output': f"Validation Failed: {message}"
            })

    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
        return jsonify({'status': 'error', 'output': f"An unexpected error occurred: {str(e)}"})

if __name__ == '__main__':
    app.run(debug=True, port=5001)