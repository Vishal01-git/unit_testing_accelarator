# app.py
from flask import Flask, render_template, request, jsonify, send_from_directory
import os
import uuid
import logging
from unit_test_validator import Validator  # Importing our V2 class

app = Flask(__name__)
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(PROJECT_DIR, 'reports')

# Ensure reports directory exists
if not os.path.exists(REPORTS_DIR):
    os.makedirs(REPORTS_DIR)

# Configure Flask logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/')
def index():
    """Renders the main UI page."""
    return render_template('index.html')

@app.route('/reports/<filename>')
def report(filename):
    """Serves the generated report file securely."""
    return send_from_directory(REPORTS_DIR, filename)

@app.route('/run_script', methods=['POST'])
def run_script():
    """
    V2.0 Endpoint: Receives JSON config, initializes Validator class, and runs tests.
    """
    try:
        data = request.json
        
        # 1. Basic Validation
        # Note: 'mssql-schema' is intentionally excluded
        required_fields = ['aws-region', 'athena-db', 'mssql-server', 'mssql-db', 'mappings']
        for field in required_fields:
            if field not in data or not data[field]:
                return jsonify({'status': 'error', 'output': f"Missing required field: {field}"})

        # 2. Concurrency Handling
        # Generate a unique ID for this specific run
        run_id = str(uuid.uuid4())
        report_filename = f"report_{run_id}.html"
        report_file_path = os.path.join(REPORTS_DIR, report_filename)

        # 3. Construct Configuration Dictionary
        config = {
            "aws-region": data['aws-region'],
            "s3-staging": data['s3-staging'],
            "athena-db": data['athena-db'],
            "athena-workgroup": data.get('athena-workgroup', 'primary'),
            "mssql-server": data['mssql-server'],
            "mssql-db": data['mssql-db'],
            "mssql-driver": data.get('mssql-driver', 'ODBC Driver 17 for SQL Server'),
            "mappings": data.get('mappings', {})
        }

        # 4. Initialize and Run Validator
        validator = Validator(
            config=config, 
            output_path=report_file_path,
            tests=data.get('tests', 'all'),
            sample_size=100,  # Default sample size
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