# web_ui.py
from flask import Flask, render_template, request, jsonify, send_from_directory
import subprocess
import os
import json
import html

app = Flask(__name__)

# Get the directory where this script is located
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

@app.route('/')
def index():
    """Renders the main UI page."""
    return render_template('index.html')

# --- NEW: Route to serve the generated report ---
@app.route('/report')
def report():
    """Serves the generated report.html file."""
    return send_from_directory(PROJECT_DIR, 'report.html')
# --- End of new section ---

@app.route('/run_script', methods=['POST'])
def run_script():
    """
    Receives form data, creates the config.json, runs the original
    unit_test_validator.py script, and returns its output.
    """
    try:
        data = request.json
        
        config_file_path = os.path.join(PROJECT_DIR, 'config.json')
        config_data = {"mappings": data.get('mappings', {})} 
        
        with open(config_file_path, 'w') as f:
            json.dump(config_data, f, indent=4)

        validator_script_path = os.path.join(PROJECT_DIR, 'unit_test_validator.py')
        report_file_path = os.path.join(PROJECT_DIR, 'report.html')

        command = [
            data['pythonPath'],
            validator_script_path,
            '--aws-region', data['aws-region'],
            '--s3-staging', data['s3-staging'],
            '--athena-db', data['athena-db'],
            '--athena-workgroup', data['athena-workgroup'],
            '--mssql-server', data['mssql-server'],
            '--mssql-db', data['mssql-db'],
            '--mssql-schema', data['mssql-schema'],
            '--config-file', config_file_path,
            '--output', report_file_path,
            '--tests', data['tests']
        ]

        result = subprocess.run(command, capture_output=True, text=True, check=False)

        stdout = html.escape(result.stdout)
        stderr = html.escape(result.stderr)

        if result.returncode == 0:
            return jsonify({
                'status': 'success',
                'output': stdout if stdout else "Script executed successfully with no output.",
                # --- MODIFIED: Return the new /report URL ---
                'report_url': '/report'
            })
        else:
            return jsonify({
                'status': 'error',
                'output': f"STDOUT:\n{stdout}\n\nSTDERR:\n{stderr}"
            })

    except Exception as e:
        return jsonify({'status': 'error', 'output': f"An unexpected error occurred in the web server: {str(e)}"})

if __name__ == '__main__':
    templates_dir = os.path.join(PROJECT_DIR, 'templates')
    if not os.path.exists(templates_dir):
        os.makedirs(templates_dir)
    app.run(debug=True, port=5001)