# Data Validation Accelerator UI

A user-friendly web interface for running data validation tests between **AWS Athena** and **Microsoft SQL Server**. This tool provides an interactive UI to configure and execute complex validation checks without writing command-line arguments.

---

## 🚀 Local Setup and Execution Guide

Follow these steps to get the project running on your local machine.

### 1. Prerequisites

- **Python 3.8+**
- **Git**
- **Microsoft ODBC Driver for SQL Server**

### 2. Clone the Repository

Open your terminal and clone the repository to your local machine.

git clone <your-repository-url>
cd <repository-folder-name>

text

### 3. Create a Virtual Environment

Using a virtual environment is highly recommended to manage project dependencies and avoid conflicts.

Create the environment:
python -m venv venv

text

Activate the environment:

On Windows:
venv\Scripts\activate

text

On macOS / Linux:
source venv/bin/activate

text

Once activated, your terminal prompt should be prefixed with `(venv)`.

### 4. Install Dependencies

Create a file named `requirements.txt` in the root of the project with the following content:

Flask
pandas
pyathena
pyodbc

text

Then, run the following command in your terminal to install all the necessary packages at once:

pip install -r requirements.txt

text

### 5. Run the Application

With your virtual environment still active, start the Flask web server.

python web_ui.py

text

The server will start, and you will see output indicating it is running on [http://127.0.0.1:5001](http://127.0.0.1:5001).

### 6. Use the Tool

- Open your web browser and navigate to [http://127.0.0.1:5001](http://127.0.0.1:5001).
- Fill in all the configuration fields in the UI.
- **Crucially**, for the _"Path to Python in venv"_ field, provide the full path to your virtual environment's Python executable.
    - Windows Example: `venv\Scripts\python.exe`
    - macOS/Linux Example: `venv/bin/python`
- Click the **"Run Validation"** button.
- The output from the validation script will appear in the console view on the page. A link to the detailed HTML report will be generated at the bottom upon successful completion.

---

Enjoy simplifying your data validation workflows!