from flask import Flask, render_template, jsonify
import pyodbc

app = Flask(__name__)

# --- SQL Server connection ---
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=10.198.29.92;"
    "DATABASE=msdb;"
    "UID=MIS_TEAM;"
    "PWD=M!$T3Am#135;"
)

# --- Route: Dashboard ---
@app.route("/")
def dashboard():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    query = open("job_status.sql").read()
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    jobs = [dict(zip(columns, row)) for row in rows]
    conn.close()
    return render_template("dashboard.html", jobs=jobs)


# --- Route: Get job steps & procedures ---
@app.route("/job_procedures/<job_name>")
def job_procedures(job_name):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    query = open("job_procedures.sql").read()
    cursor.execute(query, job_name)
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    data = [dict(zip(columns, row)) for row in rows]
    conn.close()
    return jsonify(data)


if __name__ == "__main__":
    app.run(debug=True)
