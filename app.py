from postgres_repository.hr_queries_helper import (
    get_eployees_per_quarter,
    get_eployees_hired_per_avg_dep,
)
from bd_migrator.s3_migrator_helper import (
    insert_departments_in_postgres,
    insert_jobs_in_postgres,
    insert_hired_employees_in_postgres,
)
from flask import Flask, request, jsonify

app = Flask(__name__)
print(__name__)

@app.route("/get_eployees_hired_per_avg_dep", methods=["GET"])
def request_eployees_hired_per_avg_dep():
    result = get_eployees_hired_per_avg_dep()
    if result != []:
        result_list = [{"id": row[0], "department": row[1], "hired": row[2]} for row in result]
        return jsonify(result_list)
    else:
        return jsonify({"error": f"Data not found."}), 404

@app.route("/get_eployees_per_quarter", methods=["GET"])
def request_eployees_per_quarter():
    result = get_eployees_per_quarter()
    if result != []:
        result_list = [
            {
                "department": row[0],
                "job": row[1],
                "q1": row[2],
                "q2": row[3],
                "q3": row[4],
                "q4": row[5],
            } for row in result
        ]
        return jsonify(result_list)
    else:
        return jsonify({"error": f"Data not found."}), 404

@app.route("/upload_data/", methods=["POST"])
def insert_csv_file():
    file_data = request.json
    try:
        if file_data["table"] == "departments":
            insert_departments_in_postgres(file_data["file_name"])
        elif file_data["table"] == "jobs":
            insert_jobs_in_postgres(file_data["file_name"])
        elif file_data["table"] == "hired_employees":
            insert_hired_employees_in_postgres(file_data["file_name"])
        response = jsonify(
            {
                "status": 200,
                "message": "Inserted successfully"
            }
        )
        return response
    except:
        response = jsonify(
            {
                "status": 200,
                "message": "Excecution failed"
            }
        )

