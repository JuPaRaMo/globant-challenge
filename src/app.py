from flask import Flask, jsonify, request
import json
import pyodbc
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

app = Flask(__name__)

department_schema = {
    "type": "record",
    "name": "Departments",
    "namespace": "challenge.departments",
    "fields": [{
        "name": "department",
        "type": ["null", "string"],
    }, {
        "name": "id",
        "type": ["null", "int"],
    }]
}

job_schema = {
    "type": "record",
    "name": "Jobs",
    "namespace": "challenge.jobs",
    "fields": [{
        "name": "id",
        "type": ["null", "int"],
    }, {
        "name": "job",
        "type": ["null", "string"],
    }]
}

hired_employees_schema = {
    "type": "record",
    "name": "Hired_employees",
    "namespace": "challenge.hired_employees",
    "fields": [{
        "name": "datetime",
        "type": ["null", "string"],
        
    }, {
        "name": "department_id",
        "type": ["null", "int"],
        
    }, {
        "name": "id",
        "type": ["null", "int"],
        
    }, {
        "name": "job_id",
        "type": ["null", "int"],
        
    }, {
        "name": "name",
        "type": ["null", "string"],
        
    }]
}

@app.route('/backup', methods = ['POST'])
def create_backup():
    try:
        conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-5D7DRA0;DATABASE=challenge;Trusted_Connection=yes;')
        cursor = conn.cursor()
        sql= "SELECT * FROM [challenge].[dbo].[{0}]".format(request.json['table'])
        cursor.execute(sql)
        data = cursor.fetchall()
        results = []        
        if request.json['table'] == "departments":
            schema = avro.schema.parse(json.dumps(department_schema))
            writer = DataFileWriter(open("department.avro", "wb"), DatumWriter(), schema)
            for row in data:
                department = {"id": row[0], "department": row[1]}
                writer.append(department)
        if request.json['table'] == "jobs":
            schema = avro.schema.parse(json.dumps(job_schema))
            writer = DataFileWriter(open("job.avro", "wb"), DatumWriter(), schema)
            for row in data:
                job = {"id": row[0], "job": row[1]}
                writer.append(job)
        if request.json['table'] == "hired_employees":
            schema = avro.schema.parse(json.dumps(hired_employees_schema))
            writer = DataFileWriter(open("hired_employees.avro", "wb"), DatumWriter(), schema)
            for row in data:
                hired_employee = {"id": row[0], "name": row[1], "datetime": row[2], "department_id": row[3], "job_id": row[4]}
                writer.append(hired_employee)
        writer.close()    
        
        return jsonify({'data':results, "table": request.json['table']})
    except Exception as ex:
        print(ex)
        return jsonify({'respones':"Error"})

@app.route('/insert', methods=['POST'])
def add_data():
    try:
        conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-5D7DRA0;DATABASE=challenge;Trusted_Connection=yes;')
        cursor = conn.cursor()
        is_valid_query = True
        if request.json['table'] == 'department':
            for row in request.json['data']:
                is_valid_query, message = validate_departments(row)
                if is_valid_query:
                    sql= """INSERT INTO [challenge].[dbo].[departments](id, department) 
                            VALUES ({0},'{1}')""".format(row['id'],row['department'])
                    cursor.execute(sql)
                    conn.commit()
                else:
                    sql= """INSERT INTO [challenge].[dbo].[error_log](error_message) 
                            VALUES ('{0}')""".format(message)
                    cursor.execute(sql)
                    conn.commit()
                is_valid_query = True
        elif request.json['table'] == 'jobs':
            for row in request.json['data']:
                is_valid_query, message = validate_jobs(row)
                if is_valid_query:
                    sql= """INSERT INTO [challenge].[dbo].[jobs](id, job) 
                            VALUES ({0},'{1}')""".format(row['id'],row['job'])
                    cursor.execute(sql)
                    conn.commit()
                else:
                    sql= """INSERT INTO [challenge].[dbo].[error_log](error_message) 
                            VALUES ('{0}')""".format(message)
                    cursor.execute(sql)
                    conn.commit()
                is_valid_query = True
        elif request.json['table'] == 'hired_employees':
            for row in request.json['data']:
                is_valid_query, message = validate_hired_employees(row)
                if is_valid_query:
                    sql= """INSERT INTO [challenge].[dbo].[hired_employees](id, job) 
                            VALUES ({0},'{1}')""".format(row['id'],row['job'])
                    cursor.execute(sql)
                    conn.commit()
                else:
                    sql= """INSERT INTO [challenge].[dbo].[error_log](error_message) 
                            VALUES ('{0}')""".format(message)
                    cursor.execute(sql)
                    conn.commit()
                is_valid_query = True
        return jsonify({'message':"Data Registered"})
    except Exception as ex:
        print(ex)
        return jsonify({'message':"Error"})

def validate_departments(department_row):
    is_query_valid = True
    department_message = ""
    if not "id" in department_row or not department_row['id']:
        is_query_valid = False
        department_message = "Missing Field ID on Departments"
    if not "department" in department_row or not department_row['department']:   
        is_query_valid = False
        department_message = "Missing Field Department on Departments"
    return is_query_valid, department_message

def validate_jobs(job_row):
    is_query_valid = True
    job_message = ""
    if not "id" in row or not row['id']:
        is_valid_query = False
        job_message = "Missing Field ID on Jobs"
    if not "job" in row or not row['job']:   
        is_valid_query = False
        job_message = "Missing Field Job on Jobs"
    return is_query_valid, job_message

def validate_hired_employees(hired_employees_row):
    is_query_valid = True
    hired_message = ""
    if not "id" in row or not row['id']:
        is_valid_query = False
        hired_message = "Missing Field ID on Hired employees"
    if not "name" in row or not row['name']:   
        is_valid_query = False
        hired_message = "Missing Field Name on Hired employees"
    if not "datetime" in row or not row['datetime']:   
        is_valid_query = False
        hired_message = "Missing Field Datetime on Hired employees"
    if not "department_id" in row or not row['department_id']:   
        is_valid_query = False
        hired_message = "Missing Field Department_id on Hired employees"
    if not "job_id" in row or not row['job_id']:   
        is_valid_query = False
        hired_message = "Missing Field Job_id on Hired employees"
    return is_query_valid, hired_message


if __name__ == '__main__':
    app.run(debug=True)
