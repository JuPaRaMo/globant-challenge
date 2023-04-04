from flask import Flask, jsonify, request
import json
import pyodbc
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import copy
import os
from dotenv import load_dotenv

app = Flask(__name__)

department_schema = {
    "type": "record",
    "name": "Departments",
    "namespace": "master.departments",
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
    "namespace": "master.jobs",
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
    "namespace": "master.hired_employees",
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
        conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};SERVER=' + os.getenv('SERVER_NAME') +';DATABASE=master;Trusted_Connection=yes;')
        cursor = conn.cursor()
        sql= "SELECT * FROM [master].[dbo].[{0}]".format(request.json['table'])
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

@app.route('/restore_backup', methods = ['POST'])
def restore_backup():
    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};SERVER=' + os.getenv('SERVER_NAME') +';DATABASE=master;Trusted_Connection=yes;')
    cursor = conn.cursor()
    rowsbak = []
    response = {}
    if request.json['table'] == "departments":
        if os.path.isfile('department.avro'):
            with open('department.avro', 'rb') as f:
                reader = DataFileReader(f, DatumReader())
                metadata = copy.deepcopy(reader.meta)
                schema_from_file = json.loads(metadata['avro.schema'])
                rowsbak = [department for department in reader]
                reader.close()
            if len(rowsbak) > 0:
                for row in rowsbak:
                    sql= """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='departments_backup' and xtype='U')
                                CREATE TABLE departments_backup (
                                id INT,
                                department varchar(255)
                                );
                            INSERT INTO [master].[dbo].[departments_backup](id, department) 
                            VALUES ({0},'{1}')""".format(row['id'],row['department'])
                    cursor.execute(sql)
                    conn.commit()
            response = rowsbak
        else:
            response = 'There is no backup for Departments Table'
    if request.json['table'] == "jobs":
        if os.path.isfile('job.avro'):
            with open('job.avro', 'rb') as f:
                reader = DataFileReader(f, DatumReader())
                metadata = copy.deepcopy(reader.meta)
                schema_from_file = json.loads(metadata['avro.schema'])
                rowsbak = [job for job in reader]
                reader.close()
            if len(rowsbak) > 0:
                for row in rowsbak:
                    sql= """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='jobs_backup' and xtype='U')
                                CREATE TABLE jobs_backup (
                                id INT,
                                job varchar(255)
                                );
                            INSERT INTO [master].[dbo].[jobs_backup](id, name, datetime, department_id, job_id) 
                            VALUES ({0},'{1}')""".format(row['id'],row['job'])
                    cursor.execute(sql)
                    conn.commit()
            response = rowsbak
        else:
            response = 'There is no backup for Jobs Table'
    if request.json['table'] == "hired_employees":
        if os.path.isfile('hired_employee.avro'):
            with open('hired_employee.avro', 'rb') as f:
                reader = DataFileReader(f, DatumReader())
                metadata = copy.deepcopy(reader.meta)
                schema_from_file = json.loads(metadata['avro.schema'])
                rowsbak = [hired_employee for hired_employee in reader]
                reader.close()
            if len(rowsbak) > 0:
                for row in rowsbak:
                    sql= """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='hired_employees_backup' and xtype='U')
                                CREATE TABLE hired_employees_backup (
                                id INT,
                                job varchar(255)
                                );
                            INSERT INTO [master].[dbo].[hired_employees_backup](id, job) 
                            VALUES ({0},'{1}', '{2}', {3}, {4})""".format(row['id'],row['name'], row['datetime'], row['department_id'], row['job_id'])
                    cursor.execute(sql)
                    conn.commit()
            response = rowsbak
        else:
            response = 'There is no backup for Hired_employees Table'

    return jsonify({'response':response})

@app.route('/average')
def average_hires():
    try:
        conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};SERVER=' + os.getenv('SERVER_NAME') +';DATABASE=master;Trusted_Connection=yes;')
        cursor = conn.cursor()
        sql= """SELECT 
            dp.id,
            dp.department,
            COUNT(he.department_id) hired
            FROM hired_employees he
            INNER JOIN departments dp ON dp.id = he.department_id
            WHERE YEAR(he.datetime) = '2021'
            GROUP BY dp.id,he.department_id, dp.department
            HAVING COUNT(he.department_id) > (select COUNT(id)/12from hired_employees WHERE YEAR(datetime) = '2021')
            ORDER BY hired DESC;"""
        cursor.execute(sql)
        data = cursor.fetchall()
        results = []
        for row in data:
            result = {"id": row[0], "department": row[1], "hired": row[2]}
            results.append(result)
        return jsonify({'message': results})
    except Exception as ex:
        print(ex)
        return jsonify({'message':"Error"})

@app.route('/quarter-hires')
def hires_by_quarter():
    try:
        conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};SERVER=' + os.getenv('SERVER_NAME') +';DATABASE=master;Trusted_Connection=yes;')
        cursor = conn.cursor()
        sql= """WITH cte (id,quarter,year)
            AS(SELECT id, DATEPART(quarter, datetime) quarter, DATEPART(year, datetime) year
            FROM hired_employees
            )SELECT dp.department, jb.job, 
            CASE 
                WHEN cte.quarter = 1 THEN Count(he.department_id)
                ELSE 0
            END AS Q1, 
            CASE 
                WHEN cte.quarter = 2 THEN Count(he.department_id)
                ELSE 0
            END AS Q2,
            CASE 
                WHEN cte.quarter = 3 THEN Count(he.department_id)
                ELSE 0
            END AS Q3,
            CASE 
                WHEN cte.quarter  = 4 THEN Count(he.department_id)
                ELSE 0
            END AS Q4
            FROM hired_employees he
            INNER JOIN departments dp ON dp.id = he.department_id
            INNER JOIN jobs jb ON jb.id = he.job_id
            INNER JOIN cte cte ON cte.id = he.id
            WHERE cte.year = '2021'
            GROUP BY jb.job ,dp.department, cte.quarter,cte.year
            ORDER BY jb.job  ASC;"""
        cursor.execute(sql)
        data = cursor.fetchall()
        results = []
        for row in data:
            result = {"department": row[0], "job": row[1], "Q1": row[2], "Q2": row[3], "Q3": row[4], "Q4": row[5]}
            results.append(result)
        return jsonify({'data': results})
    except Exception as ex:
        print(ex)
        return jsonify({'message':"Error"})

@app.route('/insert', methods=['POST'])
def add_data():
    try:
        conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};SERVER=' + os.getenv('SERVER_NAME') +';DATABASE=master;Trusted_Connection=yes;')
        cursor = conn.cursor()
        is_valid_query = True
        if request.json['table'] == 'department':
            for row in request.json['data']:
                is_valid_query, message = validate_departments(row)
                if is_valid_query:
                    sql= """INSERT INTO [master].[dbo].[departments](id, department) 
                            VALUES ({0},'{1}')""".format(row['id'],row['department'])
                    cursor.execute(sql)
                    conn.commit()
                else:
                    sql= """INSERT INTO [master].[dbo].[error_log](error_message) 
                            VALUES ('{0}')""".format(message)
                    cursor.execute(sql)
                    conn.commit()
                is_valid_query = True
        elif request.json['table'] == 'jobs':
            for row in request.json['data']:
                is_valid_query, message = validate_jobs(row)
                if is_valid_query:
                    sql= """INSERT INTO [master].[dbo].[jobs](id, job) 
                            VALUES ({0},'{1}')""".format(row['id'],row['job'])
                    cursor.execute(sql)
                    conn.commit()
                else:
                    sql= """INSERT INTO [master].[dbo].[error_log](error_message) 
                            VALUES ('{0}')""".format(message)
                    cursor.execute(sql)
                    conn.commit()
                is_valid_query = True
        elif request.json['table'] == 'hired_employees':
            for row in request.json['data']:
                is_valid_query, message = validate_hired_employees(row)
                if is_valid_query:
                    sql= """INSERT INTO [master].[dbo].[hired_employees](id, name, datetime, department_id, job_id) 
                            VALUES ({0},'{1}', '{2}', {3}, {4})""".format(row['id'],row['name'], row['datetime'], row['department_id'], row['job_id'])
                    cursor.execute(sql)
                    conn.commit()
                else:
                    sql= """INSERT INTO [master].[dbo].[error_log](error_message) 
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
    if not "id" in job_row or not job_row['id']:
        is_valid_query = False
        job_message = "Missing Field ID on Jobs"
    if not "job" in job_row or not job_row['job']:   
        is_valid_query = False
        job_message = "Missing Field Job on Jobs"
    return is_query_valid, job_message

def validate_hired_employees(hired_employees_row):
    is_query_valid = True
    hired_message = ""
    if not "id" in hired_employees_row or not hired_employees_row['id']:
        is_valid_query = False
        hired_message = "Missing Field ID on Hired employees"
    if not "name" in hired_employees_row or not hired_employees_row['name']:   
        is_valid_query = False
        hired_message = "Missing Field Name on Hired employees"
    if not "datetime" in hired_employees_row or not hired_employees_row['datetime']:   
        is_valid_query = False
        hired_message = "Missing Field Datetime on Hired employees"
    if not "department_id" in hired_employees_row or not hired_employees_row['department_id']:   
        is_valid_query = False
        hired_message = "Missing Field Department_id on Hired employees"
    if not "job_id" in hired_employees_row or not hired_employees_row['job_id']:   
        is_valid_query = False
        hired_message = "Missing Field Job_id on Hired employees"
    return is_query_valid, hired_message


if __name__ == '__main__':
    app.run(debug=True)
