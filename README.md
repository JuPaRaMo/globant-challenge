# globant-challenge
## This is a Step by step guide for installation and execution of the project

### SETUP ENVIRONMENT

1. Placed required files
- Create a folder in C: directory called CSV (C:\CSV). Inside this folder copy the next files:
  * departments.csv
  * hired_employees.csv
  * jobs.csv
 
 You can find these files inside src folder.
 
 2. Tables Creation
 - Open your SQL Server Management Studio, copy and execute the following store procedures:
    * PROC_CREATE_ERROR_TABLE
    * PROC_CREATE_TABLE_DEPARTMENT
    * PROC_CREATE_TABLE_EMPLOYEES
    * PROC_CREAET_TABLE_JOBS
  
 Now you have the tables migrated from the csv files.
 
 3. Setup python project
 - The python version used for this project is 3.9.13.
 Go to the root of the project and install the virtualenv
 ```python
 pip install virtualenv
 ```
 Then you will be able to activate it with:
 ```python
 virtualenv -p python3 env
 ```
 Now you are able to install the required libs from the requirements.txt file:
 ```python
 pip install -r requirements.txt
 ```
 Also you will find a .env file in hte root of the project, there you should add the "SERVER NAME" to coonect with your local DB.
 ```
 SERVER_NAME="Your Server Name"
 ```
 At this point you are all set to run the app using the following command:
 ```python
 python .\src\app.py
 ```
 ### API USAGE
 
| Method   | URL                                      | Description                                                                                                   |
| -------- | ---------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `POST`   | `insert`                                 | Add Data for any table.                                                                                       |
| `POST`   | `/backup`                                | Create an avro backup file for any table.                                                                     |
| `POST`   | `/restore_backup`                        | Restore any table from an avro file.                                                                          |
| `GET`    | `/quarter-hires`                         | Returns number of employees hired for each job and department in 2021 divided by quarter                      |
| `GET`    | `/average`                               | Returns employees hired of each department that hired more employees than the mean of employees hired in 2021 |

`POST /insert`
To add data to any table this service is expecting the specific table and the data presented as an array with the specific attributes for the table selected. The options for table are: "department", "jobs", "hired_employees"
```
{
 "table": "department",
 "data": [ 
  {
   "id": 1,
   "department": "New Department"
  }
 ]
}
```

`POST /backup`
To create a backup for a table, the specific table is expected in the body. The options for table are: "departments", "jobs", "hired_employees"
```
{
 "table": "jobs"
}
```
As result an avro file will be created in the root of the project for the specific table.

`POST /restore_backup`
To restore a table from a backup file, the specific table is expected in the body. The options for table are: "departments", "jobs", "hired_employees"
```
{
 "table": "hired_employees"
}
```
`GET /quarter-hires`
No params are required. the response for this service would look like this:
```
"data": [
        {
            "Q1": 0,
            "Q2": 1,
            "Q3": 0,
            "Q4": 0,
            "department": "Engineering",
            "job": "Account Coordinator"
        },
        {
            "Q1": 0,
            "Q2": 0,
            "Q3": 1,
            "Q4": 0,
            "department": "Engineering",
            "job": "Account Coordinator"
        },
        ...
 ]
```
`GET /average`
No params are required. the response for this service would look like this:
```
{
    "data": [
        {
            "department": "Support",
            "hired": 221,
            "id": 8
        },
        {
            "department": "Engineering",
            "hired": 208,
            "id": 5
        },
        {
            "department": "Human Resources",
            "hired": 204,
            "id": 6
        },
        ...
    ]
}
```
 
