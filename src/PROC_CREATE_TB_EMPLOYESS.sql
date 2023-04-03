CREATE OR ALTER PROCEDURE PROC_READ_TABLE_EMPLOYEES
AS
BEGIN TRY
	IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='hired_employees' and xtype='U')
	CREATE TABLE hired_employees(
    id INT,
    name varchar(255),
    datetime varchar(255),
    department_id INT,
    job_id INT
	);
	
	-- truncate the table first
	TRUNCATE TABLE hired_employees;
	
 
	-- import the file
	BULK INSERT hired_employees
	FROM 'hired_employees.CSV'
	WITH
	(
			FORMAT='CSV',
			FIRSTROW=1
	)
	;
END TRY
BEGIN CATCH
	SELECT ERROR_MESSAGE() AS ErrorMessage;  
END CATCH
;