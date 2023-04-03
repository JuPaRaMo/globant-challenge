CREATE OR ALTER PROCEDURE PROC_READ_TABLE_DEPARTMENT
AS
BEGIN TRY
	IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='departments' and xtype='U')
	CREATE TABLE departments (
    id INT,
    department varchar(255)
	);
	
	-- truncate the table first
	TRUNCATE TABLE departments;
	
 
	-- import the file
	BULK INSERT departments
	FROM 'departments.CSV'
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