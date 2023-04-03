CREATE OR ALTER PROCEDURE PROC_READ_TABLE_JOBS
AS
BEGIN TRY
	IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='jobs' and xtype='U')
	CREATE TABLE jobs (
    id INT,
    job varchar(255)
	);

	
	-- truncate the table first
	TRUNCATE TABLE jobs;
	
 
	-- import the file
	BULK INSERT jobs
	FROM 'jobs.CSV'
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