CREATE OR ALTER PROCEDURE PROC_CREATE_DB
AS
BEGIN TRY  
     CREATE DATABASE globant_challenge 
END TRY  
BEGIN CATCH  
    SELECT ERROR_MESSAGE() AS ErrorMessage;   
END CATCH  