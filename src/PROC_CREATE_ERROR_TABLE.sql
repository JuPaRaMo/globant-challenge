CREATE OR ALTER PROCEDURE PROC_CREATE_ERROR_TABLE
AS
BEGIN TRY
	CREATE TABLE [dbo].[error_log](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[error_message] [varchar](255) NULL
);
END TRY
BEGIN CATCH
	SELECT ERROR_MESSAGE() AS ErrorMessage;  
END CATCH
;