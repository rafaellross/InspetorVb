USE [DBInspetorCMS]
GO
CREATE TABLE TIPO_FLAG (
ID INT IDENTITY(1,1) PRIMARY KEY,
TIPO VARCHAR(1) NULL,
FLAG_ERRO [VARBINARY](MAX) NULL
);

