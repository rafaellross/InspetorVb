USE [DBInspetorCMS]
GO
CREATE TABLE ERROSAPLICXML (
ID INT IDENTITY(1,1) PRIMARY KEY,
NOME_XML VARCHAR(80),
DATA datetime,
ERRO VARCHAR(MAX)
);