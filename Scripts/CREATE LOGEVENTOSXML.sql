USE [DBInspetorCMS]
GO
CREATE TABLE LOGEVENTOSXML (
ID INT IDENTITY(1,1) PRIMARY KEY,
NOME_XML VARCHAR(80),
DATAEMISSAO DATETIME,
TIPO_DOC VARCHAR(1),
SETOR VARCHAR(3),
COD_PRD VARCHAR (30),
CST VARCHAR(3),
ALIQ_ICMS NUMERIC(9,2),
ALIQ_ICMSST NUMERIC(9,2),
RED_BC NUMERIC(9,2),
USUARIO VARCHAR(20),
EVENTO VARCHAR(1),
CRITICA VARCHAR(240)
);