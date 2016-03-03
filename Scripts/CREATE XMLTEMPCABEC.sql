USE [DBInspetorCMS]
GO
CREATE TABLE XMLTEMPCABEC (
ID INT IDENTITY(1,1) PRIMARY KEY,
CNPJ_CPF_FOR VARCHAR(14),
RAZAO_FOR VARCHAR(60),
EST_FOR VARCHAR (2),
CNPJ_CPF_CLI VARCHAR(14),
RAZAO_CLI VARCHAR(60),
EST_CLI VARCHAR (2),
DT_EMISSAO DATETIME,
CRT VARCHAR (1),
CHV_NFE VARCHAR(44),
ORIGEM_CFOP VARCHAR (1),
NUM_NF VARCHAR (9),
SERIE_NF VARCHAR (3),
VAL_TOT_NF FLOAT,
VAL_TOT_FRETE FLOAT,
VAL_TOT_PRD FLOAT,
VAL_TOT_SEG FLOAT,
VAL_TOT_DESC FLOAT,
VAL_TOT_OUTRO FLOAT,
VAL_TOT_ST FLOAT,
VAL_TOT_IPI FLOAT,
PLACA VARCHAR(8),
UF_PLACA VARCHAR(2),
PESO_LIQ FLOAT,
PESO_BRUTO FLOAT,
ESPECIE VARCHAR(60),
DT_SAIENT DATETIME,
MOD_FRETE VARCHAR(1),
CNPJ_TRANSP VARCHAR(14),
QTD_VOLUME FLOAT,
MARCA_TRANSP VARCHAR(10),
NOME_TRANSP VARCHAR(60),
IE_TRANSP VARCHAR(15),
END_TRANSP VARCHAR(60),
MUNIC_TRANSP VARCHAR(60),
UF_TRANSP VARCHAR(2),
VAL_TOT_ICMS FLOAT,
NUM_DI VARCHAR(12),
DT_DI DATETIME,
LOCAL_DESEMB VARCHAR(60),
UF_DESEMB VARCHAR(2),
DT_DESEMB DATETIME,
INF_CPL VARCHAR(5000),
INF_AD_FISCO VARCHAR(2000)
);