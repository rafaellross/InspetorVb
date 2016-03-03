USE [DBInspetorCMS]
GO
CREATE TABLE XMLTEMPITENS (
ID INT IDENTITY(1,1) PRIMARY KEY,
CHV_NFE VARCHAR(44),
ITEM VARCHAR(4),
CFOP VARCHAR(4),
COD_PRD_FOR VARCHAR(60),
DESCRIC VARCHAR(120),
QTD FLOAT,
UND_MED VARCHAR(6),
VAL_PRD FLOAT,
VAL_UNIT_PRD FLOAT,
VAL_FRETE FLOAT,
VAL_DESC FLOAT,
VAL_OUTROS FLOAT,
VAL_SEG FLOAT,
ALIQ_IPI FLOAT,
VAL_IPI FLOAT,
CST_COFINS VARCHAR(3),
VAL_BC_COFINS FLOAT,
ALIQ_COFINS FLOAT,
VAL_COFINS FLOAT,
COD_MUNICIPIO VARCHAR(7),
NUM_ADICAO VARCHAR(9),
NUM_DI_ITEM VARCHAR(9),
CST_PIS VARCHAR(3),
VAL_BC_PIS FLOAT,
ALIQ_PIS FLOAT,
VAL_PIS FLOAT,
CST_ISS VARCHAR(3),
VAL_BC_ISS FLOAT,
ALIQ_ISS FLOAT,
VAL_ISS FLOAT,
NUM_PED VARCHAR(60),
CST_ICMS VARCHAR(3),
ALIQ_ICMS FLOAT,
VAL_ICMS FLOAT,
VAL_BC_ICMS FLOAT,
ALIQ_RED_BC_ICMS FLOAT,
ALIQ_RED_BCST_ICMS FLOAT,
ALIQ_MVAST_ICMS FLOAT,
VAL_BCST_ICMS FLOAT,
VAL_ICMSST FLOAT,
ALIQ_ICMSST FLOAT,
ORIG_CST VARCHAR(1)
);