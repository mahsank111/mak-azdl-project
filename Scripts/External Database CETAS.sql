-- Step 1: Create Credential
CREATE DATABASE SCOPED CREDENTIAL cred_mak 
WITH
    IDENTITY = 'Managed Identity';




-- Step 2: Create External Data Source with abfss:// for Gold Layer
CREATE EXTERNAL DATA SOURCE mysource_goldlayer
WITH
(
    LOCATION = 'abfss://gold@makstoragedatalake.dfs.core.windows.net/',
    CREDENTIAL = cred_mak
);




-- Step 3: Create External File Format
CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);




-- Step 4: Create External Table as Select (CETAS)
CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales/',  -- must be relative path inside the goldlayer container
    DATA_SOURCE = mysource_goldlayer,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM goldlayer.sales;

SELECT * FROM gold.extsales
