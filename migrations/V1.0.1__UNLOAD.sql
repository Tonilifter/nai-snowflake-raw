USE ROLE SYSADMIN;
USE DATABASE DB_INGESTION_TOOLS_{{environment}};
USE SCHEMA STREAMING;

CREATE OR REPLACE TABLE UNLOAD_CONFIG(
  ID INT AUTOINCREMENT,
	TABLE_CATALOG VARCHAR(512),
	TABLE_SCHEMA VARCHAR(512),
	TABLE_NAME VARCHAR(512),
	PARTITION_FIELD VARCHAR(256),
  DATA_LAKE_PATH VARCHAR(2048),
  DAY_OF_MONTH ARRAY, -- [1,2,3,...,31] or [] as *
  MONTH ARRAY, -- [1,2,3,...,12] or [] as *
  DAY_OF_WEEK ARRAY, -- [0,1,2,3,4,5,6] or [] as *
	LAST_LOAD DATE,
	LAST_STATUS VARCHAR(50),
  THREAD NUMBER(2),
	TS_SNAPSHOT TIMESTAMP_NTZ default current_timestamp()
);

USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION unload_storage_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = 'da060e56-5e46-475d-8b74-5fb187bd2177'
  STORAGE_ALLOWED_LOCATIONS = ('azure://{{datalake}}.blob.core.windows.net/snowflake/');

CREATE STAGE unload_storage_stage
  URL = 'azure://{{datalake}}.blob.core.windows.net/snowflake/'
  STORAGE_INTEGRATION = unload_storage_integration;
