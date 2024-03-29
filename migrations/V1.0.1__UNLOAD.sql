USE ROLE {{environment}}_LND_AUTOMATION_FR;
USE DATABASE DB_INGESTION_TOOLS_{{environment}};
USE SCHEMA STREAMING;

CREATE OR REPLACE TABLE TB_UNLOAD_CONFIG(
  ID INT AUTOINCREMENT,
	CO_TABLE_CATALOG VARCHAR(512),
	CO_TABLE_SCHEMA VARCHAR(512),
	CO_TABLE_NAME VARCHAR(512),
	DS_PARTITION_FIELD_EXPRESSION VARCHAR(256),
  DS_DATA_LAKE_PATH VARCHAR(2048),
  DS_WAREHOUSE VARCHAR(256),
  DS_ROLE VARCHAR(256),
  SQ_DAY_OF_MONTH ARRAY, -- [1,2,3,...,31] or [] as *
  SQ_MONTH ARRAY, -- [1,2,3,...,12] or [] as *
  SQ_DAY_OF_WEEK ARRAY, -- [0,1,2,3,4,5,6] or [] as *
	DT_LAST_LOAD DATE,
	DS_LAST_STATUS VARCHAR(50),
  CO_THREAD NUMBER(2),
	TS_SNAPSHOT TIMESTAMP_NTZ default current_timestamp()
);

CREATE STAGE STG_UNLOAD
  URL = 'azure://{{datalake}}.blob.core.windows.net/snowflake/'
  STORAGE_INTEGRATION = INT_STORAGE_UNLOAD_{{environment}};

CREATE STAGE STG_BATCH
  URL = 'azure://{{datalake}}.blob.core.windows.net/batch/'
  STORAGE_INTEGRATION = INT_STORAGE_BATCH_{{environment}};
