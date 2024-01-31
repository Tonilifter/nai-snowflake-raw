USE ROLE {{environment}}_LND_AUTOMATION_FR;
USE DATABASE DB_INGESTION_TOOLS_{{environment}};
USE SCHEMA DATA_CONTROL;

create or replace TABLE TB_DATA_CONTROL_STATUS (
	CO_DF_RUN_ID VARCHAR(512) NOT NULL,
	DS_PROCESS VARCHAR(256) NOT NULL,
	DT_LOAD TIMESTAMP_NTZ NOT NULL,
	CO_STATUS VARCHAR(10) NOT NULL,
	DS_TARGET_CATALOG VARCHAR(128),
	DS_TARGET_SCHEMA VARCHAR(128),
	DS_TARGET_TABLE VARCHAR(128),
	DS_FILE_NAME VARCHAR(512),
	QT_TOTAL_ROWS NUMBER NOT NULL,
	QT_LOADED_ROWS NUMBER NOT NULL,
	constraint DATA_CONTROL_STATUS_PK primary key (CO_DF_RUN_ID)
);

create or replace TABLE TB_DATA_CONTROL_STATUS_DETAIL (
	CO_DF_RUN_ID VARCHAR(512) NOT NULL,
	DS_MODEL_GROUP VARCHAR(256) NOT NULL,
	DS_MODEL VARCHAR(256) NOT NULL,
	DS_REPOSITORY VARCHAR(256) NOT NULL,
	DT_LOAD TIMESTAMP_NTZ NOT NULL,
	DS_TARGET_CATALOG VARCHAR(128) NOT NULL,
	DS_TARGET_SCHEMA VARCHAR(128) NOT NULL,
	DS_TARGET_TABLE VARCHAR(128) NOT NULL,
	CO_STATUS VARCHAR(10) NOT NULL,
	SQ_INDEX NUMBER DEFAULT 0,
	DS_DESCRIPTION VARCHAR(1024),
	DS_TEMPORALITY VARCHAR(256),
	SQ_TOTAL_PROCESSED NUMBER DEFAULT 0,
	constraint DATA_CONTROL_STATUS_DETAIL_PK primary key (CO_DF_RUN_ID, DS_MODEL_GROUP, DS_MODEL, DS_REPOSITORY)
);