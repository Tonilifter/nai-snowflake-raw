-- Batch ingestion file's parametrization table
create or replace table DB_INGESTION_TOOLS_{{environment}}.BATCH.TB_FILE_CONFIG (
    ID_FILE number autoincrement start 1 increment 1 not null comment 'Unique identifier based on autoincrement',
    CO_TARGET_CATALOG varchar not null comment 'Target catalog in which the field will be loaded. It will also be part of the path for the file in stage',
    CO_TARGET_SCHEMA varchar not null comment 'Target schema in which the field will be loaded. It will also be part of the path for the file in stage',
    CO_TARGET_TABLE varchar not null comment 'Target table in which the field will be loaded. It will also be part of the path for the file in stage',
    CO_ADL_PATH varchar default concat('/', CO_TARGET_CATALOG, '/', CO_TARGET_SCHEMA, '/', CO_TARGET_TABLE, '/') comment 'This path along with the date of the process will be location of the file in Azure Data Lake',
    TX_FILE_REGEX varchar not null comment 'Regular expression of the files to be loaded on the target table',
    TX_FILE_SSEPARATOR varchar default '' comment 'Column separator for this file',
    FL_HAS_HEADER boolean default false comment 'Flag to control if the file has headers or not',
    constraint _tables_config_pk primary key (ID_FILE)
);

create or replace table DB_INGESTION_TOOLS_{{environment}}.BATCH.TB_FIELDS_CONFIG (
    ID_FILE number not null comment 'Unique identifier for the file/table of this field',
    DS_FIELD varchar not null comment 'Field that will be parsed in file and load into the file/table referenced in FILE_ID',
    CO_POSITION number not null comment 'Field position within the file/table',
    CO_FIRST_CHARACTER number comment 'Position of the first character of the field in the file starting from 0',
    FL_IS_PK boolean not null comment 'FLag that indicates whether a field is primary key of the table or not',
    CO_TYPE varchar not null comment 'Snowflake SQL type of the field',
    QT_LENGTH number default 0 comment 'Length of the field',
    QT_SCALE number default 0 comment 'Scale of the field',
    constraint _fields_config_pk primary key (ID_FILE, DS_FIELD),
    constraint _file_config_fk foreign key (ID_FILE) references DB_INGESTION_TOOLS_{{environment}}.BATCH.TB_FILE_CONFIG(ID_FILE)
);

-- Batch ingestion load history table
create or replace table DB_INGESTION_TOOLS_{{environment}}.BATCH.TB_LOAD_HISTORY (
    CO_FILE_NAME varchar not null comment 'File loaded',
    DT_LOAD timestamp_ntz not null comment 'Timestamp in which the file has beend loaded',
    CO_STATUS varchar not null comment 'Result of the load attempt',
    QT_TOTAL_ROWS number default 0 comment 'Number of rows found in the file',
    QT_LOADED_ROWS number default 0 comment 'Number of rows loaded after the process has been completed',
    CO_TARGET_CATALOG varchar not null comment 'The database in which the file has been loaded',
    CO_TARGET_SCHEMA varchar not null comment 'The schema in which the file has been loaded',
    CO_TARGET_TABLE varchar not null comment 'The table in which the file has been loaded',
    constraint _tb_load_history_pk primary key (CO_FILE_NAME, DT_LOAD)
);