-- Batch ingestion file's parametrization table
USE ROLE {{environment}}_LND_AUTOMATION_FR;
USE DATABASE DB_INGESTION_TOOLS_{{environment}};
USE SCHEMA BATCH;

create or replace table TB_BATCH_CONFIG (
    ID_SOURCE_BATCH number autoincrement start 1 increment 1 not null comment 'Unique identifier based on autoincrement for each batch source',
    DS_SOURCE_REGEX varchar not null comment 'Regular expression of the sources to be loaded on the target table',
    CO_SOURCE_TABLE_OR_FILE varchar comment 'Enumerator that determines wheter the source is a table or file',
    CO_TARGET_CATALOG varchar not null comment 'Target catalog in which the field will be loaded. It will also be part of the path for the file in stage',
    CO_TARGET_SCHEMA varchar not null comment 'Target schema in which the field will be loaded. It will also be part of the path for the file in stage',
    CO_TARGET_TABLE varchar not null comment 'Target table in which the field will be loaded. It will also be part of the path for the file in stage',
    DS_ADL_PATH varchar default concat('/', CO_TARGET_CATALOG, '/', CO_TARGET_SCHEMA, '/', CO_TARGET_TABLE, '/') comment 'This path along with the date of the process will be location of the file in Azure Data Lake',
    DS_EXTERNAL_STAGE varchar not null comment 'Stage in which the source with the information is allocated',
    DS_COPY_BOOK_PATH varchar not null comment 'Route of ADLS in which the parsing copy book has been placed',
    DS_FILE_SEPARATOR varchar default '' comment 'Column separator for this file. Specific for batch files',
    FL_HAS_HEADER boolean default false comment 'Flag to control if the source has headers or not.',
    FL_IS_INCREMENTAL boolean default false comment 'Flag to control wheter the source has incremental loads or not',
    FL_IS_BINARY boolean default false comment '',
    constraint _tables_config_pk primary key (ID_SOURCE_BATCH)
);

create or replace table TB_BATCH_FIELDS_CONFIG (
    ID_SOURCE_BATCH number not null comment 'Unique identifier for the file/table of this field',
    CO_POSITION number not null comment 'Field position within the source file/table',
    DS_FIELD_NAME varchar not null comment 'Field that will be parsed',
    CO_FIRST_CHARACTER number comment 'Position of the first character of the field starting from 0 character. Specific for batch files',
    FL_IS_PK boolean not null comment 'Flag that indicates whether a field is primary key of the target table or not',
    CO_TYPE varchar not null comment 'Snowflake SQL type of the field',
    QT_LENGTH number default 0 comment 'Length of the field',
    QT_SCALE number default 0 comment 'Scale of the field',
    constraint _fields_config_pk primary key (ID_SOURCE_BATCH, DS_FIELD_NAME),
    constraint _file_config_fk foreign key (ID_SOURCE_BATCH) references DB_INGESTION_TOOLS_{{environment}}.BATCH.TB_BATCH_CONFIG(ID_SOURCE_BATCH)
);