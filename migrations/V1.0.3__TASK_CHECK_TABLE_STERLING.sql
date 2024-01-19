USE DATABASE DB_INGESTION_TOOLS_{{environment}};
USE ROLE SYSADMIN;
USE SCHEMA STREAMING;

CREATE OR REPLACE TASK CHECK_NEW_KAKFA_STERLING_TASK WAREHOUSE = {{warehouse}} SCHEDULE = 'USING CRON 0 * * * * UTC' AS 
    call CHECK_NEW_STREAMING_TABLE('DB_LND_DES', 'DB2_STERLING_DB2STERL_CDCKAFKA', 'KAFKA_', '_CONSOLIDATED', FALSE, FALSE);

ALTER TASK CHECK_NEW_KAKFA_STERLING_TASK RESUME;