USE DATABASE DB_LND_{{environment}};
USE ROLE {{environment}}_LND_AUTOMATION_FR;
USE SCHEMA AUTOMATION;

CREATE OR REPLACE TASK TSK_CHECK_NEW_GEP_GE5_GLUE_Z002 SCHEDULE = 'USING CRON 0 0,8,16 * * * UTC' AS 
    call DB_INGESTION_TOOLS_{{environment}}.STREAMING.SP_CHECK_NEW_STREAMING_TABLE('DB_LND_{{environment}}', 'SAP_GEP_GE5_GLUE', 'Z002_', '_CONSOLIDATED', TRUE, TRUE);

--CALL DB_GOV_SECURITY_{{environment}}.RBAC_GRANT.SP_GRANT_OWNERSHIP_ON_TASK('DB_LND_{{environment}}.AUTOMATION.TSK_CHECK_NEW_GEP_GE5_GLUE_Z002', '{{environment}}_LND_AUTOMATION_FR');

ALTER TASK TSK_CHECK_NEW_GEP_GE5_GLUE_Z002 RESUME;
