USE ROLE SYSADMIN;
USE WAREHOUSE WH_ADMIN_DES;
USE DATABASE DB_LND_DES;
USE SCHEMA AUTOMATION;

-- SHOW TASKS;
-- -- TASKS OWNED BY S_SNW_NAI_KAFKA AND S_SNW_NAI_SNPGLUE

-- FIRST WE CREATE THE TASK AS SYSADMIN (WHICH IS GRANTED TO THE DEVOPS USER)
CREATE TASK IF NOT EXISTS TSK_CALL_ENABLE_TRACK_KAFKA
    SCHEDULE = 'USING CRON */5 * * * * UTC'
    CONFIG=$${"env": "DES"}$$
    AS
    CALL ENABLE_TRACK(SYSTEM$GET_TASK_GRAPH_CONFIG('env')::string);

-- AT THIS POINT SYSADMIN HAS OWNERSHIP PRIVILEGES
-- WE NEED TO GRANT ALL PRIVILEGES SO THAT SYSADMIN CAN "OPERATE" WITH THE TASK AFTER WE GRANT OWNERSHIP TO S_SNW_NAI_KAFKA/S_SNW_NAI_SNPGLUE
GRANT ALL PRIVILEGES ON TASK TSK_CALL_ENABLE_TRACK_KAFKA TO ROLE SYSADMIN;

-- FIRST WE CREATE THE TASK AS SYSADMIN (WHICH IS GRANTED TO THE DEVOPS USER)
CREATE TASK IF NOT EXISTS TSK_CALL_ENABLE_TRACK_SNPGLUE
    SCHEDULE = 'USING CRON */5 * * * * UTC'
    CONFIG=$${"env": "DES"}$$
    AS
    CALL ENABLE_TRACK(SYSTEM$GET_TASK_GRAPH_CONFIG('env')::string);

-- AT THIS POINT SYSADMIN HAS OWNERSHIP PRIVILEGES
-- WE NEED TO GRANT ALL PRIVILEGES SO THAT SYSADMIN CAN "OPERATE" WITH THE TASK AFTER WE GRANT OWNERSHIP TO S_SNW_NAI_KAFKA/S_SNW_NAI_SNPGLUE
GRANT ALL PRIVILEGES ON TASK TSK_CALL_ENABLE_TRACK_SNPGLUE TO ROLE SYSADMIN;


-- TRANSFER OWNERSHIP - TASKS RUN AS THE ROLE WHICH OWNS THEM
-- USE ROLE ACCOUNTADMIN;
-- GRANT OWNERSHIP ON TASK TSK_CALL_ENABLE_TRACK_SNPGLUE TO ROLE S_SNW_NAI_SNPGLUE COPY CURRENT GRANTS;
CALL DB_GOV_SECURITY_DES.RBAC_GRANT.SP_GRANT_OWNERSHIP_ON_TASK('TSK_CALL_ENABLE_TRACK_SNPGLUE', 'S_SNW_NAI_SNPGLUE');

-- GRANT OWNERSHIP ON TASK TSK_CALL_ENABLE_TRACK_KAFKA TO ROLE S_SNW_NAI_KAFKA COPY CURRENT GRANTS;
CALL DB_GOV_SECURITY_DES.RBAC_GRANT.SP_GRANT_OWNERSHIP_ON_TASK('TSK_CALL_ENABLE_TRACK_KAFKA', 'S_SNW_NAI_KAFKA');

--USE ROLE SYSADMIN;
-- USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE PROCEDURE ENABLE_TRACK(env VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    COMMENT = 'Stored procedure that ...'
    EXECUTE AS CALLER
    AS
    $$
 
    // Input parameters:
    // - ENV (varchar): Environment where the Stored Procedure is executed. For example: 'LAB'
   
    var env = ENV;
    var result = '';
 
    try {    
        // Recuperar las tablas con change_tracking sin habilitar
        var query = `show tables in database db_lnd_${env};`;
        var resultSet = snowflake.execute({sqlText: query});
        var query = `select "database_name" || '.' || "schema_name" || '.' || "name" from table(result_scan(last_query_id())) where "change_tracking" = 'OFF' and "owner" = current_role()`;
        var resultSet = snowflake.execute({sqlText: query});
 
        // Habilitar change_tracking en dichas tablas
        while (resultSet.next()) {
            var tb_name = resultSet.getColumnValue(1);
            var query = `alter table ${tb_name} set change_tracking = true`;
            snowflake.execute({sqlText: query});
        }
 
        result = "Succeeded";
 
    } catch (err) {
        result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
        result += "\n  Message: " + err.message;
        result += "\nStack Trace:\n" + err.stackTraceTxt;
 
        throw err;
    }
   
    return result;
   
    $$
;

GRANT USAGE ON PROCEDURE ENABLE_TRACK(STRING) TO ROLE S_SNW_NAI_KAFKA;
GRANT USAGE ON PROCEDURE ENABLE_TRACK(STRING) TO ROLE S_SNW_NAI_SNPGLUE;

-- // PRUEBAS

-- USE ROLE SYSADMIN;

-- ALTER TASK TSK_CALL_ENABLE_TRACK_KAFKA RESUME;
-- ALTER TASK TSK_CALL_ENABLE_TRACK_SNPGLUE RESUME;

-- EXECUTE TASK TSK_CALL_ENABLE_TRACK_KAFKA;
-- EXECUTE TASK TSK_CALL_ENABLE_TRACK_SNPGLUE;

-- ------------------------------------


-- USE ROLE ACCOUNTADMIN;
-- --DROP SCHEMA DB_LND_DES.AUTOMATION;

-- call DB_GOV_SECURITY_DES.RBAC.SP_REVOKE_FUTURE_GRANTS('DB_LND_DES', 'AUTOMATION');

-- -- AUTOMATION NOT MANAGED ACCESS
-- ALTER SCHEMA DB_LND_DES.AUTOMATION DISABLE MANAGED ACCESS;

-- GRANT SELECT ON FUTURE EXTERNAL TABLES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RW_AR;
-- GRANT SELECT ON FUTURE VIEWS IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RW_AR;
-- GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RW_AR;
-- GRANT USAGE, READ, WRITE ON FUTURE STAGES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RW_AR;
-- GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RW_AR;
-- GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RW_AR;
-- GRANT SELECT ON FUTURE STREAMS IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RW_AR;
-- GRANT OPERATE ON FUTURE TASKS IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RW_AR;
-- GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RW_AR;
-- GRANT OPERATE ON FUTURE PIPES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RW_AR;
-- GRANT SELECT ON FUTURE EXTERNAL TABLES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_FULL_AR;
-- GRANT SELECT ON FUTURE VIEWS IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_FULL_AR;
-- GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_FULL_AR;
-- GRANT USAGE, READ, WRITE ON FUTURE STAGES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_FULL_AR;
-- GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_FULL_AR;
-- GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_FULL_AR;
-- GRANT SELECT ON FUTURE STREAMS IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_FULL_AR;
-- GRANT OPERATE ON FUTURE TASKS IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_FULL_AR;
-- GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_FULL_AR;
-- GRANT OPERATE ON FUTURE PIPES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_FULL_AR;
-- GRANT USAGE ON DATABASE DB_LND_DES TO DATABASE ROLE LND_AUTOMATION_SH_RO_AR;
-- GRANT USAGE ON SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RO_AR;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RO_AR;
-- GRANT SELECT ON FUTURE EXTERNAL TABLES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RO_AR;
-- GRANT SELECT ON FUTURE VIEWS IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RO_AR;
-- GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RO_AR;
-- GRANT READ ON FUTURE STAGES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RO_AR;
-- GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RO_AR;
-- GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RO_AR;
-- GRANT SELECT ON FUTURE STREAMS IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RO_AR;
-- GRANT MONITOR ON FUTURE TASKS IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RO_AR;
-- GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RO_AR;
-- GRANT OPERATE ON FUTURE PIPES IN SCHEMA DB_LND_DES.AUTOMATION TO DATABASE ROLE LND_AUTOMATION_SH_RO_AR;