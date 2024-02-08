USE DATABASE DB_INGESTION_TOOLS_{{environment}};
USE ROLE {{environment}}_LND_AUTOMATION_FR;
USE SCHEMA UTILS;

-- Procedure to update configuration tables from CSV files in ADLS
create or replace procedure SP_UPDATE_CONFIG(
    ds_config_catalog varchar,
    ds_config_schema varchar,
    ds_config_table varchar,
    ds_config_file varchar
)
returns varchar
language SQL
execute as caller
as
$$
declare
    configuration_file_path varchar := :ds_config_catalog || '/' || :ds_config_schema || '/' || :ds_config_table || '/' || :ds_config_file;
    configuration_table_path varchar := :ds_config_catalog || '.' || :ds_config_schema || '.' || :ds_config_table;
begin

    -- Execute the copy into query to insert the configuration records into the parametrization table
    let copy_config_sql varchar := '';
    copy_config_sql := configuration_file_path;
    --execute immediate :copy_config_sql;

    return :copy_config_sql;
end;
$$;
