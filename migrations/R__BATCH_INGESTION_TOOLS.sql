USE DATABASE DB_INGESTION_TOOLS_{{environment}};

USE ROLE {{environment}}_LND_AUTOMATION_FR;

USE SCHEMA BATCH;

-- Procedure to create the table in which data will be loaded
create or replace procedure SP_CREATE_BATCH_TABLE(target_id number)
returns varchar
language SQL
execute as caller
as
$$
declare
    target_catalog varchar;
    target_schema varchar;
    target_table varchar;
begin
    -- Information about the target table fields
    let batch_config_rs resultset := (
        select upper(CO_TARGET_CATALOG), upper(CO_TARGET_SCHEMA), upper(CO_TARGET_TABLE)
        from DB_INGESTION_TOOLS_{{environment}}.BATCH.TB_BATCH_CONFIG
        where ID_SOURCE_BATCH = :target_id
    );
    let batch_config_c cursor for batch_config_rs;
    open batch_config_c;
    fetch batch_config_c into target_catalog, target_schema, target_table;
    close batch_config_c;
    let target_table_path varchar := :target_catalog || '.' || :target_schema || '.' || :target_table;

    -- Information about the target table fields
    let fields_config_rs resultset := (
        select concat('"', upper(DS_FIELD_NAME), '"') as DS_FIELD_NAME, CO_POSITION, CO_FIRST_CHARACTER, FL_IS_PK, CO_TYPE, QT_LENGTH, QT_SCALE
        from DB_INGESTION_TOOLS_{{environment}}.BATCH.TB_BATCH_FIELDS_CONFIG
        where ID_SOURCE_BATCH = :target_id
        order by CO_POSITION
    );
    let fields_config_c cursor for fields_config_rs;

    -- Generate the statement to create the table in case it does not exist
    let target_fields_sql varchar := '';
    let target_pk_fields_sql varchar := '';
    for field in fields_config_c do
        let field_name := field.DS_FIELD_NAME::string;
        let field_is_pk := field.FL_IS_PK::boolean;
        let field_type := field.CO_TYPE::string;
        let field_length := field.QT_LENGTH::int;
        let field_scale := field.QT_SCALE::int;
        let parsed_field_type varchar := DB_INGESTION_TOOLS_{{environment}}.UTILS.UDF_PARSE_FIELD_TYPE(field.CO_TYPE, field.QT_LENGTH, field.QT_LENGTH, field.QT_SCALE)::string;
        
        target_fields_sql := :target_fields_sql || :field_name || ' ' || :parsed_field_type || ',';

        if (field_is_pk) then
            target_pk_fields_sql := :target_pk_fields_sql || :field_name || ',';
        end if;
    end for;

    target_fields_sql := :target_fields_sql || 'TS_LOAD timestamp_ntz';
    if (length(target_pk_fields_sql) != 0) then
        target_fields_sql := :target_fields_sql || ', constraint ' || lower(:target_table) || '_pk primary key(' ||  rtrim(:target_pk_fields_sql, ',') || ')';
    end if;
 
    let create_table_sql varchar := 'create table if not exists ' || :target_table_path || '(' || :target_fields_sql || ') enable_schema_evolution = true;';
    execute immediate :create_table_sql;

    return 'OK';
end;
$$;

-- Procedure to create all the database structures (Tables, Views, Task...) to handle the consolidation of he ingested data
create or replace procedure SP_CREATE_CONSOLIDATION_STRUCTURES(target_id number)
returns varchar
language SQL
execute as caller
as
$$
declare
    target_catalog varchar;
    target_schema varchar;
    target_table varchar;
    fl_already_created_view number;
begin
    -- Information about the target table fields
    let batch_config_rs resultset := (
        select upper(CO_TARGET_CATALOG), upper(CO_TARGET_SCHEMA), upper(CO_TARGET_TABLE)
        from DB_INGESTION_TOOLS_{{environment}}.BATCH.TB_BATCH_CONFIG
        where ID_SOURCE_BATCH = :target_id
    );

    let batch_config_c cursor for batch_config_rs;
    open batch_config_c;
    fetch batch_config_c into target_catalog, target_schema, target_table;
    close batch_config_c;

    let target_table_path varchar := :target_catalog || '.' || :target_schema || '.' || :target_table;
    let target_view varchar := 'VW_' || :target_table;
    let target_view_path varchar := :target_catalog || '.' || :target_schema || '.' || :target_view;

    -- Check if the consolidation structures have been created before
    let information_schema_views varchar := :target_catalog || '.INFORMATION_SCHEMA.VIEWS';
    select count(*) into :fl_already_created_view FROM identifier(:information_schema_views) where TABLE_CATALOG = :target_catalog and table_schema = :target_schema and TABLE_NAME = :target_view;
    if (:fl_already_created_view = 1) then
        return 'OK';
    end if;

    -- Create the consolidation view
    let create_view_sql varchar := 'call DB_INGESTIONS_TOOLS_{{environment}}.STREAMING.SP_CREATE_GLUE_VIEW(\'' ||
        :target_table_path || '\',\'' ||
        :target_view_path || '\');';
    execute immediate :create_view_sql;

    -- Create the consolidation stream
    let target_stream varchar := 'STM_' || :target_view; 
    let target_stream_path varchar := :target_catalog || '.' || :target_schema || '.' || :target_stream;
    let create_stream_sql varchar := 'create or replace stream ' || :target_stream_path || ' on view ' || :target_view_path || ' APPEND_ONLY = TRUE SHOW_INITIAL_ROWS = TRUE;';
    execute immediate create_stream_sql;

    -- Create and start the consolidation task
    let consolidation_task varchar := 'TSK_' || :target_table || '_CONSOLIDATION';
    let consolidation_task_path varchar := :target_catalog || '.' || :target_schema || '.' || :consolidation_task;
    let create_task_sql varchar := 'create or replace task ' || :consolidation_task_path || ' warehouse = {{warehouse}} schedule = \'120 MINUTES\' as ' ||
        'call DB_INGESTION_TOOLS_{{environment}}.STREAMING.SP_CONSOLIDATE_TABLE_MERGE(\'' || 
            :target_catalog || '\',\'' ||
            :target_schema || '\',\'' ||
            :target_view_path || '\',\'' ||
            :target_stream_path || '\',\'' ||
            :target_table_path || '_CONSOLIDATED\',\'' ||
            'PRIMARY_KEY_COLUMNS' || '\',\'' ||
            'TS_LOAD' || '\');';
    execute immediate create_task_sentence;

    let start_task_sql varchar := 'alter task ' || :consolidation_task_path || ' resume';
    execute immediate :start_task_sql;

    return 'OK';
end;
$$;

-- Procedure to load from a ".parquet" file to the target table that will be consolidated
create or replace procedure SP_LOAD_PARQUET(
    target_id number,
    external_stage varchar,
    external_file_path varchar,
    target_path varchar
)
returns varchar
language SQL
execute as caller
as
$$
declare
    tmp_file_format_cons varchar := 'DB_INGESTION_TOOLS_{{environment}}.BATCH.FF_BATCH_' || :target_id;
    ts_load timestamp_ntz := current_timestamp();
begin
    -- Information about the target table fields
    let fields_config_rs resultset := (
        select concat('"', upper(DS_FIELD_NAME), '"') as DS_FIELD_NAME, CO_TYPE, QT_LENGTH, QT_SCALE
        from DB_INGESTION_TOOLS_{{environment}}.BATCH.TB_BATCH_FIELDS_CONFIG
        where ID_SOURCE_BATCH = :target_id
        order by CO_POSITION
    );
    let fields_config_c cursor for fields_config_rs;

    -- Iterate over the fields to generate the select subquery
    let fields_to_select_sql varchar := '';
    for field in fields_config_c do
        let field_name := field.DS_FIELD_NAME::string;
        let field_type := field.CO_TYPE::string;
        let field_length := field.QT_LENGTH::int;
        let field_scale := field.QT_SCALE::int;

        let parsed_field_type varchar := DB_INGESTION_TOOLS_{{environment}}.UTILS.UDF_PARSE_FIELD_TYPE(field.CO_TYPE, field.QT_LENGTH, field.QT_LENGTH, field.QT_SCALE)::string;

        let field_select_sql varchar := '$1:' || :field_name || '::' || :parsed_field_type || ',';
        fields_to_select_sql := :fields_to_select_sql || :field_select_sql;
    end for;

    -- Generate the statemenet to create a new file format for this load
    let create_file_format_sql varchar := 'create or replace temp file format ' || :tmp_file_format_cons || ' type = \'parquet\'';
    execute immediate :create_file_format_sql;

    -- Generate the statement to copy the data from the file in ADLS to the target table
    let file_format_sql varchar := 'file_format => ' || :tmp_file_format_cons;
    let file_pattern_sql varchar := 'pattern => \'' || '.*' || :external_file_path || '\'';
    let select_sql varchar := 'select ' || rtrim(fields_to_select_sql, ',') || ', cast(\''|| :ts_load || '\' as timestamp_ntz) 
        from @' || :external_stage || '(' || :file_format_sql || ', ' || :file_pattern_sql || ')';
    let copy_into_sql varchar := 'copy into ' || :target_path || ' from (' || :select_sql || ');';
    execute immediate :copy_into_sql;

    return 'OK';
end;
$$;

-- Procedure to load from a "text-like" (including fixed-length, csv, tsv, ...) file to the target table that will be consolidated
create or replace procedure SP_LOAD_TEXT(
    target_id number,
    external_stage varchar,
    external_file_path varchar,
    target_path varchar,
    is_fixed_width_file boolean,
    file_has_header boolean,
    file_separator varchar
)
returns varchar
language SQL
execute as caller
as
$$
declare
    tmp_file_format_cons varchar := 'DB_INGESTION_TOOLS_{{environment}}.BATCH.FF_BATCH_' || :target_id;
    ts_load timestamp_ntz := current_timestamp();
begin
    -- Information about the target table fields
    let fields_config_rs resultset := (
        select concat('"', upper(DS_FIELD_NAME), '"') as DS_FIELD_NAME, CO_POSITION, CO_FIRST_CHARACTER, FL_IS_PK, CO_TYPE, QT_LENGTH, QT_SCALE
        from DB_INGESTION_TOOLS_{{environment}}.BATCH.TB_BATCH_FIELDS_CONFIG
        where ID_SOURCE_BATCH = :target_id
        order by CO_POSITION
    );
    let fields_config_c cursor for fields_config_rs;
    
    -- Generate the statement to copy the data from the file
    let fields_to_select_sql varchar := '';
    for field in fields_config_c do

        let field_name := field.DS_FIELD_NAME::string;
        let field_is_pk := field.FL_IS_PK::boolean;
        let field_type := field.CO_TYPE::string;
        let field_length := field.QT_LENGTH::int;
        let field_scale := field.QT_SCALE::int;
        let parsed_field_type varchar := DB_INGESTION_TOOLS_{{environment}}.UTILS.UDF_PARSE_FIELD_TYPE(field.CO_TYPE, field.QT_LENGTH, field.QT_LENGTH, field.QT_SCALE)::string;
        let field_first_character := field.CO_FIRST_CHARACTER::int;
        let field_position := field.CO_POSITION::int;

        let field_select_sql varchar := '';
        if (:is_fixed_width_file) then
            let decimal_separator_character number := least(1, :field_scale);
            let exact_start_position number := :field_first_character + length(:file_separator) * (:field_position - 1) + 1;
            let substring_sql varchar := 'substr($1,' || :exact_start_position || ',' || (:field_length + decimal_separator_character) || ')';
            field_select_sql := 'cast(' || :substring_sql || ' as ' || :parsed_field_type || ') as ' || :field_name || ',';
        else
            field_select_sql := '$' || :field_position || ',';
        end if;
        fields_to_select_sql := :fields_to_select_sql || :field_select_sql;
        
    end for;
    
    let create_file_format_sql varchar := 'create or replace temp file format ' || :tmp_file_format_cons || '
        type = \'csv\'
        skip_header = ' || cast(:file_has_header as number);
    if (not :is_fixed_width_file) then
        create_file_format_sql := :create_file_format_sql || ' field_delimiter = \'' || :file_separator || '\'';
    end if;
    execute immediate :create_file_format_sql;

    let file_format_sql varchar := 'file_format => ' || :tmp_file_format_cons;
    let file_pattern_sql varchar := 'pattern => \'' || '.*' || :external_file_path || '\'';

    let select_sql varchar := 'select ' || rtrim(fields_to_select_sql, ',') || ',cast(\''|| :ts_load || '\' as timestamp_ntz)
        from @' || :external_stage || '(' || :file_format_sql || ', ' || :file_pattern_sql || ')';
    let copy_into_sql varchar := 'copy into ' || :target_path || ' from (' || :select_sql || ');';
    execute immediate :copy_into_sql;

    return 'OK';
end;
$$;

-- Procedure to save the status of a load process into the historical table
create or replace procedure SP_UPDATE_LOAD_HISTORY(
    id_data_factory_run varchar,
    ds_process_type varchar,
    co_status varchar,
    co_catalog varchar,
    co_schema varchar,
    co_table varchar,
    ds_file_name varchar
)
returns varchar
language SQL
execute as caller
as
$$
declare
    load_history_view varchar := :co_catalog || '.INFORMATION_SCHEMA.LOAD_HISTORY';
begin
    insert into DB_INGESTION_TOOLS_{{environment}}.DATA_CONTROL.TB_DATA_CONTROL_STATUS
    select
        :id_data_factory_run as CO_DF_RUN_ID,
        :ds_process_type as DS_PROCESS,
        current_timestamp() as DT_LOAD,
        :co_status as CO_STATUS,
        :co_catalog as DS_TARGET_CATALOG,
        :co_schema as DS_TARGET_SCHEMA,
        :co_table as DS_TARGET_TABLE,
        :ds_file_name as DS_FILE_NAME,
        lh.ROW_COUNT as QT_TOTAL_ROWS,
        lh.ROW_PARSED as QT_LOADED_ROWS
    from identifier(:load_history_view) lh
    where FILE_NAME like ('%' || :ds_file_name)
        and TABLE_NAME = :co_table
        and SCHEMA_NAME = :co_schema
    order by LAST_LOAD_TIME DESC
    limit 1;

    return 'OK';
end;
$$;