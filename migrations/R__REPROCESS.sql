USE DATABASE DB_INGESTION_TOOLS_{{environment}};
USE ROLE {{environment}}_LND_AUTOMATION_FR;

USE SCHEMA STREAMING;

-- Procedure to reprocess data from streaming sources
create or replace procedure SP_REPROCESS_DATA(
    dt_from varchar,
    dt_to varchar,
    ds_reprocess_data_path varchar,
    ds_target_catalog varchar,
    ds_target_schema varchar,
    ds_target_table varchar
)
returns varchar
language SQL
execute as caller
as
$$
declare
    date_format varchar := 'YYYY/MM/DD';
begin
    let start_date varchar := to_date(:dt_from, :date_format);
    let end_date varchar := to_date(:dt_to, :date_format);

    -- Diference in days between start and end
    let day_difference integer := (select datediff('day', :start_date, :end_date)::integer) + 1;
    let all_dates resultset := (
        select to_char(dateadd(DAY, SEQ4(), :start_date), 'YYYY/MM/DD') as my_date
        from TABLE(generator(rowcount => :day_difference))
    );

    -- Created a regex pattern to recover data from every date to reprocess
    let date_regex_pattern varchar := '';
    for recover_date in all_dates do
        date_regex_pattern := :date_regex_pattern || '|' || '(.*/' || recover_date.my_date || '/)';
    end for;
    date_regex_pattern := ltrim(:date_regex_pattern, '|');
    date_regex_pattern := :ds_reprocess_data_path || '(' || :date_regex_pattern || ').*[.]snappy[.]parquet';

    -- Execute the copy into query to retrieve the data to reprocess
    let copy_from_unload_sql varchar := '
        copy into ' || :ds_target_catalog || '.' || :ds_target_schema || '.' || :ds_target_table || '
        from (
            select parse_json(parse_json($1):RECORD_METADATA) as RECORD_METADATA, parse_json(parse_json($1):RECORD_CONTENT) as RECORD_CONTENT
            from @streaming.stg_unload
        )
        PATTERN = \'' || :date_regex_pattern || '\'
        FILE_FORMAT = (TYPE = \'parquet\')
    ';
    execute immediate :copy_from_unload_sql;

    return :copy_from_unload_sql;
end;
$$;
