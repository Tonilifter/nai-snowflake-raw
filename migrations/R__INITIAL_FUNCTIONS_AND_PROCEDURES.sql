CREATE OR REPLACE FUNCTION ECI_INGESTION_TOOLS.UTILS.parse_field_type ( col string , longitud int, decimales int)
  RETURNS STRING
  AS
  $$
      select
      case col
        when 'VARCHAR' then 'STRING'
        when 'VARCHAR2' then 'STRING'
        when 'CHARACTER' then 'STRING'
        when 'TIMESTAMP' then 'TIMESTAMP_NTZ'
        when 'TIMESTAMP(6' then 'TIMESTAMP_NTZ'
        when 'BLOB' then 'BINARY'
        when 'SMALLINT' then 'NUMBER'
        when 'BIGINT' then 'NUMBER'
        when 'INTEGER' then 'NUMBER'
        when 'CLOB' then 'STRING'
        when 'REAL' then 'FLOAT'
        when 'DATE' then 'DATE'
        when 'DOUBLE' then 'FLOAT'
        when 'DECIMAL' then concat('DECIMAL(',longitud,',',iff(decimales is null,0,decimales),')')
        when 'NUMBER' then concat('DECIMAL(',longitud,',',iff(decimales is null,0,decimales),')')
        when 'XML' then 'VARIANT'
        when 'TIME' then 'TIME'
        else COL
     end
  $$
  ;

-- Funcion para calcular los campos que conforman la PK de una tabla y devolverlos en un array
create or replace function ECI_INGESTION_TOOLS.UTILS.get_table_pk_array (DDL string)
returns array
language javascript
as
$$
    var keyword = "primary key";
    var ins = -1;
    var s = DDL.split("\n");
    for (var i = 0; i < s.length; i++) {  
        ins = s[i].indexOf(keyword);
        if (ins != -1) {
            var colList = s[i].substring(ins + keyword.length);
            colList = colList.replace("(", "");
            colList = colList.replace(")", "");
            var colArray = colList.split(",");
            for (c = 0; c < colArray.length; c++) {
                colArray[c] = colArray[c].trim();
            }
            return colArray;
        }
    }
    return [];
$$
;

-- Crea una vista aplanada de una tabla Kafka
CREATE OR REPLACE PROCEDURE ECI_INGESTION_TOOLS.STREAMING.CREATE_KAFKA_FLATTEN_VIEW (
  source_table_name VARCHAR, -- Nombre de la tabla Kafka, incluido base de datos y esquema
  view_name VARCHAR -- Nombre que recibirá la vista creada
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    BEGIN
        create or replace temporary table parsed_metadata as
            select f.VALUE as record
            from identifier(:source_table_name),
                lateral flatten(PARSE_JSON(RECORD_METADATA:headers.ValueRecordSchema):fields) f
            where f.value:name = 'beforeImage' and RECORD_CONTENT:A_ENTTYP <> 'DL'
            limit 1;
    
        create or replace temporary table fields as
            select COLUMN_NAME, ECI_INGESTION_TOOLS.UTILS.parse_field_type(COLUMN_TYPE,PRECISION,SCALE) as COLUMN_TYPE
            from (
                select
                    replace(f.VALUE:name,'"','') as COLUMN_NAME,
                    replace(IFF (IS_ARRAY(PARSE_JSON(f.VALUE:type)), PARSE_JSON(f.VALUE:type)[0]:logicalType, PARSE_JSON(f.VALUE:type):logicalType),'"','') as COLUMN_TYPE,
                    replace(IFF (IS_ARRAY(PARSE_JSON(f.VALUE:type)), PARSE_JSON(f.VALUE:type)[0]:length, PARSE_JSON(f.VALUE:type):length),'"','') as COLUMN_SIZE,
                    replace(IFF (IS_ARRAY(PARSE_JSON(f.VALUE:type)), PARSE_JSON(f.VALUE:type)[0]:precision, PARSE_JSON(f.VALUE:type):precision),'"','') as PRECISION,
                    replace(IFF (IS_ARRAY(PARSE_JSON(f.VALUE:type)), PARSE_JSON(f.VALUE:type)[0]:scale, PARSE_JSON(f.VALUE:type):scale),'"','') as SCALE
                from parsed_metadata, lateral flatten(RECORD:type[1].fields) f
            );
            
        let create_view_sql := concat('create view if not exists ',view_name,' as select ');
        create_view_sql := concat(create_view_sql,' RECORD_CONTENT:A_CCID::int as A_CCID,');
        create_view_sql := concat(create_view_sql,' RECORD_CONTENT:A_ENTTYP::string as A_ENTTYP,');
        create_view_sql := concat(create_view_sql,' RECORD_CONTENT:A_TIMSTAMP::datetime as A_TIMSTAMP,');
        create_view_sql := concat(create_view_sql,' RECORD_METADATA:offset::NUMBER as OFFSET,');
        create_view_sql := concat(create_view_sql,' RECORD_METADATA:partition::NUMBER as PARTITION,');
    
        let fields_cursor CURSOR FOR SELECT * FROM fields;
        OPEN fields_cursor;
        FOR field IN fields_cursor DO
            create_view_sql := concat(create_view_sql,'IFF(RECORD_CONTENT:A_ENTTYP <> \'DL\',TRIM(RECORD_CONTENT:afterImage:',field.COLUMN_NAME,'::',field.COLUMN_TYPE,'),TRIM(RECORD_CONTENT:beforeImage:',field.COLUMN_NAME,'::',field.COLUMN_TYPE,')) as ', field.COLUMN_NAME, ',');
        END FOR;
        create_view_sql := concat(create_view_sql,'ARRAY_SORT(Object_keys(RECORD_METADATA:key)) as PRIMARY_KEY_COLUMNS ');
        create_view_sql := concat(create_view_sql,'FROM ',source_table_name,';');
    
        execute immediate create_view_sql;

        RETURN create_view_sql;
    END;
$$
;

GRANT USAGE ON PROCEDURE ECI_INGESTION_TOOLS.STREAMING.CREATE_KAFKA_FLATTEN_VIEW(VARCHAR, VARCHAR) TO ROLE {{ environment }}_LND_NA_DEVELOPER_FR;


-- Crea una vista a partir de una tabla Kafka con inyección de esquema
CREATE OR REPLACE PROCEDURE ECI_INGESTION_TOOLS.STREAMING.CREATE_KAFKA_SCHEMA_VIEW (
  source_table_name VARCHAR, -- Nombre de la tabla Kafka, incluido base de datos y esquema
  view_name VARCHAR -- Nombre que recibirá la vista creada
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    BEGIN     
        let create_view_sql := concat('create view if not exists ',view_name,' as select * exclude RECORD_METADATA,');
        create_view_sql := concat(create_view_sql,' RECORD_METADATA:CreateTime::NUMBER as CREATETIME_METADATA,');
        create_view_sql := concat(create_view_sql,' RECORD_METADATA:offset::NUMBER as OFFSET,');
        create_view_sql := concat(create_view_sql,' RECORD_METADATA:partition::NUMBER as PARTITION,');
        create_view_sql := concat(create_view_sql,' ARRAY_SORT(Object_keys(RECORD_METADATA:key)) as PRIMARY_KEY_COLUMNS ');
        create_view_sql := concat(create_view_sql,'FROM ',source_table_name,';');
    
        execute immediate create_view_sql;

        RETURN create_view_sql;
    END;
$$
;

-- Crea una vista de una tabla SNP Glue
CREATE OR REPLACE PROCEDURE ECI_INGESTION_TOOLS.STREAMING.CREATE_GLUE_VIEW (
  source_table_name VARCHAR, -- Nombre de la tabla SNP Glue, incluido base de datos y esquema
  view_name VARCHAR -- Nombre que recibirá la vista creada
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    BEGIN     
        let create_view_sql := concat('create view if not exists ',view_name,' as select *, ');
        create_view_sql := concat(create_view_sql,' ECI_INGESTION_TOOLS.UTILS.get_table_pk_array(get_ddl(\'table\',\'',source_table_name,'\')) as PRIMARY_KEY_COLUMNS ');
        create_view_sql := concat(create_view_sql,' FROM ',source_table_name,';');
    
        execute immediate create_view_sql;

        RETURN create_view_sql;
    END;
$$
;


-- Procedimiento para consolidación de tablas streaming
CREATE OR REPLACE PROCEDURE ECI_INGESTION_TOOLS.STREAMING.CONSOLIDATE_TABLE_MERGE (
	database VARCHAR, -- Base de datos sobre la que trabajamos
    schema VARCHAR, -- Esquema sobre el que trabajamos
    source_view_name VARCHAR, -- Nombre de la vista aplanada de la tabla Kafka o SNP Glue (incluido base de datos y esquema)
	source_stream_name VARCHAR, -- Nombre del stream de la vista aplanada de la tabla Kafka o SNP Glue(incluido base de datos y esquema)
    target_table_name VARCHAR, -- Nombre que se le va a dar a la tabla consolidada (*_CONSOLIDATED) (incluido base de datos y esquema)
    pk_field VARCHAR, -- Nombre del campo que contiene la PK de la tabla en la vista
    order_field VARCHAR -- Nombre del campo que nos servirá para ordenar los registros
)
RETURNS TABLE()
LANGUAGE SQL
AS
$$
	DECLARE
		information_table varchar := concat(:database, '.INFORMATION_SCHEMA.TABLES');
    target_table_name_only varchar := replace(:target_table_name,:database||'.'||:schema||'.', ''); 
		res resultset;
		count_tables int := 0;

	BEGIN
		--------------------------------------------------------------------------------------
		-- Extraemos las columnas que conforman la PK y las guardamos como array (desde vista aplanada)
		let cursor_sql VARCHAR DEFAULT 'select f.value::string as pk from (select * from '||source_view_name||' limit 1) as a, lateral flatten('||pk_field||') f';
		let cursor_resultset RESULTSET DEFAULT (EXECUTE IMMEDIATE :cursor_sql);
		let pk_cursor CURSOR FOR cursor_resultset;
		let pk_columns array := ARRAY_CONSTRUCT();
		FOR field IN pk_cursor DO
			pk_columns := ARRAY_APPEND(pk_columns,field.pk::string);
		END FOR;
		CLOSE pk_cursor;
		
		--------------------------------------------------------------------------------------
		-- En primer lugar creamos la tabla consolidada si no existe desde la vista aplanada.
		SELECT count(*) INTO :count_tables FROM identifier(:information_table) WHERE TABLE_CATALOG = :database and TABLE_SCHEMA = :schema and TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = :target_table_name_only;
		IF (count_tables = 0) THEN
			let create_table_consolidated_sql := concat('CREATE TABLE ',target_table_name,' IF NOT EXISTS AS SELECT *, current_date() as dt_snapshot, current_timestamp() as ts_snapshot FROM ',source_view_name,' WHERE FALSE;');
			execute immediate create_table_consolidated_sql;
			-- Añadimos la PK a la tabla
			IF (array_size(pk_columns) > 0) THEN
				let add_pk_sql := concat('ALTER TABLE ',target_table_name,' ADD PRIMARY KEY (');
				let comma := '';
				FOR i IN 0 to array_size(pk_columns) - 1 DO
					add_pk_sql := concat(add_pk_sql,comma,GET(pk_columns,i));
					comma := ',';
				END FOR;
				add_pk_sql := concat(add_pk_sql,');');
				execute immediate add_pk_sql;
			END IF;
		END IF;
		
		--------------------------------------------------------------------------------------
		-- Iniciamos la construcción de la sentencia merge utilizando el Stream
		let merge_sql := concat('MERGE INTO ',target_table_name,' target USING ');
		-- Excluimos los campos METADATA$ROW_ID, METADATA$ACTION y METADATA$ISUPDATE que se generan el Stream
		merge_sql := concat(merge_sql,'(SELECT * EXCLUDE (METADATA$ROW_ID, METADATA$ACTION, METADATA$ISUPDATE) FROM ',source_stream_name,' ');
		merge_sql := concat(merge_sql,'QUALIFY row_number() OVER (PARTITION BY ');
		let comma := '';
		FOR i IN 0 to array_size(pk_columns) - 1 DO
			merge_sql := concat(merge_sql,comma,GET(pk_columns,i));
			comma := ',';
		END FOR;
		merge_sql := concat(merge_sql,' ORDER BY ',order_field,' desc) = 1) origin ON ');
		let union_command := '';
		FOR i IN 0 to array_size(pk_columns) - 1 DO
			merge_sql := concat(merge_sql,union_command,' target.',GET(pk_columns,i),' = origin.',GET(pk_columns,i));
			union_command := ' AND';
		END FOR;
		merge_sql := concat(merge_sql,' WHEN matched then update set ');

		-- Extraemos las columnas de la vista aplanada para poder iterarlas
		let cursor_columns_sql VARCHAR DEFAULT 'show columns in table '||source_view_name||';';
		let cursor_columns_resultset RESULTSET DEFAULT (EXECUTE IMMEDIATE :cursor_columns_sql);
		--return table(cursor_columns_resultset);
		let columns_cursor CURSOR FOR cursor_columns_resultset;
		let columns_arr array := ARRAY_CONSTRUCT();
		FOR col IN columns_cursor DO
			--return 'entraaaa';
			columns_arr := ARRAY_APPEND(columns_arr,col."column_name"::string);
		END FOR;
		--return columns_arr;
		CLOSE columns_cursor;
		comma := '';
		FOR i IN 0 to array_size(columns_arr) - 1 DO
			merge_sql := concat(merge_sql,comma,' target."',GET(columns_arr,i),'" = origin."',GET(columns_arr,i),'"');
			comma := ',';
		END FOR;
		-- Añadimos a mano los campos DT_SNAPSHOT y TS_SNAPSHOT en el update del merge
		merge_sql := concat(merge_sql,', target.DT_SNAPSHOT = current_date(), target.TS_SNAPSHOT = current_timestamp() ');
		
		merge_sql := concat(merge_sql,' WHEN not matched then insert( ');
		comma := '';
		FOR i IN 0 to array_size(columns_arr) - 1 DO
			merge_sql := concat(merge_sql,comma,'"',GET(columns_arr,i),'"');
			comma := ',';
		END FOR;
		-- Añadimos a mano los campos DT_SNAPSHOT y TS_SNAPSHOT en el insert del merge
		merge_sql := concat(merge_sql,',DT_SNAPSHOT,TS_SNAPSHOT) values ( ');
		comma := '';
		FOR i IN 0 to array_size(columns_arr) - 1 DO
			merge_sql := concat(merge_sql,comma,'origin."',GET(columns_arr,i),'"');
			comma := ',';
		END FOR;
		merge_sql := concat(merge_sql,',current_date(),current_timestamp());');
		
		-- Ejecutamos el MERGE
		res := (execute immediate :merge_sql);
		RETURN TABLE(res);
	END;
$$
;


-- Procedimiento que comprueba si existen nuevas tablas procedentes de una fuente Streaming
CREATE OR REPLACE PROCEDURE ECI_INGESTION_TOOLS.STREAMING.CHECK_NEW_STREAMING_TABLE (
    origin_database VARCHAR, -- Base de datos Origen donde buscar nuevas tablas
    origin_schema VARCHAR, -- Esquema Origen donde buscar nuevas tablas
	  table_prefix VARCHAR, -- Prefijo de las tablas a buscar
  	consolidated_table_suffix VARCHAR, -- Sufijo de las tablas consolidadas a descartar
    is_schema_infered BOOLEAN, --- Indica si la tabla se ha generado con inferencia de esquema o no. Para SNP Glue el valor siempre será TRUE
    is_glue BOOLEAN --- Indica si la tabla proviene de SNP Glue o no. (true/false)
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    DECLARE
        information_table varchar := concat(:origin_database, '.INFORMATION_SCHEMA.TABLES');
        table_name_like varchar := concat(:table_prefix,'%');
        table_name_not_like varchar := concat('%',:consolidated_table_suffix);
        res varchar := '';
    BEGIN
        -- Lanzamos consulta sobre la vista para extraer todas las tablas de Kafka
        LET tables_cursor CURSOR FOR SELECT * FROM identifier(?) I1 WHERE TABLE_CATALOG = ? and TABLE_SCHEMA = ? and TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE ? AND TABLE_NAME LIKE ? AND NOT EXISTS (SELECT 1 FROM STAGING.INFORMATION_SCHEMA.TABLES I2 where TABLE_NAME = concat(I1.TABLE_NAME,?));
        OPEN tables_cursor using(information_table, origin_database, origin_schema, table_name_not_like, table_name_like, consolidated_table_suffix);
        -- Recorremos todas las tablas Kafka
        FOR new_table IN tables_cursor DO
            -- Construimos el nombre de la tabla y la vista
            let completed_table_name := concat(new_table.TABLE_CATALOG,'.',new_table.TABLE_SCHEMA,'.',new_table.TABLE_NAME);
            let completed_view_name := concat(new_table.TABLE_CATALOG,'.',new_table.TABLE_SCHEMA,'.',new_table.TABLE_NAME,'_VW');

            -- Creamos la vista aplanada de la tabla original
            IF (is_schema_infered) THEN
                IF (is_glue) THEN
                    res := concat('call ECI_INGESTION_TOOLS.STREAMING.CREATE_GLUE_VIEW(\'',completed_table_name,'\',\'',completed_view_name,'\');');
                    execute immediate :res;
                ELSE
                    res := concat('call ECI_INGESTION_TOOLS.STREAMING.CREATE_KAFKA_SCHEMA_VIEW(\'',completed_table_name,'\',\'',completed_view_name,'\');');
                    execute immediate :res;
                END IF;
            ELSE
                res := concat('call ECI_INGESTION_TOOLS.STREAMING.CREATE_KAFKA_FLATTEN_VIEW(\'',completed_table_name,'\',\'',completed_view_name,'\');');
                execute immediate :res;
            END IF;

            -- Creamos el Stream sobre la vista recien creada
            let create_stream_view_sentence := concat('CREATE OR REPLACE STREAM ',completed_view_name,'_STREAM ON VIEW ',completed_view_name,' APPEND_ONLY = TRUE SHOW_INITIAL_ROWS = TRUE;');
            execute immediate create_stream_view_sentence;

            -- Creamos el Stream sobre la tabla recien creada
            let create_stream_table_sentence := concat('CREATE OR REPLACE STREAM ',completed_table_name,'_STREAM ON TABLE ',completed_table_name,' APPEND_ONLY = TRUE SHOW_INITIAL_ROWS = TRUE;');
            execute immediate create_stream_table_sentence;

            -- Creamos el task que ejecutará la consolidación.
            let create_task_sentence := concat('CREATE OR REPLACE TASK ',completed_table_name,'_CONSOLIDATION_TASK WAREHOUSE = SNW_CORP_ACCENTURE_BIG SCHEDULE = \'5 MINUTES\' AS ');
            create_task_sentence := concat(create_task_sentence,'call ECI_INGESTION_TOOLS.STREAMING.CONSOLIDATE_TABLE_MERGE(');
            create_task_sentence := concat(create_task_sentence, '\'', new_table.TABLE_CATALOG,'\',');
            create_task_sentence := concat(create_task_sentence, '\'', new_table.TABLE_SCHEMA,'\',');
            create_task_sentence := concat(create_task_sentence, '\'', completed_view_name,'\',');
            create_task_sentence := concat(create_task_sentence, '\'', completed_view_name,'_STREAM\',');
            create_task_sentence := concat(create_task_sentence, '\'', completed_table_name, consolidated_table_suffix,'\',');
            create_task_sentence := concat(create_task_sentence, '\'PRIMARY_KEY_COLUMNS\',');
            IF (is_glue) THEN
                create_task_sentence := concat(create_task_sentence, '\'GLCHANGETIME\'');
            ELSE
                create_task_sentence := concat(create_task_sentence, '\'OFFSET\'');
            END IF;
            create_task_sentence := concat(create_task_sentence, ');');
            execute immediate create_task_sentence;
            -- Ejecutamos el nuevo task
            let resume_task_sentence := concat('ALTER TASK ',completed_table_name,'_CONSOLIDATION_TASK RESUME;');
            execute immediate resume_task_sentence;
            
        END FOR;
        CLOSE tables_cursor;
        RETURN 'SUCCESS';
    END;
$$
;


create or replace procedure ECI_INGESTION_TOOLS.BATCH.SP_GET_ADL_PATH (file_name varchar)
returns varchar
language SQL
execute as owner
as
$$
declare
    load_error_cons varchar := 'Error';
    external_path_rs resultset;
    external_path varchar;
begin

    external_path := (
        select distinct concat(upper(CO_ADL_PATH), to_char(current_date(),'YYYY/MM/DD'), '/')
        from ECI_INGESTION_TOOLS.BATCH.TB_FILE_CONFIG
        where :file_name regexp TX_FILE_REGEX
    );
    return external_path;
    
exception
    when other then
        return :load_error_cons || ' (' || SQLCODE || '): ' || SQLERRM;
        
end;
$$;


create or replace procedure ECI_INGESTION_TOOLS.BATCH.SP_LOAD_BATCH_FILE(file_name varchar)
returns varchar
language SQL
execute as caller
as
$$
declare
    tmp_file_format_cons varchar := 'ECI_INGESTION_TOOLS.BATCH.FF_BATCH';
    load_success_cons varchar := 'Success';
    load_error_cons varchar := 'Error';
    
    batch_load_ts timestamp_ntz := current_timestamp();
    
    target_id number;
    target_catalog varchar;
    target_schema varchar;
    target_table varchar;
    
    file_has_header boolean;
    file_separator varchar;

    target_fields array := array_construct();

    create_table_sql varchar := '';
    copy_into_sql varchar := '';
begin
    -- Information about the target file
    let file_config_rs resultset := (
        select ID_FILE, upper(CO_TARGET_CATALOG), upper(CO_TARGET_SCHEMA), upper(CO_TARGET_TABLE), FL_HAS_HEADER, TX_FILE_SSEPARATOR
        from ECI_INGESTION_TOOLS.BATCH.TB_FILE_CONFIG
        where :file_name regexp TX_FILE_REGEX
    );
    let file_config_c cursor for file_config_rs;
    open file_config_c;
    fetch file_config_c into target_id, target_catalog, target_schema, target_table, file_has_header, file_separator;
    close file_config_c;
    
    let target_path varchar := :target_catalog || '.' || :target_schema || '.' || :target_table;
    let is_fixed_width_file boolean := (:file_separator = '');

    -- Information about the target table fields
    let fields_config_rs resultset := (
        select concat('"', upper(DS_FIELD), '"') as DS_FIELD, CO_POSITION, CO_FIRST_CHARACTER, FL_IS_PK, CO_TYPE, QT_LENGTH, QT_SCALE
        from ECI_INGESTION_TOOLS.BATCH.TB_FIELDS_CONFIG
        where ID_FILE = :target_id
        order by CO_POSITION
    );
    let fields_config_c cursor for fields_config_rs;

    -- Iterate over the fields to generate the creation and copy into queries
    for field in fields_config_c do
    
    	let obj variant := object_construct(
            'field', field.DS_FIELD::string,
            'position', field.CO_POSITION::int,
            'first_character', field.CO_FIRST_CHARACTER::int,
            'is_pk', field.FL_IS_PK::boolean,
            'type', field.CO_TYPE::varchar,
            'length', field.QT_LENGTH::int,
            'scale', field.QT_SCALE::int,
            'parsed_type', eci_ingestion_tools.utils.parse_field_type(field.CO_TYPE, field.QT_LENGTH, field.QT_SCALE)
        );
    	target_fields := array_append(target_fields,obj);
        
    end for;

    -- Generate the statement to create the table in case it does not exist
    let target_fields_sql varchar := '';
    let target_pk_fields_sql varchar := '';
    for i in 0 to (array_size(target_fields) - 1) do
    
        let field_name := get(target_fields, i):field::string;
        let field_is_pk := get(target_fields, i):is_pk::boolean;
        let field_type := get(target_fields, i):type::string;
        let field_length := get(target_fields, i):length::int;
        let field_scale := get(target_fields, i):scale::int;
        let parsed_field_type varchar := get(target_fields, i):parsed_type::string;
        
        target_fields_sql := :target_fields_sql || :field_name || ' ' || :parsed_field_type || ',';

        if (field_is_pk) then
            target_pk_fields_sql := :target_pk_fields_sql || :field_name || ',';
        end if;
        
    end for;

    target_fields_sql := :target_fields_sql || 'load_timestamp timestamp_ntz';
    if (length(target_pk_fields_sql) != 0) then
        target_fields_sql := :target_fields_sql || ', constraint ' || lower(:target_table) || '_pk primary key(' ||  rtrim(:target_pk_fields_sql, ',') || ')';
    end if;
 
    create_table_sql := 'create table if not exists ' || :target_path || '(' || :target_fields_sql || ') enable_schema_evolution = true;';
    execute immediate :create_table_sql;

    -- Generate the statement to copy the data from the file
    let fields_to_select_sql varchar := '';
    for i in 0 to (array_size(target_fields) - 1) do

        let field_name := get(target_fields, i):field::string;
        let field_is_pk := get(target_fields, i):is_pk::boolean;
        let field_type := get(target_fields, i):type::string;
        let field_length := get(target_fields, i):length::int;
        let field_scale := get(target_fields, i):scale::int;
        let parsed_field_type varchar := get(target_fields, i):parsed_type::string;
        let field_first_character := get(target_fields, i):first_character::int;
        let field_position := get(target_fields, i):position::int;

        let field_select_sql varchar := '';
        if (:is_fixed_width_file) then
            let decimal_separator_character number := least(1, :field_scale);
            let exact_start_position number := :field_first_character + length(:file_separator) * (:field_position - 1) + 1;
            let substring_sql varchar := 'substr($1,' || :exact_start_position || ',' || (:field_length + decimal_separator_character) || ')';
            field_select_sql := 'cast(' || :substring_sql || ' as ' || :parsed_field_type || ') as ' || :field_name || ',';
        else
            field_select_sql := '$' || (i + 1) || ',';
        end if;
        fields_to_select_sql := :fields_to_select_sql || :field_select_sql;
        
    end for;
    
    let create_file_format_sql varchar := 'create or replace file format ' || :tmp_file_format_cons || '
        type = \'csv\'
        skip_header = ' || cast(:file_has_header as number);
    if (not :is_fixed_width_file) then
        create_file_format_sql := :create_file_format_sql || ' field_delimiter = \'' || :file_separator || '\'';
    end if;
    execute immediate :create_file_format_sql;

    let file_format_sql varchar := 'file_format => ' || :tmp_file_format_cons;
    
    let file_path_regex varchar := '.*' || :target_catalog || '/' || :target_schema || '/' || :target_table || to_char(current_date(), '/YYYY/MM/DD/') || :file_name;
    let file_pattern_sql varchar := 'pattern => \'' || :file_path_regex || '\'';
    
    let select_sql varchar := 'select ' || rtrim(fields_to_select_sql, ',') || ',cast(\''|| :batch_load_ts || '\' as timestamp_ntz)
        from @STAGING.PUBLIC.BATCH_STORAGE_STAGE (' || :file_format_sql || ', ' || :file_pattern_sql || ')';
    copy_into_sql := 'copy into ' || :target_path || ' from (' || :select_sql || ');';
    execute immediate :copy_into_sql;
    
    -- Update the batch load history table
    insert into ECI_INGESTION_TOOLS.BATCH.TB_LOAD_HISTORY
    select FILE_NAME, :batch_load_ts, :load_success_cons, ROW_COUNT, ROW_PARSED, :target_catalog, SCHEMA_NAME, TABLE_NAME
    from ECI_INGESTION_TOOLS.INFORMATION_SCHEMA.LOAD_HISTORY
    where FILE_NAME like ('%' || :file_name)
        and LAST_LOAD_TIME > dateadd(days, -1, current_date())
        and TABLE_NAME = :target_table
        and SCHEMA_NAME = :target_schema
    limit 1;
    
    return :load_success_cons;
exception
    when other then

        -- Update the batch load history table
        insert into ECI_INGESTION_TOOLS.BATCH.TB_LOAD_HISTORY (CO_FILE_NAME, DT_LOAD, CO_STATUS, CO_TARGET_CATALOG, CO_TARGET_SCHEMA, CO_TARGET_TABLE)
        select FILE_NAME, :batch_load_ts, :load_error_cons, :target_catalog, SCHEMA_NAME, TABLE_NAME
        from ECI_INGESTION_TOOLS.INFORMATION_SCHEMA.LOAD_HISTORY
        where FILE_NAME like ('%' || :file_name)
            and LAST_LOAD_TIME > dateadd(days, -1, current_date())
            and TABLE_NAME = :target_table
            and SCHEMA_NAME = :target_schema
        limit 1;
        
        return :load_error_cons || ' (' || SQLCODE || '): ' || SQLERRM;
end;
$$;