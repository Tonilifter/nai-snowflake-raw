USE DATABASE DB_INGESTION_TOOLS_{{environment}};

USE ROLE {{environment}}_LND_AUTOMATION_FR;

USE SCHEMA UTILS;

CREATE OR REPLACE FUNCTION UDF_PARSE_FIELD_TYPE ( col string , longitud int, precision int, decimales int)
  RETURNS STRING
  AS
  $$
      select
      case col
        when 'VARCHAR' then iff(longitud is null,'STRING', concat('VARCHAR(',longitud,')'))
        when 'VARCHAR2' then iff(longitud is null,'STRING', concat('VARCHAR(',longitud,')'))
        when 'CHARACTER' then iff(longitud is null,'STRING', concat('VARCHAR(',longitud,')'))
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
        when 'DECIMAL' then concat('DECIMAL(',precision,',',iff(decimales is null,0,decimales),')')
        when 'NUMBER' then concat('DECIMAL(',precision,',',iff(decimales is null,0,decimales),')')
        when 'XML' then 'VARIANT'
        when 'TIME' then 'TIME'
        else COL
     end
  $$
  ;

-- Funcion para calcular los campos que conforman la PK de una tabla y devolverlos en un array
create or replace function UDF_GET_TABLE_PK_ARRAY (DDL string)
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


USE SCHEMA STREAMING;
-- Crea una vista aplanada de una tabla Kafka
CREATE OR REPLACE PROCEDURE SP_CREATE_KAFKA_FLATTEN_VIEW (
  source_table_name VARCHAR, -- Nombre de la tabla Kafka, incluido base de datos y esquema
  view_name VARCHAR -- Nombre que recibirá la vista creada
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
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
            select COLUMN_NAME, DB_INGESTION_TOOLS_{{environment}}.UTILS.UDF_PARSE_FIELD_TYPE(COLUMN_TYPE,COLUMN_SIZE,PRECISION,SCALE) as COLUMN_TYPE
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
            create_view_sql := concat(create_view_sql,'IFF(RECORD_CONTENT:A_ENTTYP <> \'DL\',TRIM(RECORD_CONTENT:afterImage:',field.COLUMN_NAME,')::',field.COLUMN_TYPE,',TRIM(RECORD_CONTENT:beforeImage:',field.COLUMN_NAME,')::',field.COLUMN_TYPE,') as ', field.COLUMN_NAME, ',');
        END FOR;
        create_view_sql := concat(create_view_sql,'ARRAY_SORT(Object_keys(RECORD_METADATA:key)) as PRIMARY_KEY_COLUMNS ');
        create_view_sql := concat(create_view_sql,'FROM ',source_table_name,';');
    
        execute immediate create_view_sql;

        let setViewTraking := concat('ALTER view ',view_name,' SET CHANGE_TRACKING = TRUE;');
        execute immediate setViewTraking;

        RETURN create_view_sql;
    END;
$$
;


-- Crea una vista a partir de una tabla Kafka con inyección de esquema
CREATE OR REPLACE PROCEDURE SP_CREATE_KAFKA_SCHEMA_VIEW (
  source_table_name VARCHAR, -- Nombre de la tabla Kafka, incluido base de datos y esquema
  view_name VARCHAR -- Nombre que recibirá la vista creada
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
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

        let setViewTraking := concat('ALTER view ',view_name,' SET CHANGE_TRACKING = TRUE;');
        execute immediate setViewTraking;

        RETURN create_view_sql;
    END;
$$
;

-- Crea una vista de una tabla SNP Glue
CREATE OR REPLACE PROCEDURE SP_CREATE_GLUE_VIEW (
  source_table_name VARCHAR, -- Nombre de la tabla SNP Glue, incluido base de datos y esquema
  view_name VARCHAR -- Nombre que recibirá la vista creada
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
    BEGIN     
        let create_view_sql := concat('create view if not exists ',view_name,' as select *, ');
        create_view_sql := concat(create_view_sql,' DB_INGESTION_TOOLS_{{environment}}.UTILS.UDF_GET_TABLE_PK_ARRAY(get_ddl(\'table\',\'',source_table_name,'\')) as PRIMARY_KEY_COLUMNS ');
        create_view_sql := concat(create_view_sql,' FROM ',source_table_name,';');
    
        execute immediate create_view_sql;

        let setViewTraking := concat('ALTER view ',view_name,' SET CHANGE_TRACKING = TRUE;');
        execute immediate setViewTraking;

        RETURN create_view_sql;
    END;
$$
;


-- Procedimiento para consolidación de tablas streaming
CREATE OR REPLACE PROCEDURE SP_CONSOLIDATE_TABLE_MERGE (
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
EXECUTE AS CALLER
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
CREATE OR REPLACE PROCEDURE SP_CHECK_NEW_STREAMING_TABLE (
    origin_database VARCHAR, -- Base de datos Origen donde buscar nuevas tablas
    origin_schema VARCHAR, -- Esquema Origen donde buscar nuevas tablas
	table_prefix VARCHAR, -- Prefijo de las tablas a buscar
  	consolidated_table_suffix VARCHAR, -- Sufijo de las tablas consolidadas a descartar
    is_schema_infered BOOLEAN, --- Indica si la tabla se ha generado con inferencia de esquema o no. Para SNP Glue el valor siempre será TRUE
    is_glue BOOLEAN --- Indica si la tabla proviene de SNP Glue o no. (true/false)
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
    DECLARE
        information_table varchar := concat(:origin_database, '.INFORMATION_SCHEMA.TABLES');
        table_name_like varchar := concat(:table_prefix,'%');
        table_name_not_like varchar := concat('%',:consolidated_table_suffix);
        res varchar := '';
        count_rows int := 0;
        is_schema_present_in_metadata boolean := FALSE;
    BEGIN
        -- Lanzamos consulta sobre la vista para extraer todas las tablas de Kafka
        LET tables_cursor CURSOR FOR SELECT * FROM identifier(?) I1 WHERE TABLE_CATALOG = ? and TABLE_SCHEMA = ? and TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE ? AND TABLE_NAME LIKE ? AND NOT EXISTS (SELECT 1 FROM identifier(?) I2 where I2.TABLE_NAME = concat(I1.TABLE_NAME,?));
        OPEN tables_cursor using(information_table, origin_database, origin_schema, table_name_not_like, table_name_like, information_table, consolidated_table_suffix);
        -- Recorremos todas las tablas Kafka
        FOR new_table IN tables_cursor DO
            -- Construimos el nombre de la tabla y la vista
            let database varchar := new_table.TABLE_CATALOG;
            let schema varchar := new_table.TABLE_SCHEMA;
            let tablename varchar := new_table.TABLE_NAME;
            let database_and_schema := concat(database,'.',schema, '.');
            let completed_table_name := concat(database_and_schema, tablename);
            let view_name := concat('VW_', tablename);
            let completed_view_name := concat(database_and_schema, view_name);
            let stream_table_name := concat('STM_', tablename);
            let completed_stream_table_name := concat(database_and_schema, stream_table_name);
            let stream_view_name := concat('STM_', view_name);
            let completed_stream_view_name := concat(database_and_schema, stream_view_name);
            let task_name := concat('TSK_', tablename, '_CONSOLIDATION');
            let completed_task_name := concat(database_and_schema, task_name);

            -- Chequeamos que la tabla no esté vacia. Si está vacía no hacemos nada
            SELECT count(*) INTO :count_rows FROM identifier(:completed_table_name);
		    IF (count_rows > 0) THEN
                -- Creamos la vista aplanada de la tabla original
                IF (is_schema_infered) THEN
                    is_schema_present_in_metadata := TRUE;
                    IF (is_glue) THEN
                        res := concat('call DB_INGESTION_TOOLS_{{environment}}.STREAMING.SP_CREATE_GLUE_VIEW(\'',completed_table_name,'\',\'',completed_view_name,'\');');
                        execute immediate :res;
                    ELSE
                        res := concat('call DB_INGESTION_TOOLS_{{environment}}.STREAMING.SP_CREATE_KAFKA_SCHEMA_VIEW(\'',completed_table_name,'\',\'',completed_view_name,'\');');
                        execute immediate :res;
                    END IF;
                ELSE
                    -- Chequeamos que la tabla Kafka venga con el esquema definido en los metadatos
                    select iff(RECORD_METADATA:headers.ValueRecordSchema is null, FALSE, TRUE) into :is_schema_present_in_metadata from identifier(:completed_table_name) where RECORD_METADATA:headers.A_ENTTYP <> 'DL' limit 1;
                    -- Si la tabla KAFKA viene sin esquema, no hacemos nada
                    IF (is_schema_present_in_metadata) THEN
                        res := concat('call DB_INGESTION_TOOLS_{{environment}}.STREAMING.SP_CREATE_KAFKA_FLATTEN_VIEW(\'',completed_table_name,'\',\'',completed_view_name,'\');');
                        execute immediate :res;
                    END IF;
                END IF;
                -- Si la tabla KAFKA viene sin esquema, no hacemos nada
                IF (is_schema_present_in_metadata) THEN
                    -- Creamos el Stream sobre la vista recien creada
                    let create_stream_view_sentence := concat('CREATE OR REPLACE STREAM ', completed_stream_view_name, ' ON VIEW ',completed_view_name,' APPEND_ONLY = TRUE SHOW_INITIAL_ROWS = TRUE;');
                    execute immediate create_stream_view_sentence;

                    -- Creamos el Stream sobre la tabla recien creada
                    let create_stream_table_sentence := concat('CREATE OR REPLACE STREAM ', completed_stream_table_name, ' ON TABLE ',completed_table_name,' APPEND_ONLY = TRUE SHOW_INITIAL_ROWS = TRUE;');
                    execute immediate create_stream_table_sentence;

                    -- Creamos el task que ejecutará la consolidación.
                    let create_task_sentence := concat('CREATE OR REPLACE TASK ', completed_task_name, ' WAREHOUSE = {{warehouse}} SCHEDULE = \'5 MINUTES\' AS ');
                    create_task_sentence := concat(create_task_sentence,'call DB_INGESTION_TOOLS_{{environment}}.STREAMING.SP_CONSOLIDATE_TABLE_MERGE(');
                    create_task_sentence := concat(create_task_sentence, '\'', database,'\',');
                    create_task_sentence := concat(create_task_sentence, '\'', schema,'\',');
                    create_task_sentence := concat(create_task_sentence, '\'', completed_view_name,'\',');
                    create_task_sentence := concat(create_task_sentence, '\'', completed_stream_view_name,'\',');
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
                    let resume_task_sentence := concat('ALTER TASK ', completed_task_name, ' RESUME;');
                    execute immediate resume_task_sentence;

                    let lake_path := concat(database, '/', schema, '/', tablename, '/');
                    let partition_expression := '';
                    IF (is_glue) THEN
                        partition_expression := 'split(GLCHANGETIME,\'.\')[0],\'YYYYMMDDHH24MISS\'';
                    ELSE
                        partition_expression := 'RECORD_CONTENT:A_TIMSTAMP';
                    END IF;
                    
                    insert into TB_UNLOAD_CONFIG(CO_TABLE_CATALOG, CO_TABLE_SCHEMA, CO_TABLE_NAME, DS_PARTITION_FIELD_EXPRESION, DS_DATA_LAKE_PATH, SQ_DAY_OF_MONTH, SQ_MONTH, SQ_DAY_OF_WEEK, CO_THREAD) 
                        select :database, :schema, :tablename, :partition_expression, :lake_path, ARRAY_CONSTRUCT('*'), ARRAY_CONSTRUCT('*'), ARRAY_CONSTRUCT('*'), 1;

                END IF;
            END IF;
        END FOR;
        CLOSE tables_cursor;
        RETURN 'SUCCESS';
    END;
$$
;

-- Procedimiento que crea todas las estructuras necesarias para trabajar con una tabla Streaming nueva
CREATE OR REPLACE PROCEDURE SP_CREATE_STREAMING_TABLE_COMPONENTS (
    origin_database VARCHAR, -- Base de datos Origen 
    origin_schema VARCHAR, -- Esquema Origen 
	table_name VARCHAR, -- Nombre de la tabla
  	consolidated_table_suffix VARCHAR, -- Sufijo de las tablas consolidadas
    is_schema_infered BOOLEAN, --- Indica si la tabla se ha generado con inferencia de esquema o no. Para SNP Glue el valor siempre será TRUE
    is_glue BOOLEAN --- Indica si la tabla proviene de SNP Glue o no. (true/false)
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
    DECLARE
        res varchar := '';
        count_rows int := 0;
        is_schema_present_in_metadata boolean := FALSE;
    BEGIN        
        -- Construimos el nombre de la tabla y la vista
        let database_and_schema := concat(origin_database,'.',origin_schema, '.');
        let completed_table_name := concat(database_and_schema, table_name);
        let view_name := concat('VW_', table_name);
        let completed_view_name := concat(database_and_schema, view_name);
        let stream_table_name := concat('STM_', table_name);
        let completed_stream_table_name := concat(database_and_schema, stream_table_name);
        let stream_view_name := concat('STM_', view_name);
        let completed_stream_view_name := concat(database_and_schema, stream_view_name);
        let task_name := concat('TSK_', table_name, '_CONSOLIDATION');
        let completed_task_name := concat(database_and_schema, task_name);

        -- Chequeamos que la tabla no esté vacia. Si está vacía no hacemos nada
        SELECT count(*) INTO :count_rows FROM identifier(:completed_table_name);
        IF (count_rows > 0) THEN
            -- Creamos la vista aplanada de la tabla original
            IF (is_schema_infered) THEN
                is_schema_present_in_metadata := TRUE;
                IF (is_glue) THEN
                    res := concat('call DB_INGESTION_TOOLS_{{environment}}.STREAMING.SP_CREATE_GLUE_VIEW(\'',completed_table_name,'\',\'',completed_view_name,'\');');
                    execute immediate :res;
                ELSE
                    res := concat('call DB_INGESTION_TOOLS_{{environment}}.STREAMING.SP_CREATE_KAFKA_SCHEMA_VIEW(\'',completed_table_name,'\',\'',completed_view_name,'\');');
                    execute immediate :res;
                END IF;
            ELSE
                -- Chequeamos que la tabla Kafka venga con el esquema definido en los metadatos
                select iff(RECORD_METADATA:headers.ValueRecordSchema is null, FALSE, TRUE) into :is_schema_present_in_metadata from identifier(:completed_table_name) where RECORD_METADATA:headers.A_ENTTYP <> 'DL' limit 1;
                -- Si la tabla KAFKA viene sin esquema, no hacemos nada
                IF (is_schema_present_in_metadata) THEN
                    res := concat('call DB_INGESTION_TOOLS_{{environment}}.STREAMING.SP_CREATE_KAFKA_FLATTEN_VIEW(\'',completed_table_name,'\',\'',completed_view_name,'\');');
                    execute immediate :res;
                END IF;
            END IF;
            -- Si la tabla KAFKA viene sin esquema, no hacemos nada
            IF (is_schema_present_in_metadata) THEN
                -- Creamos el Stream sobre la vista recien creada
                let create_stream_view_sentence := concat('CREATE OR REPLACE STREAM ', completed_stream_view_name, ' ON VIEW ',completed_view_name,' APPEND_ONLY = TRUE SHOW_INITIAL_ROWS = TRUE;');
                execute immediate create_stream_view_sentence;

                -- Creamos el Stream sobre la tabla recien creada
                let create_stream_table_sentence := concat('CREATE OR REPLACE STREAM ', completed_stream_table_name, ' ON TABLE ',completed_table_name,' APPEND_ONLY = TRUE SHOW_INITIAL_ROWS = TRUE;');
                execute immediate create_stream_table_sentence;

                -- Creamos el task que ejecutará la consolidación.
                let create_task_sentence := concat('CREATE OR REPLACE TASK ', completed_task_name, ' WAREHOUSE = {{warehouse}} SCHEDULE = \'5 MINUTES\' AS ');
                create_task_sentence := concat(create_task_sentence,'call DB_INGESTION_TOOLS_{{environment}}.STREAMING.SP_CONSOLIDATE_TABLE_MERGE(');
                create_task_sentence := concat(create_task_sentence, '\'', origin_database,'\',');
                create_task_sentence := concat(create_task_sentence, '\'', origin_schema,'\',');
                create_task_sentence := concat(create_task_sentence, '\'', completed_view_name,'\',');
                create_task_sentence := concat(create_task_sentence, '\'', completed_stream_view_name,'\',');
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
                let resume_task_sentence := concat('ALTER TASK ', completed_task_name, ' RESUME;');
                execute immediate resume_task_sentence;
            END IF;
        END IF;
        RETURN 'SUCCESS';
    END;
$$
;

CREATE OR REPLACE PROCEDURE DB_INGESTION_TOOLS_{{environment}}.STREAMING.SP_UNLOAD_TABLE (
	id int,
	table_catalog VARCHAR,
	table_schema VARCHAR,
	table_name VARCHAR,
	data_lake_path VARCHAR,
	partition_field_expression VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
  DECLARE
    query varchar;
    table_name_join varchar := concat(:table_catalog,'.',:table_schema,'.STM_', :table_name);
  BEGIN
    query := concat('copy into @STG_UNLOAD/',data_lake_path,' from (select * from ', table_name_join,')',' partition by (to_char(date(',partition_field_expression,'),\'YYYY/MM/DD\')) file_format = (type = \'parquet\') header = true',';');
    execute immediate query;

    query := concat('UPDATE DB_INGESTION_TOOLS_{{environment}}.STREAMING.TB_UNLOAD_CONFIG SET LAST_LOAD = current_date(), LAST_STATUS = \'SUCCESS\', TS_SNAPSHOT = current_timestamp()  WHERE ID = ', id);
    execute immediate query;

    RETURN 'SUCCESS';
  EXCEPTION
      WHEN OTHER THEN
        query := concat('UPDATE DB_INGESTION_TOOLS_{{environment}}.STREAMING.TB_UNLOAD_CONFIG SET LAST_LOAD = current_date(), LAST_STATUS = \'ERROR\', TS_SNAPSHOT = current_timestamp() WHERE ID = ', id);
        execute immediate query;
        RETURN 'ERROR';
  END;
$$
;
