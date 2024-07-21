DROP PROCEDURE IF EXISTS public.create_partition
;

CREATE PROCEDURE public.create_partition(
    
    _table_name TEXT DEFAULT NULL                       
    ,_schema_name TEXT DEFAULT NULL                     
    ,_is_create_index bool DEFAULT FALSE                
    ,_list_fields_key_index TEXT DEFAULT ''::text       
    ,_table_space_arhive TEXT DEFAULT 'pg_default'::text        
    ,_table_space_fact TEXT DEFAULT 'pg_default'::text      
    ,_is_create_arhive_partition bool DEFAULT FALSE     
    ,_size_arhive_partition text DEFAULT 'y'            
    ,_dt_start_arhive_partition timestamp DEFAULT NULL        
    ,_dt_end_arhive_partition timestamp DEFAULT NULL          
    ,_is_create_fact_partition bool DEFAULT FALSE       
    ,_size_fact_partition text DEFAULT 'm'              
    ,_dt_start_fact_partition timestamp DEFAULT NULL        
    ,_dt_end_fact_partition timestamp DEFAULT NULL          
    ,_is_relocate_data_to_arhive_partition boolean DEFAULT FALSE
        
)
LANGUAGE plpgsql
AS 
$$
DECLARE
    _rec record;
    _table_part_name TEXT = '';
    _table_full_name TEXT = '';
    _sql TEXT ='';
    _description_proc TEXT = '';
    _part_name_half_year_1 TEXT = '_1hy';
    _part_name_half_year_2 TEXT = '_2hy';
BEGIN
    
    IF _table_name IS NULL OR _schema_name IS NULL THEN
        _description_proc = 'Процедура для создания партиций.' || chr(10);
        _description_proc = _description_proc || 'Передаваемые параметры:' || chr(10);
        _description_proc = _description_proc || '_table_name - Сама секционированная таблица. На деле является виртуальной. Данные не содержит.'  || chr(10);
        _description_proc = _description_proc || '_schema_name - Схема, содержащая секционированную таблицу.'  || chr(10);
        _description_proc = _description_proc || '_is_create_index - Признак наличия индекса в партициях. Т.е. нужен ли индекс в создаваемых партициях.'  || chr(10);
        _description_proc = _description_proc || '_list_fields_key_index - Перечень полей, входящих в ключ индекса. Релевантен если признак наличия индекса в партициях проставлен в TRUE. Передается в виде строки, поля указываюися через Запятую. Пример: ''dt_collected, data_source_id, virtual_machine_id'''  || chr(10);
        _description_proc = _description_proc || '_table_space_arhive - Наименование табличного пространства, где будут хранится архивные партиции'  || chr(10);
        _description_proc = _description_proc || '_table_space_fact - Наименование табличного пространства, где будут хранится фактические партиции'  || chr(10);
        _description_proc = _description_proc || '_is_create_arhive_partition - Признак небходимости создания архивных партиций. Значение по умолчанию = FALSE'  || chr(10);
        _description_proc = _description_proc || '_size_arhive_partition - Размер архивной партиции. допустимые значения: d,m,q,hy,y. Значение по умолчанию = y'  || chr(10);       
        _description_proc = _description_proc || '_dt_start_arhive_partition - Начальная дата создания архивных партиций. Параметр имеет тип данных timestamp. Релевантен, если _is_create_arhive_partition = TRUE'  || chr(10);
        _description_proc = _description_proc || '_dt_end_arhive_partition - Конечная дата создания архивных партиций. Параметр имеет тип данных timestamp. Релевантен, если _is_create_arhive_partition = TRUE'  || chr(10);
        _description_proc = _description_proc || '_is_create_fact_partition - Признак небходимости создания фактических партиций. Значение по умолчанию = TRUE'  || chr(10);
        _description_proc = _description_proc || '_size_fact_partition - Размер фактической партиции. допустимые значения: d,m,q,hy,y. Значение по умолчанию = m'  || chr(10);
        _description_proc = _description_proc || '_dt_start_fact_partition - Дата начала создания фактических партиций. Параметр имеет тип данных timestamp. Релевантен, если _is_create_fact_partition = TRUE'  || chr(10);
        _description_proc = _description_proc || '_dt_end_fact_partition - Дата окончания создания фактических партиций. Параметр имеет тип данных timestamp. Релевантен, если _is_create_fact_partition = TRUE'  || chr(10);   
        _description_proc = _description_proc || '_is_relocate_data_to_arhive_partition - Признак переноса данных из фактической партиции в архивную. Значение по умолчанию = FALSE'  || chr(10);
        _description_proc = _description_proc || 'Пример выполнения процедуры:' || chr(10) || ' 
        CALL public.create_partition (
        _table_name => ''partition_table''
        ,_schema_name => ''public''
        ,_is_create_index => TRUE
        ,_list_fields_key_index => ''dt_collected, data_source_id, virtual_machine_id''
        ,_table_space_arhive => ''pg_default''
        ,_table_space_fact => ''pg_default''
        ,_is_create_arhive_partition => TRUE
        ,_size_arhive_partition => ''y''
        ,_dt_start_arhive_partition => ''2023-01-01''
        ,_dt_end_arhive_partition => ''2023-12-31''
        ,_is_create_fact_partition => TRUE 
        ,_size_fact_partition => ''m''
        ,_dt_start_fact_partition => ''2024-01-01''
        ,_dt_end_fact_partition => ''2024-12-01''
        ,_is_relocate_data_to_arhive_partition = TRUE
    )';
        RAISE NOTICE '%', _description_proc
        ;
        RETURN
        ;
    END IF
    ;   
    _table_full_name = _schema_name || '.' || _table_name;
     
--=====================================================Создание архивных партиций начало============================================================
    IF _is_create_arhive_partition THEN
        
--=====================================================Создание архивных партиций с диапазоном в год начало============================================================
        IF _size_arhive_partition = 'y' THEN
            --Проверяем значение параметров _dt_start_arhive_partition и _dt_end_arhive_partition.
            --Если они принимают значение NULL, то проверяем что бы текущая дата была равна последней дате года.
            --Делаем это для того, что бы процесс в NiFi, который будет запускаться каждый день, делал архивные партиции в случае если  
            -- _size_arhive_partition = 'y' один раз в год, _dt_end_arhive_partition при этом приравниваем к последней дате года,
            -- _dt_start_arhive_partition приравниваем к первой дате года
            IF _dt_start_arhive_partition IS NULL AND _dt_end_arhive_partition IS NULL THEN
            
                IF current_date::timestamp = date_trunc('year',current_date)::timestamp + '1 year'::INTERVAL - '1 day'::INTERVAL THEN 
                    --Определяем дату начала окончания архивных партиций
                    _dt_end_arhive_partition =  date_trunc('year',current_date)::timestamp + '1 year'::INTERVAL - '1 day'::INTERVAL
                    ;
                    _dt_start_arhive_partition = _dt_end_arhive_partition + '1 day'::INTERVAL - '1 year'::INTERVAL
                    ;
                    --При необходимости переноса данных из фактических партиций во вновь создаваемую архивную
                    --отсоединяем их от базовой таблицы, вставляем данные во вновь созданную архивную партицию, после переноса удаляем их.
                    --Иначе просто создаем архивную партицию
                    IF  _is_relocate_data_to_arhive_partition THEN 
                            
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS _table_part_name' || chr(10) || 
                        'PARTITION OF _table_full_name' || chr(10) || 
                        'FOR VALUES FROM (''_dt_start_partition '') TO (''_dt_end_partition'')' || chr(10) || 
                        'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS _ix_name  ON  _table_part_name  USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;

                        DROP TABLE IF EXISTS _temp_data_detach_partition
                        ;
                           
                        CREATE TEMP TABLE _temp_data_detach_partition
                        AS 
                        SELECT  
                            --pg_get_expr(pt.relpartbound, pt.oid, TRUE)
                            c.relname
                            --,pt.relname AS relname_partition
                            ,pn.nspname  
                            --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp AS dt_from_rng
                            --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp AS dt_to_rng
                            ,extract (year FROM gn)::text AS rng_part
                            ,gn::timestamp::text dt_start_arhive_partition
                            ,(gn::timestamp + '1 year'::INTERVAL)::text AS dt_end_arhive_partition
                            ,string_agg('DROP TABLE ' || pn.nspname || '.' || pt.relname ,';'||chr(10))  AS sql_script_drop_partition
                            ,string_agg('ALTER TABLE ' || pn.nspname || '.' || c.relname || ' DETACH PARTITION ' || pn.nspname  || '.' || pt.relname , ';'||chr(10)) AS sql_script_detach_partition
                            ,_sql AS sql_script_create_arhive_partition
                            ,string_agg('INSERT INTO ' || pn.nspname || '.' || c.relname || ' SELECT * FROM ' || pn.nspname || '.' || pt.relname , ';') AS sql_script_insert_partition
                        FROM pg_catalog.pg_class AS c
                                INNER JOIN pg_catalog.pg_namespace AS pn 
                                    ON 1=1
                                    AND c.relnamespace = pn."oid"
                                INNER JOIN pg_catalog.pg_inherits AS i 
                                    ON 1=1
                                    AND i.inhparent = c.oid         
                                INNER JOIN pg_catalog.pg_class AS pt
                                    ON 1=1
                                    AND pt.oid = i.inhrelid
                                INNER JOIN pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '1 year'::INTERVAL) AS gn
--                                  ON 1=1
--                                  AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
--                                  AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '1 year'::INTERVAL)
                                    ON (
                                        split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
                                        AND 
                                        split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '1 year'::INTERVAL)                            
                                    )
                                    OR 
                                    (
                                        gn >= split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp 
                                        AND 
                                        gn < split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp   
                                    )                                   
                        WHERE 1=1
                        AND c.relname = _table_name
                        AND pn.nspname = _schema_name
                        GROUP BY c.relname
                                 ,pn.nspname
                                 ,gn
                        ;
                        
                        FOR _rec IN 
                            SELECT 
                                nspname
                                ,relname
                                ,rng_part
                                ,nspname || '.' || relname || '_' || rng_part AS table_part_arhive_name
                                ,'ix_' || relname || '_' || rng_part AS index_name
                                ,dt_start_arhive_partition
                                ,dt_end_arhive_partition
                                ,sql_script_detach_partition
                                ,sql_script_create_arhive_partition
                                ,sql_script_insert_partition
                                ,sql_script_drop_partition
                                ,max(dt_start_arhive_partition) OVER (PARTITION BY sql_script_drop_partition) AS max_dt_drop
                                ,min (dt_start_arhive_partition) OVER (PARTITION BY sql_script_detach_partition ) AS min_dt_detach
                            FROM _temp_data_detach_partition
                            ORDER BY dt_start_arhive_partition
                        LOOP
                            _sql = REPLACE(_rec.sql_script_create_arhive_partition,'_table_part_name',_rec.table_part_arhive_name);
                            _sql = REPLACE(_sql, '_table_full_name', _table_full_name);
                            _sql = REPLACE(_sql, '_dt_start_partition', _rec.dt_start_arhive_partition);
                            _sql = REPLACE(_sql, '_dt_end_partition', _rec.dt_end_arhive_partition);
                            _sql = REPLACE(_sql, '_ix_name', _rec.index_name);
                            
                            IF _rec.min_dt_detach = _rec.dt_start_arhive_partition THEN 
                                EXECUTE _rec.sql_script_detach_partition
                                ;
                            END IF
                            ;
                            EXECUTE _sql
                            ;
                            IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN
                                EXECUTE _rec.sql_script_insert_partition
                                ;
                            END IF                      
                            ;
                            IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN 
                                EXECUTE _rec.sql_script_drop_partition
                                ;
                            END IF
                            ;
                            --RAISE NOTICE '%', _sql;
                        END LOOP
                        ;                       
                    ELSE
                        FOR _rec IN 
                            SELECT   
                                extract (year FROM gn)::text AS rng_part
                                ,gn::text dt_start_partition
                                ,(gn + '1 year'::INTERVAL)::text AS dt_end_partition
                            FROM pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '1 year'::INTERVAL) AS gn
                        LOOP
                            _table_part_name = _table_full_name || '_' ||   _rec.rng_part;
                        
                            _sql = 
                            'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                            'PARTITION OF ' || _table_full_name || chr(10) || 
                            'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                            'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                            CASE  
                                WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                            ELSE
                                ''
                            END 
                            ;
                
--                          RAISE NOTICE '%', _sql
--                          ;
                            EXECUTE _sql
                            ;             
                            _table_part_name = ''
                            ;
                        END LOOP
                        ;                                       
                    END IF
                    ;           
                END IF
                ;
            ELSE --В случае если _dt_start_arhive_partition и _dt_end_arhive_partition передаются как входящие параметры
                IF  _is_relocate_data_to_arhive_partition THEN 
                            
                    _sql = 
                    'CREATE TABLE IF NOT EXISTS _table_part_name' || chr(10) || 
                    'PARTITION OF _table_full_name' || chr(10) || 
                    'FOR VALUES FROM (''_dt_start_partition '') TO (''_dt_end_partition'')' || chr(10) || 
                    'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                    CASE  
                        WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS _ix_name  ON  _table_part_name  USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                    ELSE
                        ''
                    END 
                    ;

                    DROP TABLE IF EXISTS _temp_data_detach_partition
                    ;
                       
                    CREATE TEMP TABLE _temp_data_detach_partition
                    AS 
                    SELECT  
                        --pg_get_expr(pt.relpartbound, pt.oid, TRUE)
                        c.relname
                        --,pt.relname AS relname_partition
                        ,pn.nspname  
                        --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp AS dt_from_rng
                        --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp AS dt_to_rng
                        ,extract (year FROM gn)::text AS rng_part
                        ,gn::timestamp::text dt_start_arhive_partition
                        ,(gn::timestamp + '1 year'::INTERVAL)::text AS dt_end_arhive_partition
                        ,string_agg('DROP TABLE ' || pn.nspname || '.' || pt.relname ,';'||chr(10))  AS sql_script_drop_partition
                        ,string_agg('ALTER TABLE ' || pn.nspname || '.' || c.relname || ' DETACH PARTITION ' || pn.nspname  || '.' || pt.relname , ';'||chr(10)) AS sql_script_detach_partition
                        ,_sql AS sql_script_create_arhive_partition
                        ,string_agg('INSERT INTO ' || pn.nspname || '.' || c.relname || ' SELECT * FROM ' || pn.nspname || '.' || pt.relname , ';') AS sql_script_insert_partition
                    FROM pg_catalog.pg_class AS c
                            INNER JOIN pg_catalog.pg_namespace AS pn 
                                ON 1=1
                                AND c.relnamespace = pn."oid"
                            INNER JOIN pg_catalog.pg_inherits AS i 
                                ON 1=1
                                AND i.inhparent = c.oid         
                            INNER JOIN pg_catalog.pg_class AS pt
                                ON 1=1
                                AND pt.oid = i.inhrelid
                            INNER JOIN pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '1 year'::INTERVAL) AS gn
--                              ON 1=1
--                              AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
--                              AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '1 year'::INTERVAL)
                                ON (
                                    split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
                                    AND 
                                    split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '1 year'::INTERVAL)                            
                                )
                                OR 
                                (
                                    gn >= split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp 
                                    AND 
                                    gn < split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp   
                                )                               
                    WHERE 1=1
                    AND c.relname = _table_name
                    AND pn.nspname = _schema_name
                    GROUP BY c.relname
                             ,pn.nspname
                             ,gn
                    ;
                    
                    FOR _rec IN 
                        SELECT 
                            nspname
                            ,relname
                            ,rng_part
                            ,nspname || '.' || relname || '_' || rng_part AS table_part_arhive_name
                            ,'ix_' || relname || '_' || rng_part AS index_name
                            ,dt_start_arhive_partition
                            ,dt_end_arhive_partition
                            ,sql_script_detach_partition
                            ,sql_script_create_arhive_partition
                            ,sql_script_insert_partition
                            ,sql_script_drop_partition
                            ,max(dt_start_arhive_partition) OVER (PARTITION BY sql_script_drop_partition) AS max_dt_drop
                            ,min (dt_start_arhive_partition) OVER (PARTITION BY sql_script_detach_partition ) AS min_dt_detach
                        FROM _temp_data_detach_partition
                        ORDER BY dt_start_arhive_partition
                    LOOP
                        _sql = REPLACE(_rec.sql_script_create_arhive_partition,'_table_part_name',_rec.table_part_arhive_name);
                        _sql = REPLACE(_sql, '_table_full_name', _table_full_name);
                        _sql = REPLACE(_sql, '_dt_start_partition', _rec.dt_start_arhive_partition);
                        _sql = REPLACE(_sql, '_dt_end_partition', _rec.dt_end_arhive_partition);
                        _sql = REPLACE(_sql, '_ix_name', _rec.index_name);
                        
                        IF _rec.min_dt_detach = _rec.dt_start_arhive_partition THEN 
                            EXECUTE _rec.sql_script_detach_partition
                            ;
                        END IF
                        ;
                        EXECUTE _sql
                        ;
                        IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN
                            EXECUTE _rec.sql_script_insert_partition
                            ;
                        END IF                      
                        ;
                        IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN 
                            EXECUTE _rec.sql_script_drop_partition
                            ;
                        END IF
                        ;
                        --RAISE NOTICE '%', _sql;                       
                    END LOOP
                    ;
                ELSE
                    FOR _rec IN 
                        SELECT   
                            extract (year FROM gn)::text AS rng_part
                            ,gn::text dt_start_partition
                            ,(gn + '1 year'::INTERVAL)::text AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '1 year'::INTERVAL) AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' ||   _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                
--                      RAISE NOTICE '%', _sql
--                      ;
                        EXECUTE _sql
                        ;             
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;                                       
                END IF
                ;               
            END IF
            ;
        END IF
        ;
--=====================================================Создание архивных партиций с диапазоном в год конец============================================================  

--=====================================================Создание архивных партиций с диапазоном в полугодие начало============================================================
        IF _size_arhive_partition = 'hy' THEN
            --Проверяем значение параметров _dt_start_arhive_partition и _dt_end_arhive_partition.
            --Если они принимают значение NULL, то проверяем что бы текущая дата была равна либо последней дате года, либо 30.06.
            --Делаем это для того, что бы процесс в NiFi, который будет запускаться каждый день, делал архивные партиции в случае если  
            -- _size_arhive_partition = 'h' 2 раза в год, _dt_end_arhive_partition при этом приравниваем к последней дате года, либо 30.06.,
            -- _dt_start_arhive_partition приравниваем либо к первой дате года либо к 01.07
            IF _dt_start_arhive_partition IS NULL AND _dt_end_arhive_partition IS NULL THEN
            
                IF current_date::timestamp = (
                                                CASE
                                                    WHEN EXTRACT(month FROM current_date) BETWEEN 1 AND 6 THEN date_trunc('year', current_date) + '6 month'::INTERVAL - '1 day'::INTERVAL
                                                ELSE 
                                                    date_trunc('year', current_date) + '1 year'::INTERVAL - '1 day'::INTERVAL 
                                                END
                                             )   
                THEN 
                    --Определяем дату начала окончания архивных партиций
                    _dt_end_arhive_partition  = CASE 
                                                    WHEN EXTRACT(month FROM current_date) BETWEEN 1 AND 6 THEN date_trunc('year', current_date) + '6 month'::INTERVAL - '1 day'::INTERVAL
                                                ELSE 
                                                    date_trunc('year', current_date) + '1 year'::INTERVAL - '1 day'::INTERVAL 
                                                END  
                    ;
                    _dt_start_arhive_partition = _dt_end_arhive_partition + '1 day'::INTERVAL - '6 month'::INTERVAL
                    ;
                    --При необходимости переноса данных из фактических партиций во вновь создаваемую архивную
                    --отсоединяем их от базовой таблицы, вставляем данные во вновь созданную архивную партицию, после переноса удаляем их.
                    --Иначе просто создаем архивную партицию
                    IF  _is_relocate_data_to_arhive_partition THEN 
                            
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS _table_part_name' || chr(10) || 
                        'PARTITION OF _table_full_name' || chr(10) || 
                        'FOR VALUES FROM (''_dt_start_partition '') TO (''_dt_end_partition'')' || chr(10) || 
                        'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS _ix_name  ON  _table_part_name  USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;

                        DROP TABLE IF EXISTS _temp_data_detach_partition
                        ;
                           
                        CREATE TEMP TABLE _temp_data_detach_partition
                        AS 
                        SELECT  
                            --pg_get_expr(pt.relpartbound, pt.oid, TRUE)
                            c.relname
                            --,pt.relname AS relname_partition
                            ,pn.nspname  
                            --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp AS dt_from_rng
                            --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp AS dt_to_rng
                            ,CASE 
                                WHEN extract (month FROM gn) = 1 THEN extract (year FROM gn)::TEXT || _part_name_half_year_1
                                WHEN extract (month FROM gn) = 7 THEN extract (year FROM gn)::TEXT || _part_name_half_year_2
                            END  AS rng_part
                            ,gn::timestamp::text dt_start_arhive_partition
                            ,(gn::timestamp + '6 month'::INTERVAL)::text AS dt_end_arhive_partition
                            ,string_agg('DROP TABLE ' || pn.nspname || '.' || pt.relname ,';'||chr(10))  AS sql_script_drop_partition
                            ,string_agg('ALTER TABLE ' || pn.nspname || '.' || c.relname || ' DETACH PARTITION ' || pn.nspname  || '.' || pt.relname , ';'||chr(10)) AS sql_script_detach_partition
                            ,_sql AS sql_script_create_arhive_partition
                            ,string_agg('INSERT INTO ' || pn.nspname || '.' || c.relname || ' SELECT * FROM ' || pn.nspname || '.' || pt.relname , ';') AS sql_script_insert_partition
                        FROM pg_catalog.pg_class AS c
                                INNER JOIN pg_catalog.pg_namespace AS pn 
                                    ON 1=1
                                    AND c.relnamespace = pn."oid"
                                INNER JOIN pg_catalog.pg_inherits AS i 
                                    ON 1=1
                                    AND i.inhparent = c.oid         
                                INNER JOIN pg_catalog.pg_class AS pt
                                    ON 1=1
                                    AND pt.oid = i.inhrelid
                                INNER JOIN pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '6 month'::INTERVAL) AS gn
--                                  ON 1=1
--                                  AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
--                                  AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '6 month'::INTERVAL)
                                    ON (
                                        split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
                                        AND 
                                        split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '6 month'::INTERVAL)                           
                                    )
                                    OR 
                                    (
                                        gn >= split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp 
                                        AND 
                                        gn < split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp   
                                    )                               
                        WHERE 1=1
                        AND c.relname = _table_name
                        AND pn.nspname = _schema_name
                        GROUP BY c.relname
                                 ,pn.nspname
                                 ,gn
                        ;
                        
                        FOR _rec IN 
                            SELECT 
                                nspname
                                ,relname
                                ,rng_part
                                ,nspname || '.' || relname || '_' || rng_part AS table_part_arhive_name
                                ,'ix_' || relname || '_' || rng_part AS index_name
                                ,dt_start_arhive_partition
                                ,dt_end_arhive_partition
                                ,sql_script_detach_partition
                                ,sql_script_create_arhive_partition
                                ,sql_script_insert_partition
                                ,sql_script_drop_partition
                                ,max(dt_start_arhive_partition) OVER (PARTITION BY sql_script_drop_partition) AS max_dt_drop
                                ,min (dt_start_arhive_partition) OVER (PARTITION BY sql_script_detach_partition ) AS min_dt_detach
                            FROM _temp_data_detach_partition
                            ORDER BY dt_start_arhive_partition
                        LOOP
                            _sql = REPLACE(_rec.sql_script_create_arhive_partition,'_table_part_name',_rec.table_part_arhive_name);
                            _sql = REPLACE(_sql, '_table_full_name', _table_full_name);
                            _sql = REPLACE(_sql, '_dt_start_partition', _rec.dt_start_arhive_partition);
                            _sql = REPLACE(_sql, '_dt_end_partition', _rec.dt_end_arhive_partition);
                            _sql = REPLACE(_sql, '_ix_name', _rec.index_name);
                            
                            IF _rec.min_dt_detach = _rec.dt_start_arhive_partition THEN 
                                EXECUTE _rec.sql_script_detach_partition
                                ;
                            END IF
                            ;
                            EXECUTE _sql
                            ;
                            IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN
                                EXECUTE _rec.sql_script_insert_partition
                                ;
                            END IF                      
                            ;
                            IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN 
                                EXECUTE _rec.sql_script_drop_partition
                                ;
                            END IF
                            ;
                            --RAISE NOTICE '%', _sql;
                        END LOOP
                        ;
                    ELSE
                        FOR _rec IN 
                            SELECT   
                                CASE 
                                    WHEN extract (month FROM gn) = 1 THEN extract (year FROM gn)::TEXT || _part_name_half_year_1
                                    WHEN extract (month FROM gn) = 7 THEN extract (year FROM gn)::TEXT || _part_name_half_year_2
                                END  AS rng_part
                                ,gn::text dt_start_partition
                                ,(gn + '6 month'::INTERVAL)::text AS dt_end_partition
                            FROM pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '6 month'::INTERVAL) AS gn
                        LOOP
                            _table_part_name = _table_full_name || '_' ||   _rec.rng_part;
                        
                            _sql = 
                            'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                            'PARTITION OF ' || _table_full_name || chr(10) || 
                            'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                            'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                            CASE  
                                WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                            ELSE
                                ''
                            END 
                            ;
                
--                          RAISE NOTICE '%', _sql
--                          ;
                            EXECUTE _sql
                            ;             
                            _table_part_name = ''
                            ;
                        END LOOP
                        ;                                       
                    END IF
                    ;           
                END IF
                ;
            ELSE --В случае если _dt_start_arhive_partition и _dt_end_arhive_partition передаются как входящие параметры
                IF  _is_relocate_data_to_arhive_partition THEN 
                    
                    
                    _sql = 
                    'CREATE TABLE IF NOT EXISTS _table_part_name' || chr(10) || 
                    'PARTITION OF _table_full_name' || chr(10) || 
                    'FOR VALUES FROM (''_dt_start_partition '') TO (''_dt_end_partition'')' || chr(10) || 
                    'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                    CASE  
                        WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS _ix_name  ON  _table_part_name  USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                    ELSE
                        ''
                    END 
                    ;

                    DROP TABLE IF EXISTS _temp_data_detach_partition
                    ;
                       
                    CREATE TEMP TABLE _temp_data_detach_partition
                    AS 
                    SELECT  
                        --pg_get_expr(pt.relpartbound, pt.oid, TRUE)
                        c.relname
                        --,pt.relname AS relname_partition
                        ,pn.nspname  
                        --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp AS dt_from_rng
                        --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp AS dt_to_rng
                        ,CASE 
                            WHEN extract (month FROM gn) = 1 THEN extract (year FROM gn)::TEXT || _part_name_half_year_1
                            WHEN extract (month FROM gn) = 7 THEN extract (year FROM gn)::TEXT || _part_name_half_year_2
                        END  AS rng_part
                        ,gn::timestamp::text dt_start_arhive_partition
                        ,(gn::timestamp + '6 month'::INTERVAL)::text AS dt_end_arhive_partition
                        ,string_agg('DROP TABLE ' || pn.nspname || '.' || pt.relname ,';'||chr(10))  AS sql_script_drop_partition
                        ,string_agg('ALTER TABLE ' || pn.nspname || '.' || c.relname || ' DETACH PARTITION ' || pn.nspname  || '.' || pt.relname , ';'||chr(10)) AS sql_script_detach_partition
                        ,_sql AS sql_script_create_arhive_partition
                        ,string_agg('INSERT INTO ' || pn.nspname || '.' || c.relname || ' SELECT * FROM ' || pn.nspname || '.' || pt.relname , ';') AS sql_script_insert_partition
                    FROM pg_catalog.pg_class AS c
                            INNER JOIN pg_catalog.pg_namespace AS pn 
                                ON 1=1
                                AND c.relnamespace = pn."oid"
                            INNER JOIN pg_catalog.pg_inherits AS i 
                                ON 1=1
                                AND i.inhparent = c.oid         
                            INNER JOIN pg_catalog.pg_class AS pt
                                ON 1=1
                                AND pt.oid = i.inhrelid
                            INNER JOIN pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '6 month'::INTERVAL) AS gn
--                              ON 1=1
--                              AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
--                              AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '6 month'::INTERVAL)
                                ON (
                                    split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
                                    AND 
                                    split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '6 month'::INTERVAL)                           
                                )
                                OR 
                                (
                                    gn >= split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp 
                                    AND 
                                    gn < split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp   
                                )                               
                    WHERE 1=1
                    AND c.relname = _table_name
                    AND pn.nspname = _schema_name
                    GROUP BY c.relname
                             ,pn.nspname
                             ,gn
                    ;
                    
                    FOR _rec IN 
                        SELECT 
                            nspname
                            ,relname
                            ,rng_part
                            ,nspname || '.' || relname || '_' || rng_part AS table_part_arhive_name
                            ,'ix_' || relname || '_' || rng_part AS index_name
                            ,dt_start_arhive_partition
                            ,dt_end_arhive_partition
                            ,sql_script_detach_partition
                            ,sql_script_create_arhive_partition
                            ,sql_script_insert_partition
                            ,sql_script_drop_partition
                            ,max(dt_start_arhive_partition) OVER (PARTITION BY sql_script_drop_partition) AS max_dt_drop
                            ,min (dt_start_arhive_partition) OVER (PARTITION BY sql_script_detach_partition ) AS min_dt_detach
                        FROM _temp_data_detach_partition
                        ORDER BY dt_start_arhive_partition
                    LOOP
                        _sql = REPLACE(_rec.sql_script_create_arhive_partition,'_table_part_name',_rec.table_part_arhive_name);
                        _sql = REPLACE(_sql, '_table_full_name', _table_full_name);
                        _sql = REPLACE(_sql, '_dt_start_partition', _rec.dt_start_arhive_partition);
                        _sql = REPLACE(_sql, '_dt_end_partition', _rec.dt_end_arhive_partition);
                        _sql = REPLACE(_sql, '_ix_name', _rec.index_name);
                        
                        IF _rec.min_dt_detach = _rec.dt_start_arhive_partition THEN 
                            EXECUTE _rec.sql_script_detach_partition
                            ;
                        END IF
                        ;
                        EXECUTE _sql
                        ;
                        IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN
                            EXECUTE _rec.sql_script_insert_partition
                            ;
                        END IF                      
                        ;
                        IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN 
                            EXECUTE _rec.sql_script_drop_partition
                            ;
                        END IF
                        ;
                        --RAISE NOTICE '%', _sql;
                    END LOOP
                    ;                       
                ELSE
                    FOR _rec IN 
                        SELECT   
                            CASE 
                                WHEN extract (month FROM gn) = 1 THEN extract (year FROM gn)::TEXT || _part_name_half_year_1
                                WHEN extract (month FROM gn) = 7 THEN extract (year FROM gn)::TEXT || _part_name_half_year_2
                            END  AS rng_part
                            ,gn::text dt_start_partition
                            ,(gn + '6 month'::INTERVAL)::text AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '6 month'::INTERVAL) AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' ||   _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                
--                      RAISE NOTICE '%', _sql
--                      ;
                        EXECUTE _sql
                        ;             
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;                                       
                END IF
                ;               
            END IF
            ;
        END IF
        ;   
--=====================================================Создание архивных партиций с диапазоном в полугодие конец============================================================    
    
--=====================================================Создание архивных партиций с диапазоном в квартал начало============================================================

        IF _size_arhive_partition = 'q' THEN
            --Проверяем значение параметров _dt_start_arhive_partition и _dt_end_arhive_partition.
            --Если они принимают значение NULL, то проверяем что бы текущая дата была равна последней дате квартала.
            --Делаем это для того, что бы процесс в NiFi, который будет запускаться каждый день, делал архивные партиции в случае если  
            -- _size_arhive_partition = 'q' один раз в квартал, _dt_end_arhive_partition при этом приравниваем к последней дате квартала,
            -- _dt_start_arhive_partition приравниваем к первой дате квартала
            IF _dt_start_arhive_partition IS NULL AND _dt_end_arhive_partition IS NULL THEN
            
                IF current_date::timestamp = date_trunc('quarter',current_date)::timestamp + '3 month'::INTERVAL - '1 day'::INTERVAL THEN
                    --Определяем дату начала окончания архивных партиций
                    _dt_end_arhive_partition =  date_trunc('quarter',current_date)::timestamp + '3 month'::INTERVAL - '1 day'::INTERVAL
                    ;
                    _dt_start_arhive_partition = _dt_end_arhive_partition + '1 day'::INTERVAL - '3 month'::INTERVAL
                    ;
                    --При необходимости переноса данных из фактических партиций во вновь создаваемую архивную
                    --отсоединяем их от базовой таблицы, вставляем данные во вновь созданную архивную партицию, после переноса удаляем их.
                    --Иначе просто создаем архивную партицию
                    IF  _is_relocate_data_to_arhive_partition THEN 
                            
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS _table_part_name' || chr(10) || 
                        'PARTITION OF _table_full_name' || chr(10) || 
                        'FOR VALUES FROM (''_dt_start_partition '') TO (''_dt_end_partition'')' || chr(10) || 
                        'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS _ix_name  ON  _table_part_name  USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;

                        DROP TABLE IF EXISTS _temp_data_detach_partition
                        ;
                           
                        CREATE TEMP TABLE _temp_data_detach_partition
                        AS 
                        SELECT  
                            --pg_get_expr(pt.relpartbound, pt.oid, TRUE)
                            c.relname
                            --,pt.relname AS relname_partition
                            ,pn.nspname  
                            --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp AS dt_from_rng
                            --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp AS dt_to_rng
                            ,extract (year FROM gn)::TEXT || '_' || extract (quarter FROM gn)::text AS rng_part
                            ,gn::timestamp::text dt_start_arhive_partition
                            ,(gn::timestamp + '3 month'::INTERVAL)::text AS dt_end_arhive_partition
                            ,string_agg('DROP TABLE ' || pn.nspname || '.' || pt.relname ,';'||chr(10))  AS sql_script_drop_partition
                            ,string_agg('ALTER TABLE ' || pn.nspname || '.' || c.relname || ' DETACH PARTITION ' || pn.nspname  || '.' || pt.relname , ';'||chr(10)) AS sql_script_detach_partition
                            ,_sql AS sql_script_create_arhive_partition
                            ,string_agg('INSERT INTO ' || pn.nspname || '.' || c.relname || ' SELECT * FROM ' || pn.nspname || '.' || pt.relname , ';') AS sql_script_insert_partition
                        FROM pg_catalog.pg_class AS c
                                INNER JOIN pg_catalog.pg_namespace AS pn 
                                    ON 1=1
                                    AND c.relnamespace = pn."oid"
                                INNER JOIN pg_catalog.pg_inherits AS i 
                                    ON 1=1
                                    AND i.inhparent = c.oid         
                                INNER JOIN pg_catalog.pg_class AS pt
                                    ON 1=1
                                    AND pt.oid = i.inhrelid
                                INNER JOIN pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '3 month'::INTERVAL) AS gn
--                                  ON 1=1
--                                  AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
--                                  AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '3 month'::INTERVAL)
                                    ON (
                                        split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
                                        AND 
                                        split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '3 month'::INTERVAL)                           
                                    )
                                    OR 
                                    (
                                        gn >= split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp 
                                        AND 
                                        gn < split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp   
                                    )                               
                        WHERE 1=1
                        AND c.relname = _table_name
                        AND pn.nspname = _schema_name
                        GROUP BY c.relname
                                 ,pn.nspname
                                 ,gn
                        ;
                        
                        FOR _rec IN 
                            SELECT 
                                nspname
                                ,relname
                                ,rng_part
                                ,nspname || '.' || relname || '_' || rng_part AS table_part_arhive_name
                                ,'ix_' || relname || '_' || rng_part AS index_name
                                ,dt_start_arhive_partition
                                ,dt_end_arhive_partition
                                ,sql_script_detach_partition
                                ,sql_script_create_arhive_partition
                                ,sql_script_insert_partition
                                ,sql_script_drop_partition
                                ,max(dt_start_arhive_partition) OVER (PARTITION BY sql_script_drop_partition) AS max_dt_drop
                                ,min (dt_start_arhive_partition) OVER (PARTITION BY sql_script_detach_partition ) AS min_dt_detach
                            FROM _temp_data_detach_partition
                            ORDER BY dt_start_arhive_partition
                        LOOP
                            _sql = REPLACE(_rec.sql_script_create_arhive_partition,'_table_part_name',_rec.table_part_arhive_name);
                            _sql = REPLACE(_sql, '_table_full_name', _table_full_name);
                            _sql = REPLACE(_sql, '_dt_start_partition', _rec.dt_start_arhive_partition);
                            _sql = REPLACE(_sql, '_dt_end_partition', _rec.dt_end_arhive_partition);
                            _sql = REPLACE(_sql, '_ix_name', _rec.index_name);
                            
                            IF _rec.min_dt_detach = _rec.dt_start_arhive_partition THEN 
                                EXECUTE _rec.sql_script_detach_partition
                                ;
                            END IF
                            ;
                            EXECUTE _sql
                            ;
                            IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN
                                EXECUTE _rec.sql_script_insert_partition
                                ;
                            END IF                      
                            ;
                            IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN 
                                EXECUTE _rec.sql_script_drop_partition
                                ;
                            END IF
                            ;
                            --RAISE NOTICE '%', _sql;
                        END LOOP
                        ;
                    ELSE
                        FOR _rec IN 
                            SELECT   
                                extract (year FROM gn)::TEXT || '_' || extract (quarter FROM gn)::text AS rng_part
                                ,gn::text dt_start_partition
                                ,(gn + '3 month'::INTERVAL)::text AS dt_end_partition
                            FROM pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '3 month'::INTERVAL) AS gn
                        LOOP
                            _table_part_name = _table_full_name || '_' ||   _rec.rng_part;
                        
                            _sql = 
                            'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                            'PARTITION OF ' || _table_full_name || chr(10) || 
                            'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                            'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                            CASE  
                                WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                            ELSE
                                ''
                            END 
                            ;
                
--                          RAISE NOTICE '%', _sql
--                          ;
                            EXECUTE _sql
                            ;             
                            _table_part_name = ''
                            ;
                        END LOOP
                        ;                                       
                    END IF
                    ;           
                END IF
                ;
            ELSE --В случае если _dt_start_arhive_partition и _dt_end_arhive_partition передаются как входящие параметры
                IF  _is_relocate_data_to_arhive_partition THEN 
                            
                    _sql = 
                    'CREATE TABLE IF NOT EXISTS _table_part_name' || chr(10) || 
                    'PARTITION OF _table_full_name' || chr(10) || 
                    'FOR VALUES FROM (''_dt_start_partition '') TO (''_dt_end_partition'')' || chr(10) || 
                    'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                    CASE  
                        WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS _ix_name  ON  _table_part_name  USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                    ELSE
                        ''
                    END 
                    ;

                    DROP TABLE IF EXISTS _temp_data_detach_partition
                    ;
                       
                    CREATE TEMP TABLE _temp_data_detach_partition
                    AS 
                    SELECT  
                        --pg_get_expr(pt.relpartbound, pt.oid, TRUE)
                        c.relname
                        --,pt.relname AS relname_partition
                        ,pn.nspname  
                        --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp AS dt_from_rng
                        --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp AS dt_to_rng
                        ,extract (year FROM gn)::TEXT || '_' || extract (quarter FROM gn)::text AS rng_part
                        ,gn::timestamp::text dt_start_arhive_partition
                        ,(gn::timestamp + '3 month'::INTERVAL)::text AS dt_end_arhive_partition
                        ,string_agg('DROP TABLE ' || pn.nspname || '.' || pt.relname ,';'||chr(10))  AS sql_script_drop_partition
                        ,string_agg('ALTER TABLE ' || pn.nspname || '.' || c.relname || ' DETACH PARTITION ' || pn.nspname  || '.' || pt.relname , ';'||chr(10)) AS sql_script_detach_partition
                        ,_sql AS sql_script_create_arhive_partition
                        ,string_agg('INSERT INTO ' || pn.nspname || '.' || c.relname || ' SELECT * FROM ' || pn.nspname || '.' || pt.relname , ';') AS sql_script_insert_partition
                    FROM pg_catalog.pg_class AS c
                            INNER JOIN pg_catalog.pg_namespace AS pn 
                                ON 1=1
                                AND c.relnamespace = pn."oid"
                            INNER JOIN pg_catalog.pg_inherits AS i 
                                ON 1=1
                                AND i.inhparent = c.oid         
                            INNER JOIN pg_catalog.pg_class AS pt
                                ON 1=1
                                AND pt.oid = i.inhrelid
                            INNER JOIN pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '3 month'::INTERVAL) AS gn
--                              ON 1=1
--                              AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
--                              AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '3 month'::INTERVAL)
                                ON (
                                    split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
                                    AND 
                                    split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '3 month'::INTERVAL)                           
                                )
                                OR 
                                (
                                    gn >= split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp 
                                    AND 
                                    gn < split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp   
                                )           
                    WHERE 1=1
                    AND c.relname = _table_name
                    AND pn.nspname = _schema_name
                    GROUP BY c.relname
                             ,pn.nspname
                             ,gn
                    ;

                    FOR _rec IN 
                        SELECT 
                            nspname
                            ,relname
                            ,rng_part
                            ,nspname || '.' || relname || '_' || rng_part AS table_part_arhive_name
                            ,'ix_' || relname || '_' || rng_part AS index_name
                            ,dt_start_arhive_partition
                            ,dt_end_arhive_partition
                            ,sql_script_detach_partition
                            ,sql_script_create_arhive_partition
                            ,sql_script_insert_partition
                            ,sql_script_drop_partition
                            ,max(dt_start_arhive_partition) OVER (PARTITION BY sql_script_drop_partition) AS max_dt_drop
                            ,min (dt_start_arhive_partition) OVER (PARTITION BY sql_script_detach_partition ) AS min_dt_detach
                        FROM _temp_data_detach_partition
                        ORDER BY dt_start_arhive_partition
                    LOOP
                        _sql = REPLACE(_rec.sql_script_create_arhive_partition,'_table_part_name',_rec.table_part_arhive_name);
                        _sql = REPLACE(_sql, '_table_full_name', _table_full_name);
                        _sql = REPLACE(_sql, '_dt_start_partition', _rec.dt_start_arhive_partition);
                        _sql = REPLACE(_sql, '_dt_end_partition', _rec.dt_end_arhive_partition);
                        _sql = REPLACE(_sql, '_ix_name', _rec.index_name);
                        
                        IF _rec.min_dt_detach = _rec.dt_start_arhive_partition THEN 
                            EXECUTE _rec.sql_script_detach_partition
                            ;
                        END IF
                        ;
                        EXECUTE _sql
                        ;
                        IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN
                            EXECUTE _rec.sql_script_insert_partition
                            ;
                        END IF                      
                        ;
                        IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN 
                            EXECUTE _rec.sql_script_drop_partition
                            ;
                        END IF
                        ;
                        --RAISE NOTICE '%', _sql;
                        
                    END LOOP
                    ;
                ELSE
                    FOR _rec IN 
                        SELECT   
                            extract (year FROM gn)::TEXT || '_' || extract (quarter FROM gn)::text AS rng_part
                            ,gn::text dt_start_partition
                            ,(gn + '3 month'::INTERVAL)::text AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '3 month'::INTERVAL) AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' ||   _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                
--                      RAISE NOTICE '%', _sql
--                      ;
                        EXECUTE _sql
                        ;             
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;                                       
                END IF
                ;               
            END IF
            ;
        END IF
        ;   
--=====================================================Создание архивных партиций с диапазоном в квартал конец============================================================  
    
--=====================================================Создание архивных партиций с диапазоном в месяц начало============================================================   
    
        IF _size_arhive_partition = 'm' THEN
            --Проверяем значение параметров _dt_start_arhive_partition и _dt_end_arhive_partition.
            --Если они принимают значение NULL, то проверяем что бы текущая дата была равна последней дате месяца.
            --Делаем это для того, что бы процесс в NiFi, который будет запускаться каждый день, делал архивные партиции в случае если  
            -- _size_arhive_partition = 'm' один раз в месяц, _dt_end_arhive_partition при этом приравниваем к последней дате месяца,
            -- _dt_start_arhive_partition приравниваем к первой дате месяца
            IF _dt_start_arhive_partition IS NULL AND _dt_end_arhive_partition IS NULL THEN
            
                IF current_date::timestamp = date_trunc('month',current_date)::timestamp + '1 month'::INTERVAL - '1 day'::INTERVAL THEN
                    --Определяем дату начала окончания архивных партиций
                    _dt_end_arhive_partition =  date_trunc('month',current_date)::timestamp + '1 month'::INTERVAL - '1 day'::INTERVAL --current_date::timestamp 
                    ;
                    _dt_start_arhive_partition = _dt_end_arhive_partition + '1 day'::INTERVAL - '1 month'::INTERVAL
                    ;
                    --При необходимости переноса данных из фактических партиций во вновь создаваемую архивную
                    --отсоединяем их от базовой таблицы, вставляем данные во вновь созданную архивную партицию, после переноса удаляем их.
                    --Иначе просто создаем архивную партицию
                    IF  _is_relocate_data_to_arhive_partition THEN 
                            
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS _table_part_name' || chr(10) || 
                        'PARTITION OF _table_full_name' || chr(10) || 
                        'FOR VALUES FROM (''_dt_start_partition '') TO (''_dt_end_partition'')' || chr(10) || 
                        'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS _ix_name  ON  _table_part_name  USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;

                        DROP TABLE IF EXISTS _temp_data_detach_partition
                        ;
                           
                        CREATE TEMP TABLE _temp_data_detach_partition
                        AS 
                        SELECT  
                            --pg_get_expr(pt.relpartbound, pt.oid, TRUE)
                            c.relname
                            --,pt.relname AS relname_partition
                            ,pn.nspname  
                            --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp AS dt_from_rng
                            --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp AS dt_to_rng
                            ,to_char(gn, 'YYYYMM') AS rng_part
                            ,gn::timestamp::text dt_start_arhive_partition
                            ,(gn::timestamp + '1 month'::INTERVAL)::text AS dt_end_arhive_partition
                            ,string_agg('DROP TABLE ' || pn.nspname || '.' || pt.relname ,';'||chr(10))  AS sql_script_drop_partition
                            ,string_agg('ALTER TABLE ' || pn.nspname || '.' || c.relname || ' DETACH PARTITION ' || pn.nspname  || '.' || pt.relname , ';'||chr(10)) AS sql_script_detach_partition
                            ,_sql AS sql_script_create_arhive_partition
                            ,string_agg('INSERT INTO ' || pn.nspname || '.' || c.relname || ' SELECT * FROM ' || pn.nspname || '.' || pt.relname , ';') AS sql_script_insert_partition
                        FROM pg_catalog.pg_class AS c
                                INNER JOIN pg_catalog.pg_namespace AS pn 
                                    ON 1=1
                                    AND c.relnamespace = pn."oid"
                                INNER JOIN pg_catalog.pg_inherits AS i 
                                    ON 1=1
                                    AND i.inhparent = c.oid         
                                INNER JOIN pg_catalog.pg_class AS pt
                                    ON 1=1
                                    AND pt.oid = i.inhrelid
                                INNER JOIN pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '1 month'::INTERVAL) AS gn
--                                  ON 1=1
--                                  AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
--                                  AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '1 month'::INTERVAL)
                                    ON (
                                        split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
                                        AND 
                                        split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '1 month'::INTERVAL)                           
                                    )
                                    OR 
                                    (
                                        gn >= split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp 
                                        AND 
                                        gn < split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp   
                                    )                               
                        WHERE 1=1
                        AND c.relname = _table_name
                        AND pn.nspname = _schema_name
                        GROUP BY c.relname
                                 ,pn.nspname
                                 ,gn
                        ;
                        
                        FOR _rec IN 
                            SELECT 
                                nspname
                                ,relname
                                ,rng_part
                                ,nspname || '.' || relname || '_' || rng_part AS table_part_arhive_name
                                ,'ix_' || relname || '_' || rng_part AS index_name
                                ,dt_start_arhive_partition
                                ,dt_end_arhive_partition
                                ,sql_script_detach_partition
                                ,sql_script_create_arhive_partition
                                ,sql_script_insert_partition
                                ,sql_script_drop_partition
                                ,max(dt_start_arhive_partition) OVER (PARTITION BY sql_script_drop_partition) AS max_dt_drop
                                ,min (dt_start_arhive_partition) OVER (PARTITION BY sql_script_detach_partition ) AS min_dt_detach
                            FROM _temp_data_detach_partition
                            ORDER BY dt_start_arhive_partition
                        LOOP
                            _sql = REPLACE(_rec.sql_script_create_arhive_partition,'_table_part_name',_rec.table_part_arhive_name);
                            _sql = REPLACE(_sql, '_table_full_name', _table_full_name);
                            _sql = REPLACE(_sql, '_dt_start_partition', _rec.dt_start_arhive_partition);
                            _sql = REPLACE(_sql, '_dt_end_partition', _rec.dt_end_arhive_partition);
                            _sql = REPLACE(_sql, '_ix_name', _rec.index_name);
                            
                            IF _rec.min_dt_detach = _rec.dt_start_arhive_partition THEN 
                                EXECUTE _rec.sql_script_detach_partition
                                ;
                            END IF
                            ;
                            EXECUTE _sql
                            ;
                            IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN
                                EXECUTE _rec.sql_script_insert_partition
                                ;
                            END IF                      
                            ;
                            IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN 
                                EXECUTE _rec.sql_script_drop_partition
                                ;
                            END IF
                            ;
                            --RAISE NOTICE '%', _sql;
                        END LOOP
                        ;
                    ELSE
                        FOR _rec IN 
                            SELECT   
                                to_char(gn, 'YYYYMM') AS rng_part
                                ,gn::text dt_start_partition
                                ,(gn + '1 month'::INTERVAL)::text AS dt_end_partition
                            FROM pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '1 month'::INTERVAL) AS gn
                        LOOP
                            _table_part_name = _table_full_name || '_' ||   _rec.rng_part;
                        
                            _sql = 
                            'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                            'PARTITION OF ' || _table_full_name || chr(10) || 
                            'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                            'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                            CASE  
                                WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                            ELSE
                                ''
                            END 
                            ;
                
--                          RAISE NOTICE '%', _sql
--                          ;
                            EXECUTE _sql
                            ;             
                            _table_part_name = ''
                            ;
                        END LOOP
                        ;                                       
                    END IF
                    ;           
                END IF
                ;
            ELSE --В случае если _dt_start_arhive_partition и _dt_end_arhive_partition передаются как входящие параметры
                IF  _is_relocate_data_to_arhive_partition THEN 
                            
                    _sql = 
                    'CREATE TABLE IF NOT EXISTS _table_part_name' || chr(10) || 
                    'PARTITION OF _table_full_name' || chr(10) || 
                    'FOR VALUES FROM (''_dt_start_partition '') TO (''_dt_end_partition'')' || chr(10) || 
                    'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                    CASE  
                        WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS _ix_name  ON  _table_part_name  USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                    ELSE
                        ''
                    END 
                    ;
            
                    DROP TABLE IF EXISTS _temp_data_detach_partition
                    ;                
                    CREATE TEMP TABLE _temp_data_detach_partition
                    AS 
                    SELECT  
                        --pg_get_expr(pt.relpartbound, pt.oid, TRUE)
                        c.relname
                        --,pt.relname AS relname_partition
                        ,pn.nspname  
                        --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp AS dt_from_rng
                        --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp AS dt_to_rng
                        ,to_char(gn, 'YYYYMM') AS rng_part
                        ,gn::timestamp::text dt_start_arhive_partition
                        ,(gn::timestamp + '1 month'::INTERVAL)::text AS dt_end_arhive_partition                     
                        ,string_agg('DROP TABLE ' || pn.nspname || '.' || pt.relname ,';'||chr(10))  AS sql_script_drop_partition
                        ,string_agg('ALTER TABLE ' || pn.nspname || '.' || c.relname || ' DETACH PARTITION ' || pn.nspname  || '.' || pt.relname , ';'||chr(10)) AS sql_script_detach_partition
                        ,_sql AS sql_script_create_arhive_partition
                        ,string_agg('INSERT INTO ' || pn.nspname || '.' || c.relname || ' SELECT * FROM ' || pn.nspname || '.' || pt.relname , ';') AS sql_script_insert_partition
                    FROM pg_catalog.pg_class AS c
                            INNER JOIN pg_catalog.pg_namespace AS pn 
                                ON 1=1
                                AND c.relnamespace = pn."oid"
                            INNER JOIN pg_catalog.pg_inherits AS i 
                                ON 1=1
                                AND i.inhparent = c.oid         
                            INNER JOIN pg_catalog.pg_class AS pt
                                ON 1=1
                                AND pt.oid = i.inhrelid
                            INNER JOIN pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '1 month'::INTERVAL) AS gn
--                              ON 1=1
--                              AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
--                              AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '1 month'::INTERVAL)
                                ON (
                                    split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
                                    AND 
                                    split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '1 month'::INTERVAL)                           
                                )
                                OR 
                                (
                                    gn >= split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp 
                                    AND 
                                    gn < split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp   
                                )                           
                    WHERE 1=1
                    AND c.relname = _table_name
                    AND pn.nspname = _schema_name
                    GROUP BY c.relname
                             ,pn.nspname
                             ,gn
                    ;                   
                   
                    FOR _rec IN 
                        SELECT 
                            nspname
                            ,relname
                            ,rng_part
                            ,nspname || '.' || relname || '_' || rng_part AS table_part_arhive_name
                            ,'ix_' || relname || '_' || rng_part AS index_name
                            ,dt_start_arhive_partition
                            ,dt_end_arhive_partition
                            ,sql_script_detach_partition
                            ,sql_script_create_arhive_partition
                            ,sql_script_insert_partition
                            ,sql_script_drop_partition
                            ,max(dt_start_arhive_partition) OVER (PARTITION BY sql_script_drop_partition) AS max_dt_drop
                            ,min (dt_start_arhive_partition) OVER (PARTITION BY sql_script_detach_partition ) AS min_dt_detach
                        FROM _temp_data_detach_partition
                        ORDER BY dt_start_arhive_partition 
                    LOOP
                        _sql = REPLACE(_rec.sql_script_create_arhive_partition,'_table_part_name',_rec.table_part_arhive_name);
                        _sql = REPLACE(_sql, '_table_full_name', _table_full_name);
                        _sql = REPLACE(_sql, '_dt_start_partition', _rec.dt_start_arhive_partition);
                        _sql = REPLACE(_sql, '_dt_end_partition', _rec.dt_end_arhive_partition);
                        _sql = REPLACE(_sql, '_ix_name', _rec.index_name);
                        
                        IF _rec.min_dt_detach = _rec.dt_start_arhive_partition THEN 
                            EXECUTE _rec.sql_script_detach_partition
                            ;
                        END IF
                        ;
                        EXECUTE _sql
                        ;
                        IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN
                            EXECUTE _rec.sql_script_insert_partition
                            ;
                        END IF                      
                        ;
                        IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN 
                            EXECUTE _rec.sql_script_drop_partition
                            ;
                        END IF
                        ;
                        --RAISE NOTICE '%', _sql;
                        
                    END LOOP
                    ;
                ELSE
                    FOR _rec IN 
                        SELECT   
                            to_char(gn, 'YYYYMM') AS rng_part
                            ,gn::text dt_start_partition
                            ,(gn + '1 month'::INTERVAL)::text AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '1 month'::INTERVAL) AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' ||   _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                
--                      RAISE NOTICE '%', _sql
--                      ;
                        EXECUTE _sql
                        ;             
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;                                       
                END IF
                ;               
            END IF
            ;
        END IF
        ;   
    
--=====================================================Создание архивных партиций с диапазоном в месяц конец============================================================    
    
--=====================================================Создание архивных партиций с диапазоном в день начало============================================================    
    
        IF _size_arhive_partition = 'd' THEN
            --Проверять значение параметров _dt_start_arhive_partition и _dt_end_arhive_partition на NULL
            --нет смысла, т.к. это максимальная глубина создания партиций
                IF  _is_relocate_data_to_arhive_partition THEN 
                            
                    _sql = 
                    'CREATE TABLE IF NOT EXISTS _table_part_name' || chr(10) || 
                    'PARTITION OF _table_full_name' || chr(10) || 
                    'FOR VALUES FROM (''_dt_start_partition '') TO (''_dt_end_partition'')' || chr(10) || 
                    'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                    CASE  
                        WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS _ix_name  ON  _table_part_name  USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                    ELSE
                        ''
                    END 
                    ;
            
                    DROP TABLE IF EXISTS _temp_data_detach_partition
                    ;                
                    CREATE TEMP TABLE _temp_data_detach_partition
                    AS 
                    SELECT  
                        --pg_get_expr(pt.relpartbound, pt.oid, TRUE)
                        c.relname
                        --,pt.relname AS relname_partition
                        ,pn.nspname  
                        --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp AS dt_from_rng
                        --,split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp AS dt_to_rng
                        ,to_char(gn, 'YYYYMMDD') AS rng_part
                        ,gn::timestamp::text dt_start_arhive_partition
                        ,(gn::timestamp + '1 day'::INTERVAL)::text AS dt_end_arhive_partition                       
                        ,string_agg('DROP TABLE ' || pn.nspname || '.' || pt.relname ,';'||chr(10))  AS sql_script_drop_partition
                        ,string_agg('ALTER TABLE ' || pn.nspname || '.' || c.relname || ' DETACH PARTITION ' || pn.nspname  || '.' || pt.relname , ';'||chr(10)) AS sql_script_detach_partition
                        ,_sql AS sql_script_create_arhive_partition
                        ,string_agg('INSERT INTO ' || pn.nspname || '.' || c.relname || ' SELECT * FROM ' || pn.nspname || '.' || pt.relname , ';') AS sql_script_insert_partition
                    FROM pg_catalog.pg_class AS c
                            INNER JOIN pg_catalog.pg_namespace AS pn 
                                ON 1=1
                                AND c.relnamespace = pn."oid"
                            INNER JOIN pg_catalog.pg_inherits AS i 
                                ON 1=1
                                AND i.inhparent = c.oid         
                            INNER JOIN pg_catalog.pg_class AS pt
                                ON 1=1
                                AND pt.oid = i.inhrelid
                            INNER JOIN pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '1 day'::INTERVAL) AS gn
--                              ON 1=1
--                              AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
--                              AND split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '1 day'::INTERVAL)
                                ON (
                                    split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp >= gn
                                    AND 
                                    split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp < (gn + '1 day'::INTERVAL)                         
                                )
                                OR 
                                (
                                    gn >= split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',2)::timestamp 
                                    AND 
                                    gn < split_part(pg_get_expr(pt.relpartbound, pt.oid, TRUE),'''',4)::timestamp   
                                )                           
                    WHERE 1=1
                    AND c.relname = _table_name
                    AND pn.nspname = _schema_name
                    GROUP BY c.relname
                             ,pn.nspname
                             ,gn
                    ;                   

                    FOR _rec IN 
                        SELECT 
                            nspname
                            ,relname
                            ,rng_part
                            ,nspname || '.' || relname || '_' || rng_part AS table_part_arhive_name
                            ,'ix_' || relname || '_' || rng_part AS index_name
                            ,dt_start_arhive_partition
                            ,dt_end_arhive_partition
                            ,sql_script_detach_partition
                            ,sql_script_create_arhive_partition
                            ,sql_script_insert_partition
                            ,sql_script_drop_partition
                            ,max(dt_start_arhive_partition) OVER (PARTITION BY sql_script_drop_partition) AS max_dt_drop
                            ,min (dt_start_arhive_partition) OVER (PARTITION BY sql_script_detach_partition ) AS min_dt_detach
                        FROM _temp_data_detach_partition
                        ORDER BY dt_start_arhive_partition
                    LOOP
                        _sql = REPLACE(_rec.sql_script_create_arhive_partition,'_table_part_name',_rec.table_part_arhive_name);
                        _sql = REPLACE(_sql, '_table_full_name', _table_full_name);
                        _sql = REPLACE(_sql, '_dt_start_partition', _rec.dt_start_arhive_partition);
                        _sql = REPLACE(_sql, '_dt_end_partition', _rec.dt_end_arhive_partition);
                        _sql = REPLACE(_sql, '_ix_name', _rec.index_name);
                        
                        IF _rec.min_dt_detach = _rec.dt_start_arhive_partition THEN 
                            EXECUTE _rec.sql_script_detach_partition
                            ;
                        END IF
                        ;
                        EXECUTE _sql
                        ;
                        IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN
                            EXECUTE _rec.sql_script_insert_partition
                            ;
                        END IF                      
                        ;
                        IF _rec.max_dt_drop = _rec.dt_start_arhive_partition THEN 
                            EXECUTE _rec.sql_script_drop_partition
                            ;
                        END IF
                        ;
                        --RAISE NOTICE '%', _sql;
                        
                    END LOOP
                    ;
                ELSE
                    FOR _rec IN 
                        SELECT   
                            to_char(gn, 'YYYYMMDD') AS rng_part
                            ,gn::text dt_start_partition
                            ,(gn + '1 day'::INTERVAL)::text AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_arhive_partition, _dt_end_arhive_partition, '1 day'::INTERVAL) AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' ||   _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_arhive || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                
--                      RAISE NOTICE '%', _sql
--                      ;
                        EXECUTE _sql
                        ;             
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;                                       
                END IF
                ;               
        END IF
        ;   
    
--=====================================================Создание архивных партиций с диапазоном в день конец============================================================ 

    END IF 
    ;
--=====================================================Создание архивных партиций конец============================================================


--=====================================================Создание фактических партиций начало============================================================
    IF _is_create_fact_partition THEN
        
--=====================================================Создание фактических партиций с диапазоном равным дню начало============================================================
        IF _size_fact_partition = 'd' THEN
            --Проверяем значение параметров _dt_start_fact_partition и _dt_end_fact_partition.
            --Если они принимают значение NULL, то диапазон создания фактических партиций равен дипазону архиных партиций, т.е. если 
            --_size_arhive_partition равен 'm', то фактические партиции создаем на месяц вперед, если равен 'q', то создаем на квартал вперед и т.п.
            --Это релевантно для процесса в NiFi, который будет запускаться каждый день. Соответственно на основании  _size_arhive_partition
            --будет определяться _dt_start_fact_partition и _dt_end_fact_partition
            IF  _dt_start_fact_partition IS NULL AND _dt_end_fact_partition IS NULL THEN
            
                --смотрим что бы текущая дата равнялась последнему дню месяца в случае _size_arhive_partition = 'm'                                             
                IF _size_arhive_partition = 'd' THEN
                
                    _dt_start_fact_partition = current_date::timestamp + '1 day'::INTERVAL --следующий день
                    ;
                    _dt_end_fact_partition = _dt_start_fact_partition  --следующий день
                    ;
                    FOR _rec IN 
                        SELECT   
                            to_char(gn, 'YYYYMMDD') AS rng_part
                            ,gn::timestamp AS dt_start_partition
                            ,(gn + '1 day'::INTERVAL)::timestamp AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '1 day') AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' || _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                        
            --          RAISE NOTICE '%', _sql
            --          ;
                        EXECUTE _sql;
                      
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;
                END IF
                ;           
            
            
                --смотрим что бы текущая дата равнялась последнему дню месяца в случае _size_arhive_partition = 'm'                                             
                IF _size_arhive_partition = 'm' AND current_date = date_trunc('month',current_date)::timestamp + '1 month'::INTERVAL - '1 day'::INTERVAL  THEN
                
                    _dt_start_fact_partition = date_trunc('month',current_date)::timestamp + '1 month'::INTERVAL --первое число следующего месяца
                    ;
                    _dt_end_fact_partition = _dt_start_fact_partition + '1 month'::INTERVAL - '1 day'::INTERVAL --последнее число следующего месяца
                    ;
                    FOR _rec IN 
                        SELECT   
                            to_char(gn, 'YYYYMMDD') AS rng_part
                            ,gn::timestamp AS dt_start_partition
                            ,(gn + '1 day'::INTERVAL)::timestamp AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '1 day') AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' || _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                        
            --          RAISE NOTICE '%', _sql
            --          ;
                        EXECUTE _sql;
                      
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;
                END IF
                ;

                --смотрим что бы текущая дата равнялась последнему дню квартала в случае _size_arhive_partition = 'q'                                               
                IF _size_arhive_partition = 'q' AND current_date = date_trunc('quarter',current_date)::timestamp + '3 month'::INTERVAL - '1 day'::INTERVAL  THEN
                
                    _dt_start_fact_partition = date_trunc('quarter',current_date)::timestamp + '3 month'::INTERVAL --первое число следующего квартала
                    ;
                    _dt_end_fact_partition = _dt_start_fact_partition + '3 month'::INTERVAL - '1 day'::INTERVAL --последнее число следующего квартала
                    ;
                    FOR _rec IN 
                        SELECT   
                            to_char(gn, 'YYYYMMDD') AS rng_part
                            ,gn::timestamp AS dt_start_partition
                            ,(gn + '1 day'::INTERVAL)::timestamp AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '1 day') AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' || _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                        
            --          RAISE NOTICE '%', _sql
            --          ;
                        EXECUTE _sql;
                      
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;
                END IF
                ;
            
                --смотрим что бы текущая дата равнялась последнему дню полугодия в случае _size_arhive_partition = 'h'                                              
                IF _size_arhive_partition = 'h' AND current_date = (
                                                                        CASE
                                                                            WHEN EXTRACT(month FROM current_date) BETWEEN 1 AND 6 THEN date_trunc('year', current_date) + '6 month'::INTERVAL - '1 day'::INTERVAL
                                                                        ELSE 
                                                                            date_trunc('year', current_date) + '1 year'::INTERVAL - '1 day'::INTERVAL 
                                                                        END
                                                                    )    
                THEN
                
                    _dt_start_fact_partition = (CASE WHEN EXTRACT(month FROM current_date) BETWEEN 1 AND 6 THEN date_trunc('year', current_date) + '6 month'::INTERVAL ELSE date_trunc('year', current_date) + '1 year'::INTERVAL END)::timestamp --первое число следующего полугодия
                    ;
                    _dt_end_fact_partition = _dt_start_fact_partition + '6 month'::INTERVAL - '1 day'::interval  --последнее число следующего полугодия
                    ;
                    
                    FOR _rec IN 
                        SELECT   
                            to_char(gn, 'YYYYMMDD') AS rng_part
                            ,gn::timestamp AS dt_start_partition
                            ,(gn + '1 day'::INTERVAL)::timestamp AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '1 day') AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' || _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                        
            --          RAISE NOTICE '%', _sql
            --          ;
                        EXECUTE _sql;
                      
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;
                END IF
                ;

                --смотрим что бы текущая дата равнялась последнему дню года в случае _size_arhive_partition = 'y'                                               
                IF _size_arhive_partition = 'y' AND current_date = date_trunc('year',current_date)::timestamp + '1 year'::INTERVAL - '1 day'::INTERVAL  THEN
                
                    _dt_start_fact_partition = date_trunc('year',current_date)::timestamp + '1 year'::INTERVAL --первое число следующего года
                    ;
                    _dt_end_fact_partition = _dt_start_fact_partition + '1 year'::INTERVAL - '1 day'::INTERVAL --последнее число следующего года
                    ;
                    FOR _rec IN 
                        SELECT   
                            to_char(gn, 'YYYYMMDD') AS rng_part
                            ,gn::timestamp AS dt_start_partition
                            ,(gn + '1 day'::INTERVAL)::timestamp AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '1 day') AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' || _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                        
            --          RAISE NOTICE '%', _sql
            --          ;
                        EXECUTE _sql;
                      
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;               
                END IF
                ;   
            ELSE
                FOR _rec IN 
                    SELECT   
                        to_char(gn, 'YYYYMMDD') AS rng_part
                        ,gn::timestamp AS dt_start_partition
                        ,(gn + '1 day'::INTERVAL)::timestamp AS dt_end_partition
                    FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '1 day') AS gn
                LOOP
                    _table_part_name = _table_full_name || '_' || _rec.rng_part;
                
                    _sql = 
                    'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                    'PARTITION OF ' || _table_full_name || chr(10) || 
                    'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                    'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                    CASE  
                        WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                    ELSE
                        ''
                    END 
                    ;
                    
        --          RAISE NOTICE '%', _sql
        --          ;
                    EXECUTE _sql;
                  
                    _table_part_name = ''
                    ;
                END LOOP
                ;                               
            
            END IF
            ;
            
        END IF
        ;
    
--=====================================================Создание фактических партиций с диапазоном равным дню конец============================================================
    
    
--=====================================================Создание фактических партиций с диапазоном равным месяцу начало============================================================
        IF _size_fact_partition = 'm' THEN
            --Проверяем значение параметров _dt_start_fact_partition и _dt_end_fact_partition.
            --Если они принимают значение NULL, то диапазон создания фактических партиций равен дипазону архиных партиций, т.е. если 
            --_size_arhive_partition равен 'q', то фактические партиции создаем на квартал вперед  и т.п.
            --Это релевантно для процесса в NiFi, который будет запускаться каждый день. Соответственно на основании  _size_arhive_partition
            --будет определяться _dt_start_fact_partition и _dt_end_fact_partition
            IF  _dt_start_fact_partition IS NULL AND _dt_end_fact_partition IS NULL THEN
                --смотрим что бы текущая дата равнялась последнему дню месяца в случае _size_arhive_partition = 'm'                                             
                IF _size_arhive_partition = 'm' AND current_date = date_trunc('month',current_date)::timestamp + '1 month'::INTERVAL - '1 day'::INTERVAL  THEN
                
                    _dt_start_fact_partition = date_trunc('month',current_date)::timestamp + '1 month'::INTERVAL --первое число следующего месяца
                    ;
                    _dt_end_fact_partition = _dt_start_fact_partition + '1 month'::INTERVAL - '1 day'::INTERVAL --последнее число следующего месяца
                    ;
                    FOR _rec IN 
                        SELECT   
                            to_char(gn, 'YYYYMM') AS rng_part
                            ,gn::timestamp AS dt_start_partition
                            ,(gn + '1 month'::INTERVAL)::timestamp AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '1 month') AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' || _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                        
            --          RAISE NOTICE '%', _sql
            --          ;
                        EXECUTE _sql;
                      
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;
                END IF
                ;

                --смотрим что бы текущая дата равнялась последнему дню квартала в случае _size_arhive_partition = 'q'                                               
                IF _size_arhive_partition = 'q' AND current_date = date_trunc('quarter',current_date)::timestamp + '3 month'::INTERVAL - '1 day'::INTERVAL  THEN
                
                    _dt_start_fact_partition = date_trunc('quarter',current_date)::timestamp + '3 month'::INTERVAL --первое число следующего квартала
                    ;
                    _dt_end_fact_partition = _dt_start_fact_partition + '3 month'::INTERVAL - '1 day'::INTERVAL --последнее число следующего квартала
                    ;
                    FOR _rec IN 
                        SELECT   
                            to_char(gn, 'YYYYMM') AS rng_part
                            ,gn::timestamp AS dt_start_partition
                            ,(gn + '1 month'::INTERVAL)::timestamp AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '1 month') AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' || _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                        
            --          RAISE NOTICE '%', _sql
            --          ;
                        EXECUTE _sql;
                      
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;
                END IF
                ;
            
                --смотрим что бы текущая дата равнялась последнему дню полугодия в случае _size_arhive_partition = 'h'                                              
                IF _size_arhive_partition = 'h' AND current_date = (
                                                                        CASE
                                                                            WHEN EXTRACT(month FROM current_date) BETWEEN 1 AND 6 THEN date_trunc('year', current_date) + '6 month'::INTERVAL - '1 day'::INTERVAL
                                                                        ELSE 
                                                                            date_trunc('year', current_date) + '1 year'::INTERVAL - '1 day'::INTERVAL 
                                                                        END
                                                                    )    
                THEN
                
                    _dt_start_fact_partition = (CASE WHEN EXTRACT(month FROM current_date) BETWEEN 1 AND 6 THEN date_trunc('year', current_date) + '6 month'::INTERVAL ELSE date_trunc('year', current_date) + '1 year'::INTERVAL END)::timestamp --первое число следующего полугодия
                    ;
                    _dt_end_fact_partition = _dt_start_fact_partition + '6 month'::INTERVAL - '1 day'::interval  --последнее число следующего полугодия
                    ;
                    
                    FOR _rec IN 
                        SELECT   
                            to_char(gn, 'YYYYMM') AS rng_part
                            ,gn::timestamp AS dt_start_partition
                            ,(gn + '1 month'::INTERVAL)::timestamp AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '1 month') AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' || _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                        
            --          RAISE NOTICE '%', _sql
            --          ;
                        EXECUTE _sql;
                      
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;
                END IF
                ;

                --смотрим что бы текущая дата равнялась последнему дню года в случае _size_arhive_partition = 'y'                                               
                IF _size_arhive_partition = 'y' AND current_date = date_trunc('year',current_date)::timestamp + '1 year'::INTERVAL - '1 day'::INTERVAL  THEN
                
                    _dt_start_fact_partition = date_trunc('year',current_date)::timestamp + '1 year'::INTERVAL --первое число следующего года
                    ;
                    _dt_end_fact_partition = _dt_start_fact_partition + '1 year'::INTERVAL - '1 day'::INTERVAL --последнее число следующего года
                    ;
                    FOR _rec IN 
                        SELECT   
                            to_char(gn, 'YYYYMM') AS rng_part
                            ,gn::timestamp AS dt_start_partition
                            ,(gn + '1 month'::INTERVAL)::timestamp AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '1 month') AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' || _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                        
            --          RAISE NOTICE '%', _sql
            --          ;
                        EXECUTE _sql;
                      
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;               
                END IF
                ;   
            ELSE
                FOR _rec IN 
                    SELECT   
                        to_char(gn, 'YYYYMM') AS rng_part
                        ,gn::timestamp AS dt_start_partition
                        ,(gn + '1 month'::INTERVAL)::timestamp AS dt_end_partition
                    FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '1 month') AS gn
                LOOP
                    _table_part_name = _table_full_name || '_' || _rec.rng_part;
                
                    _sql = 
                    'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                    'PARTITION OF ' || _table_full_name || chr(10) || 
                    'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                    'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                    CASE  
                        WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                    ELSE
                        ''
                    END 
                    ;
                    
        --          RAISE NOTICE '%', _sql
        --          ;
                    EXECUTE _sql;
                  
                    _table_part_name = ''
                    ;
                END LOOP
                ;                               
            
            END IF
            ;
            
        END IF
        ;
--=====================================================Создание фактических партиций с диапазоном равным месяцу конец============================================================   
    
    
--=====================================================Создание фактических партиций с диапазоном равным кварталу начало============================================================
        IF _size_fact_partition = 'q' THEN
            --Проверяем значение параметров _dt_start_fact_partition и _dt_end_fact_partition.
            --Если они принимают значение NULL, то диапазон создания фактических партиций равен дипазону архиных партиций, т.е. если 
            --_size_arhive_partition равен 'q', то фактические партиции создаем на квартал вперед  и т.п.
            --Это релевантно для процесса в NiFi, который будет запускаться каждый день. Соответственно на основании  _size_arhive_partition
            --будет определяться _dt_start_fact_partition и _dt_end_fact_partition
            IF  _dt_start_fact_partition IS NULL AND _dt_end_fact_partition IS NULL THEN

                --смотрим что бы текущая дата равнялась последнему дню квартала в случае _size_arhive_partition = 'q'                                               
                IF _size_arhive_partition = 'q' AND current_date = date_trunc('quarter',current_date)::timestamp + '3 month'::INTERVAL - '1 day'::INTERVAL  THEN
                
                    _dt_start_fact_partition = date_trunc('quarter',current_date)::timestamp + '3 month'::INTERVAL --первое число следующего квартала
                    ;
                    _dt_end_fact_partition = _dt_start_fact_partition + '3 month'::INTERVAL - '1 day'::INTERVAL --последнее число следующего квартала
                    ;
                    FOR _rec IN 
                        SELECT   
                            extract (year FROM gn)::TEXT || '_' || extract (quarter FROM gn)::text AS rng_part
                            ,gn::timestamp AS dt_start_partition
                            ,(gn + '3 month'::INTERVAL)::timestamp AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '3 month') AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' || _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                        
            --          RAISE NOTICE '%', _sql
            --          ;
                        EXECUTE _sql;
                      
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;
                END IF
                ;
            
                --смотрим что бы текущая дата равнялась последнему дню полугодия в случае _size_arhive_partition = 'h'                                              
                IF _size_arhive_partition = 'h' AND current_date = (
                                                                        CASE
                                                                            WHEN EXTRACT(month FROM current_date) BETWEEN 1 AND 6 THEN date_trunc('year', current_date) + '6 month'::INTERVAL - '1 day'::INTERVAL
                                                                        ELSE 
                                                                            date_trunc('year', current_date) + '1 year'::INTERVAL - '1 day'::INTERVAL 
                                                                        END
                                                                    )    
                THEN
                
                    _dt_start_fact_partition = (CASE WHEN EXTRACT(month FROM current_date) BETWEEN 1 AND 6 THEN date_trunc('year', current_date) + '6 month'::INTERVAL ELSE date_trunc('year', current_date) + '1 year'::INTERVAL END)::timestamp --первое число следующего полугодия
                    ;
                    _dt_end_fact_partition = _dt_start_fact_partition + '6 month'::INTERVAL - '1 day'::interval  --последнее число следующего полугодия
                    ;
                    
                    FOR _rec IN 
                        SELECT   
                            extract (year FROM gn)::TEXT || '_' || extract (quarter FROM gn)::text AS rng_part
                            ,gn::timestamp AS dt_start_partition
                            ,(gn + '3 month'::INTERVAL)::timestamp AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '3 month') AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' || _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                        
            --          RAISE NOTICE '%', _sql
            --          ;
                        EXECUTE _sql;
                      
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;
                END IF
                ;

                --смотрим что бы текущая дата равнялась последнему дню года в случае _size_arhive_partition = 'y'                                               
                IF _size_arhive_partition = 'y' AND current_date = date_trunc('year',current_date)::timestamp + '1 year'::INTERVAL - '1 day'::INTERVAL  THEN
                
                    _dt_start_fact_partition = date_trunc('year',current_date)::timestamp + '1 year'::INTERVAL --первое число следующего года
                    ;
                    _dt_end_fact_partition = _dt_start_fact_partition + '1 year'::INTERVAL - '1 day'::INTERVAL --последнее число следующего года
                    ;
                    FOR _rec IN 
                        SELECT   
                            extract (year FROM gn)::TEXT || '_' || extract (quarter FROM gn)::text AS rng_part
                            ,gn::timestamp AS dt_start_partition
                            ,(gn + '3 month'::INTERVAL)::timestamp AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '3 month') AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' || _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                        
            --          RAISE NOTICE '%', _sql
            --          ;
                        EXECUTE _sql;
                      
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;               
                END IF
                ;   
            ELSE
                FOR _rec IN 
                    SELECT   
                        extract (year FROM gn)::TEXT || '_' || extract (quarter FROM gn)::text AS rng_part 
                        ,gn::timestamp AS dt_start_partition
                        ,(gn + '3 month'::INTERVAL)::timestamp AS dt_end_partition
                    FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '3 month') AS gn
                LOOP
                    _table_part_name = _table_full_name || '_' || _rec.rng_part;
                
                    _sql = 
                    'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                    'PARTITION OF ' || _table_full_name || chr(10) || 
                    'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                    'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                    CASE  
                        WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                    ELSE
                        ''
                    END 
                    ;
                    
        --          RAISE NOTICE '%', _sql
        --          ;
                    EXECUTE _sql;
                  
                    _table_part_name = ''
                    ;
                END LOOP
                ;                               
            
            END IF
            ;
            
        END IF
        ;
--=====================================================Создание фактических партиций с диапазоном равным кварталу конец============================================================     
    
--=====================================================Создание фактических партиций с диапазоном равным полугодию начало============================================================
        IF _size_fact_partition = 'hy' THEN
            --Проверяем значение параметров _dt_start_fact_partition и _dt_end_fact_partition.
            --Если они принимают значение NULL, то диапазон создания фактических партиций равен дипазону архиных партиций, т.е. если 
            --_size_arhive_partition равен 'q', то фактические партиции создаем на квартал вперед  и т.п.
            --Это релевантно для процесса в NiFi, который будет запускаться каждый день. Соответственно на основании  _size_arhive_partition
            --будет определяться _dt_start_fact_partition и _dt_end_fact_partition
            IF  _dt_start_fact_partition IS NULL AND _dt_end_fact_partition IS NULL THEN
            
                --смотрим что бы текущая дата равнялась последнему дню полугодия в случае _size_arhive_partition = 'h'                                              
                IF _size_arhive_partition = 'h' AND current_date = (
                                                                        CASE
                                                                            WHEN EXTRACT(month FROM current_date) BETWEEN 1 AND 6 THEN date_trunc('year', current_date) + '6 month'::INTERVAL - '1 day'::INTERVAL
                                                                        ELSE 
                                                                            date_trunc('year', current_date) + '1 year'::INTERVAL - '1 day'::INTERVAL 
                                                                        END
                                                                    )    
                THEN
                
                    _dt_start_fact_partition = (CASE WHEN EXTRACT(month FROM current_date) BETWEEN 1 AND 6 THEN date_trunc('year', current_date) + '6 month'::INTERVAL ELSE date_trunc('year', current_date) + '1 year'::INTERVAL END)::timestamp --первое число следующего полугодия
                    ;
                    _dt_end_fact_partition = _dt_start_fact_partition + '6 month'::INTERVAL - '1 day'::interval  --последнее число следующего полугодия
                    ;
                    
                    FOR _rec IN 
                        SELECT   
                            CASE 
                                WHEN extract (month FROM gn) = 1 THEN extract (year FROM gn)::TEXT || _part_name_half_year_1
                                WHEN extract (month FROM gn) = 7 THEN extract (year FROM gn)::TEXT || _part_name_half_year_2
                            END  AS rng_part
                            ,gn::timestamp AS dt_start_partition
                            ,(gn + '6 month'::INTERVAL)::timestamp AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '6 month') AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' || _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                        
            --          RAISE NOTICE '%', _sql
            --          ;
                        EXECUTE _sql;
                      
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;
                END IF
                ;

                --смотрим что бы текущая дата равнялась последнему дню года в случае _size_arhive_partition = 'y'                                               
                IF _size_arhive_partition = 'y' AND current_date = date_trunc('year',current_date)::timestamp + '1 year'::INTERVAL - '1 day'::INTERVAL  THEN
                
                    _dt_start_fact_partition = date_trunc('year',current_date)::timestamp + '1 year'::INTERVAL --первое число следующего года
                    ;
                    _dt_end_fact_partition = _dt_start_fact_partition + '1 year'::INTERVAL - '1 day'::INTERVAL --последнее число следующего года
                    ;
                    FOR _rec IN 
                        SELECT   
                            CASE 
                                WHEN extract (month FROM gn) = 1 THEN extract (year FROM gn)::TEXT || _part_name_half_year_1
                                WHEN extract (month FROM gn) = 7 THEN extract (year FROM gn)::TEXT || _part_name_half_year_2
                            END  AS rng_part
                            ,gn::timestamp AS dt_start_partition
                            ,(gn + '6 month'::INTERVAL)::timestamp AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '6 month') AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' || _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                        
            --          RAISE NOTICE '%', _sql
            --          ;
                        EXECUTE _sql;
                      
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;               
                END IF
                ;   
            ELSE
                FOR _rec IN 
                    SELECT   
                        CASE 
                            WHEN extract (month FROM gn) = 1 THEN extract (year FROM gn)::TEXT || _part_name_half_year_1
                            WHEN extract (month FROM gn) = 7 THEN extract (year FROM gn)::TEXT || _part_name_half_year_2
                        END  AS rng_part 
                        ,gn::timestamp AS dt_start_partition
                        ,(gn + '6 month'::INTERVAL)::timestamp AS dt_end_partition
                    FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '6 month') AS gn
                LOOP
                    _table_part_name = _table_full_name || '_' || _rec.rng_part;
                
                    _sql = 
                    'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                    'PARTITION OF ' || _table_full_name || chr(10) || 
                    'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                    'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                    CASE  
                        WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                    ELSE
                        ''
                    END 
                    ;
                    
        --          RAISE NOTICE '%', _sql
        --          ;
                    EXECUTE _sql;
                  
                    _table_part_name = ''
                    ;
                END LOOP
                ;                               
            
            END IF
            ;
            
        END IF
        ;
--=====================================================Создание фактических партиций с диапазоном равным полугодию конец============================================================        
    
--=====================================================Создание фактических партиций с диапазоном равным году начало============================================================
        IF _size_fact_partition = 'y' THEN
            --Проверяем значение параметров _dt_start_fact_partition и _dt_end_fact_partition.
            --Если они принимают значение NULL, то диапазон создания фактических партиций равен дипазону архиных партиций, т.е. если 
            --_size_arhive_partition равен 'q', то фактические партиции создаем на квартал вперед  и т.п.
            --Это релевантно для процесса в NiFi, который будет запускаться каждый день. Соответственно на основании  _size_arhive_partition
            --будет определяться _dt_start_fact_partition и _dt_end_fact_partition
            IF  _dt_start_fact_partition IS NULL AND _dt_end_fact_partition IS NULL THEN            

                --смотрим что бы текущая дата равнялась последнему дню года в случае _size_arhive_partition = 'y'                                               
                IF _size_arhive_partition = 'y' AND current_date = date_trunc('year',current_date)::timestamp + '1 year'::INTERVAL - '1 day'::INTERVAL  THEN
                
                    _dt_start_fact_partition = date_trunc('year',current_date)::timestamp + '1 year'::INTERVAL --первое число следующего года
                    ;
                    _dt_end_fact_partition = _dt_start_fact_partition + '1 year'::INTERVAL - '1 day'::INTERVAL --последнее число следующего года
                    ;
                    FOR _rec IN 
                        SELECT   
                            extract (year FROM gn)::text AS rng_part
                            ,gn::timestamp AS dt_start_partition
                            ,(gn + '1 year'::INTERVAL)::timestamp AS dt_end_partition
                        FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '1 year') AS gn
                    LOOP
                        _table_part_name = _table_full_name || '_' || _rec.rng_part;
                    
                        _sql = 
                        'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                        'PARTITION OF ' || _table_full_name || chr(10) || 
                        'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                        'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                        CASE  
                            WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                        ELSE
                            ''
                        END 
                        ;
                        
            --          RAISE NOTICE '%', _sql
            --          ;
                        EXECUTE _sql;
                      
                        _table_part_name = ''
                        ;
                    END LOOP
                    ;               
                END IF
                ;   
            ELSE
                FOR _rec IN 
                    SELECT   
                        extract (year FROM gn)::text AS rng_part 
                        ,gn::timestamp AS dt_start_partition
                        ,(gn + '1 year'::INTERVAL)::timestamp AS dt_end_partition
                    FROM pg_catalog.generate_series(_dt_start_fact_partition::timestamp, _dt_end_fact_partition::timestamp, '1 year') AS gn
                LOOP
                    _table_part_name = _table_full_name || '_' || _rec.rng_part;
                
                    _sql = 
                    'CREATE TABLE IF NOT EXISTS ' || _table_part_name || chr(10) || 
                    'PARTITION OF ' || _table_full_name || chr(10) || 
                    'FOR VALUES FROM (''' || _rec.dt_start_partition ||''') TO (''' || _rec.dt_end_partition || ''')' || chr(10) || 
                    'TABLESPACE ' || _table_space_fact || chr(10) || ';' || chr(10) || chr(10) ||
                    CASE  
                        WHEN _is_create_index = TRUE THEN 'CREATE INDEX IF NOT EXISTS ix_' || _table_name || '_' || _rec.rng_part || ' ON ' || _table_part_name || ' USING btree (' || _list_fields_key_index || ');' || chr(10) || chr(10)
                    ELSE
                        ''
                    END 
                    ;
                    
        --          RAISE NOTICE '%', _sql
        --          ;
                    EXECUTE _sql;
                  
                    _table_part_name = ''
                    ;
                END LOOP
                ;                               
            
            END IF
            ;
            
        END IF
        ;
--=====================================================Создание фактических партиций с диапазоном равным году конец============================================================ 
    

    END IF
    ;
--=====================================================Создание фактических партиций конец============================================================

END;
$$
;