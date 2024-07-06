DO
LANGUAGE plpgsql
$$
DECLARE 
    _create_sql_view TEXT;
    _drop_sql_view TEXT;
    _sql_partition TEXT;
    _dt_min_part timestamp;
    _dt_max_part timestamp;
    _table_name_part TEXT = 'bookings.flights_part';
    _drop_old_table boolean = FALSE;
BEGIN
    --Сохраняем текст процедур, которые используют таблицу flights, для последующего пересоздания
    SELECT 
        string_agg('CREATE OR REPLACE VIEW bookings.' || v.viewname ||  chr(10) || 'AS'|| chr(10) || definition, repeat(chr(10),2)) AS create_sql_view
        ,string_agg('DROP VIEW IF EXISTS bookings.' || v.viewname || ' CASCADE;', chr(10)) AS drop_sql_view
    FROM pg_catalog.pg_views AS v
    WHERE 1=1
    AND schemaname = 'bookings'
    AND definition ILIKE '%flights%'
    INTO _create_sql_view, _drop_sql_view
    ;

    --Создаем партиционированную таблицу bookings.flights_part
    DROP TABLE IF EXISTS bookings.flights_part
    ;
    CREATE TABLE IF NOT EXISTS bookings.flights_part (
        LIKE bookings.flights INCLUDING COMMENTS INCLUDING COMPRESSION INCLUDING DEFAULTS
    ) PARTITION BY RANGE (scheduled_departure)
    ;

    CREATE INDEX flights_part_flight_no_scheduled_departure_key ON bookings.flights_part USING btree (flight_no, scheduled_departure)
    ;

    CREATE INDEX flights_part_pkey ON bookings.flights_part USING btree (flight_id)
    ;

    COMMIT
    ;

    --Определяем минимальную и максимальную дату диапазона секционирования
    SELECT
        date_trunc('MONTH', min(scheduled_departure))
        ,date_trunc('MONTH', max(scheduled_departure))
    FROM bookings.flights
    INTO _dt_min_part, _dt_max_part
    ;

    --Создаем скрипт по созданию таблиц секций затем его запускаем
    SELECT 
        string_agg(
            'CREATE TABLE IF NOT EXISTS ' || _table_name_part || '_' || to_char(EXTRACT(MONTH FROM dt), 'FM00') || chr(10) ||
            'PARTITION OF ' || _table_name_part || chr(10) ||         
            'FOR VALUES FROM (''' || dt::text || ''') TO (''' || (dt + '1 month'::INTERVAL)::TEXT || ''');'
            ,repeat(chr(10),2)
        ) AS sql_partition
    FROM pg_catalog.generate_series(_dt_min_part, _dt_max_part, '1 month'::INTERVAL) AS gn(dt)
    INTO _sql_partition
    ;

    EXECUTE _sql_partition
    ;
    
    --Создаем партицию по умолчанию для втавки данных вне диапазона секционирования
    CREATE TABLE IF NOT EXISTS bookings.flights_part_default
    PARTITION OF bookings.flights_part
    DEFAULT
    ;

    COMMIT
    ; 

    --Переливаем данные из старой таблицы в новую
    INSERT INTO bookings.flights_part (
        flight_id
        ,flight_no
        ,scheduled_departure
        ,scheduled_arrival
        ,departure_airport
        ,arrival_airport
        ,status
        ,aircraft_code
        ,actual_departure
        ,actual_arrival
    )
    SELECT 
        flight_id
        ,flight_no
        ,scheduled_departure
        ,scheduled_arrival
        ,departure_airport
        ,arrival_airport
        ,status
        ,aircraft_code
        ,actual_departure
        ,actual_arrival 
    FROM bookings.flights
    ;    
    
    --Переименовываем не партиционированную таблицу с префиксом old
    ALTER TABLE bookings.flights RENAME TO flights_old
    ;    
    COMMIT
    ;

    --Переименовываем партиционированную таблицу в bookings.flights
    ALTER TABLE bookings.flights_part RENAME TO flights
    ;    
    COMMIT
    ;

    --Пересоздаем представления, которые ссылались на непартиционированную таблицу
    EXECUTE _drop_sql_view || repeat(chr(10),2) ||  _create_sql_view
    ;
    
    --При необходимости после тестирования удаляем старую таблицу
    IF _drop_old_table THEN
        DROP TABLE IF EXISTS bookings.flights_old
        ;
    END IF
    ;
    
END;
$$
;