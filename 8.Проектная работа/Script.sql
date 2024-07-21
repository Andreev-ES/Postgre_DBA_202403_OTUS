--Удаление партиционированной таблицы
DROP TABLE IF EXISTS  public.partition_table
;
--Создание партиционированной таблицы
CREATE TABLE IF NOT EXISTS public.partition_table(
    dt timestamp
    ,txt text
)
PARTITION BY RANGE (dt)
;

--Создание партиций
CALL public.create_partition (
    _table_name => 'partition_table'
    ,_schema_name => 'public'
    ,_is_create_index => TRUE
    ,_list_fields_key_index => 'dt'

    --=============default============================
    ,_is_create_default_partition => TRUE
    ,_table_space_default => 'pg_default'
    
    --=============arhive============================
    ,_is_create_arhive_partition => TRUE
    ,_table_space_arhive => 'pg_default'
    ,_size_arhive_partition => 'y'
    ,_dt_start_arhive_partition => '2023-01-01'
    ,_dt_end_arhive_partition => '2023-12-31'
    ,_is_relocate_data_to_arhive_partition => FALSE

    --=============fact============================
    ,_is_create_fact_partition => TRUE 
    ,_table_space_fact => 'pg_default'
    ,_size_fact_partition => 'm'
    ,_dt_start_fact_partition => '2024-01-01'
    ,_dt_end_fact_partition => '2024-06-30'
    
)
;

--Наполнение партиционированной таблицы
INSERT INTO public.partition_table(
   dt
   ,txt
)
SELECT
    dt
    ,md5(dt::text) AS txt
FROM pg_catalog.generate_series('2023-01-01', '2024-06-30', '1 day'::interval) gn(dt)
;

--Архивация/РазАрхивация данных
CALL public.create_partition (
    _table_name => 'partition_table'
    ,_schema_name => 'public'
    ,_is_create_index => TRUE
    ,_list_fields_key_index => 'dt'

    --=============arhive============================
    ,_is_create_arhive_partition => TRUE    
    ,_size_arhive_partition => 'm'
    ,_dt_start_arhive_partition => '2023-01-01'
    ,_dt_end_arhive_partition => '2023-12-31'
    ,_is_relocate_data_to_arhive_partition => TRUE
)
;