--Создание таблицы справчников
DROP TABLE IF EXISTS public.dim_product
;
CREATE TABLE public.dim_product(
    product_id integer GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL CONSTRAINT pk_dim_product PRIMARY KEY
    ,product_name varchar(255)
    ,product_type varchar(255)
)
;

COMMENT ON TABLE public.dim_product IS 'Справочник продуктов';

COMMENT ON COLUMN public.dim_product.product_id IS 'Идентификатор продукта';
COMMENT ON COLUMN public.dim_product.product_name IS 'Наименование продукта';
COMMENT ON COLUMN public.dim_product.product_id IS 'Тип продукта продукта';

--Создание таблицы фактов
DROP TABLE IF EXISTS public.fact_product_sales
;
CREATE TABLE public.fact_product_sales(
    dt_sales timestamp
    ,product_id integer 
    ,price decimal(19,4)
    ,cnt integer
)
;

COMMENT ON TABLE public.fact_product_sales IS 'Таблица продаж';

COMMENT ON COLUMN public.fact_product_sales.dt_sales IS 'Дата продажи';
COMMENT ON COLUMN public.fact_product_sales.product_id IS 'Идентификатор продукта';
COMMENT ON COLUMN public.fact_product_sales.price IS 'Цена за еденицу продукта';
COMMENT ON COLUMN public.fact_product_sales.cnt IS 'Количество проданных продуктов';

CREATE INDEX IF NOT EXISTS ix_fact_product_sales_product_id ON public.fact_product_sales(product_id);

--Наполнений таблиц
INSERT INTO public.dim_product(
    product_name
    ,product_type
)
SELECT 
    'продукт_' || id::TEXT AS product_name
    ,CASE WHEN id%2 = 0 THEN 'тип_1' ELSE 'тип_2' END AS product_type    
FROM PG_CATALOG.GENERATE_SERIES(1, 15,1) AS gn(id)
;

DO
$$
DECLARE 
    _rec record;
BEGIN
    FOR _rec IN    
        SELECT 
            dt  
        FROM PG_CATALOG.GENERATE_SERIES('2023-01-01', '2023-06-30','1 day'::interval) AS gn(dt)  
    LOOP 
        INSERT INTO public.fact_product_sales(
            dt_sales
            ,product_id
            ,price
            ,cnt
        )
        SELECT 
            _rec.dt AS dt_sales
            ,id % 10 + 1 AS product_id
            ,(random()*10*id)::decimal(19,2) AS price
            ,id AS cnt
        FROM PG_CATALOG.GENERATE_SERIES(1, 50,1) AS gn(id)
        ;
    END LOOP
    ;    
END
$$
;

/*Реализовать прямое соединение двух или более таблиц*/

SELECT 
    dp.product_name 
   ,fps.dt_sales 
   ,fps.price 
   ,fps.cnt 
FROM public.dim_product AS dp
        INNER JOIN public.fact_product_sales AS fps
            ON 1=1
            AND dp.product_id = fps.product_id
;

/*Реализовать левостороннее (или правостороннее)
соединение двух или более таблиц*/

SELECT 
    dp.product_name 
   ,fps.dt_sales 
   ,fps.price 
   ,fps.cnt 
FROM public.dim_product AS dp
        LEFT JOIN public.fact_product_sales AS fps
            ON 1=1
            AND dp.product_id = fps.product_id
;

SELECT 
    dp.product_name 
   ,fps.dt_sales 
   ,fps.price 
   ,fps.cnt 
FROM public.dim_product AS dp
        RIGHT JOIN public.fact_product_sales AS fps
            ON 1=1
            AND dp.product_id = fps.product_id
;

/*Реализовать кросс соединение двух или более таблиц*/

SELECT
    id
    ,id_2
FROM pg_catalog.generate_series(1, 5, 1) gn(id)
        CROSS JOIN pg_catalog.generate_series(6, 10, 1) gn_2(id_2)
;

SELECT
    id
    ,id_2
FROM pg_catalog.generate_series(1, 5, 1) gn(id)
        INNER JOIN pg_catalog.generate_series(6, 10, 1) gn_2(id_2)
            ON 1=1
;

/*Реализовать полное соединение двух или более таблиц*/

SELECT 
    dp.product_name 
   ,fps.dt_sales 
   ,fps.price 
   ,fps.cnt 
FROM public.dim_product AS dp
        FULL JOIN public.fact_product_sales AS fps
            ON 1=1
            AND dp.product_id = fps.product_id
;

/*Реализовать запрос, в котором будут использованы разные типы соединений*/

SELECT 
    dp.product_name 
   ,fps.dt_sales 
   ,fps.price 
   ,fps.cnt 
FROM public.fact_product_sales AS fps
        LEFT JOIN public.dim_product AS dp
            ON 1=1
            AND dp.product_id = fps.product_id
        INNER  JOIN pg_catalog.generate_series('2023-01-01', '2023-01-31', '1 day'::INTERVAL) gn(dt)
            ON gn.dt = fps.dt_sales 
;
