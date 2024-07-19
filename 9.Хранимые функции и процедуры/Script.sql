--Создание триггерной функции
CREATE OR REPLACE FUNCTION pract_functions.update_good_sum_mart()
RETURNS TRIGGER
LANGUAGE plpgsql
AS
$$
DECLARE
    _sql TEXT DEFAULT '';
    _column_list TEXT DEFAULT '';
    _table_name_log TEXT DEFAULT '';
    _schema_name_log TEXT DEFAULT '';
    _tg_op TEXT DEFAULT '';
BEGIN
    
    --Если произошла вставка строк, то:
    IF TG_OP = 'INSERT' THEN 
        --Создаем временную таблицу с новыми данными
        DROP TABLE IF EXISTS _temp_new_row
        ;
        CREATE TEMP TABLE _temp_new_row
        AS
        SELECT 
            g.good_name
            ,g.good_price * nt.sales_qty AS sum_sale
        FROM new_table AS nt
                INNER JOIN pract_functions.goods AS g
                    ON 1=1
                    AND nt.good_id = g.good_id
        ;
        
        --Вставляем новые данные в таблицу good_sum_mart
        INSERT INTO pract_functions.good_sum_mart(
           good_name
           ,sum_sale
        ) 
        SELECT 
           good_name
           ,sum_sale
        FROM _temp_new_row
        ;       
    END IF
    ;

    --Если произошло обновление строк, то:
    IF TG_OP = 'UPDATE' THEN 
        --Создаем временную таблицу c наименованием товаров по котрым произошло обновление
        DROP TABLE IF EXISTS _temp_new_row
        ;
        CREATE TEMP TABLE _temp_new_row
        AS
        SELECT 
            g.good_name            
        FROM new_table AS nt
                INNER JOIN pract_functions.goods AS g
                    ON 1=1
                    AND nt.good_id = g.good_id
        GROUP BY g.good_name  
        ; 
    
        --Расчитываем и кладем во временную таблицу  сумму продаж по обнолвенным товарам 
        DROP TABLE IF EXISTS _temp_sale
        ;
    
        CREATE TEMP TABLE _temp_sale 
        AS 
        SELECT 
            g.good_name 
            ,sum(s.sales_qty * g.good_price) AS sum_sale
        FROM pract_functions.sales AS s
                INNER JOIN pract_functions.goods AS g
                    ON 1=1
                    AND s.good_id = g.good_id 
                INNER JOIN _temp_new_row AS tnr
                    ON 1=1
                    AND tnr.good_name = g.good_name
        GROUP BY g.good_name
        ;
    
        --Расчитываем дэльту продаж
        INSERT INTO pract_functions.good_sum_mart (
           good_name
           ,sum_sale
        )
        SELECT 
            qgsm.good_name
            ,sum(qgsm.sum_sale) AS sum_sale
        FROM (
            SELECT
                s.good_name
                ,s.sum_sale
            FROM _temp_sale AS s
            UNION ALL 
            SELECT 
                gsm.good_name
                ,-1 * gsm.sum_sale AS sum_sale    
            FROM pract_functions.good_sum_mart AS gsm
            WHERE 1=1
            AND EXISTS (
                SELECT 1
                FROM _temp_new_row AS tnr
                WHERE 1=1
                AND tnr.good_name = gsm.good_name
                LIMIT 1
            )
        ) AS qgsm
        GROUP BY qgsm.good_name
        ;
    END IF 
    ;

    IF TG_OP = 'DELETE' THEN 
        --Создаем временную таблицу c наименованием товаров по котрым произошло обновление
        DROP TABLE IF EXISTS _temp_del_row
        ;
        CREATE TEMP TABLE _temp_del_row
        AS
        SELECT 
            g.good_name            
        FROM old_table AS nt
                INNER JOIN pract_functions.goods AS g
                    ON 1=1
                    AND nt.good_id = g.good_id
        GROUP BY g.good_name  
        ; 
    
        --Расчитываем и кладем во временную таблицу  сумму продаж по обнолвенным товарам 
        DROP TABLE IF EXISTS _temp_sale
        ;
    
        CREATE TEMP TABLE _temp_sale 
        AS 
        SELECT 
            g.good_name 
            ,sum(s.sales_qty * g.good_price) AS sum_sale
        FROM pract_functions.sales AS s
                INNER JOIN pract_functions.goods AS g
                    ON 1=1
                    AND s.good_id = g.good_id 
                INNER JOIN _temp_del_row AS tdr
                    ON 1=1
                    AND tdr.good_name = g.good_name
        GROUP BY g.good_name
        ;
    
        --Расчитываем дэльту продаж
        INSERT INTO pract_functions.good_sum_mart (
           good_name
           ,sum_sale
        )
        SELECT 
            qgsm.good_name
            ,sum(qgsm.sum_sale) AS sum_sale
        FROM (
            SELECT
                s.good_name
                ,s.sum_sale
            FROM _temp_sale AS s
            UNION ALL 
            SELECT 
                gsm.good_name
                ,-1 * gsm.sum_sale AS sum_sale    
            FROM pract_functions.good_sum_mart AS gsm
            WHERE 1=1
            AND EXISTS (
                SELECT 1
                FROM _temp_del_row AS tdr
                WHERE 1=1
                AND tdr.good_name = gsm.good_name
                LIMIT 1
            )
        ) AS qgsm
        GROUP BY qgsm.good_name
        ;
    END IF 
    ;

    RETURN NULL
    ;
END;
$$
;

--Создание триггеров
CREATE OR REPLACE TRIGGER tg_on_delete_sales
AFTER DELETE ON pract_functions.sales
REFERENCING 
OLD TABLE AS old_table
FOR EACH STATEMENT
EXECUTE FUNCTION pract_functions.update_good_sum_mart()
;

CREATE OR REPLACE TRIGGER tg_on_insert_sales
AFTER INSERT ON pract_functions.sales
REFERENCING 
NEW TABLE AS new_table
FOR EACH STATEMENT
EXECUTE FUNCTION pract_functions.update_good_sum_mart()
;

CREATE OR REPLACE TRIGGER tg_on_update_sales
AFTER UPDATE ON pract_functions.sales
REFERENCING 
OLD TABLE AS old_table
NEW TABLE AS new_table
FOR EACH STATEMENT
EXECUTE FUNCTION pract_functions.update_good_sum_mart()
;


--Создание предстваления для просмотра отчета
CREATE OR REPLACE VIEW pract_functions.v_good_sum_mart
AS 
SELECT 
    gsm.good_name
    ,sum(gsm.sum_sale) AS sum_sale
FROM pract_functions.good_sum_mart gsm
GROUP BY gsm.good_name
;