--Создание и наполнение таблицы:
DROP TABLE IF EXISTS public.test_index
;

CREATE TABLE public.test_index(
    id int
    ,id_txt TEXT
    ,bool_field bool
)
;

INSERT INTO public.test_index
SELECT 
    id 
    ,MD5(id::text) AS id_txt
    ,random() < 0.01 AS bool_field
FROM PG_CATALOG.GENERATE_SERIES(1, 500000,1) AS gn(id)
;

--создание индекса на поле id
CREATE INDEX IF NOT EXISTS ix_test_index_id ON public.test_index USING btree(id)
;

--Просмотр плана запроса с использованием индекса btree
EXPLAIN ANALYZE 
SELECT *
FROM public.test_index AS ti
WHERE 1=1
AND ti.id = 152
;

/*
Index Scan using ix_test_index_id on test_index ti  (cost=0.42..8.44 rows=1 width=38) (actual time=0.028..0.029 rows=1 loops=1)
  Index Cond: (id = 152)
Planning Time: 0.265 ms
Execution Time: 0.038 ms
*/
--Создание индекса для полнотекстового поиска

--Создаем таблицу
DROP TABLE IF EXISTS public.test_full_txt_index
;

CREATE TABLE public.test_full_txt_index(
    id SERIAL PRIMARY KEY
    ,config regconfig
    ,body text
)
;

--Наполняем таблицу
INSERT INTO public.test_full_txt_index(
    config
    ,body
)
VALUES ('english', 'Notice that the 2-argument version of to_tsvector is used. Only text search functions that specify a configuration name can be used in expression indexes (Section 11.7). This is because the index contents must be unaffected by default_text_search_config. If they were affected, the index contents might be inconsistent because different entries could contain tsvectors that were created with different text search configurations, and there would be no way to guess which was which. It would be impossible to dump and restore such an index correctly.
Because the two-argument version of to_tsvector was used in the index above, only a query reference that uses the 2-argument version of to_tsvector with the same configuration name will use that index. That is, WHERE to_tsvector(''english'', body) @@ ''a & b'' can use the index, but WHERE to_tsvector(body) @@ ''a & b'' cannot. This ensures that an index will be used only with the same configuration used to create the index entries.
It is possible to set up more complex expression indexes wherein the configuration name is specified by another column, e.g.
')
        ,('russian', 'Документы также можно хранить в обычных текстовых файлах в файловой системе. В этом случае база данных может быть просто 
хранилищем полнотекстового индекса и исполнителем запросов, а найденные документы будут загружаться из файловой 
системы по некоторым уникальным идентификаторам. Однако для загрузки внешних файлов требуются права суперпользователя или 
поддержка специальных функций, так что это обычно менее удобно, чем хранить все данные внутри БД. Кроме того, когда всё хранится в базе данных, это 
упрощает доступ к метаданным документов при индексации и выводе результатов
')
;

--Создаем индекс
CREATE INDEX IF NOT EXISTS ix_test_full_txt_index_body ON public.test_full_txt_index USING GIN (to_tsvector(config, body));

--Отключение последовательное сканирование для демонстрации использования индекса
SET enable_seqscan = OFF
;

EXPLAIN ANALYZE 
SELECT
    *
FROM public.test_full_txt_index
WHERE 1=1
AND to_tsvector(config, body) @@ to_tsquery('(to_tsvector) | (хранилищ | полно | файлов)')
;
--Вкл последовательного сканирования
SET enable_seqscan = ON
;


--Индекс на часть таблицы или индекс на поле с функцией

CREATE INDEX IF NOT EXISTS ix_test_index_bool_field ON public.test_index (bool_field) WHERE bool_field
;

--Индекс используется
EXPLAIN
SELECT *
FROM public.test_index
WHERE 1=1
AND bool_field
;
--Индекс не используется
EXPLAIN
SELECT *
FROM public.test_index
WHERE 1=1
AND NOT bool_field
;

-- Создать индекс на несколько полей
CREATE INDEX IF NOT EXISTS ix_test_index_id_id_txt ON public.test_index (id,id_txt)
;
EXPLAIN ANALYZE 
SELECT 
    ID
    ,ID_TXT 
FROM public.test_index
WHERE 1=1
AND id = 152
AND id_txt = 'qweqwe'

