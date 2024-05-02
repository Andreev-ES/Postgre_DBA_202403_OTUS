--�������� ���� ������
CREATE DATABASE postgre_dba_202403_otus
;

--������� ������� � ��������� ����� � ��������� ���������� ��� ���������������� ������ � ������� 1��� �����

DROP TABLE IF EXISTS test_vaccum
;

CREATE TABLE IF NOT EXISTS test_vaccum
AS 
SELECT 
    md5(id::text) AS txt
FROM PG_CATALOG.GENERATE_SERIES(1, 1000000, 1) AS gn(id)
;

--���������� ������ ����� � ��������
SELECT pg_size_pretty(pg_total_relation_size('test_vaccum'));

--5 ��� �������� ��� ������� � �������� � ������ ������� ����� ������

DO
$$
DECLARE 
    _max_i integer = 5;
BEGIN 
	FOR i IN 1.._max_i LOOP
    	
        UPDATE 	test_vaccum
        SET txt = txt || i::TEXT
        ;
    
        RAISE NOTICE '����� ���� ����� %', i
        ;
    
	END LOOP 
	;
	
END;
$$
;

--���������� ���������� ������� ������� � ������� � ����� ��������� ��� �������� ����������
ANALYZE test_vaccum
;

SELECT
    relname
    ,n_live_tup
    ,n_dead_tup
    ,trunc(100*n_dead_tup/(n_live_tup+1))::float AS "ratio%"
    ,last_autovacuum
FROM pg_stat_user_tables
WHERE 1=1
AND relname = 'test_vaccum'
;

--��������� ���������� �� test_vaccum �������
ALTER TABLE test_vaccum 
SET (autovacuum_enabled = ON)
;

--10 ��� �������� ��� ������� � �������� � ������ ������� ����� ������

DO
$$
DECLARE 
    _max_i integer = 10;
BEGIN 
    FOR i IN 1.._max_i LOOP
        
        UPDATE  test_vaccum
        SET txt = txt || i::TEXT
        ;
    
        RAISE NOTICE '����� ���� ����� %', i
        ;
    
    END LOOP 
    ;
    
END;
$$
;

--������ �������
SELECT pg_size_pretty(pg_total_relation_size('test_vaccum'));

--��� ������������:
ALTER TABLE test_vaccum 
SET (autovacuum_enabled = OFF)
;
