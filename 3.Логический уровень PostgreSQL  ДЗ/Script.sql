
--1.создайте новую базу данных

CREATE DATABASE testdb
;

--2.создайте новую схему testnm
CREATE SCHEMA IF NOT EXISTS testnm
;

--3.создайте новую таблицу t1 с одной колонкой c1 типа integer
DROP TABLE IF EXISTS testnm.t1
;
CREATE TABLE IF NOT EXISTS testnm.t1(c1 integer)
;

--4.вставьте строку со значением c1=1
INSERT INTO testnm.t1 (
    c1
) VALUES(1)
;

--5.создайте новую роль readonly
CREATE ROLE readonly
;

--6.дайте новой роли право на подключение к базе данных testdb
GRANT CONNECT ON DATABASE testdb TO readonly
;

--7.дайте новой роли право на использование схемы testnm
GRANT USAGE ON SCHEMA testnm TO readonly
;

--8.дайте новой роли право на select для всех таблиц схемы testnm
GRANT SELECT ON ALL TABLES IN SCHEMA testnm TO readonly
;

--9.создайте пользователя testread с паролем test123
CREATE USER testread WITH PASSWORD 'test123'
;

SET SESSION ROLE testread
;
SELECT * FROM testnm.t1
;

--10.дайте роль readonly пользователю testread
SET SESSION ROLE postgres
;
GRANT readonly TO testread
;

--11.зайдите под пользователем testread в базу данных testdb, сделайте select * from t1;
SET SESSION ROLE testread
;
SELECT * FROM t1
;

SHOW search_path
;
SET search_path TO testnm, public
;
SELECT * FROM t1
;

--12. вернитесь в базу данных testdb под пользователем postgres и удалите таблицу t1
SET SESSION ROLE postgres
;
DROP TABLE IF EXISTS testnm.t1
;

--13. создайте ее заново но уже с явным указанием имени схемы testnm, вставьте строку со значением c1=1, зайдите под пользователем testread в базу данных testdb
--сделайте select * from testnm.t1;

CREATE TABLE IF NOT EXISTS testnm.t1(c1 integer)
;
INSERT INTO testnm.t1 (
    c1
) VALUES(1)
;

SET SESSION ROLE testread
;
SELECT * FROM testnm.t1
;


--Т.к. таблица пересоздалась, привелегии надо выдать заново либо выдать права для readonly 
--на все таблицы схемы testnm для операции SELECT. 
--В этом случае права на выборку будут распространятся и на вновь созданные объекты в том числе.
SET SESSION ROLE postgres
;
GRANT SELECT ON ALL TABLES IN SCHEMA testnm TO readonly
;
ALTER DEFAULT PRIVILEGES IN SCHEMA testnm GRANT SELECT ON TABLES TO readonly
; 
SET SESSION ROLE testread
;
SELECT * FROM testnm.t1
;

--14.теперь попробуйте выполнить команду create table t2(c1 integer); insert into t2 values (2);
SET SESSION ROLE testread
;
SET search_path TO public
;
CREATE TABLE t2(c1 integer)
; 
INSERT INTO t2 values (2)
;

--*Т.к. в 15 версии PostgreSQL нет прав на создание объектов в схеме public даже у роли public, 
--в которую добавляются все вновь созданные пользователя, то мы получаем ошибку из результат выше

SET SESSION ROLE postgres
;
GRANT CREATE ON SCHEMA public TO public

SET SESSION ROLE testread
;
SET search_path TO public
;
CREATE TABLE t2(c1 integer)
; 
INSERT INTO t2 values (2)
;


--15.Забираем все права у роли public в базе testdb и права на создание объектов в схеме public:
SET SESSION ROLE postgres
;
REVOKE CREATE ON SCHEMA public FROM public
; 
REVOKE ALL ON DATABASE testdb FROM public
; 

--Пробуем создать таблицу t3
SET SESSION ROLE testread
;
SET search_path TO public
;
CREATE TABLE t3(c1 integer)
; 
INSERT INTO t3 values (2)
;


