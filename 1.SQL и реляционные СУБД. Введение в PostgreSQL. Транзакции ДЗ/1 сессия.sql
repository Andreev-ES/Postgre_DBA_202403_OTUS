--1.Создание таблицы
drop table if exists public.persons
;
create table if not exists public.persons(
	id serial
	,first_name text
	,second_name text
)
;
--2.Наполнение новыми данными
insert into public.persons(
	first_name
	,second_name
) 
values ('ivan', 'ivanov')
	   ,('petr', 'petrov')
;

--3.Просмотр уровня изоляции
show transaction isolation level
;
--Результат - read committed

--4.Открываем новую транзакцию с дефолтным уровнем изоляции
begin;

--5.В первой сессии добавляем новую запись
insert into public.persons(
	first_name
	,second_name
) 
values('sergey', 'sergeev')
;

--7. Фиксируем транзакцию в первой сессии
commit;

--================================repeatable===================================================

--10. Новый уровень изоляции  в обоих сессиях repeatable read
set transaction isolation level repeatable read;
begin;

--11. В первой сессии добавление новой записи 
insert into public.persons(
	first_name
	,second_name
) 
values('sveta', 'svetova')
;

--13. Фиксируем транзакцию в первой сессии
commit;