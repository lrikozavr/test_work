-- 
create view task12 as
select c_new.issue_key
	, c_new.author_key
	, c_t.from_status as status
	, c_new.created_at as start_date
	, c_t.created_at as end_date
	-- скориставшись нагодою, обчислюємо календарну різницю часу
	, EXTRACT(EPOCH from (c_t.created_at-c_new.created_at)) as calendar_duration
from   ( select *
		from (select 
				-- використовується min через агрегацію, але так як, всі значення по суті однакові (inner join), то на результат це не впливає
				min(c_.issue_key) as issue_key
				, min(c_.author_key) as author_key
				, min(c_.from_status) as from_status
				, min(c_.to_status) as to_status
				, min(c_.created_at) as created_at
				-- визначення найменшого значення різниці інтервалів
				, min(case
						-- у випадку, коли воно обирає значення з минулого, ми його викидуємо з обчислення min
						when EXTRACT(EPOCH from (c_t.created_at-c_.created_at)) <= 0 then null
						else EXTRACT(EPOCH from (c_t.created_at-c_.created_at))
					end
				) as min_time
				, c_.id
				from (	-- створюється колонка з індексами для, подальшої агрегації
						select *, row_number() over () as id from changelogs) as c_ 
						-- перетинається з собою, для виокремлення всіх записів зі спільною назвою та співпадінням статусів початку та завершення
						inner join changelogs as c_t ON c_.issue_key = c_t.issue_key and c_t.from_status = c_.to_status
				-- агрегація з ціллю обчислення найближкого з існуючих значень завершення до початку
				group by c_.id) as te
		-- видаляємо всі рядки для яких значення відсутні | також, можливо, коли об'єднується останній по часу запис з першим
		where te.min_time is not null
		) as c_new 
		-- об'єднуємо ще раз, щоб ототожнити з мінімальними значеннями
		inner join changelogs as c_t
	ON (c_new.issue_key = c_t.issue_key and c_t.from_status = c_new.to_status) and c_new.min_time = EXTRACT(EPOCH from (c_t.created_at-c_new.created_at));

-- за умовою ми обчислюємо всі рядки які мають status = 'In Progress'
create view task3 as
select *
from task12
where status = 'In Progress';

create view behackett as
select issue_key, author_key, fix_time(start_date,(1,5,10,20)) as start_date, fix_time(end_date,(1,5,10,20)) as end_date
from task3
where author_key = 'behackett'
order by start_date

------------------------------------------------------------------------------

▓▓▓▓▓░▓▓▓▓▓░░▓▓▓▓░▓▓▓▓▓░░▓▓▓▓▓░▓░░░▓░░░░░▓▓░▓░░░▓░▓▓▓░░▓░░░░▓▓▓▓▓
░░▓░░░▓░░░░░▓░░░░░░░▓░░░░▓░░░░░░▓░▓░░░░░▓░▓░▓▓░▓▓░▓░░▓░▓░░░░▓░░░░
░░▓░░░▓▓▓░░░░▓▓▓░░░░▓░░░░▓▓▓░░░░░▓░░░░░▓▓▓▓░▓░▓░▓░▓▓▓░░▓░░░░▓▓▓░░
░░▓░░░▓░░░░░░░░░▓░░░▓░░░░▓░░░░░░▓░▓░░░▓░░░▓░▓░░░▓░▓░░░░▓░░░░▓░░░░
░░▓░░░▓▓▓▓▓░▓▓▓▓░░░░▓░░░░▓▓▓▓▓░▓░░░▓░▓░░░░▓░▓░░░▓░▓░░░░▓▓▓▓░▓▓▓▓▓
	
------------------------------------------------------------------------------
	
with data as (
  select *
  from behackett
), working_days as (
  select 
    date + interval '10 hours' as start_date,
    date + interval '20 hours' as end_date  
	-- тут я вказав мінімальний і максимальний діапазони відповідно
  from generate_series('2011-02-13', '2021-12-17', interval '1 day') date
  where EXTRACT(DOW FROM date) BETWEEN 1 AND 5
), periods_by_author as(
  SELECT DISTINCT ON(author_key, timestamp) author_key, timestamp
  FROM data
  CROSS JOIN LATERAL unnest(ARRAY[start_date, end_date]) AS a(timestamp)
  ORDER BY timestamp
), in_progress_periods as (
  select
    author_key,
    timestamp as start_date,
    LEAD(timestamp) OVER (PARTITION BY author_key ORDER BY timestamp) as end_date
  from periods_by_author 
), working_periods as (
  select
    author_key,
    GREATEST(working_days.start_date, in_progress_periods.start_date) as start_date,
    LEAST(working_days.end_date, in_progress_periods.end_date) as end_date
  from in_progress_periods
  join working_days ON (working_days.start_date, working_days.end_date) OVERLAPS (in_progress_periods.start_date, in_progress_periods.end_date)
  where in_progress_periods.end_date is not null
), working_hours as (
  select 
    d.author_key,
    wp.start_date,
    wp.end_date,
    count(d.author_key)
  from data d
  join working_periods wp on (d.author_key = wp.author_key and (wp.start_date, wp.end_date) OVERLAPS (d.start_date, d.end_date))
  group by 1, 2, 3
  order by wp.start_date
)
select * from working_hours

------------------------------------------------------------------------

▓░░░▓░▓░░░▓░░░▓░░░▓░▓▓▓▓▓░▓▓▓░░░░▓▓▓▓░▓▓▓░░▓▓▓░░▓░░░▓
▓▓░▓▓░░▓░▓░░░░▓░░░▓░▓░░░░░▓░░▓░░▓░░░░░░▓░░▓░░░▓░▓▓░░▓
▓░▓░▓░░░▓░░░░░░▓░▓░░▓▓▓░░░▓▓▓░░░░▓▓▓░░░▓░░▓░░░▓░▓░▓░▓
▓░░░▓░░░▓░░░░░░▓░▓░░▓░░░░░▓░░▓░░░░░░▓░░▓░░▓░░░▓░▓░░▓▓
▓░░░▓░░░▓░░░░░░░▓░░░▓▓▓▓▓░▓░░░▓░▓▓▓▓░░▓▓▓░░▓▓▓░░▓░░░▓

------------------------------------------------------------------------

create type work_limit as (
	start_week integer, 	--:= 1;
	end_week integer, 		--:= 5;
	start_hour integer,		--:= 10;
	end_hour integer		--:= 20;
);

-- перераховує час
create or replace function fix_time(_date_ timestamp, f_p work_limit, out fix_date timestamp) as $$
declare
	start_time_interval interval := interval '1 hour' * f_p.start_hour;
begin
	fix_date :=	case
					when extract(isodow from _date_) not between f_p.start_week and f_p.end_week
					then date_trunc('day', _date_) + (7 - extract(isodow from _date_) + 1) * interval '1 day' + start_time_interval
					else case
							when extract(hour from _date_) between f_p.start_hour and f_p.end_hour-1 then _date_
							when extract(hour from _date_) < f_p.start_hour then date_trunc('day', _date_) + start_time_interval
							else 
								case 
									when extract(isodow from _date_) = f_p.end_week then date_trunc('day', _date_) + (7 - extract(isodow from _date_) + 1) * interval '1 day' + start_time_interval
									else date_trunc('day', _date_) + interval '1 day' + start_time_interval
								end
						end
				end;

end;
$$ language plpgsql;

-- визначає різницю тижнів, щоб врахувати вихідні дні
create or replace function culc_week_interval(time_start timestamp, time_end timestamp) returns integer as $$
declare
	count_of_week_in_year integer := 52;
begin
	return extract(week from time_end) - extract(week from time_start) + count_of_week_in_year * (extract(isoyear from time_end) - extract(isoyear from time_start));
end;
$$ language plpgsql;

-- рахує час
create or replace function culc_time(time_start timestamp, time_end timestamp, f_p work_limit) returns numeric as $$
declare
	not_working_time_interval interval := interval '24 hours' - (f_p.end_hour - f_p.start_hour) * interval '1 hour';
	count_of_free_days integer := 7 - (f_p.end_week - f_p.start_week + 1);
	free_day_interval interval := count_of_free_days * interval '1 day';
	week_differences integer := culc_week_interval(fix_time(time_start, f_p),fix_time(time_end, f_p));
begin
	time_start := fix_time(time_start, f_p);
	time_end := fix_time(time_end, f_p);

	--якщо різниці немає, одразу виводить нуль
	--(додано, через велику кількість нульових* задач)
	if time_start = time_end then return 0.0;
	end if;

	return extract(epoch from (time_end - time_start
						- not_working_time_interval * ( (case
														when extract(epoch from (time_end::time - time_start::time)) < 0 
															then extract(day from (time_end - time_start)) + 1
														else extract(day from (time_end - time_start))
													end)
													- count_of_free_days * week_differences)
						- free_day_interval * week_differences
						));
end;
$$ language plpgsql;

create or replace function overlap_interval(view_name text, f_p work_limit) returns table(work_duration numeric) as $$
declare
	temp_flag_index_weight integer := 0;
	first_record_flag integer := 0;
	temp_record record;
	record_id_array integer[];
	temp_record_id_array integer;
	i record;
begin

	raise notice 'overlap start at --- ', clock_timestamp();

	create temp table temp_index (
		id 					integer,
		date 				timestamp without time zone,
		flag_index 			integer
		--flag_index_weight 	integer
		);

--	cтворює колонку id, щоб після виконання основної програми, повернути список у початкове положення
	execute 'create view temp_input_view as select start_date, end_date, row_number() over () as id from ' || quote_ident(view_name);
-- створюємо таблицю і відмічаємо початок і кінець
	for i in (select * from temp_input_view) loop
		--insert into temp_index values (,'',,)
		insert into temp_index values (i.id,i.start_date,1);
		insert into temp_index values (i.id,i.end_date,-1);
--		raise notice 'start table 1 %', i.id;
	end loop;

	create temp table temp_index_weight (
		id 					integer,
		date 				timestamp without time zone,
		flag_index 			integer,
		flag_index_weight 	integer
		);

	-- рахуємо які саме інтервали нашаровуються
	for i in (select * from temp_index order by date asc, flag_index desc) loop
		--додай запобіжник, щоб не було значень -1
		--а він взагалі треба?
		insert into temp_index_weight values (i.id,i.date,i.flag_index,i.flag_index + temp_flag_index_weight);
		temp_flag_index_weight := i.flag_index + temp_flag_index_weight;
	end loop;

	--
	create temp table temp_result (
		id 			integer,
		date_start 	timestamp without time zone,
		date_end 	timestamp without time zone,
		deep 		integer
	);
	--

	for i in (select * from temp_index_weight) loop
		if first_record_flag = 0 then first_record_flag = 1; temp_record = i; continue;
		else 
			-- перевірка початку/кінця інтервалу
			case 
				when temp_record.flag_index = 1 then record_id_array := array_append(record_id_array,temp_record.id);
				when temp_record.flag_index = -1 then record_id_array := array_remove(record_id_array,temp_record.id);
				else raise notice 'flag_index column have upexpected value %', temp_record.id;
			end case;

			-- перевірка на інтервал з порожнім часом
			if temp_record.flag_index_weight = 0 then temp_record = i; continue; end if;

			--?
			foreach temp_record_id_array in array record_id_array loop
				--insert into temp_result values (temp_record_id_array,temp_record.date,i.date,abs(temp_record.flag_index_weight));
				insert into temp_result values (temp_record_id_array,temp_record.date,i.date,temp_record.flag_index_weight);
			end loop;
			
			temp_record = i;
		end if;

		--exit when i.flag_index = -1;

--		перевірка наявності значення в масиві
--		= any (record_id_array);
	end loop;
	
	
	-- вивід підрахунку часу для кожної задачі, за правилом, ділення на загальну кількість перетинання
	return query (	select sum(culc_time(date_start,date_end,f_p)/deep) as work_duration 
					from temp_result 
					group by id 
					order by id asc);
	
	raise notice 'overlap end at --- ', clock_timestamp();
	
	drop view temp_input_view;
	drop table temp_result;
	drop table temp_index;
	drop table temp_index_weight;
end;
$$ language plpgsql;


create view behackett_status as
select issue_key, author_key, status, fix_time(start_date,(1,5,10,20)) as start_date, fix_time(end_date,(1,5,10,20)) as end_date
from task3
where author_key = 'behackett'
order by start_date
--
select *
from overlap_interval('behackett_status',(1,5,10,20));

select *
from (select row_number() over () as id, *
				from overlap_interval('behackett_status',(1,5,10,20))) as ov inner join (select row_number() over () as id, * from behackett_status_without_fixtime) as ori ON ov.id = ori.id;
