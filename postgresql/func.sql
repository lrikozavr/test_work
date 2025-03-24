/*
-- видозмінює значення початку/кінця роботи над задачею у відповідності до обмежень щодо
-- -	робочого часу з 10:00:00 до 20:00:00
-- -	робочих днів з ПН по ПТ (ПН = 1, ПТ = 5, НД = 7)
-- Тонкощі реалізації:
-- -		у випадку початку/закінчення роботи у вихідні, значення автоматично переводиться на початок робочого дня вже з *наступного* тижня
-- - 		у випадку початку/закінчення роботи за межами визначеного робочого часу, 
--				або зменшується кількість часу за рахунок обмеження значення до однієї з границь інтервалу (нижня межа)
--				або переводить значення на наступний робочий день (верхня межа)
*/

create type work_limit as (
	start_week integer, 	--:= 1;
	end_week integer, 		--:= 5;
	start_hour integer,		--:= 10;
	end_hour integer		--:= 20;
);

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

-- week diff
-- interval '2 days' * (extract(week from temp_task3.end) - extract(week from temp_task3.start))
-- year diff 
-- interval '2 days' * 52 * (extract(isoyear from temp_task3.end) - extract(isoyear from temp_task3.start))
-- day diff
-- interval '14 hours' * (extract(day from temp_task3.end) - extract(day from temp_task3.start))
-- time diff
-- extract(epoch from (temp_task3.end - temp_task3.start 
--						- interval '14 hours' * (extract(day from temp_task3.end) - extract(day from temp_task3.start) - 2*(extract(week from temp_task3.end) - extract(week from temp_task3.start)))
--						- interval '2 days' * (extract(week from temp_task3.end) - extract(week from temp_task3.start))
--						- interval '2 days' * 52 * (extract(isoyear from temp_task3.end) - extract(isoyear from temp_task3.start))
--						))
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

--------------------------

create or replace function working_culc(view_name text, f_p work_limit) returns table(issue_key text, author_key text, status text, start_date timestamp, end_date timestamp, work_duration numeric) as $$
declare
	i record;
	unique_author_key text;
	flag_null integer;
begin
	create temp table temp_working_table (
		issue_key text,
		author_key text,
		status text,
		start_date timestamp without time zone,
		end_date timestamp without time zone, 
		work_duration numeric
	);

	execute	'create view input_view as
		select * 
		from ' || quote_ident(view_name);

	for i in (select distinct input_view.author_key from input_view) loop
		unique_author_key := i.author_key;

		raise notice '%', unique_author_key;

		if(unique_author_key is null) then continue; end if;
		-- прибрати null
		execute	'create view temp_author as
		select * 
		from input_view
		where input_view.author_key = ''' || unique_author_key || ''' ;';
	
		execute 'select count(*) from ' || quote_ident('temp_author') into flag_null;
		if flag_null = 0 then drop view temp_author; continue;
		end if;

		insert into temp_working_table (issue_key, author_key, status, start_date, end_date, work_duration)
		select ori.issue_key, ori.author_key, ori.status, ori.start_date, ori.end_date, ov.work_duration
		from (select row_number() over () as id, *
				from overlap_interval('temp_author',f_p)) as ov inner join (select row_number() over () as id, * from temp_author) as ori ON ov.id = ori.id;
		
		drop view temp_author;
	end loop;
	
	return query (select * from temp_working_table);

	drop view input_view;
	drop table temp_working_table;
end;
$$ language plpgsql;