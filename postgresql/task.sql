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

--

create view task4 as
select *, (case when (author_key like 'JIRAUSER%') then 1 else 2 end) as price
-- функція яка підраховує кількість часу для кожного з працівників ()
-- у () зазначені параметри (початок тижня, кінець тижня, початок дня, кінець дня) *робочого
from working_culc('task3',(1,5,10,20));

-- враховуючи, що робочий час позначений 'In Progress', використовуємо час з task4 для підрахунку
create view task5 as
select s.issue_type
	, sum((w.work_duration/3600.0)*w.price) as total_cost
from issues as s 
	inner join (select * 
				from task4
	) as w ON s.issue_key = w.issue_key and s.assignee_key = w.author_key
group by s.issue_type
