create table test1 (
	issue_key text,
	author_key text,
	status text,
	start_date timestamp,
	end_date timestamp
);

insert into test1 values ('A', 'Jason', 'In Progress', '2024-01-19 10:00:00', '2024-01-23 14:00:00');
insert into test1 values ('B', 'Jason', 'In Progress', '2024-01-22 10:00:00', '2024-01-23 20:00:00');

select *
from test1;

select *, culc_time(start_date,end_date,(1,5,10,20)) as culc, extract(epoch from (end_date - start_date)) as calendar_duration
from working_culc('test1',(1,5,10,20))


drop table test1