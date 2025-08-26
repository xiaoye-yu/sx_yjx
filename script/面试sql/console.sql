drop table if exists fact_log;

create table fact_log (
    user_id     int,
    device_id   varchar(255),
    login_date  date
);

insert into fact_log values
(1, 'd1', '2023-01-01'),
(2, 'd2', '2023-01-01'),
(1, 'd1', '2023-01-02'),
(2, 'd2', '2023-01-03'),
(3, 'd3', '2023-01-03'),
(1, 'd1', '2023-01-05'),
(4, 'd4', '2023-01-05'),
(2, 'd2', '2023-01-06'),
(5, 'd5', '2023-01-06');

with first_login as (
    select user_id, min(login_date) as first_login_date
    from fact_log
    group by user_id
)
select
    f.login_date,
    sum(case when f.login_date = fl.first_login_date then 1 else 0 end) as new_users,
    sum(case when f.login_date > fl.first_login_date then 1 else 0 end) as old_users
from fact_log f
join first_login fl
    on f.user_id = fl.user_id
group by f.login_date
order by f.login_date;








