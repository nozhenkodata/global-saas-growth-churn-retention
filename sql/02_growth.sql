/*
-------------------------------------------------------------------------------------------------
Рост числа новых пользователей и подписок
-------------------------------------------------------------------------------------------------
*/

-- Число НОВЫХ ПОЛЬЗОВАТЕЛЕЙ в месяц, общее число пользователей и процентное изменение числа пользователей в месяц (MoM growth)

with first_subscription as (
    select
        account_id,
        min(start_date::date) as first_start_date
    from ravenstack_subscriptions
    group by account_id
),
new_customers as (
	select
		date_trunc('month', first_start_date)::date as month,
    	count(account_id) as new_customers
	from first_subscription
	group by 1
)
select
	month,
	new_customers,
	sum(new_customers) over (order by month) as running_total_new_customers,
	round((new_customers-lag(new_customers) over (order by month))*1.0/
	nullif(lag(new_customers) over (order by month),0)*100,2) as mom_growth_percentage
from new_customers
order by 1;


-- Число НОВЫХ ПОДПИСОК в месяц и общее число подписок, число подписок по тарифам в месяц и общее число подписок по тарифам

select
	month,
	new_subscriptions,
	sum(new_subscriptions) over (order by month) as running_new_subscriptions,
	new_basic,
	sum(new_basic) over (order by month) as running_new_basic,
	new_pro,
	sum(new_pro) over (order by month) as running_new_pro,
	new_enterprise,
	sum(new_enterprise) over (order by month) as running_new_enterprise
from (
	select
		date_trunc('month', start_date::date)::date as month,
		count (subscription_id) as new_subscriptions,
		count(case when plan_tier='Basic' then 1 end) as new_basic,
		count(case when plan_tier='Pro' then 1 end) as new_pro,
		count(case when plan_tier='Enterprise' then 1 end) as new_enterprise
	from ravenstack_subscriptions
	group by 1
)
order by 1;
