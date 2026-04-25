/*
--------------------------------------------------------------------------------------------------
Когортный анализ и удержание пользователей (retention rate по когортам)
--------------------------------------------------------------------------------------------------

Рассчитаны несколько типов RETENTION RATE: по продуктовой активности и оплате. 
Это позволяет выявить пользователей, которые продолжают платить, но не используют продукт.
*/


--  Когортный анализ и RETENTION RATE по продуктовой активности

with cohorts as (
    select 
        account_id,
        date_trunc('month', signup_date::date)::date as cohort_month
    from accounts_summary
),
active_months as (
    select distinct
        account_id,
        date_trunc('month', usage_date::date)::date as active_month
    from ravenstack_feature_usage
    left join ravenstack_subscriptions using (subscription_id)
    where usage_date is not null
),
month_activity as (
    select 
        account_id,
        cohort_month,
        active_month,
        extract(year from age(active_month, cohort_month))*12 +
        extract(month from age(active_month, cohort_month)) as month_number
    from cohorts
    join active_months using (account_id)
    where active_month >= cohort_month
),
activity as (
    select
        account_id,
        cohort_month,
        month_number,
        row_number() over (partition by account_id order by month_number) as rn
    from month_activity
),
continuous_activity as (
    select
        account_id,
        cohort_month,
        month_number
    from activity
    where month_number = rn - 1
),
retention_base as (
    select
        cohort_month,
        month_number,
        count(distinct account_id) as active_accounts
    from continuous_activity
    group by cohort_month, month_number
),
cohort_size as (
    select 
        cohort_month,
        count(distinct account_id) as cohort_size
    from cohorts
    group by cohort_month
)
select
    cohort_month,
    month_number,
    active_accounts,
    cohort_size,
    round(active_accounts::numeric / cohort_size * 100, 2) 
        as product_retention_rate_percentage
from retention_base
join cohort_size using (cohort_month)
order by 1, 2;


-- Когортный анализ и RETENTION RATE по оплате (до первого оттока)

/*
Удержание пользователей в этой части анализа рассчитано на основе данных по оттоку. Активность 
пользователей между регистрацией и оттоком не учитывается. Эта метрика показывает долю пользователей, 
продолжающих платить за продукт.
*/


with max_date as (
    select max(usage_date)::date as max_date
    from ravenstack_feature_usage
),
first_last_date as (
	select 
		account_id,
		signup_date::date as first_date,
		coalesce(first_churn_date::date, max_date) as last_date
	from accounts_summary
	cross join max_date
),
cohorts as (
	select 
		account_id,
		first_date,
		date_trunc('month', first_date)::date as cohort_month,
		last_date
	from first_last_date
),
active_months as (
	select
		account_id,
		cohort_month,
		first_date,
		last_date,
		generate_series(date_trunc('month', first_date), 
		date_trunc('month', last_date), interval '1 month')::date as active_month
	from cohorts
),
month_num as (
	select 
		account_id,
		cohort_month,
		active_month,
		extract(year from age(active_month, cohort_month))*12
		+extract(month from age(active_month, cohort_month)) as month_number
	from active_months
),
retention_base as (
	select
		cohort_month, 
		month_number,
		count(distinct account_id) as active_accounts
	from month_num
	group by cohort_month, month_number
),
cohort_size as (
	select 
		cohort_month,
		count(distinct account_id) as cohort_size
	from cohorts
	group by cohort_month
)
select
	cohort_month,
	month_number,
	active_accounts,
	cohort_size,
	round(active_accounts::numeric/cohort_size*100,2)
	as payment_retention_rate_till_first_churn_percentage
from retention_base
	join cohort_size using (cohort_month)
order by 1,2;


-- Когортный анализ и RETENTION RATE по оплате (до последнего оттока)

with max_date as (
    select max(usage_date)::date as max_date
    from ravenstack_feature_usage
),
first_last_date as (
	select 
		account_id,
		signup_date::date as first_date,
		coalesce(max(churn_date::date), max_date) as last_date
	from ravenstack_accounts
	left join ravenstack_churn_events using (account_id)
	cross join max_date
	group by account_id, signup_date::date, max_date
),
cohorts as (
	select 
		account_id,
		first_date,
		date_trunc('month', first_date)::date as cohort_month,
		last_date
	from first_last_date
),
active_months as (
	select
		account_id,
		cohort_month,
		first_date,
		last_date,
		generate_series(date_trunc('month', first_date), 
		date_trunc('month', last_date), interval '1 month')::date as active_month
	from cohorts
),
month_num as (
	select 
		account_id,
		cohort_month,
		active_month,
		extract(year from age(active_month, cohort_month))*12
		+extract(month from age(active_month, cohort_month)) as month_number
	from active_months
),
retention_base as (
	select
		cohort_month, 
		month_number,
		count(distinct account_id) as active_accounts
	from month_num
	group by cohort_month, month_number
),
cohort_size as (
	select 
		cohort_month,
		count(distinct account_id) as cohort_size
	from cohorts
	group by cohort_month
)
select
	cohort_month,
	month_number,
	active_accounts,
	cohort_size,
	round(active_accounts::numeric/cohort_size*100,2)
	as payment_retention_rate_till_last_churn_percentage
from retention_base
join cohort_size using (cohort_month)
order by 1, 2;

