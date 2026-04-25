/*
==================================================================================== 
АНАЛИЗ МЕЖДУНАРОДНОГО SAAS-ПРОДУКТА: РОСТ, ОТТОК И УДЕРЖАНИЕ ПОЛЬЗОВАТЕЛЕЙ
====================================================================================
 
 
 
-------------------------------------------------------------------------------------
Разведочный анализ данных и подготовка общей таблицы с агрегацией на уровне аккаунтов
-------------------------------------------------------------------------------------
*/

-- 1. Подсчет количества строк и уникальных значений ключей

select 	
	count(*) as count_rows,
	count(distinct subscription_id) as count_subs,
	count(distinct account_id) as count_accounts
from ravenstack_subscriptions;


-- 2. Пропуски и типы данных 

select 
	count(*) filter (where subscription_id is null) as subs_nulls,
	count(*) filter (where account_id is null) as account_nulls,
	count(*) filter (where start_date is null) as start_date_nulls,
	count(*) filter (where end_date is null) as end_date_nulls,
	count(*) filter (where plan_tier is null) as plan_tier_nulls,
	count(*) filter (where seats is null) as seats_nulls,
	count(*) filter (where mrr_amount is null) as mrr_amount_nulls,
	count(*) filter (where arr_amount is null) as arr_amount_nulls,
	count(*) filter (where is_trial is null) as is_trial_nulls,
	count(*) filter (where upgrade_flag is null) as upgrade_nulls,
	count(*) filter (where downgrade_flag is null) as downgrade_nulls,
	count(*) filter (where churn_flag is null) as churn_nulls,
	count(*) filter (where billing_frequency is null) as billing_nulls,
	count(*) filter (where auto_renew_flag is null) as auto_renew_nulls
from ravenstack_subscriptions;


-- 3. Расчет приблизительного общего дохода от клиента (REVENUE) и приблизительного жизненного цикла пользователя в продукте (LIFETIME): минимальное, максимальное и среднее значения

/*
REVENUE рассчитывается до первого ухода клиента, потому что в датасете нет надежных данных
о фактическом окончании каждой подписки.
LIFETIME рассчитывается как разница между датой регистрации и первым уходом клиента 
(если его нет, то берется последняя дата в датасете (по активности)).
 */


-- жизненный цикл пользователя в продукте (в месяцах)
with max_date as (
    select max(usage_date)::date as max_date
    from ravenstack_feature_usage
),
first_churn as (
    select 
        account_id,
        min(churn_date::date) as first_churn_date
    from ravenstack_churn_events
    group by account_id
),
base as (
    select 
        a.account_id,
        a.signup_date::date as start_date,
        coalesce(c.first_churn_date, m.max_date) as end_date
    from ravenstack_accounts a
    left join first_churn c using (account_id)
    cross join max_date m
),
lifetime as (
    select
        account_id,
        greatest(extract(year from age(end_date, start_date))*12 +
        extract(month from age(end_date, start_date)),0) as lifetime_months_till_churn
    from base
),
-- примерный доход (через подписки)
subscriptions_dates as (
    select
        account_id,
        start_date::date as start_date_sub,
        case 
        	when nullif(end_date, '')::date is not null 
        	and (c.first_churn_date is null or nullif(end_date, '')::date < c.first_churn_date)
        	then nullif(end_date, '')::date 
        	when c.first_churn_date is not null and start_date::date <= c.first_churn_date
        	then c.first_churn_date
        	else max_date
        end as end_date_sub,
        mrr_amount
    from ravenstack_subscriptions
    left join first_churn c using (account_id)
    cross join max_date m
),
sub_duration as (
    select
        account_id,
        greatest(extract(year from age(end_date_sub, start_date_sub))*12 +
        extract(month from age(end_date_sub, start_date_sub)),0) as duration_sub_months,
        mrr_amount
    from subscriptions_dates
),
approx_revenue as (
    select
        account_id,
        sum(mrr_amount * duration_sub_months) as approx_revenue_till_churn
    from sub_duration
    group by account_id
)
select 
    min(approx_revenue_till_churn) as min_aprox_revenue_till_churn,
    max(approx_revenue_till_churn) as max_approx_revenue_till_churn,
    round(avg(approx_revenue_till_churn),2) as avg_approx_revenue_till_churn,
    min(lifetime_months_till_churn) as min_lifetime_till_churn,
    max(lifetime_months_till_churn) as max_lifetime_till_churn,
    round(avg(lifetime_months_till_churn),2) as avg_lifetime_till_churn
from approx_revenue r
join lifetime l using (account_id);


-- 4. Метрики пользователей

----- Подписки: число подписок, приблизительный доход (REVENUE), приблизительный LIFETIME, число повышений и понижений тарифа

with max_date as (
    select max(usage_date)::date as max_date
    from ravenstack_feature_usage
),
first_churn as (
    select 
        account_id,
        min(churn_date::date) as first_churn_date
    from ravenstack_churn_events
    group by account_id
),
base as (
    select 
        a.account_id,
        a.signup_date::date as start_date,
        coalesce(c.first_churn_date, m.max_date) as end_date
    from ravenstack_accounts a
    left join first_churn c using (account_id)
    cross join max_date m
),
lifetime as (
    select
        account_id,
        greatest(extract(year from age(end_date, start_date))*12 +
        extract(month from age(end_date, start_date)),0) as lifetime_months
    from base
),
subscriptions_dates as (
    select
        account_id,
        start_date::date as start_date_sub,
        case 
        	when nullif(end_date, '')::date is not null 
        	and (c.first_churn_date is null or nullif(end_date, '')::date < c.first_churn_date)
        	then nullif(end_date, '')::date 
        	when c.first_churn_date is not null and start_date::date <= c.first_churn_date
        	then c.first_churn_date
        	else max_date
        end as end_date_sub,
        mrr_amount,
        upgrade_flag,
        downgrade_flag
    from ravenstack_subscriptions
    left join first_churn c using (account_id)
    cross join max_date m
),
sub_duration as (
    select
        account_id,
        greatest(extract(year from age(end_date_sub, start_date_sub))*12 +
        extract(month from age(end_date_sub, start_date_sub)),0) as duration_sub_months,
        mrr_amount,
        upgrade_flag,
        downgrade_flag
    from subscriptions_dates
)
select  
    account_id,
    count(*) as subscriptions,
    sum(mrr_amount * duration_sub_months) as approx_revenue_till_churn,
    lifetime_months as lifetime_months_till_churn,
    max(case when upgrade_flag then 1 else 0 end) as had_upgrade,
    max(case when downgrade_flag then 1 else 0 end) as had_downgrade
from sub_duration
left join lifetime using (account_id)
group by account_id, lifetime_months;


----- Функции: число использованных функций, количество ошибок, число использованных бета-функций

select 
	account_id,
	count(distinct feature_name) as unique_features_used,
	sum(usage_count) as total_usage_count,
	sum(error_count) as sum_errors,
	max(case when is_beta_feature then 1 else 0 end) as used_beta_feature
from ravenstack_feature_usage f
	left join ravenstack_subscriptions s
	on f.subscription_id=s.subscription_id
group by account_id;


-- Обращения в поддержку: среднее время отклика, средний satisfaction_score, количество эскалаций

select
	account_id,
	count(*) as support_tickets,
	avg(resolution_time_hours) as avg_resolution_time,
	avg(satisfaction_score) filter (where satisfaction_score is not null)
	as avg_satisfaction_score,
	sum(case when escalation_flag then 1 else 0 end) as escalations
from ravenstack_support_tickets
group by account_id
order by avg_resolution_time desc;


----- Отток пользователей: дата первого отказа от подписки, причина, денежный возврат

with churn_table as (
    select 
        account_id,
        1 as churn_flag,
        churn_date,
        reason_code,
        row_number() over (partition by account_id order by churn_date) as churn_num,
        refund_amount_usd as first_refund_usd,
        sum(refund_amount_usd) over (partition by account_id) as total_refund_usd
    from ravenstack_churn_events
)
select
	account_id,
	churn_flag,
	churn_date as first_churn_date,
	reason_code,
	first_refund_usd,
	total_refund_usd
from churn_table
where churn_num=1
order by account_id;



-- 5. ОБЩАЯ ТАБЛИЦА ПО АККАУНТАМ - accounts_summary

create view public.accounts_summary as
-- Аккаунты
with accounts as (
	select
		account_id,
		industry,
		country,
		signup_date,
		referral_source,
		plan_tier as initial_plan_tier,
		seats
	from ravenstack_accounts
),
-- Подписки
max_date as (
    select max(usage_date)::date as max_date
    from ravenstack_feature_usage
),
first_churn as (
    select 
        account_id,
        min(churn_date::date) as first_churn_date
    from ravenstack_churn_events
    group by account_id
),
base as (
    select 
        a.account_id,
        a.signup_date::date as start_date,
        coalesce(c.first_churn_date, m.max_date) as end_date
    from ravenstack_accounts a
    left join first_churn c using (account_id)
    cross join max_date m
),
lifetime as (
    select
        account_id,
        greatest(extract(year from age(end_date, start_date))*12 +
        extract(month from age(end_date, start_date)),0) as lifetime_months
    from base
),
subscriptions_dates as (
    select
        account_id,
        start_date::date as start_date_sub,
        case 
        	when nullif(end_date, '')::date is not null 
        	and (c.first_churn_date is null or nullif(end_date, '')::date < c.first_churn_date)
        	then nullif(end_date, '')::date 
        	when c.first_churn_date is not null and start_date::date <= c.first_churn_date
        	then c.first_churn_date
        	else max_date
        end as end_date_sub,
        mrr_amount,
        upgrade_flag,
        downgrade_flag
    from ravenstack_subscriptions
    left join first_churn c using (account_id)
    cross join max_date m
),
sub_duration as (
    select
        account_id,
        greatest(extract(year from age(end_date_sub, start_date_sub))*12 +
        extract(month from age(end_date_sub, start_date_sub)),0) as duration_sub_months,
        mrr_amount,
        upgrade_flag,
        downgrade_flag
    from subscriptions_dates
),
subscriptions as (
	select  
    	account_id,
    	count(*) as subscriptions,
    	sum(mrr_amount * duration_sub_months) as approx_revenue_till_churn,
    	lifetime_months,
    	max(case when upgrade_flag then 1 else 0 end) as had_upgrade,
   	 	max(case when downgrade_flag then 1 else 0 end) as had_downgrade
	from sub_duration
	left join lifetime using (account_id)
	group by account_id, lifetime_months
),
-- Функции
features as (
	select 
		account_id,
		count(distinct feature_name) as unique_features_used,
		sum(usage_count) as total_usage_count,
		sum(error_count) as sum_errors,
		max(case when is_beta_feature then 1 else 0 end) as used_beta_feature
	from ravenstack_feature_usage f
		left join ravenstack_subscriptions s
		on f.subscription_id=s.subscription_id
	group by account_id
),
-- Обращения в поддержку
support_tickets as (
	select
		account_id,
		count(*) as support_tickets,
		avg(resolution_time_hours) as avg_resolution_time,
		avg(satisfaction_score) filter (where satisfaction_score is not null)
		as avg_satisfaction_score,
		sum(case when escalation_flag then 1 else 0 end) as escalations
	from ravenstack_support_tickets
	group by account_id
),
-- Отток
churn as (
	select 
		account_id,
		churn_flag,
		churn_date as first_churn_date,
		reason_code,
		first_refund_usd,
		total_refund_usd
	from (
		select 
	        account_id,
	        1 as churn_flag,
	        churn_date,
	        reason_code,
	        row_number() over (partition by account_id order by churn_date) as churn_num,
	        refund_amount_usd as first_refund_usd,
	        sum(refund_amount_usd) over (partition by account_id) as total_refund_usd
	    from ravenstack_churn_events
	)
	where churn_num=1
)
select 
	a.account_id,
	a.industry,
	a.country,
	a.signup_date,
	a.referral_source,
	a.initial_plan_tier,
	a.seats,
	coalesce(subscriptions,0) as subscriptions,
	coalesce(approx_revenue_till_churn,0) as approx_revenue_till_churn,
	coalesce(lifetime_months,0) as lifetime_months_till_churn,
	coalesce(had_upgrade,0) as had_upgrade,
	coalesce(had_downgrade,0) as had_downgrade,
	coalesce(unique_features_used,0) as unique_features_used,
	coalesce(total_usage_count,0) as total_usage_count,
	coalesce(sum_errors,0) as sum_errors,
	coalesce(used_beta_feature,0) as used_beta_feature,
	coalesce(support_tickets,0) as support_tickets,
	round(avg_resolution_time::numeric,2) as avg_resolution_time,
	round(avg_satisfaction_score::numeric,2) as avg_satisfaction_score,
	coalesce(escalations,0) as escalations,
	coalesce(churn_flag,0) as churn_flag,
	first_churn_date,
	reason_code,
	coalesce(first_refund_usd,0) as first_refund_usd,
	coalesce(total_refund_usd,0) as total_refund_usd
from accounts a
	left join subscriptions s on a.account_id=s.account_id 
	left join features f on a.account_id=f.account_id
	left join support_tickets t on a.account_id=t.account_id
	left join churn c on a.account_id=c.account_id;

 
 /*
approx_revenue_till_churn - оценка дохода от пользователя до первого churn-события.
Метрика является приближенной из-за отсутствия точных дат окончания подписок.

refund_usd = 0 означает, что либо не было отказа от подписки, либо отказ был без возврата денежных средств
(определяется через churn_flag).
*/
