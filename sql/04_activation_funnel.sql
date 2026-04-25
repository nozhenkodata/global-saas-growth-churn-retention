/*
------------------------------------------------------------------------------------------
Воронка активации: регистрация - подписки - использование
Расчет времени с момента регистрации до первой подписки и первого использования продукта 
------------------------------------------------------------------------------------------
*/


-- Конверсия и среднее время активации продукта на каждом этапе

with first_sub as (
	select
		account_id,
		min(start_date) as first_subscription
	from ravenstack_subscriptions
	group by account_id
),
first_usage as (
	select
		account_id,
		min(usage_date) as first_usage
	from ravenstack_feature_usage
	join ravenstack_subscriptions using (subscription_id)
	group by account_id 
),
activation_funnel as (
	select 
		account_id,
		signup_date,
		first_subscription,
		first_usage
	from ravenstack_accounts
	left join first_sub using (account_id)
	left join first_usage using (account_id)
),
conversion as (
	select
		count(first_subscription)::numeric/count(*) 
		as subscription_conv,
		count(first_usage)::numeric/count(*) 
		as usage_conv,
		count(first_usage)::numeric/nullif(count(first_subscription),0)
		as usage_from_subscription_conv
	from activation_funnel
),
avg_activation_time as (
	select 
		round(avg(first_subscription::date - signup_date::date) 
		filter (where first_subscription::date >= signup_date::date),2) as avg_subscription_activation_time,
		round(avg(first_usage::date-signup_date::date)
		filter (where first_usage::date >= signup_date::date),2) as avg_usage_activation_time,
		round(avg(first_usage::date-first_subscription::date)
		filter (where first_usage::date >= first_subscription::date),2) as avg_usage_from_subscription_activation_time
	from activation_funnel
)
select
	'subscription' as funnel,
	(select subscription_conv from conversion) as conversion,
	(select avg_subscription_activation_time from avg_activation_time) as avg_activation_time_days
union all
select
	'usage' as funnel,
	(select usage_conv from conversion) as conversion,
	(select avg_usage_activation_time from avg_activation_time) as avg_activation_time_days
union all
select
	'usage_from_subscription' as funnel,
	(select usage_from_subscription_conv from conversion) as conversion,
	(select avg_usage_from_subscription_activation_time from avg_activation_time) as avg_activation_time_days;


/*
 ЗАМЕЧАНИЕ:
 
 Датасет содержит сгенерированные с помощью ИИ и Python данные. В датасете пристутсвуют аномальные 
 значения: даты первого использования продукта и подписки могут предшествовать дате регистрации аккаунта, 
 а также у всех пользователей есть подписка и активность. Поэтому результаты анализа воронки активации продукта 
 на этих данных не являются информативными. 
 
 В датасете нет пользователей, которые бы начали использовать продукт после оформления подписки, что может быть
 связано с ограничениями данных или наличием пробного периода.
 
 Из-за ограничений данных в анализе использутся только расчеты медианы времени с момента регистрации аккаунта
 до первой подписки и первого использования продукта.
 */


-- Медиана времени с момента регистрации до первой подписки и первого использования продукта

with first_sub as (
	select
		account_id,
		min(start_date) as first_subscription
	from ravenstack_subscriptions
	group by account_id
),
first_usage as (
	select
		account_id,
		min(usage_date) as first_usage
	from ravenstack_feature_usage
	join ravenstack_subscriptions using (subscription_id)
	group by account_id 
),
activation_funnel as (
	select 
		account_id,
		signup_date,
		first_subscription,
		first_usage
	from ravenstack_accounts
	left join first_sub using (account_id)
	left join first_usage using (account_id)
)
select 
	percentile_cont(0.5) within group (order by (first_subscription::date - signup_date::date)) 
	filter (where first_subscription::date >= signup_date::date) 
	as median_subscription_from_signup_time_days,	
	percentile_cont(0.5) within group (order by (first_usage::date-signup_date::date))
	filter (where first_usage::date >= signup_date::date) 
	as median_usage_from_signup_time_days
from activation_funnel;


-- Качество данных: достоверные данные/недостоверные данные

with first_sub as (
	select
		account_id,
		min(start_date) as first_subscription
	from ravenstack_subscriptions
	group by account_id
),
first_usage as (
	select
		account_id,
		min(usage_date) as first_usage
	from ravenstack_feature_usage
	join ravenstack_subscriptions using (subscription_id)
	group by account_id 
),
activation_funnel as (
	select 
		account_id,
		signup_date,
		first_subscription,
		first_usage
	from ravenstack_accounts
	left join first_sub using (account_id)
	left join first_usage using (account_id)
)
select 
	count(*) as total_count,
	count(account_id) filter (where first_usage >= signup_date) as valid_usage_data,
	count(account_id) filter (where first_usage < signup_date) as invalid_usage_data
from activation_funnel;
