/*
---------------------------------------------------------------------------------------------------------------- 
Отток пользователей (churn) - общий и по сегментам: отрасль, страна, начальный тариф, реферальный источник
----------------------------------------------------------------------------------------------------------------
*/


-- Общий отток (агрегированный показатель)

select 
	count(*) filter (where churn_flag=1) as churned_accounts,
	count(*) filter (where churn_flag=1)*100.0/count(*) 
	as churn_rate_percentage
from accounts_summary;


-- Отток по сегментам

/*
В данном датасете нельзя точно определить тарифный план на момент отказа от продукта (оттока), 
поэтому в анализе используется начальный тариф аккаунта.
 */


with industry_churn as (
	select
		industry,
		count(*) as total_accounts,
		count(*) filter (where churn_flag=1) as churned_accounts
	from accounts_summary
	group by industry
),
country_churn as (
	select
		country,
		count(*) as total_accounts,
		count(*) filter (where churn_flag=1) as churned_accounts
	from accounts_summary
	group by country
),
referral_churn as (
	select
		referral_source,
		count(*) as total_accounts,
		count(*) filter (where churn_flag=1) as churned_accounts
	from accounts_summary
	group by referral_source
),
initial_plan_churn as (
	select
		initial_plan_tier,
		count(*) as total_accounts,
		count(*) filter (where churn_flag=1) as churned_accounts
	from accounts_summary
	group by initial_plan_tier
)
select 
	'industry' as segment,
	industry as subsegment,
	total_accounts,
	churned_accounts,
	round(churned_accounts::numeric/total_accounts*100,2) as churned_share_percentage
from industry_churn
union all
select 
	'country' as segment,
	country as subsegment,
	total_accounts,
	churned_accounts,
	round(churned_accounts::numeric/total_accounts*100,2) as churned_share_percentage
from country_churn
union all
select 
	'referral_source' as segment,
	referral_source as subsegment,
	total_accounts,
	churned_accounts,
	round(churned_accounts::numeric/total_accounts*100,2) as churned_share_percentage
from referral_churn
union all
select 
	'initial_plan_tier' as segment,
	initial_plan_tier as subsegment,
	total_accounts,
	churned_accounts,
	round(churned_accounts::numeric/total_accounts*100,2) as churned_share_percentage
from initial_plan_churn
order by 1, 5 desc;

/*
Уровень оттока пользователей рассчитан внутри каждого сегмента. Для сегментов с высоким уровнем 
оттока пользователей для оценки риска необходим дополнительный анализ с учетом размера сегмента 
и его доли в общей выручке.
*/
