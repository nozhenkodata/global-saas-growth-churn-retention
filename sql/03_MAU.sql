/*
----------------------------------------------------------------------------------- 
Число активных пользователей в месяц (MAU)
-----------------------------------------------------------------------------------

MAU рассчитан как количество уникальных аккаунтов, которые хотя бы один раз в месяц
пользовались продуктом. В данных обнаружены аномальные значения (использование продукта 
до регистрации аккаунта). Такие значения не учитываются при расчете MAU.
*/


with active_months as (
    select
        s.account_id,
        date_trunc('month', f.usage_date::date)::date as active_month
    from ravenstack_subscriptions s
    join ravenstack_feature_usage f using (subscription_id)
    join ravenstack_accounts a using (account_id)
    left join ravenstack_churn_events c using (account_id)
    where f.usage_date::date >= a.signup_date::date and
    	(c.churn_date is null or 
    	f.usage_date::date <= churn_date::date)
)
select 
	active_month,
	count(distinct account_id) as mau
from active_months
group by active_month
order by 1;
