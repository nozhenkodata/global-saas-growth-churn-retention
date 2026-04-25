/*
----------------------------------------------------------------------------------- 
Число активных пользователей в месяц (MAU)
-----------------------------------------------------------------------------------

MAU рассчитан как количество уникальных аккаунтов, которые хотя бы один раз в месяц
пользовались продуктом.
*/


with active_months as (
    select
        s.account_id,
        date_trunc('month', f.usage_date::date)::date as active_month
    from ravenstack_subscriptions s
    join ravenstack_feature_usage f using (subscription_id)
)
select 
    active_month,
    count(distinct account_id) as mau
from active_months
group by active_month
order by 1;
