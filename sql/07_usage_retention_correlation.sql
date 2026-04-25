/*
------------------------------------------------------------------------------------------
Связь между продуктовой активностью и удержанием пользователей 
------------------------------------------------------------------------------------------
*/

with usage_activity as (
	select
		a.account_id,
		count (distinct feature_name) as features_used,
		coalesce(sum(usage_count),0) as total_usage
	from accounts_summary a
	left join ravenstack_subscriptions s using (account_id)
	left join ravenstack_feature_usage f 
		on s.subscription_id = f.subscription_id
		and f.usage_date::date <= a.signup_date::date + interval '60 days'
	group by a.account_id
),
segments as (
	select 
		account_id,
		features_used,
		total_usage,
		churn_flag,
		lifetime_months_till_churn,
		case 
			when total_usage=0 then 'no_usage'
			when total_usage < 300 then 'low usage'
			when total_usage < 700 then 'medium usage'
			else 'high usage'
		end as usage_segment	
	from accounts_summary
	left join usage_activity using (account_id)
)
select 
	usage_segment,
	count(*) as accounts,
	round(count(*)::numeric/sum(count(*)) over()*100,2) as segment_share_percentage,
	sum(churn_flag) as churned_accounts,
	round(sum(churn_flag)::numeric/count(*)*100,2) as churn_rate_percentage,
	round(avg(lifetime_months_till_churn),2) as avg_lifetime_months
from segments
group by usage_segment
order by 
	case usage_segment
		when 'no_usage' then 1
		when 'low usage' then 2
		when 'medium usage' then 3
		when 'high usage' then 4	
	end;

/*
 В рамках этого датасета не наблюдается явной зависимости между активностью использования 
 продукта и удержанием пользователей.
 */
