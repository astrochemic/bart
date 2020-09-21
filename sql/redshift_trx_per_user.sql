select
    coalesce(rockman_id, 'Unknown') as rockman_id
    , msisdn
    , gateway
    , operator_code
    , service_identifier1
    , coalesce(service_identifier2, 'Unknown') as service_identifier2
    , platform
    , count(*) as total_transactions
    , sum(case when dnstatus = 'Delivered' then 1 else 0 end) as delivered_transactions
from transactions
where country_code = '{country_code}'
and timestamp >= '{start_date}'
and timestamp < '{end_date}'
and tariff > 0
group by 1,2,3,4,5,6,7
order by 1,2,3,4,5,6,7