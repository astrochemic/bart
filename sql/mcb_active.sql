select
    s.subscription_id as accountid
    , 'mcb' as platform
    , s.msisdn
    , s.status
    , s.create_date_time as createdate
    , s.update_date_time as updatedate
    , upper(g.code) as gateway
    , o.fqn as operator_code
    , ifnull(s.scenario_service_id, -1) as serviceid
    , ifnull(ifnull(p.product_identifier_1, srv.name), 'Unknown') as service_identifier1
    , ifnull(nullif(pd.product_identifier_2, ''), 'Unknown') as service_identifier2
    , s.rockman_id
from mcb.subscription s
left join mcb.gateway g on g.id = s.gateway_id
left join mcb.operator o on o.id = s.operator_id
left join mcb.country c on c.id = o.country_id
left join mcb.scenario_service ss on ss.id = s.scenario_service_id
left join mcb.service srv on srv.id = ss.service_id
left join mcb.product_distribution pd on pd.id = s.product_distribution_id
left join mcb.product p on p.id = pd.product_id
where c.code = '{country_code}'
and s.create_date_time < '{start_date}'
and (s.status = 'active' or s.update_date_time >= '{end_date}')