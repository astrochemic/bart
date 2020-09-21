select
    a.accountid
    , if(a.createdbyyamato = 1, 'yamato', 'sam') as platform
    , a.mphone as msisdn
    , a.status
    , convert_tz(a.createdate, '+08:00', 'UTC') as createdate
    , convert_tz(a.updatedate, '+08:00', 'UTC') as updatedate
    , substring(a.servicegroup, 4) as gateway
    , a.moperator as operator_code
    , a.serviceid
    , a.shortcode as service_identifier1
    , s.smskeyword as service_identifier2
    , null as rockman_id
from {sam_database}.account a
left join {sam_database}.service s on a.serviceid = s.serviceid
where a.createdate < convert_tz('{start_date}', 'UTC', '+08:00')
and (a.status = 'A' or a.updatedate >= convert_tz('{end_date}', 'UTC', '+08:00'))