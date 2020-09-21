select
    scheduleid
    , substring(servicegroup, 4) as gateway
    , shortcode as service_identifier1
    , serviceid
    , price as tariff
    , frequency as billing_days
    , status as schedule_status
    , telco as operator_code
    , updatedate
from {sam_database}.sub_schedule