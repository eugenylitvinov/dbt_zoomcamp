{{ config(materialized='view') }}

-- dispatching_base_num	STRING	NULLABLE	
-- pickup_datetime	TIMESTAMP	NULLABLE	
-- dropoff_datetime	TIMESTAMP	NULLABLE	
-- PULocationID	INTEGER	NULLABLE	
-- DOLocationID	INTEGER	NULLABLE	
-- SR_Flag	INTEGER	NULLABLE	

select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime', 'PULocationID']) }} as tripid,
    cast(dispatching_base_num as STRING) as dispatching_base_num,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    
    cast(SR_Flag as integer) as SR_Flag

from {{ source('staging-e','fhv') }}



-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 50

{% endif %}