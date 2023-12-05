{{config(materialized='view')}}
with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num) as rn
  from {{ source('staging','fhv_tripdata') }}
  where LOWER(dispatching_base_num) like 'b%'   
)
select

   -- identifiers
   {{dbt_utils.surrogate_key(['rn', 'dispatching_base_num'])}} as tripid,

    dispatching_base_num,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    SR_Flag as share_ride_flag,
    


from tripdata
{%if var('is_test_run', default=true)%}
limit 100
{%endif%}