{{ config(materialized='table') }}

with fhv_data as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
), 

fhv_bases as (
    select * 
    from {{ ref('fhv_bases_clean') }}
),
dim_zones as (
    select
    locationid,
    borough,
    zone
    from {{ ref('dim_zones')}}
)
select 
    tripid,
    dispatching_base_num,
    entity_name,
    type_of_base,
    pickup_zone.zone || ', ' || pickup_zone.borough as pickup_location,
    dropoff_zone.zone || ', ' || dropoff_zone.borough as dropoff_location,
    pickup_datetime,
    dropoff_datetime,
    share_ride_flag


from fhv_data
inner join fhv_bases 
on fhv_data.dispatching_base_num = fhv_bases.license_number
inner join dim_zones as pickup_zone 
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone 
on fhv_data.dropoff_locationid = dropoff_zone.locationid
