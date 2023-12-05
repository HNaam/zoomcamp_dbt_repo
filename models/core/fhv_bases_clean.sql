{{config(materialized='table')}}

select 
    license_number,
    entity_name,
    type_of_base
from {{ ref('fhv_bases')}}
