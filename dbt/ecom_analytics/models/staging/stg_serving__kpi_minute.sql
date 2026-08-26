with source as (

    select * from {{ source('serving', 'kpi_minute') }}

),

renamed as (

    select
        window_start,
        metric        as metric_name,
        value         as metric_value

    from source

)

select * from renamed