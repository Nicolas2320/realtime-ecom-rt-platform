with source as (

    select * from {{ source('serving', 'alerts') }}

),

renamed as (

    select
        alert_id,
        window_start,
        metric        as metric_name,
        value         as metric_value,
        baseline      as baseline_value,
        score         as z_score,
        severity,
        rule_name,
        created_at

    from source

)

select * from renamed