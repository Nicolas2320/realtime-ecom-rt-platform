with kpi as (

    select * from {{ ref('stg_serving__kpi_minute') }}

),

pivoted as (

    select
        window_start,
        max(case when metric_name = 'events_total'         then metric_value end) as events_total,
        max(case when metric_name = 'events_click'          then metric_value end) as events_click,
        max(case when metric_name = 'events_add_to_cart'    then metric_value end) as events_add_to_cart,
        max(case when metric_name = 'events_checkout'       then metric_value end) as events_checkout,
        max(case when metric_name = 'events_purchase'       then metric_value end) as events_purchase,
        max(case when metric_name = 'revenue'                then metric_value end) as revenue,
        max(case when metric_name = 'aov'                    then metric_value end) as aov,
        max(case when metric_name = 'add_to_cart_rate'      then metric_value end) as add_to_cart_rate,
        max(case when metric_name = 'checkout_rate'          then metric_value end) as checkout_rate,
        max(case when metric_name = 'purchase_conversion'   then metric_value end) as purchase_conversion

    from kpi
    group by window_start

)

select * from pivoted