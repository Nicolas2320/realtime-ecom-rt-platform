with kpi_minute as (

    select * from {{ ref('int_kpi_minute__pivoted') }}

),

by_hour as (

    select
        date_trunc('hour', window_start) as hour_start,

        sum(events_total)       as events_total,
        sum(events_click)       as events_click,
        sum(events_add_to_cart) as events_add_to_cart,
        sum(events_checkout)    as events_checkout,
        sum(events_purchase)    as events_purchase,
        sum(revenue)            as revenue

    from kpi_minute
    group by 1

),

final as (

    select
        hour_start,
        events_total,
        events_click,
        events_add_to_cart,
        events_checkout,
        events_purchase,
        revenue,

        case when events_purchase > 0 then revenue / events_purchase else 0 end            as aov,
        case when events_click > 0 then events_add_to_cart::numeric / events_click else 0 end        as add_to_cart_rate,
        case when events_add_to_cart > 0 then events_checkout::numeric / events_add_to_cart else 0 end as checkout_rate,
        case when events_click > 0 then events_purchase::numeric / events_click else 0 end            as purchase_conversion

    from by_hour

)

select * from final