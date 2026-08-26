-- revenue is only ever summed for `purchase` events in gold_kpi_writer.py,
-- so a hour with zero purchases must have exactly zero revenue.
-- This query should return no rows if that invariant holds.

select
    hour_start,
    events_purchase,
    revenue
from {{ ref('mart_kpi_hourly') }}
where events_purchase = 0
  and revenue != 0