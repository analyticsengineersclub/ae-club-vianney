with base as (
SELECT 
visitor_id,
timestamp as initial, 
row_number() over (partition by visitor_id order by timestamp asc) as num,
lag(timestamp) over (partition by visitor_id order by timestamp) as prev
FROM `analytics-engineers-club.web_tracking.pageviews` 
--where visitor_id = '0f67a08b-1b78-4f30-ac39-15a1316390a0'
order by timestamp asc
),
time_d as (
select 
visitor_id,
initial, prev,
timestamp_diff(initial,prev,second) as time_diff

from base
),

session as (
  select 
  *,
  cast(coalesce(time_diff > 1800, true) as integer) as is_new_session
  from time_d
),

final as (
select *,
sum(is_new_session) over (partition by visitor_id order by initial rows between unbounded preceding and current row) as session_number 
from session
)
select * from final
order by visitor_id, initial
