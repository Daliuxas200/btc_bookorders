with bids as (
	select 
	created_at,
	sum(cast(bids->>1 as numeric)) as total_bids
	from binance_order_book ob,
	json_each(ob.response) as json_data,
	json_array_elements(json_data.value) as bids
	where json_data.key = 'bids' and symbol = 'ethusdt'
	group by ob.created_at
), asks as (
	select 
	created_at,
	sum(cast(asks->>1 as numeric)) as total_asks
	from binance_order_book ob,
	json_each(ob.response) as json_data,
	json_array_elements(json_data.value) as asks
	where json_data.key = 'asks' and symbol = 'ethusdt'
	group by ob.created_at
), daily_bids as (
	select 
	date_trunc('day',created_at) as dt,
	avg(total_bids) as avg_bids
	from bids group by date_trunc('day',created_at)
), daily_asks as (
	select 
	date_trunc('day',created_at) as dt,
	avg(total_asks) as avg_asks
	from asks group by date_trunc('day',created_at)
) select 
	ha.dt,
	hb.avg_bids,
	ha.avg_asks
from daily_bids hb, daily_asks ha
where ha.dt = hb.dt
order by dt