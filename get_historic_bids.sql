select 
	ob.created_at,
	sum(cast(bids->>1 as numeric)) as total_bids
from binance_order_book ob,
json_each(ob.response) as json_data,
json_array_elements(json_data.value) as bids
where json_data.key = 'bids'
group by ob.created_at
order by ob.created_at