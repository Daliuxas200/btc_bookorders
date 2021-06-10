with ob as (
	select 
	*
	from binance_order_book 
	where symbol = 'ethusdt'
	order by created_at desc
	limit 1
), price_amount as (
	select 
	cast(json_data.value->>0 as numeric) price,
	cast(json_data.value->>1 as numeric) amount,
	cast(json_data.value->>0 as numeric) * cast(json_data.value->>1 as numeric) as total_value
	from ob, 
	json_array_elements(ob.response->'bids') as json_data
), zoomed_out as (
	select round(price / 100) * 100 as price, sum(total_value) as val
	from price_amount
	group by round(price / 100)
) select * from zoomed_out
order by price desc