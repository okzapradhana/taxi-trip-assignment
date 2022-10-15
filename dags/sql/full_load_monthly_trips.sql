-- Monthly Trips
truncate table public.monthly_trips;
with base_trips_cte as (
	select 
		*,
		extract(epoch from (tpep_dropoff_datetime - tpep_pickup_datetime))/60 as trip_duration_minutes,
		case when airport_fee > 0 then 1 else 0 end as is_trip_using_airport,
		case when tip_amount > 0 then 1 else 0 end as is_receiving_tip,
		improvement_surcharge + congestion_surcharge as all_surcharge
	from public.raw_trip_data
),
most_payment_each_month_cte as (
	select 
		trip_period as date,
		payment_type as most_payment_type
	from 
	(
		select
			trip_period,
			payment_type,
			rank() over (partition by trip_period order by COUNT(*) desc) as payment_rank
		from base_trips_cte
		group by trip_period, payment_type
		order by trip_period
	) a
	where a.payment_rank = 1
),
group_base_trip_cte as (
	select
		trip_period as date,
		count(*) num_trips,
		SUM(trip_duration_minutes) as total_duration,
		AVG(trip_duration_minutes) as avg_duration,
		SUM(passenger_count) as total_passenger,
		AVG(passenger_count) as avg_passenger,
		SUM(fare_amount) as total_fares,
		AVG(fare_amount) as avg_fares,
		SUM(is_receiving_tip) as count_tips,
		SUM(tip_amount) as total_tips,
		AVG(tip_amount) as avg_tips,
		SUM(all_surcharge) as total_surcharges,
		SUM(is_trip_using_airport) as count_to_airport,
		SUM(airport_fee) as total_airport_fee
	from base_trips_cte
	group by date
),
join_base_trip_payment_cte as (
	select 
		trip.date as date,
		num_trips,
		total_duration,
		avg_duration,
		total_passenger,
		avg_passenger,
		total_fares,
		avg_fares,
		count_tips,
		total_tips,
		avg_tips,
		total_surcharges,
		payment.most_payment_type,
		count_to_airport,
		total_airport_fee
	from group_base_trip_cte trip
	left join most_payment_each_month_cte payment on trip.date = payment.date
)
insert into public.monthly_trips (
	select * from join_base_trip_payment_cte
	order by date
)