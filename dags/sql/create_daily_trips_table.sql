CREATE TABLE IF NOT EXISTS public.daily_trips (
    date DATE,
    num_trips INTEGER,
    total_duration FLOAT,
    avg_duration FLOAT,
    total_passenger INTEGER,
    avg_passenger FLOAT,
    total_fares FLOAT,
    avg_fares FLOAT,
    count_tips INTEGER,
    total_tips FLOAT,
    avg_tips FLOAT,
    total_surcharges FLOAT,
    most_payment_type INT,
    count_to_airport INT,
    total_airport_fee FLOAT
)