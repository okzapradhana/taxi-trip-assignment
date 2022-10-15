INSERT INTO public.raw_trip_data
SELECT * FROM
(
    SELECT DISTINCT *
    FROM staging.raw_trip_data
) a