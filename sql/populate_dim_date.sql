INSERT INTO dim_date (
  date_id,
  full_date,
  day,
  month,
  year,
  quarter,
  day_of_week
)
SELECT
  TO_CHAR(d, 'YYYYMMDD')::int AS date_id,
  d                           AS full_date,
  EXTRACT(DAY FROM d)::int    AS day,
  EXTRACT(MONTH FROM d)::int  AS month,
  EXTRACT(YEAR FROM d)::int   AS year,
  EXTRACT(QUARTER FROM d)::int AS quarter,
  EXTRACT(DOW FROM d)::int    AS day_of_week
FROM generate_series(
  '2022-12-01'::date,
  '2022-12-31'::date,
  INTERVAL '1 day'
) d;


INSERT INTO dim_developer (developer_id, name)
VALUES
  (8, 'TMG'),
  (33, 'El Morshedy'),
  (87, 'IGI'),
  (65, 'SODIC'),
  (64, 'Palm Hills'),
  (16, 'Marakez');
  
