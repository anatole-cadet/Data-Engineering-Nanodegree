import configparser



config = configparser.ConfigParser()
config.read("configuration/flightxru.cfg")
# ====================  FOR CREATING THE TABLES ==============================

KEY     = config.get("AWS", "KEY")
SECRET  = config.get("AWS", "SECRET")
IAM_ROLE = config.get("IAM_ROLE","ARN")



CREATE_DIM_DATE_ARRIVAL = ("""
DROP TABLE IF EXISTS dim_date_arrivals;
CREATE TABLE dim_date_arrivals(
  date_arrival_code INTEGER IDENTITY(0,1) ,
  date_arrival      timestamp  SORTKEY,
  arrival_year      int        ,
  arrival_month     int        ,
  arrival_week      int        ,
  arrival_day       int        ,
  arrival_hour      int        ,
  arrival_minute    int ,
  PRIMARY KEY (date_arrival)
)
""")


CREATE_DIM_DATE_DEPARTURE = ("""
DROP TABLE IF EXISTS dim_date_departures;
CREATE TABLE dim_date_departures(
  date_departure_code INTEGER IDENTITY(0,1),
  date_departure      timestamp  SORTKEY,
  departure_year      int        ,
  departure_month     int        ,
  departure_week      int        ,
  departure_day       int        ,
  departure_hour      int        ,
  departure_minute    int ,
  PRIMARY KEY (date_departure)
)
""")


CREATE_DIM_TICKET = ("""
DROP TABLE IF EXISTS dim_tickets;
CREATE TABLE dim_tickets
(
  ticket_no       varchar(30) SORTKEY,
  flight_code     varchar(30),
  amount_ticket    float      ,
  boarding_no     varchar(15),
  seat_no         varchar(15),
  fare_conditions varchar(15),
  passenger_id    varchar(15),
  passenger_name  varchar(35),
  email           text       ,
  phone           varchar(15),
  book_ref        varchar(15) NOT NULL,
  PRIMARY KEY (ticket_no,flight_code)

)
""")



CREATE_DIM_AIRPORT =("""
DROP TABLE IF EXISTS dim_airports;
CREATE TABLE dim_airports
(
    aiport_code  varchar(30) NOT NULL,
    airport_name varchar(50),
    longitude    varchar(35)      ,
    latitude     varchar(35)      ,
    city         varchar(25),
    gps_code     varchar(15),
    home_link    text       ,
    elevation_ft varchar    ,
    PRIMARY KEY (aiport_code)
)
""")

CREATE_DIM_AIRCRAFT = ("""
DROP TABLE IF EXISTS dim_aircrafts;
CREATE TABLE dim_aircrafts
(
  aircraft_code      varchar(10) NOT NULL,
  model              varchar(50),
  type               varchar(50),
  manufacturer       text       ,
  country_name       text       ,
  range              bigint     ,
  total_nbr_seats    varchar        ,
  nbr_seats_business varchar        ,
  nbr_seats_economy  varchar        ,
  nbr_seats_comfort  varchar        ,
  PRIMARY KEY (aircraft_code)
);
""")


CREATE_FACT_FLIGHT = ("""
DROP TABLE IF EXISTS fact_flights;
CREATE TABLE fact_flights
(
  flight_code            varchar(20)  NULL,
  aiport_arrival_code    varchar(30)  NULL,
  airport_departure_code varchar(30)  NULL,
  aircraft_code          varchar(10)  NULL,
  date_arrival      timestamp         NULL,
  date_departure    timestamp         NULL,
  nbr_ticket        int        ,
  total_amount           float      ,
  PRIMARY KEY (flight_code)
)
""")

#======================================EXTRACT FROM THE DATASOURCE (DATABASE: demo)

EXTRACT_DATA_FOR_DIM_DATE_ARRIVAL = ("""
SELECT 
	f.actual_arrival
	,CAST(EXTRACT (YEAR FROM f.actual_arrival) AS INT) AS YEAR
	,CAST(EXTRACT (MONTH FROM f.actual_arrival) AS INT) AS MONTH
	,CAST(EXTRACT (week FROM f.actual_arrival) AS INT) AS week
	,CAST(EXTRACT (DAY FROM f.actual_arrival) AS INT) AS DAY
	,CAST(EXTRACT (HOUR FROM f.actual_arrival) AS INT) AS HOUR
  ,CAST(EXTRACT(MINUTE FROM f.actual_arrival) AS INT) AS MIINUTE
FROM flights f 
WHERE actual_arrival IS NOT NULL 
""")

EXTRACT_DATA_FOR_DIM_DATE_DEPARTURE = ("""
SELECT 
	f.actual_departure
	,CAST(EXTRACT (YEAR FROM f.actual_departure) AS INT) AS YEAR
	,CAST(EXTRACT (MONTH FROM f.actual_departure) AS INT) AS MONTH
	,CAST(EXTRACT (week FROM f.actual_departure) AS INT) AS week
	,CAST(EXTRACT (DAY FROM f.actual_departure) AS INT) AS DAY
	,CAST(EXTRACT (HOUR FROM f.actual_departure) AS INT) AS HOUR
  ,CAST(EXTRACT(MINUTE FROM f.actual_departure) AS INT) AS MIINUTE
FROM flights f 
WHERE actual_departure IS NOT NULL 
""")

EXTRACT_DATA_FOR_DIM_TICKETS = ("""
  SELECT DISTINCT 
    t.ticket_no
    ,tf.flight_id AS flight_code
    ,tf.amount AS amount_ticket
    ,bp.boarding_no
    ,bp.seat_no 
    ,tf.fare_conditions 
    ,t.passenger_id 
    ,t.passenger_name 
    ,REPLACE(CAST(t.contact_data::json->'email' AS TEXT),'"', '') AS email
    ,REPLACE(CAST(t.contact_data::json->'phone' AS TEXT),'"', '') AS phone
    ,t.book_ref 
  FROM tickets t 
  INNER JOIN ticket_flights tf ON t.ticket_no = tf.ticket_no 
  INNER JOIN boarding_passes bp ON tf.ticket_no = bp.ticket_no AND bp.flight_id = tf.flight_id 
  LIMIT 1000
""")

EXTRACT_DATA_FOR_FACT_FLIGHTS = ("""
WITH quantity_ticket AS
(
	SELECT 
		tf2.flight_id 
		,count(tf2.ticket_no) AS qty_ticket
    ,SUM(tf2.amount) AS total_amount
	FROM ticket_flights tf2
	GROUP BY tf2.flight_id 
)
SELECT DISTINCT 
	f.flight_id AS flight_code
	,f.arrival_airport AS airport_arrival_code
	,f.departure_airport AS airport_departure_code
	,f.aircraft_code 
	,f.actual_arrival AS date_arrival
	,f.actual_departure AS date_departure
	,qt.qty_ticket AS nbr_ticket
  ,qt.total_amount
FROM flights f 
INNER JOIN ticket_flights tf ON tf.flight_id = f.flight_id 
INNER JOIN quantity_ticket qt ON qt.flight_id = tf.flight_id
LIMIT 1000
""")


AIRCRAFT_SEATS_DB = ("""
WITH count_seat AS \
( \
    SELECT s.aircraft_code,count(s.seat_no) AS nbr_seat \
    FROM seats s GROUP BY s.aircraft_code \
), \
count_farecondition AS \
( \
    SELECT s.aircraft_code,upper(s.fare_conditions) AS fare_conditions, count(s.seat_no) AS nbr FROM seats s GROUP BY s.aircraft_code,s.fare_conditions \
    ORDER BY s.aircraft_code \
) \
SELECT \
    a.aircraft_code \
    , REPLACE(CAST (a.model::json->'en' AS TEXT), '"','') as model \
    , a.RANGE \
    , c.nbr_seat AS total_nbr_seats \
    , (SELECT nbr FROM count_farecondition WHERE aircraft_code = a.aircraft_code and fare_conditions = upper('Business') ) AS nbr_seats_business \
    , (SELECT nbr FROM count_farecondition WHERE aircraft_code = a.aircraft_code and fare_conditions = upper('economy') ) AS nbr_seats_economy \
    , (SELECT nbr FROM count_farecondition WHERE aircraft_code = a.aircraft_code and fare_conditions = upper('comfort') ) AS nbr_seats_comfort \
FROM aircrafts_data a \
INNER JOIN count_seat c \
ON a.aircraft_code = c.aircraft_code \

  """)


DATA_CHECK_AIRPORTS = ("""
  SELECT * FROM airports_data
""")

DATA_CHECK_AIRCRAFTS = ("""
  SELECT * FROM aircrafts_data
""")


#===========================================================COPY TO REDSHIFT===================================
COPY_DIM_AIRPORTS =("""
  COPY dim_airports (aiport_code,airport_name,longitude,latitude,city, gps_code, home_link, elevation_ft)
  FROM 's3://flightxru-bucket/dim_airports.csv'
  credentials 'aws_iam_role={}'
  IGNOREHEADER 1
  CSV;
""").format(IAM_ROLE)


COPY_DIM_AIRCRAFTS =("""
  COPY dim_aircrafts (aircraft_code,model, type, manufacturer,country_name,range,total_nbr_seats,nbr_seats_business,nbr_seats_economy,nbr_seats_comfort)
  FROM 's3://flightxru-bucket/dim_aircrafts.csv'
  CREDENTIALS 'aws_iam_role={}'
  IGNOREHEADER 1
  CSV;
""").format(IAM_ROLE)


COPY_DIM_DATE_ARRIVALS=("""
COPY dim_date_arrivals(date_arrival, arrival_year, arrival_month, arrival_week,arrival_day, arrival_hour,arrival_minute )
FROM 's3://flightxru-bucket/dim_date_arrivals.csv'
CREDENTIALS 'aws_iam_role={}'
IGNOREHEADER 1
CSV;
""").format(IAM_ROLE)


COPY_DIM_DATE_DEPARTURES=("""
COPY dim_date_departures(date_departure, departure_year, departure_month, departure_week,departure_day, departure_hour,departure_minute )
FROM 's3://flightxru-bucket/dim_date_departures.csv'
CREDENTIALS 'aws_iam_role={}'
IGNOREHEADER 1
CSV;
""").format(IAM_ROLE)

COPY_FACT_FLIGHT=("""
COPY fact_flights(flight_code,aiport_arrival_code,airport_departure_code,aircraft_code,date_arrival,date_departure,nbr_ticket,total_amount )
FROM 's3://flightxru-bucket/fact_flights.csv'
CREDENTIALS 'aws_iam_role={}'
IGNOREHEADER 1
CSV;
""").format(IAM_ROLE)


COPY_DIM_TICKET=("""
COPY dim_tickets(ticket_no,flight_code,amount_ticket,boarding_no,seat_no,fare_conditions,passenger_id,passenger_name,email,phone,book_ref )
FROM 's3://flightxru-bucket/dim_tickets.csv'
CREDENTIALS 'aws_iam_role={}'
IGNOREHEADER 1
CSV;
""").format(IAM_ROLE)








#'aws_access_key_id={}; aws_secret_access_key={}'
#'aws_iam_role={}'
    
list_query_table_to_create = [CREATE_DIM_DATE_ARRIVAL,CREATE_DIM_DATE_DEPARTURE, CREATE_FACT_FLIGHT, CREATE_DIM_AIRPORT, CREATE_DIM_AIRCRAFT, CREATE_DIM_TICKET]
list_query_load_dimensional_tables = [COPY_DIM_AIRPORTS, COPY_DIM_AIRCRAFTS, COPY_DIM_DATE_ARRIVALS, COPY_DIM_DATE_DEPARTURES,COPY_DIM_TICKET, COPY_FACT_FLIGHT,]
