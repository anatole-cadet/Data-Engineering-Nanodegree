from copy import Error
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import queries.sql_queries as sq
import configparser
import psycopg2
from io import StringIO
import boto3

config = configparser.ConfigParser()
config.read("configuration/flightxru.cfg")

spark = SparkSession \
    .builder \
    .appName("ETL -  flightxru") \
    .getOrCreate()


REGION="us-west-2"
KEY=config.get("AWS","KEY")
SECRET=config.get("AWS","SECRET")

s3_client = boto3.client('s3', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)

bucket_name = "flightxru-bucket"






def extract_data_create_dim_aircrafts_to_s3(cur_pg):
    """
    Extract data on three aircraft csv file, and complete information of aircraft of the database and make a csv file
    load to s3.

    Args:
        cur_pg (Str): The cursor for querying data on the database demo
    """
    # Get the data of ListOfAircraftModel.csv
    aircraft_model_csv = "data/ListOfAircraftModel.csv"
    aircraft_model_csv = spark.read.csv(aircraft_model_csv, header=True)
    aircraft_model_csv = aircraft_model_csv.select(
        col("IATACode").alias("aircraft_code"), 
        col("Aircraft_Manufacturer").alias("aircraft_manufacturer_code"), col("Type").alias("type"), col("Model").alias("model")
    )

    # For getting the code country, we use the Aircraft_Manufacturer.csv
    aircraft_manufacturer_csv = spark.read.csv("data/Aircraft_Manufacturer.csv", header=True)
    aircraft_manufacturer_csv = aircraft_manufacturer_csv.select(col("objectId").alias("aircraft_manufacturer_code"), col("Manufacturer").alias("manufacturer") ,col("Country").alias("country_code"))

    # Le's use the Aircraft_Manufacturer_by_Country.csv file to get their country name
    aircraft_manufacturer_country = spark.read.csv("data/Aircraft_Manufacturer_by_Country.csv", header=True)
    aircraft_manufacturer_country = aircraft_manufacturer_country.select(col("Country").alias("country_code"), col("CountryName").alias("country_name"))

    #Let's create one dataframe for aircraft model by taking others data.
    aircraft_data = aircraft_model_csv.join(aircraft_manufacturer_csv, aircraft_model_csv["aircraft_manufacturer_code"] == aircraft_manufacturer_csv["aircraft_manufacturer_code"],"left")
    aircraft_data =aircraft_data.join(aircraft_manufacturer_country, aircraft_data["country_code"] == aircraft_manufacturer_country["country_code"], "left")
    #aircraft_data.select(["aircraft_code", "type", "manufacturer", "country_name"]).show()
    #aircraft_data.count()
    cur_pg.execute(sq.AIRCRAFT_SEATS_DB)
    aircraft_data_db = cur_pg.fetchall()
    df = pd.DataFrame(aircraft_data_db, columns=["aircraft_code","model", "range", "total_nbr_seats", "nbr_seats_business", "nbr_seats_economy", "nbr_seats_comfort"])
    aircraft_data_db = spark.createDataFrame(df)
    #aircraft_data_db.show()
    aircraft_data_db.createOrReplaceTempView("aircraft_data_final_db")
    aircraft_data.createOrReplaceTempView("aircraft_data_final_csv")

    dim_aircrafts = spark.sql("""
    SELECT DISTINCT
        a.aircraft_code
        ,a.model
        ,b.type
        ,b.manufacturer
        ,b.country_name
        ,a.range
        ,a.total_nbr_seats
        ,a.nbr_seats_business
        ,a.nbr_seats_economy
        ,a.nbr_seats_comfort
    FROM aircraft_data_final_db a
    INNER JOIN aircraft_data_final_csv b
    ON a.aircraft_code = b.aircraft_code
    """)
    csv_aircrafts_temp = StringIO()
    dim_aircrafts.toPandas().to_csv(csv_aircrafts_temp,index=False)
    s3_client.put_object(Body=csv_aircrafts_temp.getvalue(), Bucket=bucket_name, Key="dim_aircrafts.csv")


def extract_data_create_dim_airports_to_s3(cur_pg):
    """
    Extract data on airports csv file, and complete information of airports of the database and make a csv file
    load to s3.

    Args:
        cur_pg (Str): The cursor for querying data on the database demo.
    """
    # Get data from the csv
    airport_file = "data/list-of-airports-in-russian-federation-hxl-tags-1.csv"
    data_airport = spark.read.csv(airport_file, header=True)
    data_airport_csv = data_airport.select(col('iata_code').alias('airport_code'),'elevation_ft','gps_code','home_link')
    
    # Get data from the table airport
    cur_pg.execute("SELECT airport_code, airport_name::json->'en' as airport_name, \
                                    city::json->'en' as city, coordinates[0] as latitude, coordinates[1] as longitude FROM airports_data;")

    data_airport_db = cur_pg.fetchall()
    data_airport_db = pd.DataFrame(data_airport_db,columns=["airport_code","airport_name","city","latitude","longitude"])
    data_airport_db = spark.createDataFrame(data_airport_db)

    data_airport_csv.createOrReplaceTempView("aiport_csv")
    data_airport_db.createOrReplaceTempView("airport_db")
    dim_airports = spark.sql("""
                                SELECT 
                                    a.airport_code,
                                    a.airport_name,
                                    a.city,
                                    a.latitude,
                                    a.longitude,
                                    b.elevation_ft,
                                    b.gps_code,
                                    b.home_link
                                FROM airport_db a
                                LEFT JOIN aiport_csv b
                                ON a.airport_code = b.airport_code
                            """)
    # Copy csv's file dim_airport to s3
    csv_airports_temp = StringIO()
    dim_airports.toPandas().to_csv(csv_airports_temp,index=False)
    s3_client.put_object(Body=csv_airports_temp.getvalue(), Bucket=bucket_name, Key="dim_airports.csv")
    
    
def extract_data_create_dim_date_to_s3(cur_pg,conn_pg):
    """Extract data and create dim_date_arrivals, dim_date_departure, from flight's tbale of the database
    and load as csv to s3.

    Args:
        cur_pg (Str): The cursor for querying data on the database demo
        conn_pg (Str): The connection string to the database postgresql
    """
    keys_csv = ["dim_date_arrivals.csv","dim_date_departures.csv"]
    queries = [sq.EXTRACT_DATA_FOR_DIM_DATE_ARRIVAL, sq.EXTRACT_DATA_FOR_DIM_DATE_DEPARTURE]
    arrival_departure_cols     = [["date_arrival","year","month","week","day","hour","minute"], 
                                ["date_departure","year","month","week","day","hour","minute"]]

    for i in range(0,len(queries)):
        try:
            cur_pg.execute(queries[i])
            conn_pg.commit()
            data = pd.DataFrame(cur_pg.fetchall(), columns=arrival_departure_cols[i])
            data = spark.createDataFrame(data)
            csv_date_temp = StringIO()
            data.toPandas().to_csv(csv_date_temp,index=False)
            s3_client.put_object(Body=csv_date_temp.getvalue(), Bucket=bucket_name, Key=keys_csv[i])
            csv_date_temp = ""
        except psycopg2.Error as e:
            print(e)


def extract_data_create_dim_tickets_flights_to_s3(cur_pg,conn_pg):
    """Extract data and create dim_tickets and fact_flight from tickets and flight's tbale of the database demo.
    And after load them as csv to s3.

    Args:
        cur_pg (Str): The cursor for querying data on the database demo
    """
    keys_csv = ["dim_tickets.csv","fact_flights.csv"]
    queries = [sq.EXTRACT_DATA_FOR_DIM_TICKETS,sq.EXTRACT_DATA_FOR_FACT_FLIGHTS]
    columns_table = [["ticket_no","flight_code","amount_ticket","boarding_no","seat_no","fare_conditions","passenger_id","passenger_name","email","phone","book_ref"],
                      ["flight_code","airport_arrival_code","airport_departure_code","aircraft_code","date_arrival","date_departure","nbr_ticket","total_amount"]
                    ]

    for i in range(0,len(queries)):
        try:
            cur_pg.execute(queries[i])
            conn_pg.commit()
            data = pd.DataFrame(cur_pg.fetchall(), columns=columns_table[i])
            data = spark.createDataFrame(data)
            csv_date_temp = StringIO()
            data.toPandas().to_csv(csv_date_temp,index=False)
            s3_client.put_object(Body=csv_date_temp.getvalue(), Bucket=bucket_name, Key=keys_csv[i])
            csv_date_temp = ""
        except psycopg2.Error as e:
            print(e)


def load_data_to_dimensional_tables(cur_aws,conn_aws):
    try:
        for query in sq.list_query_load_dimensional_tables:
            cur_aws.execute(query)
            conn_aws.commit()
    except psycopg2.Error as e:
        print(e)


def quality_check(cur_aws,conn_aws,cur_pg,conn_pg):
    """
    This function is for checking the quality of data.

    Args:
        cur_aws ([type])    : The cursor for querying data on the database on redshift
        conn_aws ([type])   : Connection string for redshift
        cur_pg ([type])     : The cursor for querying data on the database demo
        conn_pg ([type])    : Connection string for the database demo
    """
    tables_redshift = ["dim_tickets","fact_flights","dim_date_arrivals","dim_date_departures","dim_aircrafts","dim_airports"]
    queries = [sq.EXTRACT_DATA_FOR_DIM_TICKETS,sq.EXTRACT_DATA_FOR_FACT_FLIGHTS,
                sq.EXTRACT_DATA_FOR_DIM_DATE_ARRIVAL, sq.EXTRACT_DATA_FOR_DIM_DATE_DEPARTURE, sq.DATA_CHECK_AIRCRAFTS, sq.DATA_CHECK_AIRPORTS]
    try:
        for q in range(0, len(queries)):
            cur_pg.execute(f"SELECT COUNT(*) FROM ({queries[q]}) AS result")
            conn_pg.commit()
            data_source = cur_pg.fetchone()[0]

            cur_aws.execute(f"SELECT COUNT(*) FROM {tables_redshift[q]}")
            conn_aws.commit()
            data_target = cur_aws.fetchone()[0]
            if not(data_target == data_source):
                print(f"The target table {tables_redshift[q]} is not valid")

    except psycopg2.Error as e:
        print(e)

#main(cur_aws,conn_aws,
def main(cur_aws,conn_aws,cur_pg,conn_pg):
    extract_data_create_dim_airports_to_s3(cur_pg)
    extract_data_create_dim_aircrafts_to_s3(cur_pg)
    extract_data_create_dim_date_to_s3(cur_pg,conn_pg)
    extract_data_create_dim_tickets_flights_to_s3(cur_pg,conn_pg)
    load_data_to_dimensional_tables(cur_aws,conn_aws)
    quality_check(cur_aws,conn_aws,cur_pg,conn_pg)
    
    conn_aws.close()
    conn_pg.close()
    
    #data = "/home/anatole/Documents/PROJECTS/flightxru/test/dim_date_arrivals.csv"
    #df = spark.read.csv(data, header=True)
    #print(df.dtypes)
    #df = pd.read_csv(data)
    #print(df.dtypes)




if __name__ == "__main__":

    flightxru_config = configparser.ConfigParser()
    flightxru_config.read("configuration/flightxru.cfg")

    conn_aws = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*flightxru_config["CLUSTER"].values()))
    cur_aws= conn_aws.cursor()

    conn_pg = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*flightxru_config["POSTGRES"].values()))
    #f"postgresql://{config.get('POSTGRES', 'DB_USER')}:{config.get('POSTGRES', 'DB_PASSWORD')}@localhost:5432/{config.get('POSTGRES', 'DB_NAME')}"
    cur_pg= conn_pg.cursor()
    #print(conn_pg)

    main(cur_aws,conn_aws, cur_pg,conn_pg)
    #main(cur_pg,conn_pg)