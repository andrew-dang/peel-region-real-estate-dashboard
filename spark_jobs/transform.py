import argparse

import psycopg2
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from random import randint
import random
from pyspark.sql.types import IntegerType, FloatType, DateType


# Start SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

# Config
user = "db_username"
host = "db_host"
driver = "jdbc_driver"
table = "table_name>"
db_name = "db_name"

url = "jdbc_connection_string"
properties = {
    "driver": driver
}

conn_props = {
    "url": url, 
    "properties": properties,
    "table": table
}

# Create dictionary for psycopg2 connection
psycopg2_props = {
    "host": "host_name",
    "user": user,
    "password": "db_pass",
    "database": db_name,
    "port": "port_number"
}

# broadcast connection to each partition
sc = spark.sparkContext
brConnect = sc.broadcast(psycopg2_props)

# helper function 
def set_price_per_sqft(property_type):
    random.seed(88)
    if property_type == "semi-detached":
        return randint(475,505)
    elif property_type == "single-detached":
        return randint(500,525)
    else:
        return randint(460,490)
    
set_price_udf = F.udf(set_price_per_sqft, returnType=IntegerType())

def insert_row(row, cursor):
    """
    Insert new rows.  
    """
    address = row.__getitem__("address")
    age = row.__getitem__("age")
    city = row.__getitem__("city")
    finished_basement = row.__getitem__("finished_basement")
    list_date = row.__getitem__("list_date")
    number_bedrooms = row.__getitem__("number_bedrooms")
    property_type = row.__getitem__("property_type")
    sqft = row.__getitem__("sqft")
    street_name = row.__getitem__("street_name")
    street_number = row.__getitem__("street_number")
    listing_price = row.__getitem__("listing_price")
    sqft_range = row.__getitem__("sqft_range")
    age_range = row.__getitem__("age_range")
    
    sql_string = f"""
        INSERT INTO listings (address, age, city, finished_basement, list_date, number_bedrooms, property_type, sqft, street_name, street_number, listing_price, sqft_range, age_range)
        VALUES ($${address}$$, {age}, '{city}', {finished_basement}, '{list_date}', {number_bedrooms}, '{property_type}', {sqft}, $${street_name}$$, {street_number}, {listing_price}, '{sqft_range}', '{age_range}')
        ON CONFLICT ON CONSTRAINT unique_address 
        DO NOTHING;
    """
    
    cursor.execute(sql_string)

def process_partition_insert_rows(partition):
    # Get broadcasted values
    connection_properties = brConnect.value
    
    # Extract values from broadcasted variables 
    database = connection_properties.get("database")
    user = connection_properties.get("user")
    password = connection_properties.get("password")
    host = connection_properties.get("host")
    port = connection_properties.get("port")
    
    db_conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port
    )
            
    cursor = db_conn.cursor()
    
    for row in partition:
        insert_row(row, cursor)
        
    db_conn.commit()
    cursor.close()
    db_conn.close()

# add price per square foot and then multiply by sqft to get listing_price 
# write to GCS and Postgres
def main(params) -> None:
    input_file = params.input_file
    output_file = params.output_file
    
    # Read in json 
    df = spark.read.json(input_file)
    
    # Concatenate street number, street name, and city to get address
    df = df.withColumn("address", F.concat(F.col("street_number"), F.lit(" "), F.col("street_name"), F.lit(", "), F.col("city")))

    # add price per sqft
    df = df.withColumn("price_per_sqft", set_price_udf(F.col("property_type")))
    df = df.withColumn("listing_price", F.col("sqft") * F.col("price_per_sqft"))

    # bin age and sqft
    df = df.withColumn("sqft_range",
                   F.when(F.col("sqft") < 1500, F.lit("0-1499"))
                   .when(F.col("sqft") < 2000, F.lit("1500-1999"))      
                   .otherwise(F.lit("2000-2499"))) \
           .withColumn("age_range", 
                   F.when(F.col("age") < 6, F.lit("0-5"))
                   .when(F.col("age") < 11, F.lit("6-10"))
                   .when(F.col("age") < 16, F.lit("11-15"))
                   .when(F.col("age") < 21, F.lit("16-20"))
                   .when(F.col("age") < 26, F.lit("21-25"))
                   .otherwise(F.lit("26+")))

    
    # Change data types 
    df = df \
            .withColumn("list_date", F.col("list_date").cast(DateType())) \
            .withColumn("listing_price", F.col("listing_price").cast(FloatType())) \
            .withColumn("number_bedrooms", F.col("number_bedrooms").cast(IntegerType())) \
            .withColumn("sqft", F.col("sqft").cast(IntegerType())) \
            .withColumn("street_number", F.col("street_number").cast(IntegerType())) \
            .withColumn("age", F.col("age").cast(IntegerType()))

    # drop price_per_sqft from final df
    df_final = df.drop("price_per_sqft", "type")
    
    # Write df to Cloud SQL using insert statement
    df_final.rdd.repartition(8).foreachPartition(process_partition_insert_rows)
    print("Finished writing to Postgres database...")

    # Write df to GCS 
    df_final.write.parquet(output_file)


if __name__ == "__main__": 
    parser = argparse.ArgumentParser(description="Add listing price to existing listings")

    parser.add_argument('--input_file', required=True, help="Path to listings without pricing")
    parser.add_argument('--output_file', required=True, help="Save location for listings with pricing added")

    args = parser.parse_args()

    main(args)

    