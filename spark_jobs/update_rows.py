from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
import random
import psycopg2
from decimal import *
import numpy as np


# Instantiate SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("update-records") \
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

# Helper functions
# Generate spark df of rows to delete 
def gen_update_df(conn_props: dict):
    url = conn_props["url"]
    properties = conn_props["properties"]
    table = conn_props["table"]
    
    # read in Postgres db
    df = spark.read.jdbc(url=url, table=table, properties=properties)
    
    # get list of listing_ids 
    listing_ids = df.rdd.map(lambda x: x.listing_id).collect()
    
    # Get a random percentage of listings to delete
    update_pct = round(random.uniform(0.01, 0.03),2)
    # number of rows to delete 
    n_rows = len(listing_ids)
    n_rows_to_update = int(round(n_rows * update_pct))

    update_idx = random.sample(listing_ids, n_rows_to_update)
    
    print(f"{int(update_pct * 100)}% of rows will be updated")
    print(f"This is equaled to {int(n_rows_to_update)} rows")
    print(f"Indices to updated: {update_idx}")
    
    rows = []
    schema = df.schema

    # Get the index of the row in the db to delete
    for value in update_idx:
        db_index = listing_ids.index(value)
        row = df.collect()[db_index]
        
        # change the listing price
        listing_price = row.__getitem__("listing_price")
        print(f"\nOriginal price: {listing_price}")
        
        # Create 2 ranges to avoid multiplying price by 1
        lower_range = np.arange(0.97,0.99,0.01)
        upper_range = np.arange(1.01,1.03,0.01)

        # If random.random() is less than 0.5, lower price
        if random.random() < 0.5:
            print("Decreasing the price...")
            lo = lower_range[0]
            hi = lower_range[-1]
        else:
            print("Increasing price...")
            lo = upper_range[0]
            hi = upper_range[-1]

        # Get multiplier for new listing price
        multiplier = round(random.uniform(lo, hi), 2)
        
        new_price = round(listing_price * Decimal(multiplier),2)
        print(f"Updated price: {new_price}")
        
        # set new listing price
        d = row.asDict()
        d.update({'listing_price': new_price})
        updated_row = Row(**d)
        
        # append updated row
        rows.append(updated_row)
    
    update_df = spark.createDataFrame(rows, schema)
    update_df = update_df.withColumn("listing_price", F.col("listing_price").cast(FloatType()))
    
    return update_df

def update_row(row, cursor):
    """
    Upsert list price 
    """
    listing_id = row.__getitem__("listing_id")
    listing_price = row.__getitem__("listing_price")

    sql_string = f"""
        INSERT INTO listings(listing_id, listing_price)
        VALUES({listing_id}, {listing_price})
        ON CONFLICT ON CONSTRAINT unique_listing_id DO UPDATE
        SET listing_price = EXCLUDED.listing_price;
    """
    
    cursor.execute(sql_string)
    print(f"Listing with ID {listing_id} has been updated")

def process_partition_update_rows(partition):
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
        update_row(row, cursor)
        
    db_conn.commit()
    cursor.close()
    db_conn.close()

# Create dataframe and delete rows 
update_df = gen_update_df(conn_props)
update_df.rdd.coalesce(100).foreachPartition(process_partition_update_rows)