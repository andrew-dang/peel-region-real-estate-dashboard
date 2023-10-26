from pyspark.sql import SparkSession
import random
import psycopg2
from decimal import *

# Instantiate SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("delete-records") \
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
def gen_del_df(conn_props: dict):
    url = conn_props["url"]
    properties = conn_props["properties"]
    table = conn_props["table"]
    
    # read in Postgres db
    df = spark.read.jdbc(url=url, table=table, properties=properties)
    
    # get list of listing_ids 
    listing_ids = df.rdd.map(lambda x: x.listing_id).collect()
    
    # Get a random percentage of listings to delete
    delete_pct = round(random.uniform(0.01, 0.03),2)
    # number of rows to delete 
    n_rows = len(listing_ids)
    n_rows_to_delete = int(round(n_rows * delete_pct))

    delete_idx = random.sample(listing_ids, n_rows_to_delete)
    
    print(f"{int(delete_pct * 100)}% of rows will be deleted")
    print(f"This is equaled to {int(n_rows_to_delete)} rows")
    print(f"Indices to delete: {delete_idx}")
    
    rows = []
    schema = df.schema

    # Get the index of the row in the db to delete
    for value in delete_idx:
        db_index = listing_ids.index(value)

        row = df.collect()[db_index]
        rows.append(row)
    
    delete_df = spark.createDataFrame(rows, schema)
    
    return delete_df

# Functions to delete rows
def delete_row(row, cursor):
    listing_id = row.__getitem__("listing_id")
    
    sql_string = f""" DELETE FROM {table} WHERE listing_id = {listing_id};"""
    
    cursor.execute(sql_string)
    print(f"Listing with ID {listing_id} has been deleted")

def process_partition_delete_rows(partition):
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
        delete_row(row, cursor)
        
    db_conn.commit()
    cursor.close()
    db_conn.close()

# Create a dataframe of lists to delete and delete selected rows from Postgres
delete_df = gen_del_df(conn_props) 
delete_df.rdd.coalesce(100).foreachPartition(process_partition_delete_rows)