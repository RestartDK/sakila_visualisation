import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import find_dotenv, load_dotenv

# Load environment variables
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
TOKEN = os.environ.get("TOKEN")

org = "ie"
url = "http://localhost:8086"

client = influxdb_client.InfluxDBClient(url=url, token=TOKEN, org=org)

bucket="sakila"

write_api = client.write_api(write_options=SYNCHRONOUS)
   
# Initialize SQLAlchemy engine for MySQL
engine = create_engine('mysql://root:@localhost/sakila')

# Your SQL query
query = """
SELECT customer_id, count(*) as num_rentals 
FROM rental 
GROUP BY customer_id 
ORDER BY num_rentals desc 
LIMIT 5; 
"""
data = pd.read_sql(query, engine)

points = []
# Create a point for each data row
for row in data.itertuples(index=False):
    point = Point("rental").tag("customer_id", row.customer_id).field("num_rentals", row.num_rentals)
    points.append(point)

# Write points to InfluxDB
write_api.write(bucket=bucket, record=points, write_precision=WritePrecision.NS)

# Close the client
client.close()


