# %%

import pandas as pd 
import duckdb
import glob
import time

# %%

## Connecting DuckDB
conn = duckdb.connect()

# %%

# df = pd.read_csv("Data/Sales_Product_Combined.csv")
# print(df.head(10))

'''
Reading data using python
'''

cur_time = time.time()
df_combined = pd.concat([pd.read_parquet(f, engine = "fastparquet") for f in glob.glob("Data/nyc_yellow_taxi_2023_01-06/*.parquet")])
print("Time Taken :", time.time() - cur_time)
print(df_combined.head(10))

# %%

'''
Reading data using DuckDB
'''

# df = conn.execute("""
#     SELECT *
#     FROM 'Data/*.csv'
# """).df()

# print(df)

cur_time = time.time()
df_duckdb = conn.execute("""
    SELECT * 
    FROM read_parquet("Data/nyc_yellow_taxi_2023_01-06/*.parquet")
    LIMIT 10
""").fetch_df()

print(f"Time Taken : {(time.time() - cur_time)}")
print(df_duckdb.head(10))


# %%

conn.register("df_duckdb_view", df_duckdb)
conn.execute("DESCRIBE df_duckdb_view").df()

# %%

# print(df_combined.shape, df_duckdb.shape)
# %%

'''
Read the whole data
'''

df = conn.execute("""
        SELECT * 
        FROM read_parquet("Data/nyc_yellow_taxi_2023_01-06/*.parquet")
""").df()

"""
Create table/view for further reference
"""

conn.register("df_duckdb_view", df)
conn.execute("DESCRIBE df_duckdb_view").df()

# %%

conn.execute('''
     SELECT COUNT(*) AS total_rows
     FROM df_duckdb_view
''').df()
# %%

"""
Removing NULL Values
"""

df.isnull().sum()

## Dropping the NUll Values
df = df.dropna(how = "any")
# %%

'''
Final Checking
'''

df.shape
# %%

'''
CREATE A TABLE IN DUCKDB
'''

conn.execute("""
    CREATE OR REPLACE TABLE taxi_rides AS 
    SELECT  
            VendorID::BIGINT AS rider_id,
            tpep_pickup_datetime::TIMESTAMP AS pickup_time,
            tpep_dropoff_datetime::TIMESTAMP AS drop_time,
            passenger_count::INT AS passengers,
            trip_distance::DOUBLE AS distance,
            RatecodeID::INT AS ratecodeid,
            store_and_fwd_flag::VARCHAR AS flags,
            PULocationID::INT AS pulocation_id,
            DOLocationID::INT AS dolocation_id,
            payment_type::INT AS payment_type,
            fare_amount::NUMERIC AS fare_amount,
            extra::NUMERIC AS extras,
            mta_tax::NUMERIC AS mta_tax,
            tip_amount::NUMERIC AS tips,
            tolls_amount::NUMERIC AS toll_amt,
            improvement_surcharge::NUMERIC AS improvement_surcharge,
            total_amount::NUMERIC AS total_amt,
            congestion_surcharge::NUMERIC AS congestion_surcharge,
            Airport_fee::NUMERIC AS airport_fee
    FROM df""")

# %%

'''
Check the table info
'''

conn.execute("From taxi_rides LIMIT 100").df()
# %%


'''
Check Specific Columns
'''

conn.execute("""
             SELECT * 
                EXCLUDE(extras, mta_tax, tips, improvement_surcharge, total_amt, congestion_surcharge, airport_fee)
             FROM taxi_rides
             LIMIT 100   
            """).df()
# %%

'''
Minimum of all interested columns only
'''

conn.execute("""
             SELECT 
                MIN(COLUMNS(* EXCLUDE(extras, mta_tax, tips, improvement_surcharge, total_amt, congestion_surcharge, airport_fee))),
                MAX(COLUMNS(* EXCLUDE(extras, mta_tax, tips, improvement_surcharge, total_amt, congestion_surcharge, airport_fee)))
             FROM taxi_rides 
            """).df()

# %%

conn.execute("""
     SELECT DISTINCT rider_id
     FROM taxi_rides
""").df()
# %%

'''
Aggregated View
'''

conn.execute("""
    CREATE OR REPLACE VIEW aggregated_rides_view AS
    SELECT  rider_id,
            pickup_time::DATE AS pickup_date,
            payment_type,
            SUM(passengers) AS total_passengers,
            SUM(fare_amount) AS total_fare
    FROM taxi_rides
    WHERE fare_amount >= 0
    GROUP BY ALL
    ORDER BY pickup_date, total_fare DESC, total_passengers DESC
""")
# %%

'''
Checking Aggregated Data
'''

conn.execute("""FROM aggregated_rides_view""").df()

# %%

'''
Total Fare Collected by each payment type
'''

conn.execute("""
     SELECT payment_type,
            ROUND(SUM(total_fare)/1e6, 2) AS "sum_fares('M')"
     FROM aggregated_rides_view
     GROUP BY payment_type 
""").df()
# %%

