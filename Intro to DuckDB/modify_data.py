# %%


import pandas as pd
import glob
# %%

files = glob.glob("Data/nyc_yellow_taxi_2023_01-06/*.parquet")
files
# %%


'''
Read one by one file

'''

count = 1
for file in files:
    df = pd.read_parquet(file)
    print(f"Info for File {count} : {df.info()}")
    count += 1
# %%

'''
Issue with the column name in the first file, airport_fee instead of 'Airport_fee'
'''

df_1 = pd.read_parquet(files[0], engine='fastparquet')
df_1.columns

df_1.rename(columns = {'airport_fee' : "Airport_fee"}, inplace=True)
# %%

'''
Final Check
'''

df_1.info()
# %%

'''
Saving and replacing in the location
'''

df_1.to_parquet("Data/nyc_yellow_taxi_2023_01-06/yellow_tripdata_2023-01.parquet",
                engine = 'fastparquet',
                index = False)
# %%
