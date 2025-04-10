from sqlalchemy import create_engine
import pandas as pd

# Read CSV
df = pd.read_csv("data_stream.csv")

# Print the schema before conversion
print(pd.io.sql.get_schema(df, name="yellow_taxi_data"))

# Convert datetime column properly
df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
engine = create_engine('postgresql://floo:Agents1234@floo3.postgres.database.azure.com:5432/postgres')
# Print schema again (now it will still show TEXT for datetime columns unless you specify dtypes)
print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con = engine))
df.to_sql("yellow_taxi_data", engine, if_exists="replace", index=False)



# Optional: define engine and load to SQL
# engine = create_engine('postgresql://floo:Agents1234@floo3.postgres.database.azure.com:5432/postgres')

