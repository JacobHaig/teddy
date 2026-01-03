import pandas as pd

df = pd.read_csv("./data/addresses.csv")

df.to_parquet("./data/addresses_snappy.parquet")
df.to_parquet("./data/addresses.parquet", compression=None)
