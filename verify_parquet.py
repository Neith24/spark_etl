import pandas as pd

# Read the Parquet file
df = pd.read_parquet('data/output.parquet')

print(f"Total records: {len(df)}")
print(f"Columns: {list(df.columns)}")
print("\nFirst 10 rows:")
print(df.head(10))

print("\nSample of favourite products:")
print(df['favourite_product'].value_counts().head(10))

print(f"\nLongest streak statistics:")
print(df['longest_streak'].describe())