"""One-off script to inspect Parquet file from S3: schema and sample data."""
import sys
sys.path.insert(0, "/home/ec2-user/dsc291_team4-1")

from pivot_and_bootstrap.pivot_all_files import _open_parquet_file
from pivot_and_bootstrap.pivot_utils import find_pickup_datetime_col, find_pickup_location_col, find_pickup_lat_lon_cols
import pyarrow.parquet as pq

path = "s3://dsc291-ucsd/taxi/Dataset/2009/yellow_taxi/yellow_tripdata_2009-01.parquet"
print("Opening:", path)
pf, f = _open_parquet_file(path)
try:
    schema = getattr(pf, "schema_arrow", None) or getattr(pf, "schema")
    print("\n=== SCHEMA ===")
    print("Column names:", schema.names if hasattr(schema, "names") else getattr(schema, "column_names", "?"))
    for i in range(len(schema)):
        field = schema.field(i)
        print(f"  {field.name}: {field.type}")
    dt_col = find_pickup_datetime_col(schema)
    loc_col = find_pickup_location_col(schema)
    lat_col, lon_col = find_pickup_lat_lon_cols(schema)
    print("\nDetected dt_col:", dt_col, "loc_col:", loc_col, "lat_lon:", (lat_col, lon_col))
    print("\n=== FIRST BATCH (to_pandas default) ===")
    batch = next(pf.iter_batches(batch_size=1000))
    df_default = batch.to_pandas()
    print("Columns:", list(df_default.columns))
    print("dtypes:\n", df_default.dtypes)
    if dt_col and dt_col in df_default.columns:
        ser = df_default[dt_col]
        print(f"\n{dt_col}: dtype={ser.dtype}, nulls={ser.isna().sum()}, count={len(ser)}")
        print("First 5 values:", ser.head(5).tolist())
    if loc_col and loc_col in df_default.columns:
        ser = df_default[loc_col]
        print(f"\n{loc_col}: dtype={ser.dtype}, first 5:", ser.head(5).tolist())
    print("\n=== FIRST BATCH (to_pandas timestamp_as_object=True) ===")
    pf2, f2 = _open_parquet_file(path)
    batch2 = next(pf2.iter_batches(batch_size=1000))
    df_obj = batch2.to_pandas(timestamp_as_object=True)
    if f2:
        f2.close()
    if dt_col and dt_col in df_obj.columns:
        ser = df_obj[dt_col]
        print(f"{dt_col}: dtype={ser.dtype}, nulls={ser.isna().sum()}, first 5:", ser.head(5).tolist())
finally:
    if f:
        f.close()
print("\nDone.")
