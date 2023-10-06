from pyarrow import csv
import pyarrow.parquet as pq
import pandas as pd
import polars as pl
import datetime
from polars.type_aliases import SchemaDict

print(datetime.datetime.now())
fn = 'mavs3.csv'
cn = [
    'date_mm', 'date_dd', 'date_yy', 'time_hh', 'time_mm', 'time_ss', 'va',
    'vb', 'vc', 'vd', 'u', 'v', 'w', 'p', 't', 'mx', 'my', 'pitch', 'roll'
]
sel_fields = [
    'date_time', 'p', 't', 'mx', 'my', 'pitch', 'roll'
]

def test_func(row):
    dmm = row['date_mm']
    ddd = row['date_dd']
    dyy = row['date_yy']
    thh = row['time_hh']
    tmm = row['time_mm']
    tss = row['time_ss']
    return pd.to_datetime(f"{int(dyy):02d}-{int(dmm):02d}-{int(ddd):02d} {int(thh):02d}:{int(tmm):02d}:{tss:06.3f}", format="%y-%m-%d %H:%M:%S.%f")

mavs_schema: SchemaDict = {
    "date_mm": pl.Int64,
    "date_dd": pl.Int64,    
    "date_yy": pl.Int64,
    "time_hh": pl.Float64,
    "time_mm": pl.Float64,    
    "time_ss": pl.Float64,
    "va": pl.Int64,
    "vb": pl.Int64,
    "vc": pl.Int64,
    "vd": pl.Int64,
    "u": pl.Float64,
    "v": pl.Float64,
    "w": pl.Float64,
    "p": pl.Float64,
    "t": pl.Float64,
    "mx": pl.Float64,
    "my": pl.Float64,
    "pitch": pl.Float64,
    "roll": pl.Float64,
}

df = pl.scan_csv(fn, has_header=False, new_columns=cn, separator=" ", schema=mavs_schema).select(cn)

# df = df.with_columns(
#     (
#         df.select(cn).map_batches(
#             lambda x: test_func(x['date_mm'], x['date_dd'], x['date_yy'], x['time_hh'], x['time_mm'], x['time_ss'])
#         )
#     ).alias("date_time")
# )


df2 = df.select([
    pl.struct(cn).apply(test_func).alias("date_time")
] + cn).collect()
print(df2.head())
# print(df.show())
print(datetime.datetime.now())
# Select all columns including the new 'date_time' column
df3 = df2.select(sel_fields)
print(df3.head(10))
df3.write_csv("mavs3_new_columns.csv")
print(datetime.datetime.now())