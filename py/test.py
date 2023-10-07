# from pyarrow import csv
# import pyarrow.parquet as pq
# import pandas as pd
# import polars as pl
# import re
# import datetime
# from polars.type_aliases import SchemaDict
# # # Define the input and output file paths
# input_file_path = "mavs.csv"
# output_file_path = "mavs3.csv"

# # with open(input_file_path, "r") as input_file:
# #   # Read the content of the input file
# #   input_content = input_file.read()

# # # Perform the replacement for multiple consecutive spaces using regular expressions
# # output_content = re.sub(r' +', ' ', input_content)

# # # Open the output file for writing
# # with open(output_file_path, "w") as output_file:
# #   # Write the modified content to the output file
# #   output_file.write(output_content)
# ctime = datetime.datetime.now()
# print(datetime.datetime.now())
# with open(input_file_path, "r") as input_file:
#     with open(output_file_path, "w") as output_file:
#         output_file.write(re.sub(r' +', ' ', input_file.read()))

# print(datetime.datetime.now())
# fn = 'mavs3.csv'
# cn = [
#     'date_mm', 'date_dd', 'date_yy', 'time_hh', 'time_mm', 'time_ss', 'va',
#     'vb', 'vc', 'vd', 'u', 'v', 'w', 'p', 't', 'mx', 'my', 'pitch', 'roll'
# ]
# read_options = csv.ReadOptions(autogenerate_column_names=False,
#                                column_names=cn)
# parse_options = csv.ParseOptions(delimiter=" ")
# table = csv.read_csv(fn,
#                      read_options=read_options,
#                      parse_options=parse_options)
# print(len(table))
# print(datetime.datetime.now())

# # q = (pl.scan_csv(fn, has_header=False, new_columns=cn, separator=" ").filter(
# #     pl.col("p") > 0).group_by("p").agg(pl.all().sum()))

# # df = q.collect()

# def test_func(dmm, ddd, dyy, thh, tmm, tss):
#     # dmm, ddd, dyy, thh, tmm, tss, va, vb, vc, vd, u, v, w, p, t, mx, my, pitch, roll = list_of_fields
#     return pd.to_datetime(f"{int(dmm.item()):02d}-{int(ddd.item()):02d}-{int(dyy.item()):02d} {int(thh.item()):02d}:{int(tmm.item()):02d}:{tss.item()}", format="%y-%m-%d %H:%M:%S.%f")    

# mavs_schema: SchemaDict = {}

# mavs_schema = {
#     "date_mm": pl.Int64,
#     "date_dd": pl.Int64,    
#     "date_yy": pl.Int64,
#     "time_hh": pl.Float64,
#     "time_mm": pl.Float64,    
#     "time_ss": pl.Float64,
#     "va": pl.Int64,
#     "vb": pl.Int64,
#     "vc": pl.Int64,
#     "vd": pl.Int64,
#     "u": pl.Float64,
#     "v": pl.Float64,
#     "w": pl.Float64,
#     "p": pl.Float64,
#     "t": pl.Float64,
#     "mx": pl.Float64,
#     "my": pl.Float64,
#     "pitch": pl.Float64,
#     "roll": pl.Float64,
# }

# df = pl.scan_csv(fn, has_header=False, new_columns=cn, separator=" ", schema=mavs_schema).select(cn).collect()
# # Define a calculation for the new column
# # new_column = pd.to_datetime(
# #     f"{int(df['date_yy']):02d}-{int(df['date_mm']):02d}-{int(df['date_dd']):02d} {int(df['time_hh']):02d}:{int(df['time_mm']):02d}:{df['time_ss']}",
# #     format="%y-%m-%d %H:%M:%S.%f")

# # # Add the calculated column to the DataFrame
# # df = df.with_column(new_column.alias("date_time"))


# df.with_columns(
#     (
#         df.struct(cn).map_batches(
#             # x.struct.field("date_mm"), x.struct.field("date_dd"), x.struct.field("date_yy"),
#             # lambda x: test_func(x.struct.fields)
#             lambda x: test_func(x.struct.fields[0], x.struct.field("date_dd"), x.struct.field("date_yy"), x.struct.field("time_hh"), x.struct.field("time_mm"), x.struct.field("time_ss"))
#         )
#     ).alias("date_time")
# )

# print(datetime.datetime.now())
# #df = table.to_pandas()
# print(df.head())
# exit()
# # pd.to_datetime(f"{row['date_yy']:02d}-{row['date_mm']:02d}-{row['date_dd']:02d} {row['time_hh']:02d}:row['time_mm']:02d}:{int(row['time_ss'])}",format="%y-%m-%d %H:%M:%S.%f")
# df['datetime'] = df.apply(lambda row: pd.to_datetime(
#     f"{int(row['date_yy']):02d}-{int(row['date_mm']):02d}-{int(row['date_dd']):02d} {int(row['time_hh']):02d}:{int(row['time_mm']):02d}:{row['time_ss']}",
#     format="%y-%m-%d %H:%M:%S.%f"),
#                           axis=1)

# print(df.head())
import polars as pl
import datetime
import pandas as pd

fn = 'mavs3.csv'
cn = [
    'date_mm', 'date_dd', 'date_yy', 'time_hh', 'time_mm', 'time_ss', 'va',
    'vb', 'vc', 'vd', 'u', 'v', 'w', 'p', 't', 'mx', 'my', 'pitch', 'roll'
]
sel_fields = [
    #'date_time', 
    'p', 't', 'mx', 'my', 'pitch', 'roll'
]

def test_func(row):
    dmm = row['date_mm']
    ddd = row['date_dd']
    dyy = row['date_yy']
    thh = row['time_hh']
    tmm = row['time_mm']
    tss = row['time_ss']
    return pd.to_datetime(f"{int(dyy):02d}-{int(dmm):02d}-{int(ddd):02d} {int(thh):02d}:{int(tmm):02d}:{tss:06.3f}", format="%y-%m-%d %H:%M:%S.%f")

mavs_schema = {
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

df = pl.scan_csv(fn, has_header=False, new_columns=cn, separator=" ", schema=mavs_schema)

# Apply the test_func to create a new 'date_time' column
df = df.with_columns([
    df.select("*").map_batches(test_func).alias("date_time2")
] + sel_fields).collect()
#df2 = df.collect()

# Write the DataFrame to a CSV file
df.to_pandas().write_csv("mavs3_new_columns.csv")

"""
Alternatively, rowids are generated from :
                CREATE TEMP TABLE t1 AS 
SELECT  strptime('23-09-20 11:39:37', '%y-%m-%d %H:%M:%S')  + interval 1 millisecond * 10001 AS gen_time, 
                strptime(rpad(concat(cast((date_yy*100 + date_mm)*100 + date_dd as varchar), '6',
                       cast(((900 + time_hh)*100 + time_mm)*100 + time_ss::double as varchar)), 18, '0'),
                '%y%m%d69%H%M%S.%g'
                ) as pdate, 
                p, t, mx, my, pitch, roll, atan(-1.0*mx/my)*180/pi(), degrees(atan(-1.0*mx/my)) 
FROM read_csv('mavs2.csv.gz', delim=' ', header=false, 
                columns={'date_mm': 'SMALLINT', 
                         'date_dd': 'SMALLINT',
                         'date_yy': 'SMALLINT',
                         'time_hh': 'SMALLINT',
                         'time_mm': 'SMALLINT',
                         'time_ss': 'DOUBLE',
                         'va': 'SMALLINT',
                         'vb': 'SMALLINT',
                         'vc': 'SMALLINT',
                         'vd': 'SMALLINT',
                         'u': 'REAL',
                         'v': 'REAL',
                         'w': 'REAL',
                         'p': 'REAL',
                         't': 'REAL',
                         'mx': 'REAL',
                         'my': 'REAL',
                         'pitch': 'REAL',
                         'roll': 'REAL'});
                SELECT * FROM t1;
"""

#offset_months = row_count // (8 * 60 * 60 * 24 * 30)
# nr of days
#offset_minutes = 1 + row_count // (8 * 60 * 60
#offset_hours = 2 + row_count // (8 * 60 * 60)
#print(df11.len())
#df1 = pl.DataFrame({"roww": range(0,row_count)})
#df11 = df1.select(row=pl.col(['roww'])*125)
#df11 = df1.with_columns(gent = pl.col('roww')*125 )
#df1 = pl.Series('roww', range(1,df.select(pl.count())[0,0]))
#df2 = pl.concat([df11, df], how="horizontal")