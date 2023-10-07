import duckdb
import polars as pl
from datetime import datetime, timedelta

df = duckdb.sql("""
                
SELECT  strptime('23-09-20 11:39:37', '%y-%m-%d %H:%M:%S') AS seed_time, 
                strptime(rpad(concat(cast((date_yy*100 + date_mm)*100 + date_dd as varchar), '6',
                       cast(((900 + time_hh)*100 + time_mm)*100 + time_ss::double as varchar)), 18, '0'),
                '%y%m%d69%H%M%S.%g'
                ) as pdate, 
                u as Vx, v as Vy, w as Vz, p*100 as P_mBar, degrees(atan(-1.0*mx/my)) as heading 
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
                
""").pl()


row_count = df.select(pl.count())[0,0]
offset_days = 1 + row_count // (8 * 60 * 60 * 24)

start = datetime(2023, 9, 20, 11, 39, 47)
stop = start + timedelta(days=offset_days)

df11 = pl.DataFrame({"timestamp": pl.datetime_range(start, stop, interval=timedelta(milliseconds=125), eager=True)[:row_count]})
print(row_count)
print(df11.head())
df2 = pl.concat([df11, df], how="horizontal")
df2 = df2.select('timestamp', 'seed_time', 'pdate', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading')

# Print the first few rows of the resulting DataFrame
print(df2.head(100))
print(df2.tail(100))

df2 = df2.select('timestamp', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading')

# Print the first few rows of the resulting DataFrame
print(df2.head(100))
print(df2.tail(100))

df2.write_csv('mavs2_out.csv')
