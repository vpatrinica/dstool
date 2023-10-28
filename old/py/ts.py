import duckdb
import polars as pl


"""
strptime(concat(cast((date_yy*100 + date_mm)*100 + date_dd as varchar), '6',
                       cast(((900 + time_hh::real)*100 + time_mm::real)*100 + time_ss::real as varchar)),
                '%y%m%d69%H%M%S.%g'
                ),
((100.000 + time_hh::real)*100 + time_mm::real)*100 + round(time_ss, 3), ((time_hh::real)*100 + time_mm::real)*100 + round(time_ss, 3), round(time_ss, 3), time_ss
cast(((900 + time_hh::real)*100 + time_mm::real)*100 + time_ss::real as varchar))

                """
df = duckdb.sql("""
SELECT 
                strptime(rpad(concat(cast((date_yy*100 + date_mm)*100 + date_dd as varchar), '6',
                       cast(((900 + time_hh)*100 + time_mm)*100 + time_ss::double as varchar)), 18, '0'),
                '%y%m%d69%H%M%S.%g'
                ), 
                p, t, mx, my, pitch, roll, atan(-1.0*mx/my)*180/pi(), degrees(atan(-1.0*mx/my)) 
FROM read_csv('mavs2.csv', delim=' ', header=false, 
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



# df = duckdb.sql("""
# SELECT strptime(concat(cast(date_dd as varchar),'.0',cast(date_mm as varchar),'.',cast(date_yy as varchar),' ',cast(time_hh as varchar),':',cast(time_mm as varchar),':',cast(round(time_ss::REAL, 3) as varchar)), '%d.%m.%y %H:%M:%S.%g'), * FROM read_csv('mavs2.csv', delim=' ', header=false, columns={'date_mm': 'SMALLINT', 'date_dd': 'SMALLINT', 'date_yy': 'SMALLINT', 'time_hh': 'SMALLINT', 'time_mm': 'SMALLINT', 'time_ss': 'REAL', 'va': 'SMALLINT', 'vb': 'SMALLINT', 'vc': 'SMALLINT', 'vd': 'SMALLINT', 'u': 'REAL', 'v': 'REAL', 'w': 'REAL', 'p': 'REAL', 't': 'REAL', 'mx': 'REAL', 'my': 'REAL', 'pitch': 'REAL', 'roll': 'REAL'});
# """).pl()

# df = df.with_columns(
#     pl.col("date_yy") + 2000,
#     pl.col("date_mm"),
#     pl.col("date_dd"),
#     pl.col("time_hh"),
#     pl.col("time_mm"),
#     pl.col("time_ss"),
# ).cast(pl.Datetime)

# Rename the new column to 'timestamp'
# df = df.with_columns(pl.col("timestamp").alias("timestamp"))


# # Select the desired columns
# df = df.select([
#     "timestamp",
#     "p",
#     "t",
#     "mx",
#     "my",
#     "pitch",
#     "roll"
# ])

print(df.head(100))
df.head(100)
# print(df.__class__)



# import duckdb
# import polars as pl

# # Define the SQL query to load the CSV and select the desired columns
# query = """
# SELECT date_mm, date_dd, date_yy, time_hh, time_mm, time_ss,
#        DATE_ADD(DATE_ADD(DATE_ADD(DATE_ADD(DATE_ADD(DATE_ADD(
#        (date_yy + 2000)::timestamp, (date_mm - 1)::interval),
#        (date_dd - 1)::interval), time_hh::interval), time_mm::interval),
#        time_ss::interval), -1::interval) as timestamp,
#        va, vb, vc, vd, u, v, w, p, t, mx, my, pitch, roll
# FROM read_csv('mavs2.csv', delim=' ', header=false, columns={'date_mm': 'SMALLINT', 'date_dd': 'SMALLINT', 'date_yy': 'SMALLINT', 'time_hh': 'SMALLINT', 'time_mm': 'SMALLINT', 'time_ss': 'REAL', 'va': 'SMALLINT', 'vb': 'SMALLINT', 'vc': 'SMALLINT', 'vd': 'SMALLINT', 'u': 'REAL', 'v': 'REAL', 'w': 'REAL', 'p': 'REAL', 't': 'REAL', 'mx': 'REAL', 'my': 'REAL', 'pitch': 'REAL', 'roll': 'REAL'});
# """
# query = """
# SELECT 
#     TO_TIMESTAMP(
#         DATE_ADD(
#             DATE_ADD(
#                 DATE_ADD(
#                     DATE_ADD(
#                         DATE_ADD(
#                             DATE_ADD(
#                                 '20' || date_yy || '-' || date_mm || '-' || date_dd || ' ' ||
#                                 time_hh || ':' || time_mm || ':' || CAST(time_ss AS STRING), 
#                                 time_ss::interval
#                             ),
#                             time_mm::interval
#                         ),
#                         time_hh::interval
#                     ),
#                     (date_dd - 1)::interval
#                 ),
#                 (date_mm - 1)::interval
#             ),
#             (date_yy + 2000)::timestamp
#         )
#     ) as timestamp,
#     va, vb, vc, vd, u, v, w, p, t, mx, my, pitch, roll
# FROM read_csv(
#     'mavs2.csv', 
#     delim=' ', 
#     header=false, 
#     columns={
#         'date_mm': 'SMALLINT', 
#         'date_dd': 'SMALLINT', 
#         'date_yy': 'SMALLINT', 
#         'time_hh': 'SMALLINT', 
#         'time_mm': 'SMALLINT', 
#         'time_ss': 'REAL', 
#         'va': 'SMALLINT', 
#         'vb': 'SMALLINT', 
#         'vc': 'SMALLINT', 
#         'vd': 'SMALLINT', 
#         'u': 'REAL', 
#         'v': 'REAL', 
#         'w': 'REAL', 
#         'p': 'REAL', 
#         't': 'REAL', 
#         'mx': 'REAL', 
#         'my': 'REAL', 
#         'pitch': 'REAL', 
#         'roll': 'REAL'
#     }
# );
# """

# Define the SQL query to load the CSV and select the desired columns
# query = """
# SELECT 
#     TO_DATE(date_yy || '-' || date_mm || '-' || date_dd, 'YYYY-MM-DD') + TIME '00:00:00' + (time_hh || ':' || time_mm || ':' || time_ss)::TIME as timestamp,
#     va, vb, vc, vd, u, v, w, p, t, mx, my, pitch, roll
# FROM read_csv(
#     'mavs2.csv', 
#     delim=' ', 
#     header=false, 
#     columns={
#         'date_mm': 'SMALLINT', 
#         'date_dd': 'SMALLINT', 
#         'date_yy': 'SMALLINT', 
#         'time_hh': 'SMALLINT', 
#         'time_mm': 'SMALLINT', 
#         'time_ss': 'REAL', 
#         'va': 'SMALLINT', 
#         'vb': 'SMALLINT', 
#         'vc': 'SMALLINT', 
#         'vd': 'SMALLINT', 
#         'u': 'REAL', 
#         'v': 'REAL', 
#         'w': 'REAL', 
#         'p': 'REAL', 
#         't': 'REAL', 
#         'mx': 'REAL', 
#         'my': 'REAL', 
#         'pitch': 'REAL', 
#         'roll': 'REAL'
#     }
# );
# """

# Execute the SQL query and convert the result to a Polars DataFrame
# df = duckdb.sql(query).pl()

# Print the first few rows of the resulting DataFrame
# print(df.head())
