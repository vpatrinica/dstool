import duckdb
import polars as pl
from datetime import datetime, timedelta
import streamlit as st
import plotly.express as px
# import numpy as np

# proc_filename = "'mavs2.csv.gz'"
proc_filename = "'delivery21102023/output2/SN10337-1-2/mavs_delivery1.csv.gz'"
out_filename = "'delivery21102023/output_proc/SN10337-1-2/mavs_delivery1.csv.gz'"

duckdb_sql = """

SELECT  strptime('23-09-20 11:39:37', '%y-%m-%d %H:%M:%S') AS seed_time, 
                strptime(rpad(concat(cast((date_yy*100 + date_mm)*100 + date_dd as varchar), '6',
                       cast(((900 + time_hh)*100 + time_mm)*100 + time_ss::double as varchar)), 18, '0'),
                '%y%m%d69%H%M%S.%g'
                ) as pdate, 
                u as Vx, v as Vy, w as Vz, p*100 as P_mBar, degrees(atan(-1.0*mx/my)) as heading 
FROM read_csv(proc_filename, delim=' ', header=false, 
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
"""

duckdb_sql = duckdb_sql.replace('proc_filename', proc_filename)
print(proc_filename)

print(duckdb_sql)

df = duckdb.sql(duckdb_sql).pl()


row_count = df.select(pl.count())[0,0]
offset_days = 1 + row_count // (8 * 60 * 60 * 24)

start = datetime(2023, 9, 20, 11, 39, 47)
stop = start + timedelta(days=offset_days)

df11 = pl.DataFrame({"timestamp": pl.datetime_range(start, stop, interval=timedelta(milliseconds=125), eager=True)[:row_count]})

st.write("""
# Argus Data Processing
powered by *AVP Systeme*
         
""", f"Processing {proc_filename}")
 
#st.line_chart(df11)

st.code(duckdb_sql)

print(row_count)

st.write('Row count: ', row_count)

print(df11.head())
df2 = pl.concat([df11, df], how="horizontal")
df2 = df2.select('timestamp', 'seed_time', 'pdate', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading')


st.write("First 10 rows", df2.head(10))

st.write("Last 10 rows", df2.tail(10))

dfs = df2.sample(fraction=0.01, seed=0)  

fig_p = px.scatter(x=dfs['timestamp'], y=dfs['P_mBar'], labels={'x':'t', 'y':'P (mBar)'})

st.write("""
# Pressure (mBar)
""")
st.plotly_chart(fig_p, theme="streamlit")

fig_vx = px.scatter(x=dfs['timestamp'], y=dfs['Vx'], labels={'x':'t', 'y':'Vx (m/s)'})
st.write("""
# Vx(t)
""")
st.plotly_chart(fig_vx, theme="streamlit")

fig_vy = px.scatter(x=dfs['timestamp'], y=dfs['Vy'], labels={'x':'t', 'y':'Vy (m/s)'})
st.write("""
# Vy(t)
""")
st.plotly_chart(fig_vy, theme="streamlit")

fig_vz = px.scatter(x=dfs['timestamp'], y=dfs['Vz'], labels={'x':'t', 'y':'Vz (m/s)'})
st.write("""
# Vz(t)
""")
st.plotly_chart(fig_vz, theme="streamlit")


df = px.data.iris()
fig2 = px.scatter_3d(dfs, x='Vx', y='Vy', z='Vz')
#fig.show()

st.plotly_chart(fig2, theme="streamlit")

# Print the first few rows of the resulting DataFrame
print(df2.head(100))
print(df2.tail(100))

df2 = df2.select('timestamp', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading')

# Print the first few rows of the resulting DataFrame
print(df2.head(100))
print(df2.tail(100))

df2.write_csv(out_filename)
