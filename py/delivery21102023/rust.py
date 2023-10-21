import duckdb
import polars as pl
from datetime import datetime, timedelta
import streamlit as st
import plotly.express as px
# import numpy as np
import os

# proc_filename = "'mavs2.csv.gz'"
# proc_filename = "'output2/SN10337-1-2/mavs_delivery1.csv.gz'"
# out_filename = "'output_proc/SN10337-1-2/mavs_delivery1.csv.gz'"

import argparse  # Import argparse for command-line parameters

import polars.type_aliases as pta

def prepend_prefix_to_path(input_path, prefix):
    # Split the path into directory and filename
    directory, filename = os.path.split(input_path)
    
    # Prepend the prefix to the directory and filename
    new_directory = directory
    new_filename = prefix + filename
    
    # Combine the new directory and filename to create the new path
    new_path = os.path.join(new_directory, new_filename)
    
    return new_path

def main(proc_filename, out_filename, seed_time):
    duckdb_sql = """
              SELECT strptime('seed_time', '%y-%m-%d %H:%M:%S') AS seed_ts, 
                            date_ts as pdate, 
                            u as Vx, v as Vy, w as Vz, p*100 + 800 as P_mBar, degrees(atan(-1.0*mx/my)) as heading 
              FROM read_csv('proc_filename', delim=',', header=false, 
                            columns={'date_ts': 'TIMESTAMP_MS', 
                                   'va': 'SMALLINT',
                                   'vb': 'SMALLINT',
                                   'vc': 'SMALLINT',
                                   'vd': 'SMALLINT',
                                   'u': 'REAL',
                                   'v': 'REAL',
                                   'w': 'REAL',
                                   'p': 'REAL',
                                   'mx': 'REAL',
                                   'my': 'REAL',
                                   'pitch': 'REAL',
                                   'roll': 'REAL'});
    """
    
    

    duckdb_sql = duckdb_sql.replace('proc_filename', proc_filename)
    duckdb_sql = duckdb_sql.replace('seed_time', seed_time)

    df = duckdb.sql(duckdb_sql).pl()

    row_count = df.select(pl.count())[0, 0]
    offset_days = 1 + row_count // (8 * 60 * 60 * 24)

    start = datetime.strptime(seed_time, '%y-%m-%d %H:%M:%S')
    stop = start + timedelta(days=offset_days)

    df11 = pl.DataFrame({"timestamp": pl.datetime_range(start, stop, interval=timedelta(milliseconds=125), eager=True)[:row_count]})

    st.write("""
    # Argus Data Processing
    powered by *AVP Systeme*
             
    """, f"Processing {proc_filename}")

    st.code(duckdb_sql)

    print(row_count)

    st.write('Row count: ', row_count)

    print(df11.head())
    df2 = pl.concat([df11, df], how="horizontal")
    df2 = df2.select('timestamp', 'seed_ts', 'pdate', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading')

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

    fig_heading = px.scatter(x=dfs['timestamp'], y=dfs['heading'], labels={'x':'t', 'y':'Heading (°)'})
    st.write("""
    # heading °(t)
    """)
    st.plotly_chart(fig_heading, theme="streamlit")

    df = px.data.iris()
    fig2 = px.scatter_3d(dfs, x='Vx', y='Vy', z='Vz')
    st.plotly_chart(fig2, theme="streamlit")

    print(df2.head(100))
    print(df2.tail(100))

    df2 = df2.select('timestamp', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading')

    print(df2.head(100))
    print(df2.tail(100))

    output_dir = os.path.dirname(out_filename)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)  # Recreate parent directories if they don't exist
    df2.write_csv(out_filename)

    # Extract the date component and create a new column
    df2 = df2.with_columns(pl.col("timestamp").dt.strftime("%Y-%m-%d").alias("date"))

    # Group the DataFrame by the date column
    grouped = df2.group_by("date")

    # Now you have a group of DataFrames, each representing a partition
    for date, partition_df in grouped:
        partition_df = partition_df.select(pl.col("timestamp").dt.strftime("%Y-%m-%d %H:%M:%S.%3f"),
                                           pl.col('Vx').round(1), 
                                           pl.col('Vy').round(1), 
                                           pl.col('Vz').round(1),
                                           pl.col('P_mBar').round(1),  
                                           pl.col('heading').cast(pl.Int32))
        # partition_df = partition_df.select('timestamp', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading')
        out_fn = prepend_prefix_to_path(out_filename, str(date))
        print(out_fn)
        partition_df.write_csv(out_fn, separator=",", quote_style="non_numeric")
        # print(f"Partition for {date}:")
        # print(partition_df)


if __name__ == "__main__":
    # Create a command-line argument parser
    parser = argparse.ArgumentParser(description='Argus Data Processing')
    parser.add_argument('--proc_filename', type=str, help='Input processing filename')
    parser.add_argument('--out_filename', type=str, help='Output filename')
    parser.add_argument('--seed_time', type=str, help='Seed Time')

    args = parser.parse_args()

    if not args.proc_filename or not args.out_filename or not args.seed_time:
        print("Please provide both --proc_filename and --out_filename and --seed_time.")
    else:
        main(args.proc_filename, args.out_filename, args.seed_time)



# """
#               SELECT  strptime('seed_time', '%y-%m-%d %H:%M:%S') AS seed_ts, 
#                             date_ts as pdate, 
#                             u as Vx, v as Vy, w as Vz, p*100 as P_mBar, degrees(atan(-1.0*mx/my)) as heading 
#               FROM read_csv('proc_filename', delim=',', header=false, 
#                             columns={'date_ts': 'TIMESTAMP_MS', 
#                                    'va': 'SMALLINT',
#                                    'vb': 'SMALLINT',
#                                    'vc': 'SMALLINT',
#                                    'vd': 'SMALLINT',
#                                    'u': 'REAL',
#                                    'v': 'REAL',
#                                    'w': 'REAL',
#                                    'p': 'REAL',
#                                    't': 'REAL',
#                                    'mx': 'REAL',
#                                    'my': 'REAL',
#                                    'pitch': 'REAL',
#                                    'roll': 'REAL'});
#     """

# """

# SELECT  strptime('23-09-20 11:39:37', '%y-%m-%d %H:%M:%S') AS seed_time, 
#                 strptime(rpad(concat(cast((date_yy*100 + date_mm)*100 + date_dd as varchar), '6',
#                        cast(((900 + time_hh)*100 + time_mm)*100 + time_ss::double as varchar)), 18, '0'),
#                 '%y%m%d69%H%M%S.%g'
#                 ) as pdate, 
#                 u as Vx, v as Vy, w as Vz, p*100 as P_mBar, degrees(atan(-1.0*mx/my)) as heading 
# FROM read_csv(proc_filename, delim=',', header=false, 
#                 columns={'date_mm': 'SMALLINT', 
#                          'date_dd': 'SMALLINT',
#                          'date_yy': 'SMALLINT',
#                          'time_hh': 'SMALLINT',
#                          'time_mm': 'SMALLINT',
#                          'time_ss': 'DOUBLE',
#                          'va': 'SMALLINT',
#                          'vb': 'SMALLINT',
#                          'vc': 'SMALLINT',
#                          'vd': 'SMALLINT',
#                          'u': 'REAL',
#                          'v': 'REAL',
#                          'w': 'REAL',
#                          'p': 'REAL',
#                          't': 'REAL',
#                          'mx': 'REAL',
#                          'my': 'REAL',
#                          'pitch': 'REAL',
#                          'roll': 'REAL'});
# """