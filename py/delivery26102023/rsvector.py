import duckdb
import polars as pl
from datetime import datetime, timedelta
import streamlit as st
# import plotly.express as px
# import numpy as np
import os
import argparse  # Import argparse for command-line parameters


def prepend_prefix_to_path(input_path, prefix):
    # Split the path into directory and filename
    directory, filename = os.path.split(input_path)
    
    # Prepend the prefix to the directory and filename
    new_directory = directory
    new_filename = prefix + filename
    
    # Combine the new directory and filename to create the new path
    new_path = os.path.join(new_directory, new_filename)
    
    return new_path


def prepare_output_dir(_file, _file_flag):
    _file_dir = os.path.dirname(_file)
    if not os.path.exists(_file_dir):
        os.makedirs(_file_dir)  # Recreate parent directories if they don't exist
    if (('w' in _file_flag) and os.path.exists(_file)):
        os.remove(_file)

#TIMESTAMP strptime('seed_time', '%y-%m-%d %H:%M:%S') + INTERVAL 125000*Ensemble microsecond as pdate,    
def main(proc_filename, out_filename, heading, seed_time, step=125000, seconds_to_spread=0):
    duckdb_sql = """
              SELECT         
                            Ensemble,                         
                            -Velocity2 as Vx, 
                            Velocity1 as Vy, 
                            Velocity3 as Vz, 
                            round(Analog1, 3) as P_mBar, 
                            placeholder_heading as heading
              FROM read_csv('proc_filename', delim=',', header=false, 
                            columns={
                                   'Burst': 'SMALLINT',
                                   'Ensemble': 'BIGINT',
                                   'Velocity1': 'REAL',
                                   'Velocity2': 'REAL',
                                   'Velocity3': 'REAL',
                                   'Amplitude1': 'SMALLINT',
                                   'Amplitude2': 'SMALLINT',
                                   'Amplitude3': 'SMALLINT',
                                   'SNR1': 'REAL',
                                   'SNR2': 'REAL',
                                   'SNR3': 'REAL',
                                   'Correlation1': 'SMALLINT',
                                   'Correlation2': 'SMALLINT',
                                   'Correlation3': 'SMALLINT',
                                   'Pressure': 'REAL',
                                   'Analog1': 'REAL',
                                   'Analog2': 'REAL',
                                   'Checksum': 'SMALLINT'
                            });
    """
    duckdb_sql = duckdb_sql.replace('proc_filename', proc_filename)
    duckdb_sql = duckdb_sql.replace('seed_time', seed_time)
    duckdb_sql = duckdb_sql.replace('placeholder_heading', str(heading))        
    
    df = duckdb.sql(duckdb_sql).pl()
    row_count = df.select(pl.count())[0, 0]

    offset_days = 1 + row_count // (8 * 60 * 60 * 24)

    start = datetime.strptime(seed_time, '%Y-%m-%d %H:%M:%S.%f')
    stop = start + timedelta(days=offset_days)

    df11 = pl.DataFrame({"pdate": pl.datetime_range(start, stop, interval=timedelta(microseconds=int(step)), eager=True)[:row_count]})
    df = pl.concat([df11, df], how="horizontal")
    
    st.write("""
    # Argus Data Processing
    powered by *AVP Systeme*
             
    """, f"Processing {proc_filename}")

    st.code(duckdb_sql)
    print(row_count)
    
    df_columns = ['pdate', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading']
    
    df = df.select(df_columns)

    st.write('Row count: ', row_count)
    st.write("First 10 rows", df.head(150))
    st.write("Last 10 rows", df.tail(150))
    
    df = df.with_columns(pl.col('pdate') + pl.duration(microseconds=int(step)))

    # dfs = df.sample(fraction=0.01, seed=0)
    if seconds_to_spread:
        us_to_spread = seconds_to_spread * 1e6 / row_count
        df = df.with_columns(pl.col('pdate') + pl.duration(microseconds=us_to_spread))

    prepare_output_dir(out_filename, 'w')
    df.write_csv(out_filename, separator=",", quote_style="non_numeric")    
    
    df_columns = ['pdate', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading']
    # Extract the date component and create a new column
    df = df.with_columns(pl.col("pdate").dt.strftime("%Y-%m-%d").alias("date"))

    # # Group the DataFrame by the date column
    grouped = df.group_by("date")

    # # Now you have a group of DataFrames, each representing a partition
    for date, partition_df in grouped:
        partition_df = partition_df.select(pl.col("pdate").dt.strftime("%Y-%m-%d %H:%M:%S.%3f"),
                                           pl.col('Vx').round(1), 
                                           pl.col('Vy').round(1), 
                                           pl.col('Vz').round(1),
                                           pl.col('P_mBar').round(1),  
                                           pl.col('heading').cast(pl.Int32))
        
        out_fn = prepend_prefix_to_path(out_filename, str(date))
        prepare_output_dir(out_fn, 'w')
        print(out_fn)
        partition_df.write_csv(out_fn, separator=",", quote_style="non_numeric")


if __name__ == "__main__":
    # Create a command-line argument parser
    parser = argparse.ArgumentParser(description='Argus Data Processing')
    parser.add_argument('--proc_filename', type=str, help='Input processing filename', required=True)
    parser.add_argument('--out_filename', type=str, help='Output filename', required=True)
    parser.add_argument('--heading', type=float, help='Heading as a float', required=True)
    parser.add_argument('--seed_time', type=str, help='Seed time as a string', required=True)
    parser.add_argument('--step', type=str, help='Step us def 125000')
    parser.add_argument('--seconds_to_spread', type=int, help='Seconds to spread as an integer')

    args = parser.parse_args()

    if not args.proc_filename or not args.out_filename or not args.heading or not args.seed_time:
        print("Please provide both --proc_filename and --out_filename --heading --seed_time ")
    else:
        main(args.proc_filename, args.out_filename, args.heading, args.seed_time, args.step, args.seconds_to_spread)

