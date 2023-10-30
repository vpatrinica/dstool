import duckdb
import polars as pl
from datetime import datetime, timedelta
import streamlit as st
# import plotly.express as px
# import numpy as np
import os
import argparse  # Import argparse for command-line parameters

from file_utils import prepend_prefix_to_path,\
    prepare_output_file,\
    make_dirs


def write_df_to_csv(_df, _out_fn, _append_mode):
    prepare_output_file(_out_fn, _append_mode)
    print(_out_fn)
    _of_flags = 'ab' if _append_mode else 'wb'
    _has_header = False if _append_mode else True

    with open(_out_fn, mode=_of_flags) as of:
        _df.write_csv(of, separator=",", has_header=_has_header, quote_style="non_numeric")


#TIMESTAMP strptime('seed_time', '%y-%m-%d %H:%M:%S') + INTERVAL 125000*Ensemble microsecond as pdate,    
def main(proc_filename, out_filename, heading, seed_time, end_time, time_fmt='%Y-%m-%d %H:%M:%S.%f', append_mode=False):
    # prepare output folder
    make_dirs(out_filename, append_mode)

    duckdb_sql = """
              SELECT         
                            Ensemble,                         
                            -100*Velocity2 as Vx, 
                            100*Velocity1 as Vy, 
                            100*Velocity3 as Vz, 
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
    duckdb_sql = duckdb_sql.replace('placeholder_heading', str(heading))        
    
    df = duckdb.sql(duckdb_sql).pl()
    row_count = df.select(pl.count())[0, 0]    

    start = datetime.strptime(seed_time, time_fmt)
    end_ref = datetime.strptime(end_time, time_fmt)
    step = 1e9 * ((end_ref - start).total_seconds()  / float(row_count) )
    print(step)

    df = df.with_columns(pl.Series("pdate", [start]*row_count).alias("pdate"))
    df = df.with_columns(pl.Series("steps", range(row_count)).alias("steps"))
    df = df.with_columns(pl.col('pdate') + pl.duration(nanoseconds=step*pl.col('steps')).alias("pdate"))

    print(df[:2])
    print(df[-2:]) 
    print(row_count)

    st.write("""
    # Argus Data Processing
    powered by *AVP Systeme*
             
    """, f"Processing {proc_filename}")

    st.code(duckdb_sql)    
    
    df_columns = ['pdate', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading']    
    df = df.select(df_columns)

    st.write('Row count: ', row_count)
    st.write("First 10 rows", df.head(150))
    st.write("Last 10 rows", df.tail(150))

    write_df_to_csv(df, out_filename, append_mode)

    #####
    # Dayly split
    ######
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
        write_df_to_csv(partition_df, out_fn, append_mode)
    print("Finished export")


if __name__ == "__main__":
    # Create a command-line argument parser
    parser = argparse.ArgumentParser(description='Argus Data Processing')
    parser.add_argument('--proc_filename', type=str, help='Input processing filename', required=True)
    parser.add_argument('--out_filename', type=str, help='Output filename', required=True)
    parser.add_argument('--heading', type=float, help='Heading as a float', required=True)
    parser.add_argument('--seed_time', type=str, help='Seed time as a string', required=True)
    parser.add_argument('--end_time', type=str, help='End time as a string', required=True)
    parser.add_argument('--time_fmt', type=str, help='Timeformat as a string', required=True)
    parser.add_argument("--append", "-a", action="store_true", help="This is a sample flag.")

    args = parser.parse_args()

    if not args.proc_filename or not args.out_filename or not args.heading or not args.seed_time or not args.end_time or not args.time_fmt:
        print("Please provide both --proc_filename and --out_filename --heading --seed_time --end_time --time_fmt")
    else:
        main(args.proc_filename, args.out_filename, args.heading, args.seed_time, args.end_time, args.time_fmt, args.append)

