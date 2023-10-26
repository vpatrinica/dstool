import duckdb
import polars as pl
from datetime import datetime, timedelta
import streamlit as st
import plotly.express as px
# import numpy as np
import os
import time

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

def main(proc_filename, out_filename):
    duckdb_sql = """
              SELECT         timestamp, 
                             pdate, 
                             tdelta,
                            Vx, Vy, Vz, P_mBar, heading 
              FROM read_csv('proc_filename', delim=',', header=true, 
                            columns={'timestamp': 'TIMESTAMP_MS', 
                                   'pdate': 'TIMESTAMP_MS',
                                   'tdelta': 'BIGINT',
                                   'Vx': 'REAL',
                                   'Vy': 'REAL',
                                   'Vz': 'REAL',
                                   'P_mBar': 'REAL',
                                   'heading': 'SMALLINT'});
    """
    


    duckdb_sql = duckdb_sql.replace('proc_filename', proc_filename)
    

    df = duckdb.sql(duckdb_sql).pl()

    row_count = df.select(pl.count())[0, 0]
    
    st.write("""
    # Argus Data Processing
    powered by *AVP Systeme*
             
    """, f"Processing {proc_filename}")

    st.code(duckdb_sql)

    print(row_count)

    st.write('Row count: ', row_count)


    st.write("First 10 rows", df.head(10))

    st.write("Last 10 rows", df.tail(10))

    #time.sleep(1999999)

    dfs = df.sample(fraction=0.01, seed=0)


    output_dir = os.path.dirname(out_filename)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)  # Recreate parent directories if they don't exist

    if os.path.exists(out_filename):
        os.remove(out_filename)
    df2 = df.select('pdate', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading')
    df2.write_csv(out_filename, separator=",", quote_style="non_numeric")

    # Extract the date component and create a new column
    df2 = df2.with_columns(pl.col("pdate").dt.strftime("%Y-%m-%d").alias("date"))

    # # Group the DataFrame by the date column
    grouped = df2.group_by("date")

    # # Now you have a group of DataFrames, each representing a partition
    for date, partition_df in grouped:
        partition_df = partition_df.select(pl.col("pdate").dt.strftime("%Y-%m-%d %H:%M:%S.%3f"),
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

    args = parser.parse_args()

    if not args.proc_filename or not args.out_filename:
        print("Please provide both --proc_filename and --out_filename ")
    else:
        main(args.proc_filename, args.out_filename)


