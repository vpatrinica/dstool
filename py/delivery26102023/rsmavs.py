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


def get_time_roll_vec(_df, _span,  _labels):
    [_colname_step] = _labels
    
    _step_series_span = pl.Series(_colname_step, (((_df[_span:] - _df[:-_span]).cast(pl.Int64)*float(1e3/_span))).cast(pl.Int64))
    _first_step = int((_df[_span] - _df[0]).total_seconds()*1e6/float(_span))
    
    print(_first_step)
    print(_step_series_span[:10])
    
    _step_series = pl.concat([pl.DataFrame({_colname_step: [_first_step]*_span})[_colname_step], _step_series_span])
        
    return _step_series


def prepare_output_dir(_file, _file_flag):
    _file_dir = os.path.dirname(_file)
    if not os.path.exists(_file_dir):
        os.makedirs(_file_dir)  # Recreate parent directories if they don't exist
    if (('w' in _file_flag) and os.path.exists(_file)):
        os.remove(_file)


def main(proc_filename, out_filename, seconds_to_spread=0):
    duckdb_sql = """
              SELECT         
                             pdate,                              
                            u as Vx, v as Vy, w as Vz, p*100 + 800 as P_mBar, degrees(atan(-1.0*mx/my)) as heading
              FROM read_csv('proc_filename', delim=',', header=false, 
                            columns={'pdate': 'TIMESTAMP_MS', 
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
                                   'roll': 'REAL'
                            });
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
    
    span_list = [100, 10, 1]
    df_current = df.select(pl.col('pdate')).to_series()
    
    df_columns = ['pdate', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading']
    for span in span_list:
        step_label_col = 'step'+str(span)
        new_columns_list = [step_label_col]

        ts_1 = get_time_roll_vec(df_current, span, new_columns_list)
    
        st.write("Statistics step span:", span)
        st.write("steps:", ts_1.describe())
               
        ts_1_df = pl.DataFrame({step_label_col: ts_1 })
        df = pl.concat([ts_1_df, df], how="horizontal")
        df_columns = [step_label_col] + df_columns    

    df = df.select(df_columns)

    st.write('Row count: ', row_count)
    st.write("First 10 rows", df.head(150))
    st.write("Last 10 rows", df.tail(150))

    # dfs = df.sample(fraction=0.01, seed=0)
    us_to_spread = seconds_to_spread * 1e6 / row_count

    if us_to_spread > 0:
        df = df.with_columns(pl.col('pdate') + pl.duration(microseconds=us_to_spread).alias("pdatep"))
    
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
    parser.add_argument('--proc_filename', type=str, help='Input processing filename')
    parser.add_argument('--out_filename', type=str, help='Output filename')

    args = parser.parse_args()

    if not args.proc_filename or not args.out_filename:
        print("Please provide both --proc_filename and --out_filename ")
    else:
        main(args.proc_filename, args.out_filename)


