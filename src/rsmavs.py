import duckdb
import polars as pl
from datetime import datetime, timedelta
import streamlit as st
# import plotly.express as px
# import numpy as np
import shutil
import os
import argparse  # Import argparse for command-line parameters

from file_utils import prepend_prefix_to_path,\
    prepare_output_file,\
    make_dirs

def get_mavs_schema(_type):
    if _type == 1:
        return """
                       {'pdate': 'TIMESTAMP_MS', 
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
                        }
            """       
    else:
        return """
                       {'pdate': 'TIMESTAMP_MS', 
                        'va': 'SMALLINT',
                        'vb': 'SMALLINT',
                        'vc': 'SMALLINT',
                        'vd': 'SMALLINT',
                        'u': 'REAL',
                        'v': 'REAL',
                        'w': 'REAL',
                        't': 'REAL',
                        'p': 'REAL',
                        'mx': 'REAL',
                        'my': 'REAL',
                        'pitch': 'REAL',
                        'roll': 'REAL'
                        }
            """ 


def get_time_roll_vec(_df, _span,  _labels):
    [_colname_step] = _labels
    
    _step_series_span = pl.Series(_colname_step, (((_df[_span:] - _df[:-_span]).cast(pl.Int64)*float(1e3/_span))).cast(pl.Int64))
    _first_step = int((_df[_span] - _df[0]).total_seconds()*1e6/float(_span))
    
    print(_first_step)
    print(_step_series_span[:10])
    
    _step_series = pl.concat([pl.DataFrame({_colname_step: [_first_step]*_span})[_colname_step], _step_series_span])
        
    return _step_series


def write_df_to_csv(_df, _out_fn, _append_mode):
    prepare_output_file(_out_fn, _append_mode)
    print(_out_fn)
    _of_flags = 'ab' if _append_mode else 'wb'
    _has_header = False if _append_mode else True

    with open(_out_fn, mode=_of_flags) as of:
        _df.write_csv(of, separator=",", has_header=_has_header, quote_style="non_numeric")


def main(proc_filename, out_filename, mavs_type=1, append_mode=False, seconds_to_spread=0):
    # prepare output folder
    make_dirs(out_filename, append_mode)

    duckdb_sql = """
              SELECT         
                             pdate,                              
                            u as Vx, v as Vy, w as Vz, p*100 + 800 as P_mBar, degrees(atan(-1.0*mx/my)) as heading
              FROM read_csv('proc_filename', delim=',', header=false, 
                            columns=placeholder_columns);
    """
    duckdb_sql = duckdb_sql.replace('proc_filename', proc_filename)
    duckdb_sql = duckdb_sql.replace('placeholder_columns', get_mavs_schema(mavs_type))

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

    st.write('Row count: ', row_count)
    st.write("First 10 rows", df.head(150))
    st.write("Last 10 rows", df.tail(150))

    if seconds_to_spread:
        ns_to_spread = seconds_to_spread * 1e9 / row_count
        df = df.with_columns(pl.Series("steps", range(row_count)).alias("steps"))
        df = df.with_columns(pl.col('pdate') + pl.duration(nanoseconds=ns_to_spread*pl.col('steps')).alias("pdate"))
    
    df = df.select(df_columns)

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
    parser.add_argument('--proc_filename', type=str, help='Input processing filename')
    parser.add_argument('--out_filename', type=str, help='Output filename')
    parser.add_argument('--seconds_to_spread', type=float, help='Seconds to spread a float')
    parser.add_argument('--mavs_type', type=float, help='Seconds to spread as an integer')

    parser.add_argument("--append", "-a", action="store_true", help="This is a sample flag.")
    args = parser.parse_args()

    if not args.proc_filename or not args.out_filename:
        print("Please provide both --proc_filename and --out_filename ")
    else:
        main(args.proc_filename, args.out_filename, args.mavs_type, args.append, args.seconds_to_spread)


