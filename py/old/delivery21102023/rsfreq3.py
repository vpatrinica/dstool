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


def get_time_roll_vec(_start, _span, _df, _labels):
    [_colname_step, _colname_ts, _colname_sum] = _labels
    _row_count = len(_df)
   
    _colname_step = 'step' + str(_span)
    _step_series_span = pl.Series(_colname_step, (((_df[_span:] - _df[:-_span]).cast(pl.Int64)*float(1e3/_span))).cast(pl.Int64))
    
    _first_step = ((_df[_span] - _df[0]).total_seconds()*1e6/float(_span))
    _first_step = int(_first_step)
    
    print(_first_step)
    print(_step_series_span[:10])
    
    _init_list = [0] + ([_first_step]*_span) 
    _step_series = pl.concat([pl.DataFrame({_colname_step: _init_list})[_colname_step], _step_series_span])
    
    _td_series = pl.Series(_colname_sum, _step_series.cumsum())
    
    _t_series = pl.Series(_colname_ts, [_start] *(_row_count+1))
    _dft = pl.DataFrame({"ts": _t_series, "td": _td_series})
    _t_series = pl.Series(_colname_ts, _dft.select(pl.col('ts') + pl.duration(microseconds=pl.col('td'))).to_series())

    _t_series = pl.Series(_colname_ts, _t_series)
    return (_step_series[1:], _t_series[:-1], _td_series[:-1])


def get_series(_start, _seed_freq, _row_count, _labels):
    [_ts_label] = _labels
    _freq_hz = _seed_freq   
    _period_us = int(1000000.0/_freq_hz)
    _freq_hz_act = 1000000.0 /_period_us
    _offset_days = 1 + _row_count // (_freq_hz_act * 60 * 60 * 24)

    _stop = _start + timedelta(days=_offset_days)
    _dft = pl.DataFrame({_ts_label: pl.datetime_range(_start, _stop, interval=timedelta(microseconds=_period_us), eager=True)[:_row_count]})
    return (_freq_hz_act, _period_us, _dft)


def main(proc_filename, out_filename):
    duckdb_sql = """
              SELECT         
                             pdate, 
                             
                            Vx, Vy, Vz, P_mBar, cx, cy, heading 
              FROM read_csv('proc_filename', delim=',', header=false, 
                            columns={'pdate': 'TIMESTAMP_MS', 
                                   'Vx': 'REAL',
                                   'Vy': 'REAL',
                                   'Vz': 'REAL',
                                   'P_mBar': 'REAL',
                                   'cx': 'REAL',
                                   'cy': 'REAL',
                                   'roll': 'REAL',
                                   'heading': 'REAL'});
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

    #seed_time = datetime.strptime("23-09-21 09:47:15.860", "%y-%m-%d %H:%M:%S.%f")
    seed_time = "2023-05-08 09:22:59.660"
    start_time = datetime.strptime(seed_time, '%Y-%m-%d %H:%M:%S.%f')

    freq_init_ap = 8.000
    timestamp_freq_ap_label_col = 'timestamp'+str(freq_init_ap).replace('.', '')
    
    (freq_hz_act_ap, period_us_ap, dft_ap) = get_series(start_time, freq_init_ap, row_count, [timestamp_freq_ap_label_col])
    
    st.write('Test init frequency in Hz: ', freq_init_ap)
    st.write('Test init period: ', period_us_ap)
    st.write('Test actual frequency in Hz: ', freq_hz_act_ap)

    span_list = [100, 1000, 10000]
    df_current = df.select(pl.col('pdate')).to_series()
    
    df_columns = ['pdate', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading']
    for span in span_list:
        date_label_col = 'ts'+str(span)
        step_label_col = 'step'+str(span)
        sum_label_col = 'sum'+str(span)
        new_columns_list = [step_label_col, date_label_col, sum_label_col]

        (ts_1, dt_1, s1) = get_time_roll_vec(start_time, span, df_current, new_columns_list)
    
        st.write("Statistics step span1:", span)
        st.write("steps:", ts_1.describe())
        st.write("cum steps:", s1.describe())
        st.write("timeseries:", dt_1.describe())

        dt_1_df = pl.DataFrame({date_label_col: dt_1 })
        ts_1_df = pl.DataFrame({step_label_col: ts_1 })
        s1_df = pl.DataFrame({sum_label_col: s1})
        df = pl.concat([dt_1_df, ts_1_df, s1_df, df], how="horizontal")
        df_columns = new_columns_list + df_columns
    
    df_columns = [timestamp_freq_ap_label_col] + df_columns
    df = pl.concat([dft_ap, df], how="horizontal")
    
    df = df.select(df_columns)

    st.write('Row count: ', row_count)
    st.write("First 10 rows", df.head(150))
    st.write("Last 10 rows", df.tail(150))

    # dfs = df.sample(fraction=0.01, seed=0)
    
    output_dir = os.path.dirname(out_filename)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)  # Recreate parent directories if they don't exist

    if os.path.exists(out_filename):
        os.remove(out_filename) 
    df.write_csv(out_filename, separator=",", quote_style="non_numeric")

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


