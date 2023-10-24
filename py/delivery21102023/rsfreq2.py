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

def get_time_roll(_seed_time, _span, _row_count, _df):
    # s = pl.Series("a", [1, 2, 3, 4, 5])
    _start = datetime.strptime(_seed_time, '%y-%m-%d %H:%M:%S.%f').timestamp() * 1000
    _dt_array = [_start]
    # _step_array = []
    if _span + 1 < _row_count:
        _first_time = _df[_span]
        _first_step = int((_first_time - _start)/(1.0*_span))
        _step_array = [_first_step] * _span
        _dt_array = [_first_time] * _span
        for _iter in range(_span-1):
            #_dt_array.append(_dt_array[_iter-1] + _step_array[_iter-1])
            _dt_array[_iter+1] = _dt_array[_iter] + _step_array[_iter]

        for _iter in range(_span, _row_count):
            _next_step = int((_df[_iter] - _df[_iter-_span])/(1.0*_span))
            _step_array.append(_next_step)
            _dt_array.append(_dt_array[_iter-1] + _step_array[_iter-1])
            # _dt_array[_iter] = _dt_array[_iter-1] + _step_array[_iter-1] 
    _step_series = pl.Series("step-"+str(_span), _step_array)
    _time_series = pl.Series("ts-"+str(_span), _dt_array)
    return (_step_series, _time_series)


def get_time_roll_vec(_seed_time, _span, _df):
    _row_count = len(_df)
    _start = datetime.strptime(_seed_time, '%y-%m-%d %H:%M:%S.%f')
    _new_count = _row_count - _span
    _step_series = pl.Series('step' + str(_span), ((_df[_span:] - _df[:-_span]).cast(pl.Int64)/float(1000*_span)))
    #_init_series = pl.Series('init', [_start] * _new_count)
    _td_series = _step_series.cumsum()
    _time_series =  [_df[_span]] * _new_count
    for _iter in range(1, _new_count):
        _time_series[_iter] = _time_series[_iter - 1] + timedelta(seconds=_step_series[_iter -1])
    _t_series = pl.Series('ts-'+str(_span), _time_series)
    return (_step_series, _t_series, _td_series)


def get_time_roll_ts(_seed_time, _span, _df, _ds):
    _row_count = len(_ds)
    _start = datetime.strptime(_seed_time, '%y-%m-%d %H:%M:%S.%f')
    _delta = timedelta(seconds = (_ds[_span] - _start).total_seconds()/_span)
    _dfs = pl.datetime_range(_start, _start + timedelta(weeks=3), interval=_delta, eager=True)[:_row_count]
    # _step_array = [_first_step] * _span
    for _iter in range(1, _row_count):
        _dfs[_iter] = 0
    _dft = pl.DataFrame({"tbase": _dfs})
    print(_dft.head())
    # for _iter in range(_span, _row_count):
    #     _next_step = int((_df[_iter] - _df[_iter-_span])/(1.0*_span))
    #     _step_array.append(_next_step)
    #     _dt_array.append(_dt_array[_iter-1] + _step_array[_iter-1])
    # _new_count = _row_count - _span
    # _step_series = pl.Series('step' + str(_span), ((_df[_span:] - _df[:-_span])/float(_span)).cast(pl.Int64))
    # _init_series = pl.Series('init', [_start] * _new_count)
    # _time_series =  pl.Series('ts' + str(_span), _step_series.cumsum() + _init_series )
    # return (_step_series, _time_series)


def get_series(_seed_time, _seed_freq, _row_count):
    _freq_hz = _seed_freq   
    _period_us = int(1000000.0/_freq_hz)
    _freq_hz_act = _period_us/1000000.0
    _offset_days = 1 + _row_count // (_freq_hz_act * 60 * 60 * 24)

    _start = datetime.strptime(_seed_time, '%y-%m-%d %H:%M:%S.%f')
    _stop = _start + timedelta(days=_offset_days)
    _dft = pl.DataFrame({"timestamp"+str(_seed_freq): pl.datetime_range(_start, _stop, interval=timedelta(microseconds=_period_us), eager=True)[:_row_count]})
    return (_freq_hz_act, _period_us, _dft)


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

    #seed_time = datetime.strptime("23-09-21 09:47:15.860", "%y-%m-%d %H:%M:%S.%f")
    seed_time = "23-09-21 10:28:49.290"


    freq_init_ap = 6.829929
    freq_init_jg = 6.666

    (freq_hz_act_ap, period_us_ap, dft_ap) = get_series(seed_time, freq_init_ap, row_count)
    (freq_hz_act_jg, period_us_jg, dft_jg) = get_series(seed_time, freq_init_jg, row_count)

    st.write('Test init frequency in Hz: ', freq_init_ap)
    st.write('Test init period: ', period_us_ap)
    st.write('Test actual frequency in Hz: ', freq_hz_act_ap)

    st.write('Test init frequency in Hz: J', freq_init_jg)
    st.write('Test init period: J', period_us_jg)
    st.write('Test actual frequency in Hz: J', freq_hz_act_jg)

    span = 100
    # df_current = df.select(pl.col('pdate')).to_series()
    # (ts_1, dt_1) = get_time_roll(seed_time, span, row_count, df_current)

    df_current = df.select(pl.col('pdate')).to_series()
    df_current.describe()
    (ts_1, dt_1, s1) = get_time_roll_vec(seed_time, span, df_current)
    # df_current = df.select(pl.col('pdate'))
    # df_current_series = df_current.to_series()
    # (ts_1, dt_1) = get_time_roll_ts(seed_time, span, df_current, df_current_series)
    st.write("Statistics step span:", span)
    st.write("steps:", ts_1.describe())
    st.write("timeseries:", dt_1.describe())
    st.write("timeseries:", s1.describe())
    

    date_label_col = 'ts'+str(span)
    step_label_col = 'step'+str(span)

    sum_label_col = 'sum'+str(span)
    
    #dt_1_df = pl.DataFrame({date_label_col: dt_1 }).select(pl.col(date_label_col).cast(pl.Utf8).str.to_datetime(time_unit="ms"))
    # print(dt_1.head())

    # dt_1 = dt_1.apply(lambda ms: datetime.fromtimestamp(int(ms / 1000.0)))
    dt_1_df = pl.DataFrame({date_label_col: dt_1 })
    
    ts_1_df = pl.DataFrame({step_label_col: ts_1 })

    s1_df = pl.DataFrame({sum_label_col: s1})
    #ts_1_df = dt_1.apply(lambda ms: datetime.fromtimestamp(int(ms / 1000.0)))

    # df2 = pl.concat([dft_ap, dft_jg, dt_1_df, ts_1_df, df], how="horizontal")
    df2 = pl.concat([dft_ap[span:], dft_jg[span:], dt_1_df, ts_1_df, s1_df, df[span:][:]], how="horizontal")
    
    df2 = df2.select('timestamp'+str(freq_init_ap),
                     'timestamp'+str(freq_init_jg), 
                     'pdate', 
                     date_label_col,
                     #pl.col(date_label_col).cast(pl.Utf8).str.to_datetime("ms"), 
                     step_label_col, 
                     sum_label_col,

                     'Vx', 'Vy', 'Vz', 'P_mBar', 'heading')





    st.write('Row count: ', row_count)


    st.write("First 10 rows", df2.head(10))

    st.write("Last 10 rows", df2.tail(10))

    #time.sleep(1999999)

    # dfs = df.sample(fraction=0.01, seed=0)
    
    output_dir = os.path.dirname(out_filename)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)  # Recreate parent directories if they don't exist

    if os.path.exists(out_filename):
        os.remove(out_filename) 
    df2.write_csv(out_filename, separator=",", quote_style="non_numeric")

    # # Extract the date component and create a new column
    # df2 = df2.with_columns(pl.col("pdate").dt.strftime("%Y-%m-%d").alias("date"))

    # # # Group the DataFrame by the date column
    # grouped = df2.group_by("date")

    # # # Now you have a group of DataFrames, each representing a partition
    # for date, partition_df in grouped:
    #     partition_df = partition_df.select(pl.col("pdate").dt.strftime("%Y-%m-%d %H:%M:%S.%3f"),
    #                                        pl.col('Vx').round(1), 
    #                                        pl.col('Vy').round(1), 
    #                                        pl.col('Vz').round(1),
    #                                        pl.col('P_mBar').round(1),  
    #                                        pl.col('heading').cast(pl.Int32))
    #     # partition_df = partition_df.select('timestamp', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading')
    #     out_fn = prepend_prefix_to_path(out_filename, str(date))
    #     print(out_fn)
    #     partition_df.write_csv(out_fn, separator=",", quote_style="non_numeric")
    #     # print(f"Partition for {date}:")
    #     # print(partition_df)


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


