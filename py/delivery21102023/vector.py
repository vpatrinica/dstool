import duckdb
import polars as pl
from datetime import datetime, timedelta

import re

import streamlit as st

def timer_on():
    print('Starting Timer...')
    _start = datetime.now() 
    print(_start)
    return _start
    
def timer_off():
    print('Stopping Timer...')
    _stop = datetime.now() 
    print(_stop)
    return _stop
 

def prep_file2(input_file_path, chunk_size):
    start_time = timer_on()    
    output_file_path = input_file_path + str(chunk_size) + 'p2'
    with open(input_file_path, 'r') as input_file, open(output_file_path, 'w') as output_file:
        while 1:
            str_conv = input_file.readlines(chunk_size)
            if len(str_conv)>0:
                cleaned_lines = [' '.join(l.split()) + '\n' for l in str_conv]
                output_file.writelines(cleaned_lines)
            else:
                break
        
    end_time = timer_off() 
    print(end_time-start_time)

def prep_file4(input_file_path, chunk_size):
    start_time = timer_on()    
    output_file_path = input_file_path + 'p4'
    with open(input_file_path, "r") as input_file, open(output_file_path, "w") as output_file:
        _line_no = 0
        payload = []
        for line in input_file.readlines():
            _line_no =+ 1
            payload.append(' '.join(line.split()) + '\n')
            if _line_no > chunk_size:
                output_file.writelines(payload)
                payload = []
                _line_no = 0
        if len(payload) > 0:
            output_file.writelines(payload)
                            
    end_time = timer_off() 
    print(end_time-start_time)


if1 = 'mavs1g.csv.bak'
# if1 = 'vector.csv'

# input_file_path = "NK2-402.dat"
# chunk_size = 1000  # Number of lines to process at a time

[
    #prep_file(if1), 
 #prep_file2(if1, 10000), 
 prep_file2(if1, 1000000),#<--winner
 #prep_file2(if1, 10000),
 # prep_file3(if1), 
 prep_file4(if1, 1000000)]#<--alternate


# def process_line(line):
#     # Replace multiple whitespaces with a single space
#     cleaned_line = ' '.join(line.split())
#     return cleaned_line

# C:\prj\testrs\msplit\py>py vector.py
# Starting Timer...
# 2023-10-06 21:19:29.050265
# Stopping Timer...
# 2023-10-06 21:20:03.924732
# 0:00:34.874467
# Starting Timer...
# 2023-10-06 21:20:03.924732
# Stopping Timer...
# 2023-10-06 21:20:40.824076
# 0:00:36.899344
# Starting Timer...
# 2023-10-06 21:20:40.832078
# Stopping Timer...
# 2023-10-06 21:21:16.970298
# 0:00:36.138220


# def prep_file(input_file_path):
#     start_time = timer_on()
#     output_file_path = input_file_path + 'p1'
    
#     with open(input_file_path, "r") as input_file, open(output_file_path, "w") as output_file:
#         output_file.write(re.sub(r' +', ' ', input_file.read()))
    
#     end_time = timer_off() 
#     print(end_time-start_time)


# def prep_file3(input_file_path):
#     start_time = timer_on()    
#     output_file_path = input_file_path + 'p3'
#     with open(input_file_path, "r") as input_file, open(output_file_path, "w") as output_file:
#         in_str = input_file.read()
#         len_in_str = len(in_str)
#         out_str = ''
#         # out_str = 'x'*len_in_str
#         i = 0
#         j = 0
#         for i in range(0, len_in_str):
#             c = in_str[i]
#             cn = in_str[i+1]
#             if not ((c == ' ') and (cn == ' ')):
#                 j += 1
#                 out_str = out_str + c
#         out_str = out_str[:j]

#         output_file.write(out_str)

#     end_time = timer_off() 
#     print(end_time-start_time)

# df = duckdb.sql("""
                
# SELECT  strptime('23-09-20 11:39:37', '%y-%m-%d %H:%M:%S') AS seed_time, 
#                 strptime(rpad(concat(cast((date_yy*100 + date_mm)*100 + date_dd as varchar), '6',
#                        cast(((900 + time_hh)*100 + time_mm)*100 + time_ss::double as varchar)), 18, '0'),
#                 '%y%m%d69%H%M%S.%g'
#                 ) as pdate, 
#                 u as Vx, v as Vy, w as Vz, p*100 as P_mBar, degrees(atan(-1.0*mx/my)) as heading 
# FROM read_csv('mavs2.csv.gz', delim=' ', header=false, 
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
                
# """).pl()


# row_count = df.select(pl.count())[0,0]
# offset_days = 1 + row_count // (8 * 60 * 60 * 24)

# start = datetime(2023, 9, 20, 11, 39, 47)
# stop = start + timedelta(days=offset_days)

# df11 = pl.DataFrame({"timestamp": pl.datetime_range(start, stop, interval=timedelta(milliseconds=125), eager=True)[:row_count]})
# print(row_count)
# print(df11.head())
# df2 = pl.concat([df11, df], how="horizontal")
# df2 = df2.select('timestamp', 'seed_time', 'pdate', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading')

# # Print the first few rows of the resulting DataFrame
# print(df2.head(100))
# print(df2.tail(100))

# df2 = df2.select('timestamp', 'Vx', 'Vy', 'Vz', 'P_mBar', 'heading')

# # Print the first few rows of the resulting DataFrame
# print(df2.head(100))
# print(df2.tail(100))

# df2.write_csv('mavs2_out.csv')
