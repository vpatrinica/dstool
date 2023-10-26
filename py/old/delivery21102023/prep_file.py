import duckdb
import polars as pl
from datetime import datetime, timedelta

import inspect
# default nr of lines for batch I/O
C_I_DEFAULT_CHUNK_SIZE = 1000000

def timer_on(f_name):
    print('Starting Timer... ' + f_name)
    _start = datetime.now() 
    print(_start)
    return _start
    

def timer_off(f_name):
    print('Stopping Timer... ' + f_name)
    _stop = datetime.now() 
    print(_stop)
    return _stop
 
 
def prep_file(input_file_path, output_file_path, chunk_size=C_I_DEFAULT_CHUNK_SIZE):
    # profiling
    _current_function_name = inspect.currentframe().f_code.co_name
    _start_time = timer_on(_current_function_name)   
    # read/write in equal chunks
    with open(input_file_path, 'r') as input_file, open(output_file_path, 'w') as output_file:
        while 1:
            _chunk_lines = input_file.readlines(chunk_size)
            if len(_chunk_lines) > 0:
                output_file.writelines([' '.join(_line.split()) + '\n' for _line in _chunk_lines])
            else:
                break
    # profiling
    _end_time = timer_off(_current_function_name) 
    print(_end_time - _start_time)


# if1 = 'mavs1g.csv.bak'
# of1 = 'mavs1g_prep.csv'
# of2 = 'mavs1g_prep_alt.csv'
# if1 = 'vector.csv'

# input_file_path = "NK2-402.dat"
# chunk_size = 1000  # Number of lines to process at a time

# [
#  prep_file(if1, of1)
# ]
