# file_utils.py

from datetime import datetime
import inspect
import json
import csv

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

def prep_file(input_file_path, output_file_path, chunk_size=C_I_DEFAULT_CHUNK_SIZE, profile=True, skip_lines=0, datetime_format='', input_delimiter=',', output_delimiter='  '):
    _current_function_name = inspect.currentframe().f_code.co_name
    if profile:
        _start_time = timer_on(_current_function_name)
    
    with open(input_file_path, 'r') as input_file, open(output_file_path, 'w') as output_file:
        for _ in range(skip_lines):
            next(input_file)
        
        while True:
            _chunk_lines = input_file.readlines(chunk_size)
            if len(_chunk_lines) > 0:
                parsed_lines = []
                #print('+' + input_delimiter + '#')
                for _line in _chunk_lines:
                    #print(_line)
                    row = _line.strip().split(input_delimiter)
                    #print(row)
                    if datetime_format:
                        try:
                            # Parse the datetime components directly from row[0] through row[5]
                            # print(row[0] + '-' + row[1] + '-' + row[2] + ' ' + row[3] + ':' + row[4] + ':' + row[5], datetime_format)
                            parsed_datetime = datetime.strptime(
                                row[0] + '-' + row[1] + '-' + row[2] + ' ' + row[3] + ':' + row[4] + ':' + row[5], datetime_format)
                            row[:6] = [parsed_datetime.strftime('%Y-%m-%d %H:%M:%S.%g')]
                            # print(row)
                        except ValueError:
                            pass  # Handle invalid datetime format here if needed
                            print('fail')
                            print(row[0] + '-' + row[1] + '-' + row[2] + ' ' + row[3] + ':' + row[4] + ':' + row[5], datetime_format)
                            print(row)
                    parsed_lines.append(output_delimiter.join(row) + '\n')
                output_file.writelines(parsed_lines)
            else:
                break
    
    if profile:
        _end_time = timer_off(_current_function_name)
        print(_end_time - _start_time)
